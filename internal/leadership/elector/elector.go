/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package elector

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/go-logr/logr"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/informer"
	"github.com/diagridio/go-etcd-cron/internal/leadership/partitioner"
)

type Options struct {
	Log logr.Logger

	// Client is the etcd client.
	Client client.Interface

	// Key is the ETCD key generator.
	Key *key.Key

	// ReplicaData is the replicaData for the instance using the cron library.
	// This could contain data for example `host:port` to keeping track of active
	// replicas.
	ReplicaData *anypb.Any

	// LeaseID is the lease ID to use for the leadership key.
	LeaseID clientv3.LeaseID

	// Informer is the informer to use for leadership election.
	Informer *informer.Informer
}

// Elector does leadership election for this replica. It measures what the
// total partition total currently is, based on the current written leadership
// key space, and attempts to add a new leadership key. Will not write over an
// existing replica for the same key. Elector cancels the returned context once
// the number of leaders has changed. The consumer should attempt to re-elect
// in this case to continue leadership with the new partition total.
type Elector struct {
	log      logr.Logger
	client   client.Interface
	leaseID  clientv3.LeaseID
	informer *informer.Informer

	key         *key.Key
	leaderKey   string
	leaderNS    string
	uid         uint64
	replicaData *anypb.Any
}

// Elected is returned when leadership election quorum has been achieved.
type Elected struct {
	// Partitioner is the partitioner for the current leadership key space.
	Partitioner partitioner.Interface

	// LeadershipData is the replica data associated with every leader.
	LeadershipData []*anypb.Any
}

func New(opts Options) *Elector {
	return &Elector{
		log:         opts.Log.WithName("elector").WithValues("key", opts.Key.LeadershipKey()),
		client:      opts.Client,
		leaseID:     opts.LeaseID,
		replicaData: opts.ReplicaData,
		informer:    opts.Informer,
		key:         opts.Key,
		leaderKey:   opts.Key.LeadershipKey(),
		leaderNS:    opts.Key.LeadershipNamespace(),
		uid:         rand.Uint64(), //nolint:gosec
	}
}

// Elect attempts to gain leadership of our key. Uses the informer to wait for
// events to either try to elect ourselves, or check to see if all leaders have
// quorum.
// The returned context will be cancelled once quorum has been lost.
// A returned error is fatal.
func (e *Elector) Elect(ctx context.Context) (context.Context, *Elected, error) {
	e.log.Info("attempting to get initial leadership")

	resp, err := e.client.Get(ctx, e.leaderNS, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	for {
		ok, err := e.attemptNewLeadership(ctx, resp)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get initial leadership: %w", err)
		}

		if ok {
			break
		}

		resp, err = e.informer.Next(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next informer: %w", err)
		}
	}

	e.log.Info("fetched initial leadership, waiting for quorum for partition total", "total", resp.Count)

	resp, err = e.client.Get(ctx, e.leaderNS, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	var ldata []*anypb.Any
	var ok bool
	for {
		ldata, ok, err = e.quorumReconcile(ctx, resp)
		if err != nil {
			e.log.Error(err, "failed to reconcile to leadership quorum")
		}

		if ctx.Err() != nil {
			return nil, nil, err
		}

		if ok {
			break
		}

		resp, err = e.informer.Next(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next informer: %w", err)
		}
	}

	e.log.Info("leadership quorum reached", "total", len(ldata))

	part, err := partitioner.New(partitioner.Options{
		Key:     e.key,
		Leaders: resp.Kvs,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create partitioner: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		for {
			r, err := e.informer.Next(ctx)
			if err != nil {
				e.log.Error(err, "failed to get next informer")
				return
			}

			if err := e.haveQuorum(r, uint64(len(ldata))); err != nil {
				e.log.Error(err, "leadership quorum lost")
				return
			}
		}
	}()

	return ctx, &Elected{
		Partitioner:    part,
		LeadershipData: ldata,
	}, nil
}

// Reelect attempts to regain quorum after it has been lost. Attempt to update
// our own leadership key with the new state of the world.
// Returned context is cancelled when re-election is required again.
// A retuned error is fatal.
func (e *Elector) Reelect(ctx context.Context) (context.Context, *Elected, error) {
	e.log.Info("attempting to reelect leadership")

	resp, err := e.client.Get(ctx, e.leaderNS, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	var ldata []*anypb.Any
	var ok bool
	for {
		ldata, ok, err = e.quorumReconcile(ctx, resp)
		if err != nil {
			e.log.Error(err, "failed to reconcile to leadership quorum")
		}

		if ctx.Err() != nil {
			return nil, nil, err
		}

		if ok {
			break
		}

		resp, err = e.informer.Next(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next informer: %w", err)
		}
	}

	e.log.Info("leadership quorum reached", "total", len(ldata))
	part, err := partitioner.New(partitioner.Options{
		Key:     e.key,
		Leaders: resp.Kvs,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create partitioner: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		for {
			r, err := e.informer.Next(ctx)
			if err != nil {
				e.log.Error(err, "failed to get next informer")
				return
			}

			if err := e.haveQuorum(r, uint64(len(ldata))); err != nil {
				e.log.Error(err, "leadership quorum lost")
				return
			}
		}
	}()

	return ctx, &Elected{
		Partitioner:    part,
		LeadershipData: ldata,
	}, nil
}

// haveQuorum checks that all leaders agree on the total partition size and
// that our key hasn't been tampered with. Returns an error if we no longer
// have quorum and should re-elect.
func (e *Elector) haveQuorum(resp *clientv3.GetResponse, expTotal uint64) error {
	var selfFound bool

	//nolint:gosec
	currentPartitionTotal := uint64(resp.Count)

	if currentPartitionTotal != expTotal {
		return fmt.Errorf("partition total changed: %d -> %d, fatal leadership error", expTotal, currentPartitionTotal)
	}

	for _, kv := range resp.Kvs {
		var lead stored.Leadership
		if err := proto.Unmarshal(kv.Value, &lead); err != nil {
			return err
		}

		if currentPartitionTotal != lead.GetTotal() {
			return fmt.Errorf("%s: partition total changed: %d -> %d, fatal leadership error", string(kv.Key), lead.GetTotal(), currentPartitionTotal)
		}

		// Ensure that our leadership has not been tampered with.
		if string(kv.Key) == e.leaderKey {
			selfFound = true

			if lead.GetUid() != e.uid {
				return fmt.Errorf("own uid changed: %d -> %d, fatal leadership error", e.uid, lead.GetUid())
			}
			if !proto.Equal(lead.GetReplicaData(), e.replicaData) {
				return fmt.Errorf("own replica data changed: %v -> %v, fatal leadership error", e.replicaData, lead.GetReplicaData())
			}
		}
	}

	if !selfFound {
		return fmt.Errorf("%s: own leadership key has been deleted, fatal leadership error", e.leaderKey)
	}

	return nil
}

// quorumReconcile ensures both that we have set our replica total to the
// correct number, and that all other leaders in the cluster agree on the total
// partition number. If all agree, returns leadership data for all leaders.
func (e *Elector) quorumReconcile(ctx context.Context, resp *clientv3.GetResponse) ([]*anypb.Any, bool, error) {
	selfFound := false
	allAgree := true

	//nolint:gosec
	currentPartitionTotal := uint64(resp.Count)

	ldata := make([]*anypb.Any, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		var existing stored.Leadership
		if err := proto.Unmarshal(kv.Value, &existing); err != nil {
			return nil, false, err
		}

		ldata[i] = existing.GetReplicaData()

		// Reconcile our own leadership key.
		if string(kv.Key) == e.leaderKey {
			if err := e.reconcileSelf(ctx, kv, &existing, currentPartitionTotal); err != nil {
				return nil, false, fmt.Errorf("failed to reconcile own leadership: %w", err)
			}

			selfFound = true
			continue
		}

		// If a leader does not agree to the leadership total, then mark as not all
		// agree however we do _not_ exit early as we _must_ update our own
		// understanding. If we didn't update our own understanding, no one would
		// make any progress!
		if existing.GetTotal() != currentPartitionTotal {
			allAgree = false
		}
	}

	// We didn't find ourselves!
	if !selfFound {
		return nil, false, errors.New("own leadership key has been deleted, fatal leadership error")
	}

	return ldata, allAgree, nil
}

// reconcileSelf ensures that our leadership value has not been changed by
// someone else, and updating our stored data if the partition total has
// changed.
func (e *Elector) reconcileSelf(ctx context.Context, self *mvccpb.KeyValue, selfLeader *stored.Leadership, partitionTotal uint64) error {
	if selfLeader.GetUid() != e.uid {
		return fmt.Errorf("own uid changed: %d -> %d, fatal leadership error", e.uid, selfLeader.GetUid())
	}

	if !proto.Equal(selfLeader.GetReplicaData(), e.replicaData) {
		return fmt.Errorf("own replica data changed: %v -> %v, fatal leadership error", e.replicaData, selfLeader.GetReplicaData())
	}

	// If the partition total has not changed, we don't need to re-elect.
	if partitionTotal == selfLeader.GetTotal() {
		return nil
	}

	// The partition total has changed from what we have previously observed, so
	// we need to update our leadership data.
	newLeadership := &stored.Leadership{
		Total:       partitionTotal,
		Uid:         e.uid,
		ReplicaData: e.replicaData,
	}

	leaderBytes, err := proto.Marshal(newLeadership)
	if err != nil {
		return fmt.Errorf("failed to marshal leadership data: %w", err)
	}

	// Sanity check to ensure out leadership key has not change.
	tif := clientv3.Compare(clientv3.Value(e.leaderKey), "=", string(self.Value))
	// Write the leadership key with the new total.
	tthen := clientv3.OpPut(e.leaderKey, string(leaderBytes), clientv3.WithLease(e.leaseID))
	gresp, err := e.client.Txn(ctx).If(tif).Then(tthen).Commit()
	if err != nil {
		return err
	}

	if !gresp.Succeeded {
		return fmt.Errorf("failed to update leadership key, fatal leadership error: %v", gresp.Responses)
	}

	return nil
}

// attemptNewLeadership attempts to achieve leadership for this replica
// leadership ID. This is done by first determining the number of existing
// leaders, then attempting to write to this replica's leadership key with the
// leader data.
// The uid of this leader is written to ensure that no other leader with the
// same ID assumes the key.
// Saves the replica data associated with this leader.
// Writing the leader key will always result in a leadership rebalance, however
// we write $existing+1 so don't need to reshuffle ourselves again as well.
func (e *Elector) attemptNewLeadership(ctx context.Context, resp *clientv3.GetResponse) (bool, error) {
	//nolint:gosec
	expectedTotal := uint64(resp.Count)

	sl := &stored.Leadership{
		Total:       expectedTotal + 1,
		Uid:         e.uid,
		ReplicaData: e.replicaData,
	}

	leaderBytes, err := proto.Marshal(sl)
	if err != nil {
		return false, fmt.Errorf("failed to marshal leadership data: %w", err)
	}

	// If Revision is 0, the key does not exist which means we can assume
	// leadership for this key.
	tif := clientv3.Compare(clientv3.CreateRevision(e.leaderKey), "=", 0)

	// Write the leadership key with the new total (expectedTotal + 1).
	tthen := clientv3.OpPut(e.leaderKey, string(leaderBytes), clientv3.WithLease(e.leaseID))

	gresp, err := e.client.Txn(ctx).If(tif).Then(tthen).Commit()
	if err != nil {
		return false, err
	}

	return gresp.Succeeded, nil
}
