/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/concurrency"
	"github.com/dapr/kit/events/batcher"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
)

// Options are the options for the Leadership.
type Options struct {
	// Log is the logger for the leadership.
	Log logr.Logger

	// Client is the etcd client.
	Client client.Interface

	// PartitionTotal is the total number of partitions.
	PartitionTotal uint32

	// Key is the ETCD key generator.
	Key *key.Key

	// ReplicaData is the replicaData for the instance using the cron library.
	// This will contain data like host + port for keeping track of active replicas.
	ReplicaData *anypb.Any
}

// Leadership gates until this partition has become the leader of the
// partition, as well as ensuring that there are no other active partitions
// which are acting on a different partition total.
type Leadership struct {
	log     logr.Logger
	client  client.Interface
	batcher *batcher.Batcher[int, []*anypb.Any]
	lock    sync.RWMutex

	partitionTotal atomic.Uint32
	key            *key.Key

	activeReplicas atomic.Pointer[[]*anypb.Any]
	replicaData    *anypb.Any

	readyCh chan struct{}
	running atomic.Bool
}

func New(opts Options) *Leadership {
	l := &Leadership{
		batcher:     batcher.New[int, []*anypb.Any](0),
		lock:        sync.RWMutex{},
		log:         opts.Log.WithName("leadership"),
		client:      opts.Client,
		key:         opts.Key,
		readyCh:     make(chan struct{}),
		replicaData: opts.ReplicaData,
	}
	l.partitionTotal.Store(opts.PartitionTotal)
	return l
}

// Run runs the Leadership. Attempts to acquire the partition lease key and
// holds that leadership until the given context is cancelled.
func (l *Leadership) Run(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("leadership already running")
	}
	//defer func() {
	//	if l.batcher != nil {
	//		l.batcher.Close()
	//	}
	//}()

	l.log.Info("Attempting to acquire partition leadership")

	lease, err := l.client.Grant(ctx, 20)
	if err != nil {
		return err
	}

	return concurrency.NewRunnerManager(
		func(ctx context.Context) error {
			ch, err := l.client.KeepAlive(ctx, lease.ID)
			if err != nil {
				return err
			}
			for {
				_, ok := <-ch
				if !ok {
					break
				}
			}
			rctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			//nolint:contextcheck
			_, err = l.client.Revoke(rctx, lease.ID)
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		},
		func(ctx context.Context) error {
			// Check if leadership key exists, if so wait.
			// If not, create leadership key.
			// List leadership namespace, ensure all values are the same as partitionTotal.

			resp, err := l.client.Get(ctx, l.key.LeadershipKey())
			if err != nil {
				return err
			}

			watcherCtx, watcherCancel := context.WithCancel(ctx)
			defer watcherCancel()

			ch := l.client.Watch(watcherCtx, l.key.LeadershipKey(), clientv3.WithRev(resp.Header.Revision))
			for {
				// write leadership key
				ok, err := l.attemptPartitionLeadership(ctx, lease.ID)
				if err != nil {
					return err
				}

				if ok {
					l.log.Info("Partition leadership acquired")
					//watcherCancel()
					break
				}

				l.log.Info("Partition leadership acquired by another replica, waiting for leadership to be dropped...")

				select {
				case <-ctx.Done():
					return ctx.Err()
				case w := <-ch:
					if err := l.handleLeadershipChangeEvents(ctx, w); err != nil {
						return nil
					}
				}
			}

			for {
				ok, err := l.checkLeadershipKeys(ctx)
				if err != nil {
					return err
				}

				if ok {
					l.log.Info("All partition leadership keys match partition total, ready processing")
					break
				}

				l.log.Info("Not all partition leadership keys match partition total, waiting for leadership to be dropped...")

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second / 2):
				}
			}

			close(l.readyCh)
			<-ctx.Done()

			//if l.batcher != nil {
			//	l.batcher.Close()
			//}
			//l.batcher.Close()
			return nil
		},
	).Run(ctx)
}

func (l *Leadership) handleLeadershipChangeEvents(ctx context.Context, w clientv3.WatchResponse) error {
	var leadershipChangedReplicaData []*anypb.Any

	// Dynamic update for partitionTotal based on the current count of leadership keys
	if err := l.updatePartitionTotal(ctx); err != nil {
		return err
	}

	for _, ev := range w.Events {
		var leadershipData stored.Leadership
		if err := proto.Unmarshal(ev.Kv.Value, &leadershipData); err != nil {
			return err
		}

		storedReplicaDataAny, err := anypb.New(leadershipData.GetReplicaData())
		if err != nil {
			l.log.Error(err, "Failed to wrap leadership message in anypb.Any")
			continue
		}
		// only send back the replicaData, not total count
		leadershipChangedReplicaData = append(leadershipChangedReplicaData, storedReplicaDataAny)
	}

	l.activeReplicas.Store(&leadershipChangedReplicaData)
	l.batcher.Batch(0, leadershipChangedReplicaData)
	if err := w.Err(); err != nil {
		return err
	}
	return nil
}

// updatePartitionTotal checks and updates the partition total dynamically
func (l *Leadership) updatePartitionTotal(ctx context.Context) error {
	resp, err := l.client.Get(ctx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		return errors.New("leadership namespace has no keys")
	}
	newTotal := strconv.Itoa(int(resp.Count))

	if strconv.Itoa(int(l.partitionTotal.Load())) != newTotal {
		total, err := strconv.Atoi(newTotal)
		if err != nil {
			return err
		}
		l.partitionTotal.Store(uint32(total))
		l.log.Info("Updated partition total", "total", total)
	}

	return nil
}

// checkLeadershipKeys keys will check if all leadership keys are the same as
// the dynamic partition total.
func (l *Leadership) checkLeadershipKeys(ctx context.Context) (bool, error) {
	resp, err := l.client.Get(ctx, l.key.LeadershipKey())
	if err != nil {
		return false, err
	}
	var leader stored.Leadership
	err = proto.Unmarshal(resp.Kvs[0].Value, &leader)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
	}
	if resp.Count == 0 || leader.Total != l.partitionTotal.Load() {
		return false, errors.New("lost partition leadership key")
	}

	resp, err = l.client.Get(ctx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
	if err != nil {
		return false, err
	}

	if resp.Count == 0 {
		return false, errors.New("leadership namespace has no keys")
	}

	// TODO: @joshvanl:
	// We can be more aggressive starting earlier here by only returning an error
	// if there is a different partition total which _also_ overlaps with this
	// partition.
	for _, kv := range resp.Kvs {
		var leader stored.Leadership
		err = proto.Unmarshal(kv.Value, &leader)
		if err != nil {
			return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
		}

		if leader.Total != l.partitionTotal.Load() {
			l.log.WithValues("key", string(kv.Key), "value", string(kv.Value)).Info("leadership key does not match partition total, waiting for leadership to be dropped")
			return false, nil
		}
	}

	return true, nil
}

// attemptPartitionLeadership attempts to write to the partition leadership key if
// it does not exist.
// If it does exist, and we successfully wrote the leadership key, it will return true.
func (l *Leadership) attemptPartitionLeadership(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	leader := &stored.Leadership{
		Total:       l.partitionTotal.Load(),
		ReplicaData: l.replicaData,
	}

	leaderBytes, err := proto.Marshal(leader)
	if err != nil {
		return false, fmt.Errorf("failed to marshal leadership data: %w", err)
	}

	tx := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.key.LeadershipKey()), "=", 0)).
		Then(clientv3.OpPut(l.key.LeadershipKey(), string(leaderBytes), clientv3.WithLease(leaseID)))
	resp, err := tx.Commit()
	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

// WaitForLeadership will block until the leadership is ready. If the context is
// cancelled, it will return the context error.
func (l *Leadership) WaitForLeadership(ctx context.Context) error {
	select {
	case <-l.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe returns a channel for leadership key space updates, as well as the
// initial set.
func (l *Leadership) Subscribe(ctx context.Context) (chan []*anypb.Any, []*anypb.Any) {
	ch := make(chan []*anypb.Any)
	l.batcher.Subscribe(ctx, ch)

	// returns active set of replicas
	if activeReplicas := l.activeReplicas.Load(); activeReplicas != nil {
		return ch, *activeReplicas
	}

	return ch, nil
}
