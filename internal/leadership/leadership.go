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
	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
)

// Options are the options for the Leadership.
type Options struct {
	// Log is the logger for the leadership.
	Log logr.Logger

	// Client is the etcd client.
	Client client.Interface

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
	batcher *batcher.Batcher[int, struct{}]
	lock    sync.RWMutex
	wg      sync.WaitGroup

	partitionTotal  atomic.Uint32
	key             *key.Key
	allReplicaDatas []*anypb.Any
	replicaData     *anypb.Any

	changeCh chan struct{}
	readyCh  chan struct{}
	closeCh  chan struct{}
	running  atomic.Bool
}

func New(opts Options) *Leadership {
	l := &Leadership{
		log:             opts.Log.WithName("leadership"),
		batcher:         batcher.New[int, struct{}](0),
		client:          opts.Client,
		key:             opts.Key,
		allReplicaDatas: []*anypb.Any{},
		replicaData:     opts.ReplicaData,
		readyCh:         make(chan struct{}),
		changeCh:        make(chan struct{}),
		closeCh:         make(chan struct{}),
	}
	l.partitionTotal.Store(1)
	return l
}

// Run runs the Leadership. Attempts to acquire the partition lease key and
// holds that leadership until the given context is cancelled.
func (l *Leadership) Run(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("leadership already running")
	}

	defer close(l.closeCh)
	defer l.batcher.Close()
	defer l.wg.Wait()

	// reset closeCh between restarts
	l.lock.Lock()
	l.closeCh = make(chan struct{})
	l.lock.Unlock()

	for {
		if err := l.loop(ctx); err != nil {
			return err
		}
	}
}

// loop is the main leadership loop. It will attempt to acquire the partition
// leadership key for itself, then wait until all other partitions have gained
// their leadership. After election, if any changes are observed, this loop
// will exit, to be restarted.
func (l *Leadership) loop(ctx context.Context) error {
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

			ch := l.client.Watch(watcherCtx, l.key.LeadershipNamespace(), clientv3.WithRev(resp.Header.Revision), clientv3.WithPrefix())

			// Leadership acquisition loop
			for {
				ok, err := l.attemptPartitionLeadership(ctx, lease.ID)
				if err != nil {
					return err
				}

				if ok {
					l.log.Info("Partition leadership acquired")
					break
				}

				l.log.Info("Partition leadership acquired by another replica, waiting for leadership to be dropped...")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ch:
					l.log.Info("Partition leadership changed, attempting to acquire partition leadership")
				}
			}

			// Check leadership keys for consistency
			for {
				ok, err := l.checkLeadershipKeys(ctx)
				if err != nil {
					return err
				}

				if ok {
					break
				}

				l.log.Info("Not all partition leadership keys match partition total, waiting for leadership to be dropped...")

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ch:
				}
			}

			l.log.Info("All partition leadership keys match partition total and are in quorum, leadership is ready")
			l.lock.Lock()
			close(l.readyCh)
			l.lock.Unlock()

			// Continually monitor leadership key consistency
			for {
				select {
				case <-ctx.Done():
					break
				case <-ch:
				}

				ok, err := l.checkLeadershipKeys(ctx)
				if err != nil {
					l.log.Error(err, "Dropping leadership due to error")
					break
				}

				if !ok {
					break
				}
			}

			l.log.Info("Leadership key inconsistency detected, dropping leadership...")

			l.lock.Lock()
			defer l.lock.Unlock()

			l.readyCh = make(chan struct{})
			close(l.changeCh)

			// TODO: cassie write tests for this
			var leader stored.Leadership
			for _, v := range l.allReplicaDatas {
				if err = proto.Unmarshal(v.GetValue(), &leader); err != nil {
					l.log.Error(err, "failed to unmarshall leadership all replica datas. Incorrect format. Skipping...")
				}
				if leader.ReplicaData == l.replicaData {
					// remove this partition since re-looping
					l.allReplicaDatas = l.allReplicaDatas[:len(l.allReplicaDatas)-1]
				}
			}

			l.batcher.Batch(0, struct{}{})
			l.changeCh = make(chan struct{})

			return ctx.Err()
		},
	).Run(ctx)
}

// checkLeadershipKeys verifies that all leadership keys are consistent with the partition total. Once consistent,
// it updates the allReplicaDatas value to contain all active instances.
func (l *Leadership) checkLeadershipKeys(ctx context.Context) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	getKeyCtx, getKeyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer getKeyCancel()

	// check individual partition
	resp, err := l.client.Get(getKeyCtx, l.key.LeadershipKey())
	if err != nil {
		return false, err
	}

	if resp.Kvs == nil {
		return false, fmt.Errorf("failed to check leaderhship keys. keys are nil")
	}

	var storedLeaderTotal stored.Leadership
	if err = proto.Unmarshal(resp.Kvs[0].Value, &storedLeaderTotal); err != nil {
		return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
	}

	if resp.Count == 0 {
		return false, errors.New("lost partition leadership key")
	}

	if storedLeaderTotal.Total != l.partitionTotal.Load() {
		return false, errors.New("stored leader total is out of sync")
	}

	if !proto.Equal(storedLeaderTotal.ReplicaData, l.replicaData) {
		return false, errors.New("leadership data isn't consistent")
	}

	getNSCtx, getNSCancel := context.WithTimeout(ctx, 5*time.Second)
	defer getNSCancel()
	resp, err = l.client.Get(getNSCtx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
	if err != nil {
		return false, err
	}

	if resp.Count == 0 {
		return false, errors.New("leadership namespace has no keys")
	}

	if int(storedLeaderTotal.Total) != len(resp.Kvs) {
		return false, errors.New("individual leadership partition total doesn't match total stored keys count")
	}

	if uint32(len(resp.Kvs)) != l.partitionTotal.Load() {
		return false, errors.New("leadership has incorrect partition total and is not in quorum")
	}

	// TODO: @joshvanl:
	// We can be more aggressive starting earlier here by only returning an error
	// if there is a different partition total which _also_ overlaps with this
	// partition.

	for _, kv := range resp.Kvs {
		var otherLeader stored.Leadership
		if err = proto.Unmarshal(kv.Value, &otherLeader); err != nil {
			return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
		}

		if otherLeader.Total != l.partitionTotal.Load() {
			ptotal := strconv.FormatUint(uint64(otherLeader.Total), 10)
			l.log.WithValues("key", string(kv.Key), "value", ptotal).Info(
				"leadership key does not match partition total, waiting for leadership to be dropped",
			)
			return false, nil
		}
		if storedLeaderTotal.Total != otherLeader.Total {
			return false, fmt.Errorf("leadership total does not match other partitions") // nil or err????
		}

		l.allReplicaDatas = append(l.allReplicaDatas, otherLeader.ReplicaData)
	}

	return true, nil
}

// watchForLeadershipConsistency monitors the leadership keys for consistency and triggers a leadership refresh if
// discrepancies are detected. Note: this could be a leadership data (& total) change, count of keys change, or if
// the partition leadership process goes down and is not found
func (l *Leadership) watchForLeadershipConsistency(ctx context.Context) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	getKeyCtx, getKeyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer getKeyCancel()

	// ensure the individual partition
	resp, err := l.client.Get(getKeyCtx, l.key.LeadershipKey())
	if err != nil {
		return false, err
	}
	if resp.Kvs == nil {
		return false, fmt.Errorf("failed to watch leaderhship keys for consistency. keys are nil")
	}

	var leader stored.Leadership
	if err = proto.Unmarshal(resp.Kvs[0].Value, &leader); err != nil {
		return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
	}

	if resp.Count == 0 {
		return false, errors.New("lost partition leadership key while watching for consistency")
	}

	if leader.Total != l.partitionTotal.Load() {
		return false, errors.New("leadership partition totals aren't consistent while watching for consistency")
	}

	if !proto.Equal(leader.ReplicaData, l.replicaData) {
		return false, errors.New("leadership data isn't consistent while watching for consistency")
	}

	getNSCtx, getNSCancel := context.WithTimeout(ctx, 5*time.Second)
	defer getNSCancel()
	resp, err = l.client.Get(getNSCtx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
	if resp == nil || len(resp.Kvs) == 0 {
		return false, errors.New("no leadership keys in namespace")
	}

	if resp.Count == 0 {
		return false, errors.New("leadership was lost")
	}

	if uint32(len(resp.Kvs)) != l.partitionTotal.Load() {
		return false, errors.New("leadership has incorrect partition total and is not in quorum")
	}

	if leader.Total != uint32(len(resp.Kvs)) {
		return false, errors.New("leadership has incorrect stored partition total and is not in quorum")

	}

	for _, kv := range resp.Kvs {
		var otherLeader stored.Leadership

		if err = proto.Unmarshal(kv.Value, &otherLeader); err != nil {
			return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
		}
		if leader.Total != otherLeader.Total {
			return false, errors.New("leadership doesn't match quorum partition total")
		}
	}
	return true, nil
}

// attemptPartitionLeadership attempts to write to the partition leadership key if
// it does not exist.
// If it does exist, and we successfully wrote the leadership key, it will return true.
func (l *Leadership) attemptPartitionLeadership(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	// check if a leader is already running, if so, up the partition total to a new total
	getNSCtx, getNSCancel := context.WithTimeout(ctx, 5*time.Second)
	defer getNSCancel()
	getResp, err := l.client.Get(getNSCtx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
	if err != nil {
		return false, err
	}

	// this is not the first instance to spin up, so
	// put in the total count of records +1 for the new partition total count
	if uint32(len(getResp.Kvs)) != l.partitionTotal.Load() {
		l.partitionTotal.Store(uint32(len(getResp.Kvs) + 1))
	}

	leaderCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	leaderBytes, err := proto.Marshal(&stored.Leadership{
		Total:       l.partitionTotal.Load(),
		ReplicaData: l.replicaData,
	})
	if err != nil {
		return false, fmt.Errorf("failed to marshal leadership data: %w", err)
	}

	tx := l.client.Txn(leaderCtx).
		If(clientv3.Compare(clientv3.CreateRevision(l.key.LeadershipKey()), "=", 0)).
		Then(clientv3.OpPut(l.key.LeadershipKey(), string(leaderBytes), clientv3.WithLease(leaseID)))
	resp, err := tx.Commit()
	if err != nil {
		return false, err
	}

	// leadership acquired
	if resp.Succeeded {
		return true, nil
	}

	l.log.Info("Leadership already acquired by another replica, waiting for leadership to be dropped...")
	return false, nil
}

// WaitForLeadership will block until the leadership is ready. If the context is
// cancelled, it will return the context error.
func (l *Leadership) WaitForLeadership(ctx context.Context) (context.Context, error) {
	l.lock.RLock()
	readyCh := l.readyCh
	changeCh := l.changeCh
	closeCh := l.closeCh
	l.lock.RUnlock()

	select {
	case <-closeCh:
		return nil, errors.New("leadership closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-readyCh:
	}
	leaderCtx, cancel := context.WithCancel(ctx)

	l.wg.Add(1)
	go func(ctx context.Context) {
		defer l.wg.Done()
		defer cancel()

		select {
		case <-leaderCtx.Done():
		case <-closeCh:
		case <-changeCh:
			// Leadership change detected; cancel context to signal leadership shift
		}
	}(ctx)

	return leaderCtx, nil
}

// Subscribe returns a channel for leadership key space updates, as well as the
// initial set.
func (l *Leadership) Subscribe(ctx context.Context) ([]*anypb.Any, chan struct{}) {
	l.lock.RLock()
	readyCh := l.readyCh
	l.lock.RUnlock()

	select {
	case <-ctx.Done():
		return nil, make(chan struct{})
	case <-readyCh:
	}

	l.lock.RLock()
	defer l.lock.RUnlock()

	ch := make(chan struct{})
	l.batcher.Subscribe(ctx, ch)

	return l.allReplicaDatas, ch
}
