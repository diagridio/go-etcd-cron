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
	log    logr.Logger
	client client.Interface
	lock   sync.RWMutex
	wg     sync.WaitGroup

	partitionTotal  uint32
	key             *key.Key
	allReplicaDatas []*anypb.Any
	replicaData     *anypb.Any

	changeCh chan struct{}
	readyCh  chan struct{}
	closeCh  chan struct{}
	running  atomic.Bool
}

func New(opts Options) *Leadership {
	return &Leadership{
		log:            opts.Log.WithName("leadership"),
		client:         opts.Client,
		key:            opts.Key,
		partitionTotal: opts.PartitionTotal,
		replicaData:    opts.ReplicaData,
		readyCh:        make(chan struct{}),
		changeCh:       make(chan struct{}),
		closeCh:        make(chan struct{}),
	}
}

// Run runs the Leadership. Attempts to acquire the partition lease key and
// holds that leadership until the given context is cancelled.
func (l *Leadership) Run(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("leadership already running")
	}

	defer l.wg.Wait()
	defer close(l.closeCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := l.loop(ctx); err != nil {
				return err
			}
		}
	}
}

// loop is the main leadership loop. It will attempt to acquire the partition
// leadership key for itself, then wait until all other partitions have gained
// their leadership. After election, if any changes are observed, this loop
// will exit, to be restarted.
func (l *Leadership) loop(ctx context.Context) error {
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

			l.log.Info("All partition leadership keys match partition total, processing is ready")
			l.lock.Lock()
			close(l.readyCh)
			l.lock.Unlock()

			for {
				select {
				case <-ctx.Done():
				case <-ch:
				}

				if ctx.Err() != nil {
					break
				}

				ok, err := l.checkLeadershipKeys(ctx)
				if err != nil {
					return err
				}

				if !ok {
					break
				}
			}

			l.lock.Lock()
			defer l.lock.Unlock()

			l.readyCh = make(chan struct{})
			close(l.changeCh)
			l.changeCh = make(chan struct{})

			return ctx.Err()
		},
	).Run(ctx)
}

// checkLeadershipKeys keys will check if all leadership keys are the same as
// the partition total.
func (l *Leadership) checkLeadershipKeys(ctx context.Context) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	resp, err := l.client.Get(ctx, l.key.LeadershipKey())
	if err != nil {
		return false, err
	}

	if resp.Kvs == nil {
		return false, fmt.Errorf("failed to check leaderhship keys. keys are nil")
	}

	var leader stored.Leadership
	if err = proto.Unmarshal(resp.Kvs[0].Value, &leader); err != nil {
		return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
	}

	// TODO: Cassie check that the replica data matches here as well
	if resp.Count == 0 || leader.Total != l.partitionTotal {
		return false, errors.New("lost partition leadership key")
	}

	resp, err = l.client.Get(ctx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
	if err != nil {
		return false, err
	}

	if resp.Count == 0 {
		return false, errors.New("leadership namespace has no keys")
	}

	l.allReplicaDatas = make([]*anypb.Any, 0, resp.Count)
	// TODO: @joshvanl:
	// We can be more aggressive starting earlier here by only returning an error
	// if there is a different partition total which _also_ overlaps with this
	// partition.
	for _, kv := range resp.Kvs {
		var leader stored.Leadership
		if err = proto.Unmarshal(kv.Value, &leader); err != nil {
			return false, fmt.Errorf("failed to unmarshal leadership data: %w", err)
		}

		if leader.Total != l.partitionTotal {
			ptotal := strconv.FormatUint(uint64(leader.Total), 10)
			l.log.WithValues("key", string(kv.Key), "value", ptotal).Info(
				"leadership key does not match partition total, waiting for leadership to be dropped",
			)
			return false, nil
		}

		l.allReplicaDatas = append(l.allReplicaDatas, leader.ReplicaData)
	}

	return true, nil
}

// attemptPartitionLeadership attempts to write to the partition leadership key if
// it does not exist.
// If it does exist, and we successfully wrote the leadership key, it will return true.
func (l *Leadership) attemptPartitionLeadership(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	leaderBytes, err := proto.Marshal(&stored.Leadership{
		Total:       l.partitionTotal,
		ReplicaData: l.replicaData,
	})
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
	l.lock.RUnlock()

	select {
	case <-readyCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	leaderCtx, cancel := context.WithCancel(ctx)

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer cancel()

		select {
		case <-ctx.Done():
		case <-changeCh:
			// Leadership change detected; cancel context to signal leadership shift
		}
	}()

	return leaderCtx, nil
}
