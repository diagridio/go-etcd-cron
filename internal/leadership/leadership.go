/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/concurrency"
	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/internal/key"
)

// Options are the options for the Leadership.
type Options struct {
	// Log is the logger for the leadership.
	Log logr.Logger

	// Client is the etcd client.
	Client *clientv3.Client

	// PartitionTotal is the total number of partitions.
	PartitionTotal uint32

	// Key is the ETCD key generator.
	Key *key.Key
}

// Leadership gates until this partition has become the leader of the
// partition, as well as ensuring that there are no other active partitions
// which are acting on a different partition total.
type Leadership struct {
	log     logr.Logger
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher

	partitionTotal string
	key            *key.Key

	readyCh chan struct{}
	running atomic.Bool
}

func New(opts Options) *Leadership {
	return &Leadership{
		log:            opts.Log.WithName("leadership"),
		kv:             opts.Client.KV,
		lease:          opts.Client.Lease,
		watcher:        opts.Client.Watcher,
		partitionTotal: strconv.Itoa(int(opts.PartitionTotal)),
		key:            opts.Key,
		readyCh:        make(chan struct{}),
	}
}

// Run runs the Leadership. Attempts to acquire the partition lease key and
// holds that leadership until the given context is cancelled.
func (l *Leadership) Run(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("leadership already running")
	}

	l.log.Info("Attempting to acquire partition leadership")

	lease, err := l.lease.Grant(ctx, 20)
	if err != nil {
		return err
	}

	return concurrency.NewRunnerManager(
		func(ctx context.Context) error {
			ch, err := l.lease.KeepAlive(ctx, lease.ID)
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
			_, err = l.lease.Revoke(rctx, lease.ID)
			return err
		},
		func(ctx context.Context) error {
			// Check if leadership key exists, if so wait.
			// If not, create leadership key.
			// List leadership namespace, ensure all values are the same as partitionTotal.

			resp, err := l.kv.Get(ctx, l.key.LeaseKey())
			if err != nil {
				return err
			}

			watcherCtx, watcherCancel := context.WithCancel(ctx)
			defer watcherCancel()
			ch := l.watcher.Watch(watcherCtx, l.key.LeaseKey(), clientv3.WithRev(resp.Header.Revision))

			for {
				ok, err := l.attemptPartitionLeadership(ctx, lease.ID)
				if err != nil {
					return err
				}

				if ok {
					l.log.Info("Partition leadership acquired")
					watcherCancel()
					break
				}

				l.log.Info("Partition leadership acquired by another replica, waiting for leadership to be dropped...")

				select {
				case <-ctx.Done():
					return ctx.Err()
				case w := <-ch:
					if err := w.Err(); err != nil {
						return err
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

			return nil
		},
	).Run(ctx)
}

// checkLeadershipKeys keys will check if all leadership keys are the same as
// the partition total.
func (l *Leadership) checkLeadershipKeys(ctx context.Context) (bool, error) {
	resp, err := l.kv.Get(ctx, l.key.LeadershipKey())
	if err != nil {
		return false, err
	}

	if resp.Count == 0 || string(resp.Kvs[0].Value) != l.partitionTotal {
		return false, errors.New("lost partition leadership key")
	}

	resp, err = l.kv.Get(ctx, l.key.LeadershipNamespace(), clientv3.WithPrefix())
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
		if string(kv.Value) != l.partitionTotal {
			l.log.WithValues("key", string(kv.Key), "value", string(kv.Value)).Info("leadership key does not match partition total, waiting for leadership to be dropped")
			return false, nil
		}
	}

	return true, nil
}

// attemptPartitionLeadership attempts to write to the partition leadership key if
// it does not exist.
// If it does exist and we successfully wrote, it will return true.
func (l *Leadership) attemptPartitionLeadership(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	tx := l.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.key.LeadershipKey()), "=", 0)).
		Then(clientv3.OpPut(l.key.LeadershipKey(), l.partitionTotal, clientv3.WithLease(leaseID)))
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
