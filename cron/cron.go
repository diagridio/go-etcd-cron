/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/concurrency"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	internalapi "github.com/diagridio/go-etcd-cron/internal/api"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

// Options are the options for creating a new cron instance.
type Options struct {
	// Log is the logger to use for logging.
	Log logr.Logger

	// Client is the etcd client to use for storing cron entries.
	Client *clientv3.Client

	// Namespace is the etcd namespace to use for storing cron entries.
	Namespace string

	// PartitionID is the partition ID to use for storing cron entries.
	PartitionID uint32

	// PartitionTotal is the total number of partitions to use for storing cron
	// entries.
	PartitionTotal uint32

	// TriggerFn is the function to call when a cron job is triggered.
	TriggerFn api.TriggerFunction

	// CounterGarbageCollectionInterval is the interval at which to run the
	// garbage collection for counters is run. Counters are also garbage
	// collected on shutdown. Counters are batch deleted, so a larger value
	// increases the counter bucket and reduces the number of database
	// operations.
	// This value rarely needs to be set and is mostly used for testing. A small
	// interval value will increase database operations and thus degrade cron
	// performance.
	// Defaults to 180 seconds.
	CounterGarbageCollectionInterval *time.Duration

	// ReplicaData is custom data associated with the replica, for example,
	// host + port for the active replica
	ReplicaData *anypb.Any
}

// cron is the implementation of the cron interface.
type cron struct {
	api  api.API
	opts Options
	wg   sync.WaitGroup

	//restartLock  sync.Mutex
	lock         sync.RWMutex
	log          logr.Logger
	informer     *informer.Informer
	queue        *queue.Queue
	leadership   *leadership.Leadership
	collector    garbage.Interface
	schedBuilder *scheduler.Builder
	client       client.Interface
	yard         *grave.Yard
	key          *key.Key
	part         partitioner.Interface

	apiLock    sync.Mutex
	apiReadyCh chan struct{}

	//restarting    atomic.Bool
	running       atomic.Bool
	restartDoneCh chan struct{}
	closeCh       chan struct{}
	readyCh       chan struct{}
}

func (c *cron) restart() error {
	//c.restartLock.Lock()
	//defer c.restartLock.Unlock()

	//if !c.restarting.CompareAndSwap(false, true) {
	//	return nil // Another restart is already in progress
	//}
	//defer c.restarting.Store(false)

	c.lock.Lock()
	defer c.lock.Unlock()

	c.closeCh = make(chan struct{})
	c.readyCh = make(chan struct{})
	c.apiLock.Lock()
	//defer c.apiLock.Unlock()
	c.apiReadyCh = make(chan struct{})
	c.apiLock.Unlock()

	c.restartDoneCh = make(chan struct{})

	var err error
	c.part, err = partitioner.New(partitioner.Options{
		ID:    c.opts.PartitionID,
		Total: c.opts.PartitionTotal,
	})
	if err != nil {
		return fmt.Errorf("failed to create partitioner: %w", err)
	}

	c.client = client.New(client.Options{
		Log:    c.log,
		Client: c.opts.Client,
	})

	c.collector, err = garbage.New(garbage.Options{
		Log:                c.log,
		Client:             c.client,
		CollectionInterval: c.opts.CounterGarbageCollectionInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create garbage collector: %w", err)
	}

	c.key = key.New(key.Options{
		Namespace:   c.opts.Namespace,
		PartitionID: c.opts.PartitionID,
	})

	c.yard = grave.New()
	c.informer = informer.New(informer.Options{
		Key:         c.key,
		Client:      c.client,
		Collector:   c.collector,
		Partitioner: c.part,
		Yard:        c.yard,
	})

	c.leadership = leadership.New(leadership.Options{
		Log:            c.log,
		Client:         c.client,
		PartitionTotal: c.opts.PartitionTotal,
		Key:            c.key,
		ReplicaData:    c.opts.ReplicaData,
	})

	c.schedBuilder = scheduler.NewBuilder()

	c.queue = queue.New(queue.Options{
		Log:              c.log,
		Client:           c.client,
		Clock:            clock.RealClock{},
		Key:              c.key,
		SchedulerBuilder: c.schedBuilder,
		TriggerFn:        c.opts.TriggerFn,
		Collector:        c.collector,
		Yard:             c.yard,
	})

	c.api = internalapi.New(internalapi.Options{
		Client:           c.client,
		Key:              c.key,
		SchedulerBuilder: c.schedBuilder,
		Queue:            c.queue,
		ReadyCh:          c.readyCh,
		CloseCh:          c.closeCh,
	})

	close(c.apiReadyCh)
	close(c.restartDoneCh)
	return nil
}

// New creates a new cron instance.
func New(opts Options) (api.Interface, error) {
	if opts.TriggerFn == nil {
		return nil, errors.New("trigger function is required")
	}

	if opts.Client == nil {
		return nil, errors.New("client is required")
	}

	log := opts.Log
	if log.GetSink() == nil {
		sink, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
		if err != nil {
			return nil, err
		}
		log = zapr.NewLogger(sink)
		log = log.WithName("diagrid-cron")
	}

	c := &cron{
		opts:          opts,
		log:           log,
		restartDoneCh: make(chan struct{}),
		readyCh:       make(chan struct{}),
		closeCh:       make(chan struct{}),
	}

	c.lock.Lock()
	if err := c.restart(); err != nil {
		return nil, err
	}
	c.lock.Unlock()

	select {
	case <-c.restartDoneCh:
		log.Info("Cron instance is initialized")
	}
	return c, nil
}

// Run is a blocking function that runs the cron instance.
func (c *cron) Run(ctx context.Context) error {
	//c.lock.RLock() // Cassie this caused it to hang for the go test cmd
	//defer c.lock.RUnlock()

	if !c.running.CompareAndSwap(false, true) {
		return errors.New("cron already running")
	}

	defer c.running.Store(false)

	runners := []concurrency.Runner{
		c.collector.Run,
		c.queue.Run,
		func(ctx context.Context) error {
			//c.lock.RLock()
			//defer c.lock.RUnlock()
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}

			err := c.informer.Run(ctx)
			//c.lock.RUnlock()
			return err
			//return c.informer.Run(ctx)
		},
		func(ctx context.Context) error {
			//c.lock.RLock()
			//defer c.lock.RUnlock()
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}
			//c.lock.RUnlock()
			ev, err := c.informer.Events()
			if err != nil {
				return err
			}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case e := <-ev:
					if err := c.queue.HandleInformerEvent(ctx, e); err != nil {
						return err
					}
				}
			}
		},
		func(ctx context.Context) error {
			defer func() {
				if c.closeCh != nil {
					close(c.closeCh)
				}
			}()
			//c.lock.RLock()
			//defer c.lock.RUnlock()
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}
			if err := c.informer.Ready(ctx); err != nil {
				return err
			}
			//c.lock.RUnlock()

			//c.lock.Lock()
			close(c.readyCh)
			//c.lock.Unlock()
			c.log.Info("cron is ready")
			<-ctx.Done()

			return nil
		},
	}

	err := concurrency.NewRunnerManager(
		c.leadership.Run,
		func(ctx context.Context) error {
			//c.lock.RLock()
			_, replicaUpdateCh := c.leadership.Subscribe(ctx)
			//c.lock.RUnlock()

			for {
				err := concurrency.NewRunnerManager(
					append(
						runners,
						func(ctx context.Context) error {
							select {
							case <-replicaUpdateCh:
								//c.lock.RUnlock() //added
								c.log.Info("Leadership change detected, reinitializing cron")
								//c.lock.Lock()
								if err := c.restart(); err != nil {
									return fmt.Errorf("failed to re-initialize cron: %w", err)
								}
								//c.lock.Unlock()
								//c.lock.RLock()
								// Restart all runners bc there is a change in the total replicas (in leadership table)
								return nil
							case <-ctx.Done():
							}
							return nil
						},
					)...,
				).Run(ctx)

				if err != nil {
					return err // something went wrong with one runner
				}

				if ctx.Err() != nil {
					return ctx.Err() // top level ctx is errored
				}
				<-c.restartDoneCh // wait for restart to complete
				c.log.Info("Cron restarted")
			}

		},
	).Run(ctx)

	return err
}

// Add forwards the call to the embedded API.
func (c *cron) Add(ctx context.Context, name string, job *api.Job) error {
	if err := c.waitAPIReady(ctx); err != nil {
		return err
	}
	return c.api.Add(ctx, name, job)
}

// Get forwards the call to the embedded API.
func (c *cron) Get(ctx context.Context, name string) (*api.Job, error) {
	if err := c.waitAPIReady(ctx); err != nil {
		return nil, err
	}

	return c.api.Get(ctx, name)
}

// Delete forwards the call to the embedded API.
func (c *cron) Delete(ctx context.Context, name string) error {
	if err := c.waitAPIReady(ctx); err != nil {
		return err
	}

	return c.api.Delete(ctx, name)
}

// DeletePrefixes forwards the call to the embedded API.
func (c *cron) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	if err := c.waitAPIReady(ctx); err != nil {
		return err
	}

	return c.api.DeletePrefixes(ctx, prefixes...)
}

// List forwards the call to the embedded API.
func (c *cron) List(ctx context.Context, prefix string) (*api.ListResponse, error) {
	if err := c.waitAPIReady(ctx); err != nil {
		return nil, err
	}

	return c.api.List(ctx, prefix)
}

// DeliverablePrefixes forwards the call to the embedded API.
func (c *cron) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	if err := c.waitAPIReady(ctx); err != nil {
		return nil, err
	}

	return c.api.DeliverablePrefixes(ctx, prefixes...)
}

// WatchLeadership forwards the call to the embedded API.
func (c *cron) WatchLeadership(ctx context.Context) (chan []*anypb.Any, error) {
	if err := c.waitAPIReady(ctx); err != nil {
		return nil, err
	}

	return c.api.WatchLeadership(ctx)
}

func (c *cron) waitAPIReady(ctx context.Context) error {
	c.apiLock.Lock()
	ch := c.apiReadyCh
	c.apiLock.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
