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
	API  api.API
	opts Options

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

	running       atomic.Bool
	restartDoneCh chan struct{}
	closeCh       chan struct{}
	readyCh       chan struct{}
}

func (c *cron) restart() error {
	var err error
	c.lock.Lock()
	defer c.lock.Unlock()

	c.restartDoneCh = make(chan struct{})
	c.readyCh = make(chan struct{})

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

	c.API = internalapi.New(internalapi.Options{
		Client:           c.client,
		Key:              c.key,
		SchedulerBuilder: c.schedBuilder,
		Queue:            c.queue,
		ReadyCh:          c.readyCh,
		CloseCh:          c.closeCh,
	})

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
		lock:          sync.RWMutex{},
		opts:          opts,
		log:           log,
		restartDoneCh: make(chan struct{}),
		readyCh:       make(chan struct{}),
		closeCh:       make(chan struct{}),
	}

	if err := c.restart(); err != nil {
		return nil, err
	}

	select {
	case <-c.restartDoneCh:
		log.Info("Cron instance is initialized")
	}
	return c, nil
}

// Run is a blocking function that runs the cron instance.
func (c *cron) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("cron already running")
	}

	runners := []concurrency.Runner{
		c.collector.Run,
		c.queue.Run,
		func(ctx context.Context) error {
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}

			return c.informer.Run(ctx)
		},
		func(ctx context.Context) error {
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}

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
			defer close(c.closeCh)
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}
			if err := c.informer.Ready(ctx); err != nil {
				return err
			}

			close(c.readyCh)
			c.log.Info("cron is ready")
			<-ctx.Done()

			return nil
		},
	}

	return concurrency.NewRunnerManager(
		c.leadership.Run,
		func(ctx context.Context) error {
			for {
				replicaUpdateCh, _ := c.leadership.Subscribe(ctx)

				err := concurrency.NewRunnerManager(
					append(
						runners,
						func(ctx context.Context) error {
							select {
							case <-replicaUpdateCh:
								c.log.Info("Leadership change detected, reinitializing cron")
								if err := c.restart(); err != nil {
									return fmt.Errorf("failed to re-initialize cron: %w", err)
								}
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
}

// Add forwards the call to the embedded API.
func (c *cron) Add(ctx context.Context, name string, job *api.Job) error {
	return c.API.Add(ctx, name, job)
}

// Get forwards the call to the embedded API.
func (c *cron) Get(ctx context.Context, name string) (*api.Job, error) {
	return c.API.Get(ctx, name)
}

// Delete forwards the call to the embedded API.
func (c *cron) Delete(ctx context.Context, name string) error {
	return c.API.Delete(ctx, name)
}

// DeletePrefixes forwards the call to the embedded API.
func (c *cron) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	return c.API.DeletePrefixes(ctx, prefixes...)
}

// List forwards the call to the embedded API.
func (c *cron) List(ctx context.Context, prefix string) (*api.ListResponse, error) {
	return c.API.List(ctx, prefix)
}

// DeliverablePrefixes forwards the call to the embedded API.
func (c *cron) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	return c.API.DeliverablePrefixes(ctx, prefixes...)
}

// WatchLeadership forwards the call to the embedded API.
func (c *cron) WatchLeadership(ctx context.Context) chan []*anypb.Any {
	return c.API.WatchLeadership(ctx)
}
