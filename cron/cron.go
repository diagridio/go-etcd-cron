/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/concurrency"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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
}

// cron is the implementation of the cron interface.
type cron struct {
	api.API

	log        logr.Logger
	informer   *informer.Informer
	queue      *queue.Queue
	leadership *leadership.Leadership
	collector  garbage.Interface

	running atomic.Bool
	closeCh chan struct{}
	readyCh chan struct{}
}

// New creates a new cron instance.
func New(opts Options) (api.Interface, error) {
	if opts.TriggerFn == nil {
		return nil, errors.New("trigger function is required")
	}

	if opts.Client == nil {
		return nil, errors.New("client is required")
	}

	part, err := partitioner.New(partitioner.Options{
		ID:    opts.PartitionID,
		Total: opts.PartitionTotal,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create partitioner: %w", err)
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

	client := client.New(opts.Client)

	collector, err := garbage.New(garbage.Options{
		Log:                log,
		Client:             client,
		CollectionInterval: opts.CounterGarbageCollectionInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create garbage collector: %w", err)
	}

	key := key.New(key.Options{
		Namespace:   opts.Namespace,
		PartitionID: opts.PartitionID,
	})

	yard := grave.New()
	informer := informer.New(informer.Options{
		Key:         key,
		Client:      client,
		Collector:   collector,
		Partitioner: part,
		Yard:        yard,
	})

	leadership := leadership.New(leadership.Options{
		Log:            log,
		Client:         client,
		PartitionTotal: opts.PartitionTotal,
		Key:            key,
	})

	schedBuilder := scheduler.NewBuilder()

	queue := queue.New(queue.Options{
		Log:              log,
		Client:           client,
		Clock:            clock.RealClock{},
		Key:              key,
		SchedulerBuilder: schedBuilder,
		TriggerFn:        opts.TriggerFn,
		Collector:        collector,
		Yard:             yard,
	})

	readyCh := make(chan struct{})
	closeCh := make(chan struct{})

	api := internalapi.New(internalapi.Options{
		Client:           client,
		Key:              key,
		SchedulerBuilder: schedBuilder,
		Queue:            queue,
		ReadyCh:          readyCh,
		CloseCh:          closeCh,
	})

	return &cron{
		API:        api,
		log:        log,
		leadership: leadership,
		informer:   informer,
		collector:  collector,
		queue:      queue,
		readyCh:    readyCh,
		closeCh:    closeCh,
	}, nil
}

// Run is a blocking function that runs the cron instance.
func (c *cron) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("already running")
	}

	return concurrency.NewRunnerManager(
		c.leadership.Run,
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
	).Run(ctx)
}
