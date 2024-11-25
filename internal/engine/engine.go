/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	internalapi "github.com/diagridio/go-etcd-cron/internal/api"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

// Options are the options for creating a new engine instance.
type Options struct {
	// Log is the logger to use for logging.
	Log logr.Logger

	// Key is the key to use for storing cron entries.
	Key *key.Key

	// Partitioner is the partitioner to use for partitioning cron entries.
	Partitioner partitioner.Interface

	// Client is the etcd client to use for storing cron entries.
	Client client.Interface

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

type Engine struct {
	log       logr.Logger
	collector garbage.Interface
	queue     *queue.Queue
	informer  *informer.Informer
	api       internalapi.Interface
	running   atomic.Bool
	wg        sync.WaitGroup
}

func New(opts Options) (*Engine, error) {
	collector, err := garbage.New(garbage.Options{
		Log:                opts.Log,
		Client:             opts.Client,
		CollectionInterval: opts.CounterGarbageCollectionInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create garbage collector: %w", err)
	}

	yard := grave.New()
	informer := informer.New(informer.Options{
		Key:         opts.Key,
		Client:      opts.Client,
		Collector:   collector,
		Partitioner: opts.Partitioner,
		Yard:        yard,
	})

	schedBuilder := scheduler.NewBuilder()
	queue := queue.New(queue.Options{
		Log:              opts.Log,
		Client:           opts.Client,
		Clock:            clock.RealClock{},
		Key:              opts.Key,
		SchedulerBuilder: schedBuilder,
		TriggerFn:        opts.TriggerFn,
		Collector:        collector,
		Yard:             yard,
	})

	api := internalapi.New(internalapi.Options{
		Client:           opts.Client,
		Key:              opts.Key,
		SchedulerBuilder: schedBuilder,
		Queue:            queue,
		Informer:         informer,
		Log:              opts.Log,
	})

	return &Engine{
		log:       opts.Log.WithName("engine"),
		collector: collector,
		queue:     queue,
		informer:  informer,
		api:       api,
		wg:        sync.WaitGroup{},
	}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		return errors.New("engine is already running")
	}
	defer e.running.Store(false)

	e.log.Info("starting cron engine")
	defer e.log.Info("cron engine shut down")

	return concurrency.NewRunnerManager(
		e.collector.Run,
		e.queue.Run,
		e.informer.Run,
		e.api.Run,
		func(ctx context.Context) error {
			ev, err := e.informer.Events()
			if err != nil {
				return err
			}

			for {
				select {
				case <-ctx.Done():
					return nil
				case event := <-ev:
					if err := e.queue.HandleInformerEvent(ctx, event); err != nil {
						return err
					}
				}
			}
		},
	).Run(ctx)
}

func (e *Engine) API() internalapi.Interface {
	return e.api
}
