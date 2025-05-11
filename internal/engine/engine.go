/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dapr/kit/concurrency"
	"github.com/go-logr/logr"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	apiqueue "github.com/diagridio/go-etcd-cron/internal/api/queue"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/engine/handler"
	"github.com/diagridio/go-etcd-cron/internal/engine/informer"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/partitioner"
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
	Client clientapi.Interface

	// TriggerFn is the function to call when a cron job is triggered.
	TriggerFn api.TriggerFunction

	// ConsumerSink is an optional sink to receive informer events.
	ConsumerSink chan<- *api.InformerEvent
}

type Interface interface {
	Run(ctx context.Context) error
	API() handler.Interface
}

type engine struct {
	log        logr.Logger
	queue      *queue.Queue
	informer   *informer.Informer
	informerCh <-chan *apiqueue.Informed
	handler    handler.Interface
	running    atomic.Bool
	wg         sync.WaitGroup
}

func New(opts Options) (Interface, error) {
	informer, informerCh := informer.New(informer.Options{
		Key:         opts.Key,
		Client:      opts.Client,
		Partitioner: opts.Partitioner,
	})

	schedBuilder := scheduler.NewBuilder()
	queue := queue.New(queue.Options{
		Log:              opts.Log,
		Client:           opts.Client,
		Clock:            clock.RealClock{},
		Key:              opts.Key,
		SchedulerBuilder: schedBuilder,
		TriggerFn:        opts.TriggerFn,
		ConsumerSink:     opts.ConsumerSink,
	})

	handler := handler.New(handler.Options{
		Client:           opts.Client,
		Key:              opts.Key,
		SchedulerBuilder: schedBuilder,
		Queue:            queue,
		Informer:         informer,
		Log:              opts.Log,
	})

	return &engine{
		log:        opts.Log.WithName("engine"),
		queue:      queue,
		informer:   informer,
		informerCh: informerCh,
		handler:    handler,
		wg:         sync.WaitGroup{},
	}, nil
}

func (e *engine) Run(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		return errors.New("engine is already running")
	}
	defer e.running.Store(false)

	e.log.Info("starting cron engine")
	defer e.log.Info("cron engine shut down")

	return concurrency.NewRunnerManager(
		e.queue.Run,
		e.informer.Run,
		e.handler.Run,
		func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case event := <-e.informerCh:
					e.queue.Inform(ctx, event)
				}
			}
		},
	).Run(ctx)
}

func (e *engine) API() handler.Interface {
	return e.handler
}
