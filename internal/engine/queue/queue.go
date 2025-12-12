/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"log"

	"github.com/dapr/kit/concurrency"
	eventsqueue "github.com/dapr/kit/events/queue"
	"github.com/go-logr/logr"
	"k8s.io/utils/clock"

	"github.com/dapr/kit/events/loop"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/engine/informer/consumer"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/control"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/router"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/worker"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

// Options are the options for the Queue.
type Options struct {
	// Log is the logger to use for Queue logging.
	Log logr.Logger

	// Client is the etcd client to use for Queue operations.
	Client clientapi.Interface

	// Clock is the clock to use for Queue operations.
	Clock clock.Clock

	// Key is the key to use for Queue operations.
	Key *key.Key

	// SchedulerBuilder is the scheduler builder to use for Queue operations.
	SchedulerBuilder *scheduler.Builder

	// TriggerFn is the trigger function to use for Queue operations.
	TriggerFn api.TriggerFunction

	// ConsumerSink is an optional channel to send informer events to.
	ConsumerSink chan<- *api.InformerEvent

	// Workers is the number of workers to use for processing events in the
	// queue.
	Workers uint32
}

type Queue struct {
	log logr.Logger

	triggerFn        api.TriggerFunction
	schedulerBuilder *scheduler.Builder
	client           clientapi.Interface
	key              *key.Key

	consumer    *consumer.Consumer
	controlLoop loop.Interface[*queue.ControlEvent]
	queue       *eventsqueue.Processor[int64, counter.Interface]
	workers     uint32

	readyCh chan struct{}
}

func New(opts Options) *Queue {
	q := &Queue{
		log:              opts.Log.WithName("queue"),
		readyCh:          make(chan struct{}),
		triggerFn:        opts.TriggerFn,
		schedulerBuilder: opts.SchedulerBuilder,
		client:           opts.Client,
		key:              opts.Key,
		consumer: consumer.New(consumer.Options{
			Sink: opts.ConsumerSink,
		}),
		workers: opts.Workers,
	}

	q.queue = eventsqueue.NewProcessor[int64, counter.Interface](
		eventsqueue.Options[int64, counter.Interface]{
			Clock:     opts.Clock,
			ExecuteFn: q.execute,
		},
	)

	return q
}

func (q *Queue) Run(ctx context.Context) error {
	defer q.consumer.DropAll()

	act := actioner.New(actioner.Options{
		Queue:        q.queue,
		TriggerFn:    q.triggerFn,
		ControlLoop:  &q.controlLoop,
		SchedBuilder: q.schedulerBuilder,
		Client:       q.client,
		Key:          q.key,
		Consumer:     q.consumer,
	})

	q.log.Info("Starting queue with workers", "count", q.workers)
	workers := make([]loop.Interface[*queue.JobEvent], q.workers)
	for i := range workers {
		workers[i] = worker.New(worker.Options{
			Actioner: act,
			Log:      q.log,
		})
	}

	routerLoop := router.New(router.Options{
		Log:     q.log,
		Workers: workers,
	})

	q.controlLoop = control.New(control.Options{
		Actioner: act,
		Router:   routerLoop,
	})

	runners := make([]concurrency.Runner, 0, len(workers)+2)
	for _, w := range workers {
		runners = append(runners, w.Run)
	}
	runners = append(runners, routerLoop.Run, q.controlLoop.Run)

	runners = append(runners, func(ctx context.Context) error {
		<-ctx.Done()

		q.controlLoop.Close(&queue.ControlEvent{
			Action: new(queue.ControlEvent_Close),
		})
		return nil
	})

	close(q.readyCh)

	return concurrency.NewRunnerManager(runners...).Run(ctx)
}

func (q *Queue) Inform(ctx context.Context, event *queue.Informed) {
	select {
	case <-q.readyCh:
	case <-ctx.Done():
		return
	}

	q.controlLoop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_Informed{Informed: event},
	})
}

func (q *Queue) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelCauseFunc, error) {
	select {
	case <-q.readyCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	q.controlLoop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_DeliverablePrefixes{
			DeliverablePrefixes: &queue.DeliverablePrefixes{
				Prefixes: prefixes,
			},
		},
	})

	return func(cause error) {
		log.Printf("Marking prefixes as undeliverable for one caller: %v", cause)
		q.controlLoop.Enqueue(&queue.ControlEvent{
			Action: &queue.ControlEvent_UndeliverablePrefixes{
				UndeliverablePrefixes: &queue.UndeliverablePrefixes{
					Prefixes: prefixes,
				},
			},
		})
	}, nil
}

func (q *Queue) execute(counter counter.Interface) {
	q.controlLoop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_ExecuteRequest{
			ExecuteRequest: &queue.ExecuteRequest{
				ModRevision: counter.Key(),
			},
		},
	})
}
