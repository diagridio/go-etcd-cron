/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"

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
}

type Queue struct {
	log logr.Logger

	triggerFn        api.TriggerFunction
	schedulerBuilder *scheduler.Builder
	client           clientapi.Interface
	key              *key.Key

	consumer    *consumer.Consumer
	controlLoop loop.Interface[*queue.ControlEvent]
	queue       *eventsqueue.Processor[string, counter.Interface]

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
	}

	q.queue = eventsqueue.NewProcessor[string, counter.Interface](
		eventsqueue.Options[string, counter.Interface]{
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

	ictx, cancel := context.WithCancel(context.Background())
	routerLoop := router.New(router.Options{
		Actioner: act,
		Log:      q.log,
		Cancel:   cancel,
	})

	q.controlLoop = control.New(control.Options{
		Actioner: act,
		Jobs:     routerLoop,
	})

	close(q.readyCh)

	return concurrency.NewRunnerManager(
		routerLoop.Run,
		q.controlLoop.Run,
		func(context.Context) error {
			// Use the real func context here, rather than the background one we control
			// cancelling in-loop.
			<-ctx.Done()

			q.controlLoop.Close(&queue.ControlEvent{
				Action: new(queue.ControlEvent_Close),
			})

			return nil
		}).Run(ictx)
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

func (q *Queue) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
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

	return func() {
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
				JobName:    counter.JobName(),
				CounterKey: counter.Key(),
			},
		},
	})
}
