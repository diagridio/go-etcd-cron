/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"errors"

	eventsqueue "github.com/dapr/kit/events/queue"
	"github.com/go-logr/logr"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/queue/actioner"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops/control"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops/jobs"
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
}

type Queue struct {
	log logr.Logger

	triggerFn        api.TriggerFunction
	schedulerBuilder *scheduler.Builder
	client           clientapi.Interface
	key              *key.Key

	ctrlloop loops.Interface[*queue.ControlEvent]
	queue    *eventsqueue.Processor[string, counter.Interface]

	readyCh chan struct{}
}

func New(opts Options) *Queue {
	q := &Queue{
		log:              opts.Log.WithName("manager"),
		readyCh:          make(chan struct{}),
		triggerFn:        opts.TriggerFn,
		schedulerBuilder: opts.SchedulerBuilder,
		client:           opts.Client,
		key:              opts.Key,
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
	act := actioner.New(actioner.Options{
		Queue:        q.queue,
		TriggerFn:    q.triggerFn,
		ControlLoop:  &q.ctrlloop,
		SchedBuilder: q.schedulerBuilder,
		Client:       q.client,
		Key:          q.key,
	})

	ictx, cancel := context.WithCancel(context.Background())
	jobsLoop := jobs.New(jobs.Options{
		Actioner: act,
		Log:      q.log,
		Cancel:   cancel,
	})

	q.ctrlloop = control.New(control.Options{
		Actioner: act,
		Jobs:     jobsLoop,
	})

	errCh := make(chan error, 2)
	go func() {
		errCh <- jobsLoop.Run(ictx)
	}()
	go func() {
		errCh <- q.ctrlloop.Run(ictx)
	}()

	close(q.readyCh)
	<-ctx.Done()

	q.ctrlloop.Close(&queue.ControlEvent{
		Action: new(queue.ControlEvent_Close),
	})

	return errors.Join(<-errCh, <-errCh)
}

func (q *Queue) Inform(event *queue.Informed) {
	<-q.readyCh
	q.ctrlloop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_Informed{Informed: event},
	})
}

func (q *Queue) DeliverablePrefixes(prefixes ...string) context.CancelFunc {
	<-q.readyCh
	q.ctrlloop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_DeliverablePrefixes{
			DeliverablePrefixes: &queue.DeliverablePrefixes{
				Prefixes: prefixes,
			},
		},
	})

	return func() {
		q.ctrlloop.Enqueue(&queue.ControlEvent{
			Action: &queue.ControlEvent_UndeliverablePrefixes{
				UndeliverablePrefixes: &queue.UndeliverablePrefixes{
					Prefixes: prefixes,
				},
			},
		})
	}
}

func (q *Queue) execute(counter counter.Interface) {
	q.ctrlloop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_ExecuteRequest{
			ExecuteRequest: &queue.ExecuteRequest{
				JobName:    counter.JobName(),
				CounterKey: counter.Key(),
			},
		},
	})
}
