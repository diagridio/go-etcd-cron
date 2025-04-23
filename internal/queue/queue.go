/*
Copyright (c) 2024 Diagrid Inc.
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
	"github.com/diagridio/go-etcd-cron/internal/queue/cache"
	"github.com/diagridio/go-etcd-cron/internal/queue/lineguide"
	"github.com/diagridio/go-etcd-cron/internal/queue/loop"
	"github.com/diagridio/go-etcd-cron/internal/queue/staging"
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

// Queue is responsible for managing the cron queue and triggering jobs.
type Queue struct {
	log logr.Logger

	queue *eventsqueue.Processor[string, counter.Interface]
	loop  *loop.Loop
}

func New(opts Options) *Queue {
	q := &Queue{log: opts.Log.WithName("queue")}

	q.queue = eventsqueue.NewProcessor[string, counter.Interface](
		eventsqueue.Options[string, counter.Interface]{
			ExecuteFn: func(counter counter.Interface) {
				q.loop.Enqueue(&queue.ControlEvent{
					Action: &queue.ControlEvent_ExecuteRequest{
						ExecuteRequest: &queue.ExecuteRequest{
							JobName:    counter.JobName(),
							CounterKey: counter.Key(),
						},
					},
				})
			},
			Clock: opts.Clock,
		},
	)

	cache := cache.New()

	q.loop = loop.New(loop.Options{
		Log:       q.log,
		TriggerFn: opts.TriggerFn,
		Queue:     q.queue,
		Cache:     cache,
		Staging: staging.New(staging.Options{
			Queue: q.queue,
		}),
		LineGuide: lineguide.New(lineguide.Options{
			Key:          opts.Key,
			Client:       opts.Client,
			SchedBuilder: opts.SchedulerBuilder,
			Queue:        q.queue,
			Cache:        cache,
		}),
	})

	return q
}

// Run starts the cron queue.
func (q *Queue) Run(ctx context.Context) error {
	q.log.Info("queue is ready")
	defer func() {
		//nolint:errcheck
		q.queue.Close()
		q.log.Info("shut down queue")
	}()

	err := q.loop.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (q *Queue) Inform(event *queue.Informed) {
	q.loop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_Informed{Informed: event},
	})
}

func (q *Queue) DeliverablePrefixes(prefixes ...string) context.CancelFunc {
	q.loop.Enqueue(&queue.ControlEvent{
		Action: &queue.ControlEvent_DeliverablePrefixes{
			DeliverablePrefixes: &queue.DeliverablePrefixes{
				Prefixes: prefixes,
			},
		},
	})

	return func() {
		q.loop.Enqueue(&queue.ControlEvent{
			Action: &queue.ControlEvent_UndeliverablePrefixes{
				UndeliverablePrefixes: &queue.UndeliverablePrefixes{
					Prefixes: prefixes,
				},
			},
		})
	}
}
