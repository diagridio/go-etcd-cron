/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package actioner

import (
	"context"

	eventsqueue "github.com/dapr/kit/events/queue"

	"github.com/dapr/kit/events/loop"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/engine/informer/consumer"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/staging"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

type Options struct {
	Queue        *eventsqueue.Processor[string, counter.Interface]
	TriggerFn    api.TriggerFunction
	ControlLoop  *loop.Interface[*queue.ControlEvent]
	SchedBuilder *scheduler.Builder
	Key          *key.Key
	Client       clientapi.Interface
	Consumer     *consumer.Consumer
}

type Interface interface {
	Schedule(context.Context, string, int64, *stored.Job) (counter.Interface, error)
	Enqueue(counter.Interface)
	Deschedule(counter.Interface)
	RemoveConsumer(counter.Interface)
	Trigger(*api.TriggerRequest, func(*api.TriggerResponse))
	AddToControlLoop(*queue.ControlEvent)
	DeliverablePrefixes(...string) []string
	UnDeliverablePrefixes(...string)
	Stage(string) bool
	Unstage(string)
}

type actioner struct {
	staging      *staging.Staging
	queue        *eventsqueue.Processor[string, counter.Interface]
	triggerFn    api.TriggerFunction
	ctrlloop     *loop.Interface[*queue.ControlEvent]
	schedBuilder *scheduler.Builder
	key          *key.Key
	client       clientapi.Interface
	consumer     *consumer.Consumer
}

func New(opts Options) Interface {
	return &actioner{
		staging:      staging.New(),
		queue:        opts.Queue,
		triggerFn:    opts.TriggerFn,
		ctrlloop:     opts.ControlLoop,
		schedBuilder: opts.SchedBuilder,
		key:          opts.Key,
		client:       opts.Client,
		consumer:     opts.Consumer,
	}
}

func (a *actioner) Schedule(ctx context.Context, jobName string, revision int64, job *stored.Job) (counter.Interface, error) {
	schedule, err := a.schedBuilder.Schedule(job)
	if err != nil {
		return nil, err
	}

	counter, ok, err := counter.New(ctx, counter.Options{
		Name:           jobName,
		Key:            a.key,
		Schedule:       schedule,
		Client:         a.client,
		Job:            job,
		JobModRevision: revision,
	})
	if err != nil || !ok {
		return nil, err
	}

	a.consumer.Put(jobName, job)
	a.queue.Enqueue(counter)

	return counter, nil
}

// Enqueue adds a counter to the counter scheduling queue.
func (a *actioner) Enqueue(counter counter.Interface) {
	a.queue.Enqueue(counter)
}

// Deschedule removes a counter job from the counter scheduling queue.
func (a *actioner) Deschedule(counter counter.Interface) {
	a.queue.Dequeue(counter.Key())
}

// RemoveConsumer sends a signal to the consumer that the job no longer exists.
func (a *actioner) RemoveConsumer(counter counter.Interface) {
	a.consumer.Delete(counter.JobName(), counter.Job())
}

func (a *actioner) Trigger(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
	a.triggerFn(req, fn)
}

func (a *actioner) AddToControlLoop(event *queue.ControlEvent) {
	(*a.ctrlloop).Enqueue(event)
}

func (a *actioner) DeliverablePrefixes(prefixes ...string) []string {
	return a.staging.DeliverablePrefixes(prefixes...)
}

func (a *actioner) UnDeliverablePrefixes(prefixes ...string) {
	a.staging.UnDeliverablePrefixes(prefixes...)
}

func (a *actioner) Stage(jobName string) bool {
	return a.staging.Stage(jobName)
}

func (a *actioner) Unstage(jobName string) {
	a.staging.Unstage(jobName)
}
