/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/counter"
)

type Fake struct {
	scheduleFn              func(context.Context, string, *queue.QueuedJob) (counter.Interface, error)
	enqueueFn               func(counter.Interface)
	descheduleFn            func(counter.Interface)
	removeConsumerFn        func(counter.Interface)
	triggerFn               func(*api.TriggerRequest, func(*api.TriggerResponse))
	addToControlLoopFn      func(*queue.ControlEvent)
	deliverablePrefixesFn   func(...string) []int64
	unDeliverablePrefixesFn func(...string)
	stageFn                 func(int64, string) bool
	unstageFn               func(int64)
}

func New() *Fake {
	return &Fake{
		scheduleFn:              func(context.Context, string, *queue.QueuedJob) (counter.Interface, error) { return nil, nil },
		enqueueFn:               func(counter.Interface) {},
		descheduleFn:            func(counter.Interface) {},
		triggerFn:               func(*api.TriggerRequest, func(*api.TriggerResponse)) {},
		removeConsumerFn:        func(counter.Interface) {},
		addToControlLoopFn:      func(*queue.ControlEvent) {},
		deliverablePrefixesFn:   func(...string) []int64 { return nil },
		unDeliverablePrefixesFn: func(...string) {},
		stageFn:                 func(int64, string) bool { return false },
		unstageFn:               func(int64) {},
	}
}

func (f *Fake) WithSchedule(fn func(context.Context, string, *queue.QueuedJob) (counter.Interface, error)) *Fake {
	f.scheduleFn = fn
	return f
}

func (f *Fake) WithEnqueue(fn func(counter.Interface)) *Fake {
	f.enqueueFn = fn
	return f
}

func (f *Fake) WithDeschedule(fn func(counter.Interface)) *Fake {
	f.descheduleFn = fn
	return f
}

func (f *Fake) WithTrigger(fn func(*api.TriggerRequest, func(*api.TriggerResponse))) *Fake {
	f.triggerFn = fn
	return f
}

func (f *Fake) WithAddToControlLoop(fn func(*queue.ControlEvent)) *Fake {
	f.addToControlLoopFn = fn
	return f
}

func (f *Fake) WithDeliverablePrefixes(fn func(...string) []int64) *Fake {
	f.deliverablePrefixesFn = fn
	return f
}

func (f *Fake) WithUnDeliverablePrefixes(fn func(...string)) *Fake {
	f.unDeliverablePrefixesFn = fn
	return f
}

func (f *Fake) WithRemoveConsumer(fn func(counter.Interface)) *Fake {
	f.removeConsumerFn = fn
	return f
}

func (f *Fake) WithStage(fn func(int64, string) bool) *Fake {
	f.stageFn = fn
	return f
}

func (f *Fake) WithUnstage(fn func(int64)) *Fake {
	f.unstageFn = fn
	return f
}

func (f *Fake) Schedule(ctx context.Context, id string, job *queue.QueuedJob) (counter.Interface, error) {
	return f.scheduleFn(ctx, id, job)
}

func (f *Fake) Enqueue(c counter.Interface) {
	f.enqueueFn(c)
}

func (f *Fake) Deschedule(c counter.Interface) {
	f.descheduleFn(c)
}

func (f *Fake) Trigger(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
	f.triggerFn(req, fn)
}

func (f *Fake) AddToControlLoop(event *queue.ControlEvent) {
	f.addToControlLoopFn(event)
}

func (f *Fake) DeliverablePrefixes(prefixes ...string) []int64 {
	return f.deliverablePrefixesFn(prefixes...)
}

func (f *Fake) UnDeliverablePrefixes(prefixes ...string) {
	f.unDeliverablePrefixesFn(prefixes...)
}

func (f *Fake) Stage(id int64, name string) bool {
	return f.stageFn(id, name)
}

func (f *Fake) RemoveConsumer(c counter.Interface) {
	f.removeConsumerFn(c)
}

func (f *Fake) Unstage(id int64) {
	f.unstageFn(id)
}
