/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/counter"
)

type Fake struct {
	scheduleFn              func(context.Context, string, int64, *stored.Job) (counter.Interface, error)
	enqueueFn               func(counter.Interface)
	descheduleFn            func(counter.Interface)
	removeConsumerFn        func(counter.Interface)
	triggerFn               func(context.Context, *api.TriggerRequest) *api.TriggerResponse
	addToControlLoopFn      func(*queue.ControlEvent)
	deliverablePrefixesFn   func(...string) []string
	unDeliverablePrefixesFn func(...string)
	stageFn                 func(string) bool
	unstageFn               func(string)
}

func New() *Fake {
	return &Fake{
		scheduleFn:              func(context.Context, string, int64, *stored.Job) (counter.Interface, error) { return nil, nil },
		enqueueFn:               func(counter.Interface) {},
		descheduleFn:            func(counter.Interface) {},
		triggerFn:               func(context.Context, *api.TriggerRequest) *api.TriggerResponse { return nil },
		removeConsumerFn:        func(counter.Interface) {},
		addToControlLoopFn:      func(*queue.ControlEvent) {},
		deliverablePrefixesFn:   func(...string) []string { return nil },
		unDeliverablePrefixesFn: func(...string) {},
		stageFn:                 func(string) bool { return false },
		unstageFn:               func(string) {},
	}
}

func (f *Fake) WithSchedule(fn func(context.Context, string, int64, *stored.Job) (counter.Interface, error)) *Fake {
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

func (f *Fake) WithTrigger(fn func(context.Context, *api.TriggerRequest) *api.TriggerResponse) *Fake {
	f.triggerFn = fn
	return f
}

func (f *Fake) WithAddToControlLoop(fn func(*queue.ControlEvent)) *Fake {
	f.addToControlLoopFn = fn
	return f
}

func (f *Fake) WithDeliverablePrefixes(fn func(...string) []string) *Fake {
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

func (f *Fake) WithStage(fn func(string) bool) *Fake {
	f.stageFn = fn
	return f
}

func (f *Fake) WithUnstage(fn func(string)) *Fake {
	f.unstageFn = fn
	return f
}

func (f *Fake) Schedule(ctx context.Context, id string, t int64, job *stored.Job) (counter.Interface, error) {
	return f.scheduleFn(ctx, id, t, job)
}

func (f *Fake) Enqueue(c counter.Interface) {
	f.enqueueFn(c)
}

func (f *Fake) Deschedule(c counter.Interface) {
	f.descheduleFn(c)
}

func (f *Fake) Trigger(ctx context.Context, req *api.TriggerRequest) *api.TriggerResponse {
	return f.triggerFn(ctx, req)
}

func (f *Fake) AddToControlLoop(event *queue.ControlEvent) {
	f.addToControlLoopFn(event)
}

func (f *Fake) DeliverablePrefixes(prefixes ...string) []string {
	return f.deliverablePrefixesFn(prefixes...)
}

func (f *Fake) UnDeliverablePrefixes(prefixes ...string) {
	f.unDeliverablePrefixesFn(prefixes...)
}

func (f *Fake) Stage(id string) bool {
	return f.stageFn(id)
}

func (f *Fake) RemoveConsumer(c counter.Interface) {
	f.removeConsumerFn(c)
}

func (f *Fake) Unstage(id string) {
	f.unstageFn(id)
}
