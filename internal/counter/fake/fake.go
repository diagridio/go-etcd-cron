/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"
	"time"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
)

type Fake struct {
	scheduledTimeFn  func() time.Time
	key              int64
	jobName          string
	job              *stored.Job
	triggerRequestFn func() *api.TriggerRequest
	triggerSuccessFn func(context.Context) (bool, error)
	triggerFailedFn  func(context.Context) (bool, error)
}

func New() *Fake {
	return &Fake{
		scheduledTimeFn: time.Now,
		key:             1,
		jobName:         "job",
		triggerRequestFn: func() *api.TriggerRequest {
			return &api.TriggerRequest{}
		},
		triggerSuccessFn: func(context.Context) (bool, error) {
			return true, nil
		},
		triggerFailedFn: func(context.Context) (bool, error) {
			return false, nil
		},
	}
}

func (f *Fake) WithScheduledTime(fn func() time.Time) *Fake {
	f.scheduledTimeFn = fn
	return f
}

func (f *Fake) WithKey(key int64) *Fake {
	f.key = key
	return f
}

func (f *Fake) WithJobName(jobName string) *Fake {
	f.jobName = jobName
	return f
}

func (f *Fake) WithJob(job *stored.Job) *Fake {
	f.job = job
	return f
}

func (f *Fake) WithTriggerRequest(fn func() *api.TriggerRequest) *Fake {
	f.triggerRequestFn = fn
	return f
}

func (f *Fake) WithTriggerSuccess(fn func(context.Context) (bool, error)) *Fake {
	f.triggerSuccessFn = fn
	return f
}

func (f *Fake) WithTriggerFailed(fn func(context.Context) (bool, error)) *Fake {
	f.triggerFailedFn = fn
	return f
}

func (f *Fake) ScheduledTime() time.Time {
	return f.scheduledTimeFn()
}

func (f *Fake) Key() int64 {
	return f.key
}

func (f *Fake) JobName() string {
	return f.jobName
}

func (f *Fake) Job() *stored.Job {
	return f.job
}

func (f *Fake) TriggerRequest() *api.TriggerRequest {
	return f.triggerRequestFn()
}

func (f *Fake) TriggerSuccess(ctx context.Context) (bool, error) {
	return f.triggerSuccessFn(ctx)
}

func (f *Fake) TriggerFailed(ctx context.Context) (bool, error) {
	return f.triggerFailedFn(ctx)
}
