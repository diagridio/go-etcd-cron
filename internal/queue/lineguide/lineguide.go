/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package lineguide

import (
	"context"

	"github.com/dapr/kit/events/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/queue/cache"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

type Options struct {
	Key          *key.Key
	Client       api.Interface
	SchedBuilder *scheduler.Builder
	Queue        *queue.Processor[string, counter.Interface]
	Cache        *cache.Cache
}

type LineGuide struct {
	key          *key.Key
	client       api.Interface
	schedBuilder *scheduler.Builder
	queue        *queue.Processor[string, counter.Interface]
	cache        *cache.Cache
}

func New(opts Options) *LineGuide {
	return &LineGuide{
		key:          opts.Key,
		client:       opts.Client,
		schedBuilder: opts.SchedBuilder,
		queue:        opts.Queue,
		cache:        opts.Cache,
	}
}

// LineGuide schedules a job to the queue for its next scheduled time.
func (l *LineGuide) Schedule(ctx context.Context, jobName string, revision int64, job *stored.Job) error {
	schedule, err := l.schedBuilder.Schedule(job)
	if err != nil {
		return err
	}

	counter, ok, err := counter.New(ctx, counter.Options{
		Name:           jobName,
		Key:            l.key,
		Schedule:       schedule,
		Client:         l.client,
		Job:            job,
		JobModRevision: revision,
	})
	if err != nil || !ok {
		return err
	}

	l.cache.Store(jobName, counter)
	l.queue.Enqueue(counter)

	return nil
}

// Deschedule removes a job from the queue.
func (l *LineGuide) Deschedule(jobName string, job *stored.Job) {
	if counter, ok := l.cache.LoadAndDelete(jobName); ok {
		l.queue.Dequeue(counter.Key())
	}
}
