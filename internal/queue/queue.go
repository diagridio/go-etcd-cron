/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dapr/kit/concurrency"
	"github.com/dapr/kit/events/queue"
	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

// Options are the options for the Queue.
type Options struct {
	// Log is the logger to use for Queue logging.
	Log logr.Logger

	// Client is the etcd client to use for Queue operations.
	Client client.Interface

	// Clock is the clock to use for Queue operations.
	Clock clock.Clock

	// Key is the key to use for Queue operations.
	Key *key.Key

	// SchedulerBuilder is the scheduler builder to use for Queue operations.
	SchedulerBuilder *scheduler.Builder

	// TriggerFn is the trigger function to use for Queue operations.
	TriggerFn api.TriggerFunction

	// Collector is the garbage collector to use for Queue operations.
	Collector garbage.Interface

	// Yard is the grave yard to use for Queue operations.
	Yard *grave.Yard
}

// Queue is responsible for managing the cron queue and triggering jobs.
type Queue struct {
	log       logr.Logger
	triggerFn api.TriggerFunction
	clock     clock.Clock

	client client.Interface

	key          *key.Key
	schedBuilder *scheduler.Builder
	collector    garbage.Interface
	cache        concurrency.Map[string, struct{}]
	yard         *grave.Yard

	// lock prevents an informed schedule from overwriting a job as it is being
	// triggered, i.e. prevent a PUT and mid-trigger race condition.
	lock  concurrency.MutexMap[string]
	wg    sync.WaitGroup
	queue *queue.Processor[string, *counter.Counter]

	running atomic.Bool
	readyCh chan struct{}
	errCh   chan error
}

func New(opts Options) *Queue {
	cl := opts.Clock
	if cl == nil {
		cl = clock.RealClock{}
	}

	return &Queue{
		log:          opts.Log.WithName("queue"),
		client:       opts.Client,
		key:          opts.Key,
		triggerFn:    opts.TriggerFn,
		collector:    opts.Collector,
		schedBuilder: opts.SchedulerBuilder,
		cache:        concurrency.NewMap[string, struct{}](),
		yard:         opts.Yard,
		clock:        cl,
		lock:         concurrency.NewMutexMap[string](),
		readyCh:      make(chan struct{}),
		errCh:        make(chan error, 10),
	}
}

// Run starts the cron queue.
func (q *Queue) Run(ctx context.Context) error {
	defer q.wg.Wait()
	if !q.running.CompareAndSwap(false, true) {
		return errors.New("queue is already running")
	}

	q.queue = queue.NewProcessor[string, *counter.Counter](
		func(counter *counter.Counter) {
			q.lock.RLock(counter.Key())
			_, ok := q.cache.Load(counter.Key())
			if !ok || ctx.Err() != nil {
				q.lock.DeleteRUnlock(counter.Key())
				return
			}

			q.wg.Add(1)
			go func() {
				defer q.wg.Done()
				if q.handleTrigger(ctx, counter) {
					q.lock.RUnlock(counter.Key())
				} else {
					q.cache.Delete(counter.Key())
					q.lock.DeleteRUnlock(counter.Key())
				}
			}()
		},
	).WithClock(q.clock)

	close(q.readyCh)

	var err error
	select {
	case <-ctx.Done():
	case err = <-q.errCh:
		if errors.Is(err, queue.ErrProcessorStopped) {
			err = nil
		}
	}

	return errors.Join(q.queue.Close(), err)
}

func (q *Queue) Delete(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.readyCh:
	}

	key := q.key.JobKey(name)

	q.lock.Lock(key)
	defer q.lock.DeleteUnlock(key)

	if _, err := q.client.Delete(ctx, key); err != nil {
		return err
	}

	if _, ok := q.cache.Load(key); !ok {
		return nil
	}

	q.cache.Delete(key)
	return q.queue.Dequeue(key)
}

func (q *Queue) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.readyCh:
	}

	var errs []error
	for _, prefix := range prefixes {
		keyPrefix := q.key.JobKey(prefix)
		q.log.V(3).Info("deleting jobs with prefix", "prefix", keyPrefix)

		resp, err := q.client.Delete(ctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete jobs with prefix %q: %w", prefix, err))
			continue
		}

		for _, kv := range resp.PrevKvs {
			errs = append(errs, q.cacheDelete(string(kv.Key)))
		}
	}

	return errors.Join(errs...)
}

// HandleInformerEvent handles an etcd informed event for the cron queue.
func (q *Queue) HandleInformerEvent(ctx context.Context, e *informer.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.readyCh:
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := q.scheduleEvent(ctx, e); !errors.Is(err, queue.ErrProcessorStopped) {
		return err
	}

	return nil
}

func (q *Queue) scheduleEvent(ctx context.Context, e *informer.Event) error {
	q.lock.Lock(string(e.Key))
	if e.IsPut {
		defer q.lock.Unlock(string(e.Key))
		return q.schedule(ctx, q.key.JobName(e.Key), e.Job)
	}

	defer q.lock.DeleteUnlock(string(e.Key))
	q.cache.Delete(string(e.Key))
	q.collector.Push(q.key.CounterKey(q.key.JobName(e.Key)))
	return q.queue.Dequeue(string(e.Key))
}

func (q *Queue) cacheDelete(jobKey string) error {
	q.lock.Lock(jobKey)
	defer q.lock.DeleteUnlock(jobKey)

	if _, ok := q.cache.Load(jobKey); ok {
		return nil
	}

	q.cache.Delete(jobKey)
	return q.queue.Dequeue(jobKey)
}

// handleTrigger handles triggering a schedule job.
// Returns true if the job is being re-enqueued, false otherwise.
func (q *Queue) handleTrigger(ctx context.Context, counter *counter.Counter) bool {
	if !q.triggerFn(ctx, counter.TriggerRequest()) {
		ok, err := counter.TriggerFailed(ctx)
		if err != nil {
			q.log.Error(err, "failure failing job for next retry trigger", "name", counter.Key())
		}

		return q.enqueueCounter(ctx, counter, ok)
	}

	ok, err := counter.TriggerSuccess(ctx)
	if err != nil {
		q.log.Error(err, "failure marking job for next trigger", "name", counter.Key())
	}

	return q.enqueueCounter(ctx, counter, ok)
}

// enqueueCounter enqueues the job to the queue at this count tick.
func (q *Queue) enqueueCounter(ctx context.Context, counter *counter.Counter, ok bool) bool {
	if ok && ctx.Err() == nil {
		if err := q.queue.Enqueue(counter); err != nil {
			select {
			case <-ctx.Done():
			case q.errCh <- err:
			}
		}

		return true
	}

	return false
}

// schedule schedules a job to it's next scheduled time.
func (q *Queue) schedule(ctx context.Context, name string, job *stored.Job) error {
	schedule, err := q.schedBuilder.Schedule(job)
	if err != nil {
		return err
	}

	counter, ok, err := counter.New(ctx, counter.Options{
		Name:      name,
		Key:       q.key,
		Schedule:  schedule,
		Yard:      q.yard,
		Client:    q.client,
		Job:       job,
		Collector: q.collector,
	})
	if err != nil || !ok {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	q.cache.Store(counter.Key(), struct{}{})
	return q.queue.Enqueue(counter)
}
