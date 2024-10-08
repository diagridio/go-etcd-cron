/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	yard         *grave.Yard
	collector    garbage.Interface

	// counter cache tracks counters which are active. Used to ensure there is no
	// race condition whereby a Delete operation on a job which was mid execution
	// on the same scheduler instance, would not see that job as not deleted from
	// the in-memory. Used to back out of an execution if it is no longer in that
	// cache (deleted).
	cache concurrency.Map[string, struct{}]

	// lock prevents an informed schedule from overwriting a job as it is being
	// triggered, i.e. prevent a PUT and mid-trigger race condition.
	lock  concurrency.MutexMap[string]
	wg    sync.WaitGroup
	queue *queue.Processor[string, counter.Interface]

	// staged are the counters that have been staged for later triggering as the
	// consumer has signalled that the job is current undeliverable. When the
	// consumer signals a prefix has become deliverable, counters in that prefix
	// will be enqueued. Indexed by the counters Job Name.
	staged     map[string]counter.Interface
	stagedLock sync.Mutex

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0.
	deliverablePrefixes map[string]*atomic.Int32

	running atomic.Bool
	readyCh chan struct{}
}

func New(opts Options) *Queue {
	cl := opts.Clock
	if cl == nil {
		cl = clock.RealClock{}
	}

	return &Queue{
		log:                 opts.Log.WithName("queue"),
		client:              opts.Client,
		key:                 opts.Key,
		triggerFn:           opts.TriggerFn,
		collector:           opts.Collector,
		schedBuilder:        opts.SchedulerBuilder,
		cache:               concurrency.NewMap[string, struct{}](),
		yard:                opts.Yard,
		clock:               cl,
		deliverablePrefixes: make(map[string]*atomic.Int32),
		staged:              make(map[string]counter.Interface),
		lock:                concurrency.NewMutexMap[string](),
		readyCh:             make(chan struct{}),
	}
}

// Run starts the cron queue.
func (q *Queue) Run(ctx context.Context) error {
	defer q.wg.Wait()
	if !q.running.CompareAndSwap(false, true) {
		return errors.New("queue is already running")
	}

	q.queue = queue.NewProcessor[string, counter.Interface](
		func(counter counter.Interface) {
			q.lock.RLock(counter.Key())
			_, ok := q.cache.Load(counter.Key())
			if !ok || ctx.Err() != nil {
				q.lock.DeleteRUnlock(counter.Key())
				return
			}

			q.wg.Add(1)
			go func() {
				defer q.wg.Done()
				if !q.handleTrigger(ctx, counter) {
					q.cache.Delete(counter.Key())
					q.lock.DeleteRUnlock(counter.Key())
					return
				}

				q.lock.RUnlock(counter.Key())
			}()
		},
	).WithClock(q.clock)

	close(q.readyCh)

	<-ctx.Done()

	return q.queue.Close()
}

func (q *Queue) Delete(ctx context.Context, jobName string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.readyCh:
	}

	key := q.key.JobKey(jobName)

	q.lock.Lock(key)
	defer q.lock.DeleteUnlock(key)

	q.stagedLock.Lock()
	delete(q.staged, jobName)
	q.stagedLock.Unlock()

	if _, err := q.client.Delete(ctx, key); err != nil {
		return err
	}

	if _, ok := q.cache.LoadAndDelete(key); ok {
		q.queue.Dequeue(key)
	}

	return nil
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
			q.cacheDelete(string(kv.Key))
		}

		q.stagedLock.Lock()
		for jobName := range q.staged {
			if strings.HasPrefix(jobName, prefix) {
				delete(q.staged, jobName)
			}
		}
		q.stagedLock.Unlock()
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

	if err := q.scheduleEvent(ctx, e); err != nil {
		return err
	}

	return nil
}

func (q *Queue) scheduleEvent(ctx context.Context, e *informer.Event) error {
	q.lock.Lock(string(e.Key))

	jobName := q.key.JobName(e.Key)

	q.stagedLock.Lock()
	delete(q.staged, jobName)
	q.stagedLock.Unlock()

	if e.IsPut {
		defer q.lock.Unlock(string(e.Key))
		return q.schedule(ctx, jobName, e.Job)
	}

	defer q.lock.DeleteUnlock(string(e.Key))
	q.cache.Delete(string(e.Key))
	q.collector.Push(q.key.CounterKey(jobName))
	q.queue.Dequeue(string(e.Key))

	return nil
}

func (q *Queue) cacheDelete(jobKey string) {
	q.lock.Lock(jobKey)
	defer q.lock.DeleteUnlock(jobKey)

	if _, ok := q.cache.Load(jobKey); !ok {
		return
	}

	q.cache.Delete(jobKey)
	q.queue.Dequeue(jobKey)
}

// handleTrigger handles triggering a schedule job.
// Returns true if the job is being re-enqueued, false otherwise.
func (q *Queue) handleTrigger(ctx context.Context, counter counter.Interface) bool {
	result := q.triggerFn(ctx, counter.TriggerRequest()).GetResult()
	if ctx.Err() != nil {
		return false
	}

	switch result {
	// Job was successfully triggered. Re-enqueue if the Job has more triggers
	// according to the schedule.
	case api.TriggerResponseResult_SUCCESS:
		ok, err := counter.TriggerSuccess(ctx)
		if err != nil {
			q.log.Error(err, "failure marking job for next trigger", "name", counter.Key())
		}

		if ok {
			q.queue.Enqueue(counter)
		}

		return ok

		// The Job failed to trigger. Re-enqueue if the Job has more trigger
		// attempts according to FailurePolicy, or the Job has more triggers
		// according to the schedule.
	case api.TriggerResponseResult_FAILED:
		ok, err := counter.TriggerFailed(ctx)
		if err != nil {
			q.log.Error(err, "failure failing job for next retry trigger", "name", counter.Key())
		}

		if ok {
			q.queue.Enqueue(counter)
		}
		return ok

		// The Job was undeliverable so will be moved to the staging queue where it
		// will stay until it become deliverable. Due to a race, if the job is in
		// fact now deliverable, we need to re-enqueue immediately, else simply
		// keep it in staging until the prefix is deliverable.
	case api.TriggerResponseResult_UNDELIVERABLE:
		if !q.stage(counter) {
			q.queue.Enqueue(counter)
		}
		return true

	default:
		q.log.Error(errors.New("unknown trigger response result"), "unknown trigger response result", "name", counter.Key(), "result", result)
		return false
	}
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
	q.queue.Enqueue(counter)
	return nil
}
