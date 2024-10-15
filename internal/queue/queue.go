/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dapr/kit/concurrency/cmap"
	"github.com/dapr/kit/concurrency/fifo"
	"github.com/dapr/kit/events/queue"
	"github.com/go-logr/logr"
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
	queue        *queue.Processor[string, counter.Interface]
	eventsLock   *fifo.Mutex

	// counterLock prevents an informed schedule from overwriting a job as it is
	// being triggered, i.e. prevent a PUT and mid-trigger race condition.
	counterLock fifo.Map[string]

	// counterCache tracks counters which are active. Used to ensure there is no
	// race condition whereby a Delete operation on a job which was mid execution
	// on the same scheduler instance, would not see that job as not deleted from
	// the in-memory queue. Used to back out of an execution if it is no longer
	// in that cache (deleted).
	counterCache cmap.Map[string, struct{}]

	// staged are the counters that have been staged for later triggering as the
	// consumer has signalled that the job is current undeliverable. When the
	// consumer signals a prefix has become deliverable, counters in that prefix
	// will be enqueued. Indexed by the counters Job Name.
	staged map[string]counter.Interface

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0.
	deliverablePrefixes map[string]*atomic.Int32

	wg      sync.WaitGroup
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
		eventsLock:          fifo.New(),
		counterLock:         fifo.NewMap[string](),
		counterCache:        cmap.NewMap[string, struct{}](),
		yard:                opts.Yard,
		clock:               cl,
		deliverablePrefixes: make(map[string]*atomic.Int32),
		staged:              make(map[string]counter.Interface),
		readyCh:             make(chan struct{}),
	}
}

// Run starts the cron queue.
func (q *Queue) Run(ctx context.Context) error {
	defer q.wg.Wait()
	if !q.running.CompareAndSwap(false, true) {
		return errors.New("queue is already running")
	}

	q.queue = queue.NewProcessor[string, counter.Interface](q.executeFn(ctx)).WithClock(q.clock)

	close(q.readyCh)
	<-ctx.Done()
	q.queue.Close()
	return nil
}

// HandleInformerEvent handles an etcd informed event for the cron queue.
func (q *Queue) HandleInformerEvent(ctx context.Context, e *informer.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.readyCh:
	}

	q.eventsLock.Lock()
	defer q.eventsLock.Unlock()

	q.counterLock.Lock(string(e.Key))
	defer q.counterLock.Unlock(string(e.Key))

	jobName := q.key.JobName(e.Key)
	delete(q.staged, jobName)

	if e.IsPut {
		return q.schedule(ctx, jobName, e.Job)
	}

	if _, ok := q.counterCache.LoadAndDelete(string(e.Key)); ok {
		q.collector.Push(q.key.CounterKey(jobName))
		q.queue.Dequeue(string(e.Key))
	}

	return nil
}

// handleTrigger handles triggering a schedule job.
// Returns true if the job is being re-enqueued, false otherwise.
func (q *Queue) handleTrigger(ctx context.Context, counter counter.Interface) bool {
	result := q.triggerFn(ctx, counter.TriggerRequest()).GetResult()

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

func (q *Queue) executeFn(ctx context.Context) func(counter.Interface) {
	return func(counter counter.Interface) {
		q.counterLock.Lock(counter.Key())

		_, ok := q.counterCache.Load(counter.Key())
		if !ok || ctx.Err() != nil {
			q.counterLock.Unlock(counter.Key())
			return
		}

		q.wg.Add(1)
		go func() {
			if !q.handleTrigger(ctx, counter) {
				q.counterCache.Delete(counter.Key())
			}

			q.counterLock.Unlock(counter.Key())
			q.wg.Done()
		}()
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

	q.counterCache.Store(counter.Key(), struct{}{})
	q.queue.Enqueue(counter)
	return nil
}
