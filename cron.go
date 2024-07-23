/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

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
	"github.com/go-logr/zapr"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

// TriggerFunction is a function that is called when a cron job is triggered.
// Returning false will cause the job to be re-enqueued and triggered
// immediately.
type TriggerFunction func(context.Context, *api.TriggerRequest) bool

// Interface is a cron interface. It schedules and manages job which are stored
// and informed from ETCD. It uses a trigger function to call when a job is
// triggered.
// Jobs may be oneshot or recurring. Recurring jobs are scheduled to run at
// their next scheduled time. Oneshot jobs are scheduled to run once and are
// removed from the schedule after they are triggered.
type Interface interface {
	// Run is a blocking function that runs the cron instance. It will return an
	// error if the instance is already running.
	// Returns when the given context is cancelled, after doing all cleanup.
	Run(ctx context.Context) error

	// Add adds a job to the cron instance.
	Add(ctx context.Context, name string, job *api.Job) error

	// Get gets a job from the cron instance.
	Get(ctx context.Context, name string) (*api.Job, error)

	// Delete deletes a job from the cron instance.
	Delete(ctx context.Context, name string) error
}

// Options are the options for creating a new cron instance.
type Options struct {
	// Log is the logger to use for logging.
	Log logr.Logger

	// Client is the etcd client to use for storing cron entries.
	Client *clientv3.Client

	// Namespace is the etcd namespace to use for storing cron entries.
	Namespace string

	// PartitionID is the partition ID to use for storing cron entries.
	PartitionID uint32

	// PartitionTotal is the total number of partitions to use for storing cron
	// entries.
	PartitionTotal uint32

	// TriggerFn is the function to call when a cron job is triggered.
	TriggerFn TriggerFunction
}

// cron is the implementation of the cron interface.
type cron struct {
	log       logr.Logger
	triggerFn TriggerFunction

	client               client.Interface
	part                 partitioner.Interface
	key                  *key.Key
	informer             *informer.Informer
	yard                 *grave.Yard
	queue                *queue.Processor[string, *counter.Counter]
	schedBuilder         *scheduler.Builder
	leadership           *leadership.Leadership
	collector            garbage.Interface
	validateNameReplacer *strings.Replacer

	clock   clock.Clock
	running atomic.Bool
	closeCh chan struct{}
	errCh   chan error
	readyCh chan struct{}
	wg      sync.WaitGroup
	// queueLock prevents an informed schedule from overwriting a job as it is
	// being triggered, i.e. prevent a PUT and mid-trigger race condition.
	queueLock  concurrency.MutexMap[string]
	queueCache *sync.Map
}

// New creates a new cron instance.
func New(opts Options) (Interface, error) {
	if opts.TriggerFn == nil {
		return nil, errors.New("trigger function is required")
	}

	part, err := partitioner.New(partitioner.Options{
		ID:    opts.PartitionID,
		Total: opts.PartitionTotal,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create partitioner: %w", err)
	}

	log := opts.Log
	if log.GetSink() == nil {
		sink, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
		if err != nil {
			return nil, err
		}
		log = zapr.NewLogger(sink)
		log = log.WithName("diagrid-cron")
	}

	client := client.New(opts.Client)

	collector := garbage.New(garbage.Options{
		Log:    log,
		Client: client,
	})

	key := key.New(key.Options{
		Namespace:   opts.Namespace,
		PartitionID: opts.PartitionID,
	})

	yard := grave.New()
	informer := informer.New(informer.Options{
		Key:         key,
		Client:      client,
		Collector:   collector,
		Partitioner: part,
		Yard:        yard,
	})

	leadership := leadership.New(leadership.Options{
		Log:            log,
		Client:         client,
		PartitionTotal: opts.PartitionTotal,
		Key:            key,
	})

	return &cron{
		log:                  log,
		client:               client,
		triggerFn:            opts.TriggerFn,
		key:                  key,
		leadership:           leadership,
		yard:                 yard,
		informer:             informer,
		collector:            collector,
		part:                 part,
		schedBuilder:         scheduler.NewBuilder(),
		validateNameReplacer: strings.NewReplacer("_", "", ":", "", "-", ""),
		clock:                clock.RealClock{},
		readyCh:              make(chan struct{}),
		closeCh:              make(chan struct{}),
		errCh:                make(chan error),
		queueLock:            concurrency.NewMutexMap[string](),
		queueCache:           new(sync.Map),
	}, nil
}

// Run is a blocking function that runs the cron instance.
func (c *cron) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("already running")
	}
	defer c.wg.Wait()

	c.queue = queue.NewProcessor[string, *counter.Counter](
		func(counter *counter.Counter) {
			c.queueLock.RLock(counter.Key())
			_, ok := c.queueCache.Load(counter.Key())
			if !ok || ctx.Err() != nil {
				c.queueLock.DeleteRUnlock(counter.Key())
				return
			}

			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				if c.handleTrigger(ctx, counter) {
					c.queueLock.RUnlock(counter.Key())
				} else {
					c.queueCache.Delete(counter.Key())
					c.queueLock.DeleteRUnlock(counter.Key())
				}
			}()
		},
	).WithClock(c.clock)

	return concurrency.NewRunnerManager(
		c.leadership.Run,
		c.collector.Run,
		func(ctx context.Context) error {
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}

			return c.informer.Run(ctx)
		},
		func(ctx context.Context) error {
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}

			ev, err := c.informer.Events()
			if err != nil {
				return err
			}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case e := <-ev:
					if err := c.handleInformerEvent(ctx, e); err != nil {
						return err
					}
				}
			}
		},
		func(ctx context.Context) error {
			defer close(c.closeCh)
			if err := c.leadership.WaitForLeadership(ctx); err != nil {
				return err
			}
			if err := c.informer.Ready(ctx); err != nil {
				return err
			}

			close(c.readyCh)
			c.log.Info("cron is ready")
			<-ctx.Done()

			return nil
		},
		func(ctx context.Context) error {
			var err error
			select {
			case <-ctx.Done():
			case err = <-c.errCh:
			}

			return errors.Join(c.queue.Close(), err)
		},
	).Run(ctx)
}

// handleTrigger handles triggering a schedule job.
// Returns true if the job is being re-enqueued, false otherwise.
func (c *cron) handleTrigger(ctx context.Context, counter *counter.Counter) bool {
	if !c.triggerFn(ctx, counter.TriggerRequest()) {
		// If the trigger function returns false, i.e. failed client side,
		// re-enqueue the job immediately.
		if err := c.queue.Enqueue(counter); err != nil {
			select {
			case <-ctx.Done():
			case c.errCh <- err:
			}
		}
		return true
	}

	ok, err := counter.Trigger(ctx)
	if err != nil {
		c.log.Error(err, "failure marking job for next trigger", "name", counter.Key())
	}
	if ok && ctx.Err() == nil {
		if err := c.queue.Enqueue(counter); err != nil {
			select {
			case <-ctx.Done():
			case c.errCh <- err:
			}
		}

		return true
	}

	return false
}

// handleInformerEvent handles an etcd informed event.
func (c *cron) handleInformerEvent(ctx context.Context, e *informer.Event) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	c.queueLock.Lock(string(e.Key))
	if e.IsPut {
		defer c.queueLock.Unlock(string(e.Key))
		return c.schedule(ctx, c.key.JobName(e.Key), e.Job)
	}

	defer c.queueLock.DeleteUnlock(string(e.Key))
	c.queueCache.Delete(string(e.Key))
	return c.queue.Dequeue(string(e.Key))
}

// schedule schedules a job to it's next scheduled time.
func (c *cron) schedule(ctx context.Context, name string, job *api.JobStored) error {
	scheduler, err := c.schedBuilder.Scheduler(job)
	if err != nil {
		return err
	}

	counter, ok, err := counter.New(ctx, counter.Options{
		Name:      name,
		Key:       c.key,
		Schedule:  scheduler,
		Yard:      c.yard,
		Client:    c.client,
		Job:       job,
		Collector: c.collector,
	})
	if err != nil || !ok {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.queueCache.Store(counter.Key(), nil)
	return c.queue.Enqueue(counter)
}
