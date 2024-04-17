/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	etcdclient "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	anypb "google.golang.org/protobuf/types/known/anypb"

	"github.com/dapr/kit/events/queue"
	"github.com/diagridio/go-etcd-cron/collector"
	"github.com/diagridio/go-etcd-cron/counting"
	"github.com/diagridio/go-etcd-cron/partitioning"
	"github.com/diagridio/go-etcd-cron/rhythm"
	"github.com/diagridio/go-etcd-cron/storage"
)

const (
	defaultEtcdEndpoint = "127.0.0.1:2379"
	defaultNamespace    = "etcd_cron"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	namespace              string
	pendingOperations      []func(context.Context) *Entry
	pendingOperationsMutex sync.RWMutex
	liveOperation          chan func(ctx context.Context) *Entry
	entries                map[string]*Entry
	//entriesMutex           sync.RWMutex
	//snapshot          chan []*Entry
	etcdErrorsHandler func(context.Context, *Job, error)
	errorsHandler     func(context.Context, *Job, error)
	funcCtx           func(context.Context, *Job) context.Context
	triggerFunc       TriggerFunction
	running           bool
	runWaitingGroup   sync.WaitGroup
	etcdclient        *etcdclient.Client
	jobStore          storage.JobStore
	organizer         partitioning.Organizer
	partitioning      partitioning.Partitioner
	collector         collector.Collector

	queue   *queue.Processor[string, *Entry]
	readyCh chan struct{}

	logger *zap.Logger
}

type TriggerRequest struct {
	JobName  string
	Metadata map[string]string
	Payload  *anypb.Any
}

type TriggerResult int

const (
	OK TriggerResult = iota
	Failure
	Delete
)

type TriggerFunction func(ctx context.Context, req TriggerRequest) (TriggerResult, error)

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule rhythm.Schedule

	// The Job o run.
	Job *Job

	// Counter if has limit on number of triggers
	counter counting.Counter

	// Mutex to handle concurrent updates to entry
	// Locks: Job and Schedule
	mutex sync.RWMutex
}

func (e *Entry) Key() string {
	return e.Job.Name
}

func (e *Entry) ScheduledTime() time.Time {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	start := e.Job.StartTime

	now := time.Now()
	if start.After(now) {
		return start
	}

	return e.Schedule.Next(start, now)
}

type CronOpt func(cron *Cron)

func WithEtcdErrorsHandler(f func(context.Context, *Job, error)) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.etcdErrorsHandler = f
	})
}

func WithErrorsHandler(f func(context.Context, *Job, error)) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.errorsHandler = f
	})
}

func WithEtcdClient(c *etcdclient.Client) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.etcdclient = c
	})
}

func WithJobStore(s storage.JobStore) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.jobStore = s
	})
}

func WithFuncCtx(f func(context.Context, *Job) context.Context) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.funcCtx = f
	})
}

func WithTriggerFunc(f TriggerFunction) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.triggerFunc = f
	})
}

func WithNamespace(n string) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.namespace = n
	})
}

func WithPartitioning(p partitioning.Partitioner) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.partitioning = p
	})
}

func WithLogConfig(c *zap.Config) CronOpt {
	return CronOpt(func(cron *Cron) {
		if c != nil {
			logger, err := c.Build()
			if err == nil {
				cron.logger = logger
			}
		}
	})
}

// New returns a new Cron job runner.
func New(opts ...CronOpt) (*Cron, error) {
	cron := &Cron{
		pendingOperations: []func(context.Context) *Entry{},
		liveOperation:     make(chan func(context.Context) *Entry),
		entries:           map[string]*Entry{},
		//snapshot:          make(chan []*Entry),
		running: false,
		readyCh: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(cron)
	}
	if cron.partitioning == nil {
		cron.partitioning = partitioning.NoPartitioning()
	}
	if cron.etcdclient == nil {
		etcdClient, err := etcdclient.New(etcdclient.Config{
			Endpoints: []string{defaultEtcdEndpoint},
		})
		if err != nil {
			return nil, err
		}
		cron.etcdclient = etcdClient
	}
	if cron.etcdErrorsHandler == nil {
		cron.etcdErrorsHandler = func(ctx context.Context, j *Job, err error) {
			log.Printf("[etcd-cron] etcd error when handling '%v' job: %v", j.Name, err)
		}
	}
	if cron.errorsHandler == nil {
		cron.errorsHandler = func(ctx context.Context, j *Job, err error) {
			log.Printf("[etcd-cron] error when handling '%v' job: %v", j.Name, err)
		}
	}
	if cron.namespace == "" {
		cron.namespace = defaultNamespace
	}
	cron.organizer = partitioning.NewOrganizer(cron.namespace, cron.partitioning)
	if cron.jobStore == nil {
		cron.jobStore = storage.NewEtcdJobStore(
			cron.etcdclient,
			cron.organizer,
			cron.partitioning,
			func(ctx context.Context, jobName string, r *storage.JobRecord) error {
				return cron.scheduleJob(jobFromJobRecord(jobName, r))
			},
			func(ctx context.Context, s string) error {
				return cron.onJobDeleted(ctx, s)
			})
	}

	cron.collector = collector.New(time.Hour, time.Minute)

	if cron.logger == nil {
		logger, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
		if err != nil {
			return nil, err
		}
		cron.logger = logger.Named("diagrid-cron")
	}

	return cron, nil
}

// AddJob adds a Job.
func (c *Cron) AddJob(ctx context.Context, job Job) error {
	if c.jobStore == nil {
		return fmt.Errorf("cannot persist job: no job store configured")
	}
	record := job.toJobRecord()
	return c.jobStore.Put(ctx, job.Name, record)
}

// DeleteJob removes a job.
func (c *Cron) DeleteJob(ctx context.Context, jobName string) error {
	if c.jobStore == nil {
		return fmt.Errorf("cannot delete job: no job store configured")
	}
	return c.jobStore.Delete(ctx, jobName)
}

func (c *Cron) onJobDeleted(ctx context.Context, jobName string) error {
	c.killJob(jobName)
	// Best effort to delete the counter (if present)
	// Jobs deleted by expiration will delete their counter first.
	// Jobs deleted manually need this logic.
	partitionId := c.partitioning.CalculatePartitionId(jobName)
	counterKey := c.organizer.CounterPath(partitionId, jobName)
	counter := counting.NewEtcdCounter(c.etcdclient, counterKey, 0)
	err := counter.Delete(ctx)
	if err != nil {
		c.errorsHandler(ctx, &Job{Name: jobName}, err)
	}
	// Ignore error as it is a best effort.
	return nil
}

func (c *Cron) killJob(name string) {
	c.appendOperation(func(ctx context.Context) *Entry {
		_, ok := c.entries[name]
		if !ok {
			return nil
		}

		delete(c.entries, name)
		return nil
	})
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) scheduleJob(job *Job) error {
	s, repeats, err := rhythm.Parse(job.Rhythm)
	if err != nil {
		return err
	}

	if (repeats > 0) && (job.Repeats > 0) && (job.Repeats != int32(repeats)) {
		return fmt.Errorf("conflicting number of repeats: %v vs %v", repeats, job.Repeats)
	}

	if repeats > 0 {
		job.Repeats = int32(repeats)
	}

	return c.schedule(s, job)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) schedule(schedule rhythm.Schedule, job *Job) error {
	<-c.readyCh
	partitionId := c.partitioning.CalculatePartitionId(job.Name)
	if !c.partitioning.CheckPartitionLeader(partitionId) {
		// It means the partitioning changed and persisted jobs are in the wrong partition now.
		return fmt.Errorf("host does not own partition %d", partitionId)
	}

	var counter counting.Counter
	if job.Repeats > 0 {
		counterKey := c.organizer.CounterPath(partitionId, job.Name)
		// Needs to count the number of invocations.
		counter = counting.NewEtcdCounter(c.etcdclient, counterKey, int(job.Repeats))
	}

	c.queue.Enqueue(&Entry{
		Schedule: schedule,
		Job:      job,
		counter:  counter,
	})

	// TODO:
	//c.appendOperation(func(ctx context.Context) *Entry {
	//	existing, exists := c.entries[entry.jobName]
	//	if exists {
	//		existing.mutex.Lock()
	//		defer existing.mutex.Unlock()

	//		// Updates existing job.
	//		existing.Job = entry.Job
	//		existing.Schedule = entry.Schedule
	//		// Counter is not reused.
	//		return existing
	//	} else {
	//		c.entries[entry.jobName] = entry
	//	}
	//	return entry
	//})
	return nil
}

func (c *Cron) appendOperation(op func(ctx context.Context) *Entry) {
	if !c.running {
		c.pendingOperationsMutex.Lock()
		defer c.pendingOperationsMutex.Unlock()

		c.pendingOperations = append(c.pendingOperations, op)
		return
	}

	c.liveOperation <- op
}

//// Entries returns a snapshot of the cron entries.
//func (c *Cron) Entries() []*Entry {
//	if c.running {
//		c.snapshot <- nil
//		x := <-c.snapshot
//		return x
//	}
//
//	c.entriesMutex.RLock()
//	defer c.entriesMutex.RUnlock()
//	entries := []*Entry{}
//	for _, e := range c.entries {
//		e.mutex.RLock()
//		entries = append(entries, &Entry{
//			Schedule: e.Schedule,
//			Next:     e.Next,
//			Prev:     e.Prev,
//			Job:      e.Job,
//			jobName:  e.jobName,
//		})
//		e.mutex.RUnlock()
//	}
//	return entries
//}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start(ctx context.Context) error {
	err := c.jobStore.Start(ctx)
	if err != nil {
		return err
	}
	c.collector.Start(ctx)
	c.running = true
	c.runWaitingGroup.Add(1)
	return c.run(ctx)
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) error {
	//localMutexer := locking.NewMutexer(c.collector)
	//mutexStore := locking.NewMutexStore(locking.NewDistributedMutexBuilderFunc(c.etcdclient), c.collector)
	// Figure out the next activation times for each entry.
	//now := time.Now().Local()

	//entries := []*Entry{}

	// Pending operations only matter before running, ignored afterwards.
	//c.pendingOperationsMutex.Lock()
	//c.entriesMutex.Lock()
	//for _, op := range c.pendingOperations {
	//	newEntry := op(ctx)
	//	if newEntry != nil {
	//		newEntry.tick(now)
	//	}
	//}
	//for _, e := range c.entries {
	//	entries = append(entries, e)
	//}
	//c.entriesMutex.Unlock()
	//c.pendingOperations = []func(context.Context) *Entry{}
	//c.pendingOperationsMutex.Unlock()

	updateAsDeleted := func(ctx context.Context, e *Entry) error {
		//e.Job.Status = JobStatusDeleted
		//e.Job.Payload = nil
		//e.Job.Metadata = nil
		//record := e.Job.toJobRecord()

		// Now that all other records related to the job are deleted, we can delete the job
		//err := c.jobStore.Put(ctx, e.Job.Name, record)
		//if err != nil {
		//	return err
		//}

		// Proactive try to clean up right away, if fails then we try again in next trigger
		return c.jobStore.Delete(ctx, e.Job.Name)
	}

	// TODO:
	c.queue = queue.NewProcessor[string, *Entry](func(e *Entry) {
		//if e.counter != nil {
		//	// Must refresh counter since it might have been updated by another instance before.
		//	// Can only use the counter after the tick's mutex.
		//	value, err := e.counter.Refresh(ctx)
		//	if (err != nil) && (value <= 0) {
		//		// Job already exhausted the counter, will not trigger - tries to delete instead.
		//		err := updateAsDeleted(ctx, e)
		//		if err != nil {
		//			c.errorsHandler(ctx, e.Job, err)
		//		}
		//		return
		//	}
		//}

		if e.Job.Status == JobStatusActive && e.Job.expired(time.Now()) {
			err := updateAsDeleted(ctx, e)
			if err != nil {
				c.errorsHandler(ctx, e.Job, err)
			}
			return
		}

		if e.Job.Status == JobStatusDeleted {
			if err := c.jobStore.Delete(ctx, e.Job.Name); err != nil {
				c.errorsHandler(ctx, e.Job, err)
			}
			return
		}

		result, err := c.triggerFunc(ctx, TriggerRequest{
			JobName:  e.Job.Name,
			Metadata: e.Job.Metadata,
			Payload:  e.Job.Payload,
		})
		if err != nil {
			c.errorsHandler(ctx, e.Job, err)
			return
		}

		if result == Delete {
			err := updateAsDeleted(ctx, e)
			if err != nil {
				c.errorsHandler(ctx, e.Job, err)
			}
			return
		}

		if result == OK && e.counter != nil {
			value := e.counter.Value()
			if value <= 1 {
				err := updateAsDeleted(ctx, e)
				if err != nil {
					c.errorsHandler(ctx, e.Job, err)
				}
			} else {
				// Needs to check number of triggers
				_, _, err = e.counter.Increment(ctx, -1)
				if err != nil {
					c.errorsHandler(ctx, e.Job, err)
					// No need to abort if updating the count failed.
					// The count solution is not transactional anyway.
				}

				if err := c.queue.Enqueue(e); err != nil {
					c.errorsHandler(ctx, e.Job, err)
				}
			}
		}
	})

	close(c.readyCh)
	<-ctx.Done()

	return c.queue.Close()

	//for {
	//	sort.Sort(byTime(entries))

	//	var effective time.Time
	//	if len(entries) == 0 || entries[0].Next.IsZero() {
	//		// If there are no entries yet, just sleep - it still handles new entries
	//		// and stop requests.
	//		effective = now.AddDate(10, 0, 0)
	//	} else {
	//		effective = entries[0].Next
	//	}

	//	c.logger.Info("new iteration", zap.Time("nextTrigger", effective), zap.Int("entries", len(entries)))

	//	select {
	//	case op := <-c.liveOperation:
	//		c.entriesMutex.Lock()
	//		newEntry := op(ctx)
	//		if newEntry != nil {
	//			newEntry.tick(now)
	//		}
	//		entries = []*Entry{}
	//		for _, e := range c.entries {
	//			entries = append(entries, e)
	//		}
	//		c.entriesMutex.Unlock()

	//	case now = <-time.After(effective.Sub(now)):
	//		// Run every entry whose next time was this effective time.
	//		for _, e := range entries {
	//			if e.Next != effective {
	//				break
	//			}
	//			e.Prev = e.Next
	//			e.tick(effective)

	//			go func(ctx context.Context, e *Entry, next time.Time) {
	//				if c.funcCtx != nil {
	//					ctx = c.funcCtx(ctx, e.Job)
	//				}

	//				tickLock := e.distMutexPrefix + fmt.Sprintf("%v", effective.Unix())
	//				m, err := mutexStore.Get(tickLock)
	//				if err != nil {
	//					c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to create etcd mutex for job '%v'", e.jobName))
	//					return
	//				}

	//				lockCtx, cancel := context.WithTimeout(ctx, time.Second)
	//				defer cancel()

	//				// Local mutex is needed to avoid race condition on reusing the etcd mutex object.
	//				localMutex := localMutexer.Get(tickLock)
	//				localMutex.Lock()
	//				err = m.Lock(lockCtx)
	//				localMutex.Unlock()
	//				if err == context.DeadlineExceeded {
	//					return
	//				} else if err != nil {
	//					c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to lock mutex '%v'", m.Key()))
	//					return
	//				}

	//				if e.counter != nil {
	//					// Must refresh counter since it might have been updated by another instance before.
	//					// Can only use the counter after the tick's mutex.
	//					value, err := e.counter.Refresh(ctx)
	//					if (err != nil) && (value <= 0) {
	//						// Job already exhausted the counter, will not trigger - tries to delete instead.
	//						err := updateAsDeleted(ctx, e)
	//						if err != nil {
	//							c.errorsHandler(ctx, e.Job, err)
	//						}
	//					}
	//				}

	//				if e.Job.Status == JobStatusActive && e.Job.expired(effective) {
	//					err := updateAsDeleted(ctx, e)
	//					if err != nil {
	//						c.errorsHandler(ctx, e.Job, err)
	//					}
	//					return
	//				}

	//				if e.Job.Status == JobStatusDeleted {
	//					err = jobCleanUp(ctx, e)
	//					if err != nil {
	//						c.errorsHandler(ctx, e.Job, err)
	//					}
	//					return
	//				}

	//				result, err := c.triggerFunc(ctx, TriggerRequest{
	//					JobName:  e.jobName,
	//					Metadata: e.Job.Metadata,
	//					Payload:  e.Job.Payload,
	//				})
	//				if err != nil {
	//					c.errorsHandler(ctx, e.Job, err)
	//					return
	//				}

	//				if result == Delete {
	//					err := updateAsDeleted(ctx, e)
	//					if err != nil {
	//						c.errorsHandler(ctx, e.Job, err)
	//					}
	//					return
	//				}

	//				if result == OK && e.counter != nil {
	//					// Needs to check number of triggers
	//					value, _, err := e.counter.Increment(ctx, -1)
	//					if err != nil {
	//						c.errorsHandler(ctx, e.Job, err)
	//						// No need to abort if updating the count failed.
	//						// The count solution is not transactional anyway.
	//					}

	//					if value <= 0 {
	//						err := updateAsDeleted(ctx, e)
	//						if err != nil {
	//							c.errorsHandler(ctx, e.Job, err)
	//						}
	//						return
	//					}
	//				}
	//				// Cannot unlock because it can open a chance for double trigger since two instances
	//				// can have a clock skew and compete for the lock at slight different windows.
	//				// So, we keep the lock during its ttl
	//			}(ctx, e, e.Next)
	//		}
	//		continue

	//	case <-c.snapshot:
	//		c.snapshot <- c.entrySnapshot(entries)

	//	case <-ctx.Done():
	//		c.runWaitingGroup.Done()
	//		return
	//	}
	//}
}

//// Wait the cron to stop after context is cancelled.
//func (c *Cron) Wait() {
//	c.runWaitingGroup.Wait()
//	c.jobStore.Wait()
//	c.running = false
//}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot(input []*Entry) []*Entry {
	entries := []*Entry{}
	for _, e := range input {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			//Next:     e.Next,
			//Prev: e.Prev,
			Job: e.Job,
		})
	}
	return entries
}
