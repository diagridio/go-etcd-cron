/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	etcdclient "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	anypb "google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/collector"
	"github.com/diagridio/go-etcd-cron/counting"
	"github.com/diagridio/go-etcd-cron/locking"
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
	entriesMutex           sync.RWMutex
	etcdErrorsHandler      func(context.Context, Job, error)
	errorsHandler          func(context.Context, Job, error)
	funcCtx                func(context.Context, Job) context.Context
	triggerFunc            TriggerFunction
	running                bool
	runWaitingGroup        sync.WaitGroup
	etcdclient             *etcdclient.Client
	jobStore               storage.JobStore
	organizer              partitioning.Organizer
	partitioning           partitioning.Partitioner
	collector              collector.Collector
	jobCollector           collector.Collector
	deletedJobs            sync.Map
	compressJobRecord      bool

	logger *zap.Logger
}

type TriggerRequest struct {
	JobName   string
	Timestamp time.Time
	Actual    time.Time
	Metadata  map[string]string
	Payload   *anypb.Any
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
	Job Job

	// Optimization to avoid accessing Job's object.
	// This is OK because the job's name never changes for a given entry.
	JobName string

	// Attributes below are not to be copied:

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	prev time.Time

	// Prefix for the ticker mutex
	distMutexPrefix string

	// Counter if has limit on number of triggers
	counter counting.Counter

	// Mutex to handle concurrent updates to entry
	// Locks: Job and Schedule
	mutex sync.RWMutex
}

func (e *Entry) tick(now time.Time) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	var next time.Time
	start := e.Job.StartTime.Truncate(time.Second)
	if start.After(now) {
		next = start
	} else {
		next = e.Schedule.Next(start, now)
	}

	if e.Job.expired(next) {
		// Job will expire before next trigger, so we expire in the next second.
		effectiveExpiration := e.Job.Expiration.Truncate(time.Second).Add(time.Second)
		if !effectiveExpiration.After(now) {
			// Expiration overdue, randomly pick a time in the future within 60s.
			// No need to use crypto rand.
			effectiveExpiration = now.Add(time.Second * time.Duration(rand.Intn(60)))
		}

		if effectiveExpiration.After(now) && effectiveExpiration.Before(next) {
			next = effectiveExpiration
		}
	}

	e.prev = e.next
	e.next = next
}

func (e *Entry) updateSelf(op func(entry *Entry)) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	op(e)
}

func (e *Entry) shallowCopy() *Entry {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return &Entry{
		JobName:  e.JobName,
		Job:      e.Job,
		Schedule: e.Schedule,
	}
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].next.IsZero() {
		return false
	}
	if s[j].next.IsZero() {
		return true
	}
	return s[i].next.Before(s[j].next)
}

type CronOpt func(cron *Cron)

func WithEtcdErrorsHandler(f func(context.Context, Job, error)) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.etcdErrorsHandler = f
	})
}

func WithErrorsHandler(f func(context.Context, Job, error)) CronOpt {
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

func WithFuncCtx(f func(context.Context, Job) context.Context) CronOpt {
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

func WithCompression(b bool) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.compressJobRecord = b
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
		running:           false,
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
		cron.etcdErrorsHandler = func(ctx context.Context, j Job, err error) {
			log.Printf("[etcd-cron] etcd error when handling '%v' job: %v", j.Name, err)
		}
	}
	if cron.errorsHandler == nil {
		cron.errorsHandler = func(ctx context.Context, j Job, err error) {
			log.Printf("[etcd-cron] error when handling '%v' job: %v", j.Name, err)
		}
	}
	if cron.namespace == "" {
		cron.namespace = defaultNamespace
	}
	cron.organizer = partitioning.NewOrganizer(cron.namespace, cron.partitioning)

	cron.collector = collector.New(time.Hour, time.Minute)
	cron.jobCollector = collector.New(time.Duration(1), time.Duration(1))

	if cron.logger == nil {
		logger, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
		if err != nil {
			return nil, err
		}
		cron.logger = logger.Named("diagrid-cron")
	}

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
			},
			storage.StoreOptions{
				Compress: cron.compressJobRecord,
			},
			cron.logger)
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

// DeleteJob removes a job from store, eventually removed from cron.
func (c *Cron) DeleteJob(ctx context.Context, jobName string) error {
	if c.jobStore == nil {
		return fmt.Errorf("cannot delete job: no job store configured")
	}
	return c.jobStore.Delete(ctx, jobName)
}

// FetchJob retrieves a job by name from the job store, even if not running in this instance.
func (c *Cron) FetchJob(ctx context.Context, jobName string) (*Job, error) {
	record, err := c.jobStore.Fetch(ctx, jobName)
	if err != nil {
		return nil, err
	}

	job := jobFromJobRecord(jobName, record)
	return job, nil
}

func (c *Cron) newCounterInstance(jobName string) counting.Counter {
	partitionId := c.partitioning.CalculatePartitionId(jobName)
	counterKey := c.organizer.CounterPath(partitionId, jobName)
	return counting.NewEtcdCounter(c.etcdclient, counterKey, 0)
}

func (c *Cron) onJobDeleted(ctx context.Context, jobName string) error {
	c.killJob(jobName)
	c.deletedJobs.Store(jobName, true)

	// Best effort to delete the counter (if present)
	// Jobs deleted by expiration will delete their counter first.
	// Jobs deleted manually need this logic.
	counter := c.newCounterInstance(jobName)
	err := counter.Delete(ctx)
	if err != nil {
		c.errorsHandler(ctx, Job{Name: jobName}, err)
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

// GetJob retrieves a running job by name.
func (c *Cron) GetJob(jobName string) *Job {
	_, deleted := c.deletedJobs.Load(jobName)
	if deleted {
		return nil
	}

	c.entriesMutex.RLock()
	defer c.entriesMutex.RUnlock()

	entry, ok := c.entries[jobName]
	if !ok || (entry == nil) {
		return nil
	}

	job := entry.Job
	return &job
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

	entry := &Entry{
		JobName:         job.Name,
		Schedule:        schedule,
		Job:             *job,
		distMutexPrefix: c.organizer.TicksPath(partitionId) + "/",
		counter:         counter,
	}

	c.appendOperation(func(ctx context.Context) *Entry {
		existing, exists := c.entries[entry.JobName]
		if exists {
			existing.updateSelf(func(e *Entry) {
				// Updates existing job.
				// Counter is not reused.
				e.Job = entry.Job
				e.Schedule = entry.Schedule
			})
			return existing
		} else {
			c.entries[entry.JobName] = entry
		}
		return entry
	})
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

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	c.entriesMutex.RLock()
	defer c.entriesMutex.RUnlock()
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, e.shallowCopy())
	}
	return entries
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start(ctx context.Context) error {
	err := c.jobStore.Start(ctx)
	if err != nil {
		return err
	}
	c.collector.Start(ctx)
	c.jobCollector.Start(ctx)
	c.running = true
	c.runWaitingGroup.Add(1)
	go c.run(ctx)
	return nil
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) {
	localMutexer := locking.NewMutexer(c.collector)
	mutexStore := locking.NewMutexStore(locking.NewDistributedMutexBuilderFunc(c.etcdclient), c.collector)
	// Figure out the next activation times for each entry.
	now := time.Now().Local()

	entries := []*Entry{}

	// Pending operations only matter before running, ignored afterwards.
	c.pendingOperationsMutex.Lock()
	c.entriesMutex.Lock()
	for _, op := range c.pendingOperations {
		newEntry := op(ctx)
		if newEntry != nil {
			newEntry.tick(now)
		}
	}
	for _, e := range c.entries {
		entries = append(entries, e)
	}
	c.entriesMutex.Unlock()
	c.pendingOperations = []func(context.Context) *Entry{}
	c.pendingOperationsMutex.Unlock()

	deleteJob := func(ctx context.Context, job Job) error {
		job.Status = JobStatusDeleted
		job.Metadata = nil
		job.Payload = nil
		record := job.toJobRecord()

		// Update the database with a the updated job status
		err := c.jobStore.Put(ctx, job.Name, record)
		if err != nil {
			return err
		}

		counter := c.newCounterInstance(job.Name)
		err = counter.Delete(ctx)
		if err != nil {
			return err
		}

		// Now that all other records related to the job are deleted, we can delete the job
		return c.jobStore.Delete(ctx, job.Name)
	}

	for {
		sort.Sort(byTime(entries))

		var effective time.Time
		if len(entries) == 0 || entries[0].next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = entries[0].next
		}

		c.logger.Debug("new iteration", zap.Time("nextTrigger", effective), zap.Int("entries", len(entries)))

		select {
		case op := <-c.liveOperation:
			c.entriesMutex.Lock()
			newEntry := op(ctx)
			if newEntry != nil {
				newEntry.tick(now)
			}
			entries = []*Entry{}
			for _, e := range c.entries {
				entries = append(entries, e)
			}
			c.entriesMutex.Unlock()

		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			currentWindow := time.Now().Truncate(time.Second)

			for _, e := range entries {
				if e.next != effective {
					break
				}
				e.prev = e.next

				e.tick(effective)

				// Copy since we don't need to reference
				job := e.Job

				req := TriggerRequest{
					JobName:   job.Name,
					Timestamp: effective,
					Actual:    time.Now(),
					Metadata:  job.Metadata,
					Payload:   job.Payload,
				}

				deleteJobCallback := func(ctx context.Context) error {
					return deleteJob(ctx, job)
				}

				if job.Status == JobStatusDeleted || e.Job.expired(effective) {
					_, deleted := c.deletedJobs.Load(job.Name)
					if !deleted {
						// No cron hard-deleted it yet from the db, still just a soft delete.
						c.deletedJobs.Store(job.Name, true)
						c.jobCollector.Add(deleteJobCallback)
						// Won't trigger.
						continue
					}
				}

				// If it is an old trigger (not in the same second window), we skip.
				// Delays of the trigger function should not delay this step.
				if currentWindow != effective {
					// Skip iteration for an old time window (truncated to second).
					// We still need to continue with all entries to make sure they
					// all get updated with the next tick (catch up).
					continue
				}

				tickLock := e.distMutexPrefix + fmt.Sprintf("%v", effective.Unix())
				go func(ctx context.Context, lockKey string, counter counting.Counter, job Job, req TriggerRequest) {
					_, deleted := c.deletedJobs.Load(req.JobName)
					if deleted {
						return
					}

					effective := req.Timestamp
					if c.funcCtx != nil {
						ctx = c.funcCtx(ctx, job)
					}

					m, err := mutexStore.Get(lockKey)
					if err != nil {
						c.etcdErrorsHandler(ctx, job, errors.Wrapf(err, "fail to create etcd mutex for job '%v'", job.Name))
						return
					}

					lockCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()

					// Local mutex is needed to avoid race condition on reusing the etcd mutex object.
					localMutex := localMutexer.Get(lockKey)
					localMutex.Lock()
					err = m.Lock(lockCtx)
					localMutex.Unlock()
					if err == context.DeadlineExceeded {
						return
					} else if err != nil {
						c.etcdErrorsHandler(ctx, job, errors.Wrapf(err, "fail to lock mutex '%v'", m.Key()))
						return
					}

					if job.Status == JobStatusActive && job.expired(effective) {
						c.deletedJobs.Store(job.Name, true)
						c.jobCollector.Add(deleteJobCallback)
						return
					}

					// Must reload counter since it might have been updated by another instance before.
					// Can only use the counter after the tick's mutex.
					if counter != nil {
						value, err := counter.Refresh(ctx)
						if (err == nil) && (value <= 0) {
							c.deletedJobs.Store(job.Name, true)
							c.jobCollector.Add(deleteJobCallback)
							return
						}
					}

					result, err := c.triggerFunc(ctx, req)
					if err != nil {
						c.errorsHandler(ctx, job, err)
						return
					}

					if result == Delete {
						c.deletedJobs.Store(job.Name, true)
						c.jobCollector.Add(deleteJobCallback)
						return
					}

					if result == OK && counter != nil {
						// Needs to check number of triggers
						value, _, err := counter.Increment(ctx, -1)
						if err != nil {
							c.errorsHandler(ctx, job, err)
							// No need to abort if updating the count failed.
							// The count solution is not transactional anyway.
						}

						if value <= 0 {
							c.deletedJobs.Store(job.Name, true)
							c.jobCollector.Add(deleteJobCallback)
							return
						}
					}
					// Cannot unlock because it can open a chance for double trigger since two instances
					// can have a clock skew and compete for the lock at slight different windows.
					// So, we keep the lock during its ttl
				}(ctx, tickLock, e.counter, job, req)
			}
			continue

		case <-ctx.Done():
			c.runWaitingGroup.Done()
			return
		}
	}
}

// Wait the cron to stop after context is cancelled.
func (c *Cron) Wait() {
	c.runWaitingGroup.Wait()
	c.jobStore.Wait()
	c.collector.Wait()
	c.jobCollector.Wait()
	c.running = false
}
