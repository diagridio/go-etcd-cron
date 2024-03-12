/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	etcdclient "go.etcd.io/etcd/client/v3"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

const (
	defaultEtcdEndpoint = "127.0.0.1:2379"
	defaultNamespace    = "etcd_cron"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	namespace         string
	pendingOperations []func(context.Context) *Entry
	operationsMutex   sync.RWMutex
	entries           map[string]*Entry
	stop              chan struct{}
	cancel            context.CancelFunc
	snapshot          chan []*Entry
	etcdErrorsHandler func(context.Context, Job, error)
	errorsHandler     func(context.Context, Job, error)
	funcCtx           func(context.Context, Job) context.Context
	triggerFunc       func(context.Context, string, *anypb.Any) error
	running           bool
	runWaitingGroup   sync.WaitGroup
	etcdclient        EtcdMutexBuilder
	jobStore          JobStore
	organizer         Organizer
	partitioning      Partitioning
}

// Job contains 3 mandatory options to define a job
type Job struct {
	// Name of the job
	Name string
	// Cron-formatted rhythm (ie. 0,10,30 1-5 0 * * *)
	Rhythm string
	// The type of trigger
	Type string
	// The payload containg all the information for the trigger
	Payload *anypb.Any
	// Optional number of seconds until this job expires (if > 0)
	TTL int64
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job o run.
	Job Job

	// Prefix for the ticker mutex
	distMutexPrefix string
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
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
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

func WithEtcdMutexBuilder(b EtcdMutexBuilder) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.etcdclient = b
	})
}

func WithJobStore(s JobStore) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.jobStore = s
	})
}

func WithFuncCtx(f func(context.Context, Job) context.Context) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.funcCtx = f
	})
}

func WithTriggerFunc(f func(context.Context, string, *anypb.Any) error) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.triggerFunc = f
	})
}

func WithNamespace(n string) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.namespace = n
	})
}

func WithPartitioning(p Partitioning) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.partitioning = p
	})
}

// New returns a new Cron job runner.
func New(opts ...CronOpt) (*Cron, error) {
	cron := &Cron{
		pendingOperations: []func(context.Context) *Entry{},
		entries:           map[string]*Entry{},
		stop:              make(chan struct{}),
		snapshot:          make(chan []*Entry),
		running:           false,
	}
	for _, opt := range opts {
		opt(cron)
	}
	if cron.partitioning == nil {
		cron.partitioning = NoPartitioning()
	}
	if cron.etcdclient == nil {
		etcdClient, err := NewEtcdMutexBuilder(etcdclient.Config{
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
	cron.organizer = NewOrganizer(cron.namespace, cron.partitioning)
	if cron.jobStore == nil {
		cron.jobStore = cron.etcdclient.NewJobStore(
			cron.organizer,
			cron.partitioning,
			func(ctx context.Context, j Job) error {
				return cron.scheduleJob(j)
			},
			func(ctx context.Context, s string) error {
				cron.killJob(s)
				return nil
			})
	}
	return cron, nil
}

// AddJob adds a Job.
func (c *Cron) AddJob(ctx context.Context, job Job) error {
	if c.jobStore == nil {
		return fmt.Errorf("cannot persist job: no job store configured")
	}
	return c.jobStore.Put(ctx, job)
}

// DeleteJob removes a job.
func (c *Cron) DeleteJob(ctx context.Context, jobName string) error {
	if c.jobStore == nil {
		return fmt.Errorf("cannot delete job: no job store configured")
	}
	return c.jobStore.Delete(ctx, jobName)
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

// GetJob retrieves a job by name.
func (c *Cron) GetJob(jobName string) *Job {
	c.operationsMutex.RLock()
	defer c.operationsMutex.RUnlock()

	entry, ok := c.entries[jobName]
	if !ok || (entry == nil) {
		return nil
	}

	return &entry.Job
}

func (c *Cron) ListJobsByPrefix(prefix string) []*Job {
	var appJobs []*Job
	c.operationsMutex.RLock()
	for _, entry := range c.entries {
		if strings.HasPrefix(entry.Job.Name, prefix) {
			// Job belongs to the specified app_id
			appJobs = append(appJobs, &entry.Job)
		}
	}
	c.operationsMutex.RUnlock()
	return appJobs
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) scheduleJob(job Job) error {
	s, err := Parse(job.Rhythm)
	if err != nil {
		return err
	}

	return c.schedule(s, job)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) schedule(schedule Schedule, job Job) error {
	partitionId := c.partitioning.CalculatePartitionId(job.Name)
	if !c.partitioning.CheckPartitionLeader(partitionId) {
		// It means the partitioning changed and persisted jobs are in the wrong partition now.
		return fmt.Errorf("host does not own partition %d", partitionId)
	}

	entry := &Entry{
		Schedule:        schedule,
		Job:             job,
		Prev:            time.Unix(0, 0),
		distMutexPrefix: c.organizer.TicksPath(partitionId) + "/",
	}

	c.appendOperation(func(ctx context.Context) *Entry {
		c.entries[entry.Job.Name] = entry
		return entry
	})
	return nil
}

func (c *Cron) appendOperation(op func(ctx context.Context) *Entry) {
	c.operationsMutex.Lock()
	defer c.operationsMutex.Unlock()

	c.pendingOperations = append(c.pendingOperations, op)
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}

	c.operationsMutex.RLock()
	defer c.operationsMutex.RUnlock()
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start(ctx context.Context) error {
	c.running = true
	ctxWithCancel, cancel := context.WithCancel(ctx)
	err := c.jobStore.Init(ctxWithCancel)
	if err != nil {
		return err
	}
	c.cancel = cancel
	c.runWaitingGroup.Add(1)
	go c.run(ctxWithCancel)
	return nil
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) {
	mutexStore := NewMutexStore(c.etcdclient)
	// Figure out the next activation times for each entry.
	now := time.Now().Local()

	changed := make(chan bool)
	entries := []*Entry{}

	c.runWaitingGroup.Add(1)
	go func(ctx context.Context) {
		for {
			hasPendingOperations := false
			c.operationsMutex.RLock()
			hasPendingOperations = len(c.pendingOperations) > 0
			c.operationsMutex.RUnlock()

			if hasPendingOperations {
				changed <- true
			}

			select {
			case <-ctx.Done():
				c.runWaitingGroup.Done()
				return
			case <-time.After(time.Second):
			}
		}
	}(ctx)

	for {
		sort.Sort(byTime(entries))

		var effective time.Time
		if len(entries) == 0 || entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = entries[0].Next
		}

		select {
		case <-changed:
			c.operationsMutex.Lock()
			for _, op := range c.pendingOperations {
				newEntry := op(ctx)
				if newEntry != nil {
					newEntry.Next = newEntry.Schedule.Next(now)
				}
			}
			c.pendingOperations = []func(context.Context) *Entry{}
			entries = []*Entry{}
			for _, e := range c.entries {
				entries = append(entries, &Entry{
					Schedule: e.Schedule,
					Next:     e.Next,
					Prev:     e.Prev,
					Job:      e.Job,
				})
			}
			c.operationsMutex.Unlock()

		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for _, e := range entries {
				if e.Next != effective {
					break
				}
				e.Prev = e.Next
				e.Next = e.Schedule.Next(effective)

				go func(ctx context.Context, e *Entry) {
					defer func() {
						r := recover()
						if r != nil {
							err, ok := r.(error)
							if !ok {
								err = fmt.Errorf("%v", r)
							}
							err = fmt.Errorf("panic: %v, stacktrace: %s", err, string(debug.Stack()))
							go c.errorsHandler(ctx, e.Job, err)
						}
					}()

					if c.funcCtx != nil {
						ctx = c.funcCtx(ctx, e.Job)
					}

					tickLock := e.distMutexPrefix + fmt.Sprintf("%v", effective.Unix())
					m, err := mutexStore.Get(tickLock)
					if err != nil {
						go c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to create etcd mutex for job '%v'", e.Job.Name))
						return
					}
					lockCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()

					err = m.Lock(lockCtx)
					if err == context.DeadlineExceeded {
						return
					} else if err != nil {
						go c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to lock mutex '%v'", m.Key()))
						return
					}

					err = c.triggerFunc(ctx, e.Job.Type, e.Job.Payload)
					if err != nil {
						go c.errorsHandler(ctx, e.Job, err)
						return
					}
					// Cannot unlock because it can open a chance for double trigger since two instances
					// can have a clock skew and compete for the lock at slight different windows.
					// So, we keep the lock during its ttl
				}(ctx, e)
			}
			continue

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot(entries)

		case <-c.stop:
			c.runWaitingGroup.Done()
			return
		}
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.stop <- struct{}{}
	c.cancel()
	c.running = false
	c.runWaitingGroup.Wait()
	c.jobStore.Wait()
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot(input []*Entry) []*Entry {
	entries := []*Entry{}
	for _, e := range input {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}
