package etcdcron

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	etcdclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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
	entries           []*Entry
	entriesMutex      sync.RWMutex
	stop              chan struct{}
	add               chan *Entry
	snapshot          chan []*Entry
	etcdErrorsHandler func(context.Context, Job, error)
	errorsHandler     func(context.Context, Job, error)
	funcCtx           func(context.Context, Job) context.Context
	running           bool
	runWaitingGroup   sync.WaitGroup
	etcdclient        EtcdMutexBuilder
	partitioning      *Partitioning
	partitionMutexMap sync.Map
}

// Job contains 3 mandatory options to define a job
type Job struct {
	// Name of the job
	Name string
	// Cron-formatted rhythm (ie. 0,10,30 1-5 0 * * *)
	Rhythm string
	// Routine method
	Func func(context.Context) error

	Repeats  int32
	DueTime  string
	TTL      string
	Data     *anypb.Any
	Metadata map[string]string
}

func (j Job) Run(ctx context.Context) error {
	return j.Func(ctx)
}

var (
	nonAlphaNumerical = regexp.MustCompile("[^a-z0-9_]")
)

func (j Job) canonicalName() string {
	return strcase.ToSnake(
		nonAlphaNumerical.ReplaceAllString(
			strings.ToLower(j.Name),
			"_",
		),
	)
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

	// Mutex to hold ownership for this job.
	distMutex DistributedMutex
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

func WithFuncCtx(f func(context.Context, Job) context.Context) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.funcCtx = f
	})
}

func WithNamespace(n string) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.namespace = n
	})
}

func WithPartitioning(p *Partitioning) CronOpt {
	return CronOpt(func(cron *Cron) {
		cron.partitioning = p
	})
}

// New returns a new Cron job runner.
func New(opts ...CronOpt) (*Cron, error) {
	cron := &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		stop:     make(chan struct{}),
		snapshot: make(chan []*Entry),
		running:  false,
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
	return cron, nil
}

// AddFunc adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(job Job) error {
	schedule, err := Parse(job.Rhythm)
	if err != nil {
		return err
	}
	return c.Schedule(schedule, job)
}

// GetJob retrieves a job by name.
func (c *Cron) GetJob(jobName string) *Job {
	c.entriesMutex.RLock()
	defer c.entriesMutex.RUnlock()

	for _, entry := range c.entries {
		if entry.Job.Name == jobName {
			return &entry.Job
		}
	}
	return nil
}

// DeleteJob deletes a job by name.
func (c *Cron) DeleteJob(jobName string) error {
	c.entriesMutex.Lock()
	defer c.entriesMutex.Unlock()

	var updatedEntries []*Entry
	found := false
	for _, entry := range c.entries {
		if entry.Job.Name == jobName {
			found = true
			continue
		}
		// Keep the entries that don't match the specified jobName
		updatedEntries = append(updatedEntries, entry)
	}
	if !found {
		return fmt.Errorf("job not found: %s", jobName)
	}
	c.entries = updatedEntries
	return nil
}

func (c *Cron) ListJobsByPrefix(prefix string) []*Job {
	c.entriesMutex.RLock()
	defer c.entriesMutex.RUnlock()

	var appJobs []*Job
	for _, entry := range c.entries {
		if strings.HasPrefix(entry.Job.Name, prefix) {
			// Job belongs to the specified app_id
			appJobs = append(appJobs, &entry.Job)
		}
	}
	return appJobs
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, job Job) error {
	partitionId := c.partitioning.CalculatePartitionId(job.canonicalName())
	if !c.partitioning.CheckPartitionLeader(partitionId) {
		return fmt.Errorf("host does not own partition %d", partitionId)
	}
	mutexName := fmt.Sprintf("%s/partition/%d", c.namespace, partitionId)
	distMutexAny, ok := c.partitionMutexMap.Load(mutexName)
	if !ok {
		distMutex, err := c.etcdclient.NewMutex(mutexName)
		if err != nil {
			err := errors.Wrapf(err, "fail to create etcd mutex '%v'", mutexName)
			go c.etcdErrorsHandler(context.Background(), job, err)
			return err
		}
		// TODO: make mutex comparable and use CompareAndSwap.
		c.partitionMutexMap.Store(mutexName, distMutex)
		distMutexAny = distMutex
	}

	entry := &Entry{
		Schedule:  schedule,
		Job:       job,
		distMutex: distMutexAny.(DistributedMutex),
	}
	if !c.running {
		c.entriesMutex.Lock()
		c.entries = append(c.entries, entry)
		return nil
	}

	c.add <- entry
	return nil
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.entriesMutex.RLock()
		defer c.entriesMutex.RUnlock()

		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start(ctx context.Context) {
	c.running = true
	go c.run(ctx)
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) {
	c.runWaitingGroup.Add(1)
	// Figure out the next activation times for each entry.
	now := time.Now().Local()

	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var effective time.Time
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = c.entries[0].Next
		}

		select {
		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for _, e := range c.entries {
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

					lockCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()

					err := e.distMutex.TryLock(lockCtx)
					if err == context.DeadlineExceeded || err == concurrency.ErrLocked {
						return
					} else if err != nil {
						go c.etcdErrorsHandler(ctx, e.Job, errors.Wrapf(err, "fail to lock mutex '%v'", e.distMutex.Key()))
						return
					}

					err = e.Job.Run(ctx)
					if err != nil {
						go c.errorsHandler(ctx, e.Job, err)
						return
					}
					// Cannot unlock because it can open a chance for double trigger since two instances
					// can have a clock skew and compete for the lock at slight different windows.
					// So, we keep to keep the lock over and over.
				}(ctx, e)
			}
			continue

		case newEntry := <-c.add:
			c.entries = append(c.entries, newEntry)
			newEntry.Next = newEntry.Schedule.Next(now)

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			log.Printf("[etcd-cron] unlocking %d jobs ...", len(c.entries))
			for _, entry := range c.entries {
				err := entry.distMutex.Unlock(context.Background())
				if err != nil {
					log.Printf("[etcd-cron] error when trying to unlock '%v' job: %v", entry.Job.Name, err)
				}
			}
			log.Printf("[etcd-cron] successfully unlocked %d jobs.", len(c.entries))
			c.runWaitingGroup.Done()
			return
		}

		// 'now' should be updated after newEntry and snapshot cases.
		now = time.Now().Local()
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.stop <- struct{}{}
	c.running = false
	c.runWaitingGroup.Wait()
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	c.entriesMutex.RLock()
	defer c.entriesMutex.RUnlock()

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
