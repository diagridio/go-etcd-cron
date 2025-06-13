/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counter

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

// Options are the options for creating a new counter.
type Options struct {
	// Name is the name of the job to count.
	Name string

	// Key handles the key to use writing to etcd.
	Key *key.Key

	// Client is the etcd client to use.
	Client clientapi.Interface

	// Schedule is the schedule of this job.
	Schedule scheduler.Interface

	// Job is the job to count.
	Job *stored.Job

	// JobModRevision is the mod revision of the job which is shared by this
	// counter.
	JobModRevision int64
}

// Interface is a counter, tracking state of a scheduled job as it is triggered
// over time. Returns, if necessary, the time the job should be triggered next.
// Counter handles the deletion of the associated job if it has expired and
// adding the counter object to the garbage collector.
type Interface interface {
	ScheduledTime() time.Time
	Key() string
	JobName() string
	Job() *stored.Job
	TriggerRequest() *api.TriggerRequest
	TriggerSuccess(ctx context.Context) (bool, error)
	TriggerFailed(ctx context.Context) (bool, error)
}

// counter is the implementation of the counter interface.
type counter struct {
	name           string
	jobKey         string
	counterKey     string
	client         clientapi.Interface
	schedule       scheduler.Interface
	job            *stored.Job
	count          *stored.Counter
	next           time.Time
	triggerRequest *api.TriggerRequest
	modRevision    int64
}

func New(ctx context.Context, opts Options) (Interface, bool, error) {
	counterKey := opts.Key.CounterKey(opts.Name)
	jobKey := opts.Key.JobKey(opts.Name)

	// Get the existing counter, if it exists.
	res, err := opts.Client.Get(ctx, counterKey)
	if err != nil {
		panic(err)
		return nil, false, err
	}

	c := &counter{
		name:        opts.Name,
		counterKey:  counterKey,
		jobKey:      jobKey,
		client:      opts.Client,
		schedule:    opts.Schedule,
		job:         opts.Job,
		modRevision: opts.JobModRevision,
		triggerRequest: &api.TriggerRequest{
			Name:     opts.Name,
			Metadata: opts.Job.GetJob().GetMetadata(),
			Payload:  opts.Job.GetJob().GetPayload(),
		},
	}

	if res.Count == 0 {
		c.count = &stored.Counter{JobPartitionId: opts.Job.GetPartitionId()}

		if ok, err := c.tickNext(); err != nil || !ok {
			return nil, false, err
		}

		return c, true, nil
	}

	count := new(stored.Counter)
	if err := proto.Unmarshal(res.Kvs[0].Value, count); err != nil {
		return nil, false, err
	}

	// If the job partition ID is the same, recover the counter state, else we
	// start again.
	if count.GetJobPartitionId() != opts.Job.GetPartitionId() {
		count = &stored.Counter{JobPartitionId: opts.Job.GetPartitionId()}
		if ok, err := c.put(ctx, count); err != nil || !ok {
			return nil, ok, err
		}
	}

	c.count = count

	if ok, err := c.tickNext(); err != nil || !ok {
		return nil, false, err
	}

	return c, true, nil
}

// ScheduledTime is the time at which the job is scheduled to be triggered
// next. Implements the kit events queueable item.
func (c *counter) ScheduledTime() time.Time {
	return c.next
}

// Key returns the Etcd key of the job. Implements the kit events queueable
// item.
func (c *counter) Key() string {
	return c.jobKey
}

// JobName returns the consumer name of the job.
func (c *counter) JobName() string {
	return c.name
}

// TriggerRequest is the trigger request representation for the job.
func (c *counter) TriggerRequest() *api.TriggerRequest {
	return c.triggerRequest
}

// Job returns the job representation for the job.
func (c *counter) Job() *stored.Job {
	return c.job
}

// TriggerSuccess updates the counter state given what the next trigger time
// was. Returns true if the job will be triggered again.
func (c *counter) TriggerSuccess(ctx context.Context) (bool, error) {
	// Update the last trigger time as the next trigger time, and increment the
	// counter.
	// Set attempts to 0 as this trigger was successful.
	//nolint:protogetter
	if lt := c.schedule.Next(c.count.GetCount(), c.count.LastTrigger); lt != nil {
		c.count.LastTrigger = timestamppb.New(*lt)
	}
	c.count.Count++
	c.count.Attempts = 0

	if ok, err := c.tickNext(); err != nil || !ok {
		return false, err
	}

	// Update the counter in etcd and return the next trigger time.
	return c.put(ctx, c.count)
}

// TriggerFailed is called when trigging the job has been marked as failed from
// the consumer. The counter is persisted at every attempt to ensure the number
// of attempts are durable.
// Returns true if the job failure policy indicates that the job should be
// tried again. Returns false if the job should not be attempted again and was
// deleted.
func (c *counter) TriggerFailed(ctx context.Context) (bool, error) {
	// Increment the attempts counter as this count tick failed.
	c.count.Attempts++

	// If the failure policy indicates that this tick should not be tried again,
	// we set the attempts to 0 and move to the next tick.
	if !c.policyTryAgain() {
		c.count.Count++
		c.count.Attempts = 0
		if ok, err := c.tickNext(); err != nil || !ok {
			return false, err
		}
	}

	// Update the counter in etcd and return the next trigger time.
	return c.put(ctx, c.count)
}

// policyTryAgain returns true if the failure policy indicates this job should
// be tried again at this tick.
func (c *counter) policyTryAgain() bool {
	fp := c.job.GetJob().GetFailurePolicy()
	if fp == nil {
		c.count.LastTrigger = timestamppb.New(c.next)
		return false
	}

	//nolint:protogetter
	switch p := fp.Policy.(type) {
	case *api.FailurePolicy_Drop:
		c.count.LastTrigger = timestamppb.New(c.next)
		return false
	case *api.FailurePolicy_Constant:
		// Attempts need to be MaxRetries+1 for this counter tick to be dropped.
		//nolint:protogetter
		tryAgain := p.Constant.MaxRetries == nil || *p.Constant.MaxRetries >= c.count.Attempts
		if tryAgain {
			c.next = c.next.Add(p.Constant.GetInterval().AsDuration())
		} else {
			// We set the LastTrigger to the first attempt to ensure consistency of
			// the Job schedule, regardless of the failure policy cadence and
			// attempts.
			//nolint:protogetter
			if lt := c.schedule.Next(c.count.GetCount(), c.count.LastTrigger); lt != nil {
				c.count.LastTrigger = timestamppb.New(*lt)
			}
		}
		return tryAgain
	default:
		c.count.LastTrigger = timestamppb.New(c.next)
		return false
	}
}

// tickNext updates the next trigger time, and deletes the counter record if
// needed.
func (c *counter) tickNext() (bool, error) {
	if c.updateNext() {
		return true, nil
	}

	// Delete the job and counter keys.
	// If the Job have been updated, then we leave the job alone to preserve it.
	// Always attempt to delete the counter key.
	return false, c.client.DeleteBothIfOtherHasRevision(context.Background(),
		clientapi.DeleteBothIfOtherHasRevisionOpts{
			Key:           c.counterKey,
			OtherKey:      c.jobKey,
			OtherRevision: c.modRevision,
		},
	)
}

// updateNext updates the counter's next trigger time.
// Returns false if the job and counter should be deleted because it has
// expired.
func (c *counter) updateNext() bool {
	// If job completed repeats, delete the counter.
	if c.job.GetJob().Repeats != nil && (c.count.GetCount() >= c.job.GetJob().GetRepeats()) {
		return false
	}

	// If the job will never trigger again, delete the counter.
	//nolint:protogetter
	next := c.schedule.Next(c.count.GetCount(), c.count.LastTrigger)
	if next == nil {
		return false
	}

	// If the job has an expiration, delete the counter if the next trigger is
	// after the expiration.
	//nolint:protogetter
	if c.job.Expiration != nil && (next.After(c.job.GetExpiration().AsTime())) {
		return false
	}

	c.next = *next

	return true
}

// put will attempt to commit the count to the database. If the target Job has
// been changed, then we don't commit the changes and signal this counter is no
// longer active. The next Job counter handler will pick up the counter key,
// and correctly overwrite (or delete) the counter key.
func (c *counter) put(ctx context.Context, count *stored.Counter) (bool, error) {
	b, err := proto.Marshal(count)
	if err != nil {
		return false, err
	}

	return c.client.PutIfOtherHasRevision(ctx, clientapi.PutIfOtherHasRevisionOpts{
		Key:           c.counterKey,
		Val:           string(b),
		OtherKey:      c.jobKey,
		OtherRevision: c.modRevision,
	})
}
