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
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
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
	Client client.Interface

	// Schedule is the schedule of this job.
	Schedule scheduler.Interface

	// Job is the job to count.
	Job *stored.Job

	// Yard is a graveyard for signalling that a job has just been deleted and
	// therefore it's Delete informer event should be ignored.
	Yard *grave.Yard

	// Collector is a garbage collector for pushing counter keys that are no
	// longer needed, and should be deleted at some point.
	Collector garbage.Interface
}

// Counter is a counter, tracking state of a scheduled job as it is triggered
// over time. Returns, if necessary, the time the job should be triggered next.
// Counter handles the deletion of the associated job if it has expired and
// adding the counter object to the garbage collector.
type Counter struct {
	jobKey         string
	counterKey     string
	client         client.Interface
	schedule       scheduler.Interface
	yard           *grave.Yard
	collector      garbage.Interface
	job            *stored.Job
	count          *stored.Counter
	next           time.Time
	triggerRequest *api.TriggerRequest
}

func New(ctx context.Context, opts Options) (*Counter, bool, error) {
	counterKey := opts.Key.CounterKey(opts.Name)
	jobKey := opts.Key.JobKey(opts.Name)

	// Pop the counter key from the garbage collector as we are going to use it.
	opts.Collector.Pop(counterKey)

	// Get the existing counter, if it exists.
	res, err := opts.Client.Get(ctx, counterKey)
	if err != nil {
		return nil, false, err
	}

	if res.Count == 0 {
		c := &Counter{
			jobKey:     jobKey,
			counterKey: counterKey,
			client:     opts.Client,
			schedule:   opts.Schedule,
			job:        opts.Job,
			count:      &stored.Counter{JobPartitionId: opts.Job.GetPartitionId()},
			yard:       opts.Yard,
			collector:  opts.Collector,
			triggerRequest: &api.TriggerRequest{
				Name:     opts.Name,
				Metadata: opts.Job.GetJob().GetMetadata(),
				Payload:  opts.Job.GetJob().GetPayload(),
			},
		}

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
		b, err := proto.Marshal(count)
		if err != nil {
			return nil, false, err
		}
		if _, err := opts.Client.Put(ctx, counterKey, string(b)); err != nil {
			return nil, false, err
		}
	}

	c := &Counter{
		counterKey: counterKey,
		jobKey:     jobKey,
		client:     opts.Client,
		schedule:   opts.Schedule,
		job:        opts.Job,
		count:      count,
		yard:       opts.Yard,
		collector:  opts.Collector,
		triggerRequest: &api.TriggerRequest{
			Name:     opts.Name,
			Metadata: opts.Job.GetJob().GetMetadata(),
			Payload:  opts.Job.GetJob().GetPayload(),
		},
	}

	if ok, err := c.tickNext(); err != nil || !ok {
		return nil, false, err
	}

	return c, true, nil
}

// ScheduledTime is the time at which the job is scheduled to be triggered
// next. Implements the kit events queueable item.
func (c *Counter) ScheduledTime() time.Time {
	return c.next
}

// Key returns the name of the job. Implements the kit events queueable item.
func (c *Counter) Key() string {
	return c.jobKey
}

// TriggerRequest is the trigger request representation for the job.
func (c *Counter) TriggerRequest() *api.TriggerRequest {
	return c.triggerRequest
}

// TriggerSuccess updates the counter state given what the next trigger time
// was. Returns true if the job will be triggered again.
func (c *Counter) TriggerSuccess(ctx context.Context) (bool, error) {
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

	b, err := proto.Marshal(c.count)
	if err != nil {
		return false, err
	}

	// Update the counter in etcd and return the next trigger time.
	_, err = c.client.Put(ctx, c.counterKey, string(b))
	return true, err
}

// TriggerFailed is called when trigging the job has been marked as failed from
// the consumer. The counter is persisted at every attempt to ensure the number
// of attempts are durable.
// Returns true if the job failure policy indicates that the job should be
// tried again. Returns false if the job should not be attempted again and was
// deleted.
func (c *Counter) TriggerFailed(ctx context.Context) (bool, error) {
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

	b, err := proto.Marshal(c.count)
	if err != nil {
		return true, err
	}

	// Update the counter in etcd and return the next trigger time.
	_, err = c.client.Put(ctx, c.counterKey, string(b))
	return true, err
}

// policyTryAgain returns true if the failure policy indicates this job should
// be tried again at this tick.
func (c *Counter) policyTryAgain() bool {
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
func (c *Counter) tickNext() (bool, error) {
	if c.updateNext() {
		return true, nil
	}

	if err := c.client.DeleteMulti(c.jobKey); err != nil {
		return false, err
	}
	// Mark the job as just been deleted, and push the counter key for garbage
	// collection.
	c.yard.Deleted(c.jobKey)
	c.collector.Push(c.counterKey)
	return false, nil
}

// updateNext updates the counter's next trigger time.
// Returns false if the job and counter should be deleted because it has
// expired.
func (c *Counter) updateNext() bool {
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
