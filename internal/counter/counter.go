/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counter

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
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
	Client clientv3.KV

	// Schedule is the schedule of this job.
	Schedule scheduler.Interface

	// Job is the job to count.
	Job *api.JobStored

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
	client         clientv3.KV
	schedule       scheduler.Interface
	yard           *grave.Yard
	collector      garbage.Interface
	job            *api.JobStored
	count          *api.Counter
	next           time.Time
	triggerRequest *api.TriggerRequest
}

//nolint:contextcheck
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
			count:      &api.Counter{JobUuid: opts.Job.GetUuid()},
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

	count := new(api.Counter)
	if err := proto.Unmarshal(res.Kvs[0].Value, count); err != nil {
		return nil, false, err
	}

	// If the job UUID is the same, recover the counter state, else we start
	// again.
	if count.GetJobUuid() != opts.Job.GetUuid() {
		count = &api.Counter{JobUuid: opts.Job.GetUuid()}
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

// Trigger updates the counter state given what the next trigger time was.
// Returns true if the job will be triggered again.
//
//nolint:contextcheck
func (c *Counter) Trigger(ctx context.Context) (bool, error) {
	// Increment the counter and update the last trigger time as the next trigger
	// time.
	c.count.Count++
	c.count.LastTrigger = timestamppb.New(c.next)
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

// tickNext updates the next trigger time, and deletes the counter record if
// needed.
//
//nonlint:contextcheck
func (c *Counter) tickNext() (bool, error) {
	if !c.updateNext() {
		if err := client.Delete(c.client, c.jobKey); err != nil {
			return false, err
		}
		// Mark the job as just been deleted, and push the counter key for garbage
		// collection.
		c.yard.Deleted(c.jobKey)
		c.collector.Push(c.counterKey)
		return false, nil
	}

	return true, nil
}

// updateNext updates the counter's next trigger time.
// returns false if the job and counter should be deleted because it has
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
