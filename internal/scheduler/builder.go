/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/dapr/kit/cron"
	"github.com/dapr/kit/ptr"
	kittime "github.com/dapr/kit/time"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
)

// Builder is a builder for creating a new scheduler.
type Builder struct {
	// clock is the clock used to get the current time. Used for manipulating
	// time in tests.
	clock clock.Clock

	// parser parses job schedules.
	parser cron.Parser
}

// NewBuilder creates a new scheduler builder.
func NewBuilder() *Builder {
	return &Builder{
		clock: clock.RealClock{},
		parser: cron.NewParser(cron.Second |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
		),
	}
}

// Schedule returns the schedule based on the given stored job.
func (b *Builder) Schedule(job *stored.Job) (Interface, error) {
	if job.GetJob().Schedule == nil {
		return &oneshot{
			dueTime: job.GetDueTime().AsTime(),
		}, nil
	}

	cronSched, err := b.parser.Parse(job.GetJob().GetSchedule())
	if err != nil {
		return nil, fmt.Errorf(">>ERROR HERE: %w", err)
	}

	//nolint:protogetter
	r := &repeats{
		exp:   job.Expiration,
		cron:  cronSched,
		total: job.GetJob().Repeats,
	}

	switch t := job.GetBegin().(type) {
	case *stored.Job_Start:
		r.start = ptr.Of(t.Start.AsTime())
	case *stored.Job_DueTime:
		r.dueTime = ptr.Of(t.DueTime.AsTime())
	}

	return r, nil
}

// Parse parses a job into a stored job which contains a random partition ID.
func (b *Builder) Parse(job *api.Job) (*stored.Job, error) {
	if job.DueTime == nil && job.Schedule == nil {
		return nil, errors.New("job must have either a due time or a schedule")
	}

	if job.Schedule == nil && job.GetRepeats() > 1 {
		return nil, errors.New("job must have a schedule to repeat")
	}

	if job.Repeats != nil && job.GetRepeats() < 1 {
		return nil, errors.New("defined repeats must be greater than 0")
	}

	if job.Schedule == nil && job.Ttl != nil {
		return nil, errors.New("job must have a schedule to have a ttl")
	}

	if job.Schedule != nil {
		_, err := b.parser.Parse(job.GetSchedule())
		if err != nil {
			return nil, fmt.Errorf(">>HERE2: %w", err)
		}
	}

	//nolint:protogetter
	if job.FailurePolicy == nil {
		job.FailurePolicy = &api.FailurePolicy{
			Policy: &api.FailurePolicy_Constant{
				Constant: &api.FailurePolicyConstant{
					Interval:   durationpb.New(time.Second),
					MaxRetries: ptr.Of(uint32(3)),
				},
			},
		}
	}

	storedJob := &stored.Job{
		// PartionId has no need to be crypto random.
		//nolint:gosec
		PartitionId: rand.Uint64(),
		Job:         job,
	}

	now := b.clock.Now().UTC().Truncate(time.Second)
	begin := now

	if job.DueTime != nil {
		var err error
		begin, err = parsePointInTime(job.GetDueTime(), now)
		if err != nil {
			return nil, err
		}
		storedJob.Begin = &stored.Job_DueTime{
			DueTime: timestamppb.New(begin),
		}
	} else {
		storedJob.Begin = &stored.Job_Start{
			Start: timestamppb.New(now),
		}
	}

	if job.Ttl != nil {
		start := now
		if job.DueTime != nil {
			start = storedJob.GetDueTime().AsTime()
		}
		expiration, err := parsePointInTime(job.GetTtl(), start)
		if err != nil {
			return nil, errors.New("ttl format not recognized")
		}

		if job.DueTime != nil && begin.After(expiration) {
			return nil, errors.New("ttl must be greater than due time")
		}

		storedJob.Expiration = timestamppb.New(expiration)
	}

	return storedJob, nil
}

func parsePointInTime(str string, now time.Time) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, str)
	if err == nil {
		return t, nil
	}

	dur, err := time.ParseDuration(str)
	if err == nil {
		return now.Add(dur), nil
	}

	years, months, days, duration, repeats, err := kittime.ParseISO8601Duration(str)
	if err != nil {
		return time.Time{}, errors.New("due time format not recognized")
	}

	if repeats > 0 {
		return time.Time{}, errors.New("repeats not supported for due time")
	}

	return now.AddDate(years, months, days).Add(duration), nil
}
