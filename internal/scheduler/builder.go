/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"errors"
	"math/rand"
	"time"

	"github.com/dapr/kit/cron"
	"github.com/dapr/kit/ptr"
	kittime "github.com/dapr/kit/time"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/api"
)

// Builder is a builder for creating a new scheduler.
type Builder struct {
	// clock is the clock used to get the current time. Used for manipulating
	// time in tests.
	clock clock.Clock
}

// NewBuilder creates a new scheduler builder.
func NewBuilder() *Builder {
	return &Builder{
		clock: clock.RealClock{},
	}
}

// Scheduler returns the scheduler based on the given stored job.
func (b *Builder) Scheduler(job *api.JobStored) (Interface, error) {
	if job.GetJob().Schedule == nil {
		return &oneshot{
			dueTime: job.GetDueTime().AsTime(),
		}, nil
	}

	cronSched, err := cron.ParseStandard(job.GetJob().GetSchedule())
	if err != nil {
		return nil, err
	}

	//nolint:protogetter
	r := &repeats{
		exp:   job.Expiration,
		cron:  cronSched,
		total: job.GetJob().Repeats,
	}

	switch t := job.GetBegin().(type) {
	case *api.JobStored_Start:
		r.start = ptr.Of(t.Start.AsTime())
	case *api.JobStored_DueTime:
		r.dueTime = ptr.Of(t.DueTime.AsTime())
	}

	return r, nil
}

// Parse parses a job into a stored job which contains a random UUID.
func (b *Builder) Parse(job *api.Job) (*api.JobStored, error) {
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
		_, err := cron.ParseStandard(job.GetSchedule())
		if err != nil {
			return nil, err
		}
	}

	//nolint:gosec
	stored := &api.JobStored{
		Uuid: rand.Uint32(),
		Job:  job,
	}

	now := b.clock.Now().UTC().Truncate(time.Second)
	begin := now

	if job.DueTime != nil {
		var err error
		begin, err = parsePointInTime(job.GetDueTime(), now)
		if err != nil {
			return nil, err
		}
		stored.Begin = &api.JobStored_DueTime{
			DueTime: timestamppb.New(begin),
		}
	} else {
		stored.Begin = &api.JobStored_Start{
			Start: timestamppb.New(now),
		}
	}

	if job.Ttl != nil {
		start := now
		if job.DueTime != nil {
			start = stored.GetDueTime().AsTime()
		}
		expiration, err := parsePointInTime(job.GetTtl(), start)
		if err != nil {
			return nil, errors.New("ttl format not recognized")
		}

		if job.DueTime != nil && begin.After(expiration) {
			return nil, errors.New("ttl must be greater than due time")
		}

		stored.Expiration = timestamppb.New(expiration)
	}

	return stored, nil
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
