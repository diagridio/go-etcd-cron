/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"time"

	"github.com/dapr/kit/cron"
	"github.com/dapr/kit/ptr"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// repeats is a schedule which repeats at some interval.
type repeats struct {
	// start is the epoch time of the job whereby the clock starts on the
	// schedule. The job _will not_ trigger at this time.
	// Will not be set with dueTime.
	start *time.Time

	// dueTime is the epoch time of the job whereby the clock starts on the
	// schedule. The job _will_ trigger at this time.
	// Will not be set with start.
	dueTime *time.Time

	// exp is the optional time at which the schedule ends.
	exp *timestamppb.Timestamp

	// cron is the cron schedule.
	cron cron.Schedule

	// total is the optional total number of times the schedule should repeat.
	total *uint32
}

func (r *repeats) Next(count uint32, last *timestamppb.Timestamp) *time.Time {
	if r.total != nil && count >= *r.total {
		return nil
	}

	if last == nil {
		if r.dueTime != nil {
			return r.dueTime
		}

		return ptr.Of(r.cron.Next(*r.start))
	}

	next := r.cron.Next(last.AsTime())
	if r.exp != nil && next.After(r.exp.AsTime()) {
		return nil
	}

	return &next
}
