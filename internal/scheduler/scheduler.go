/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Interface is an interface which returns the next trigger time for a given
// parsed schedule.
type Interface interface {
	// Next returns the next trigger time for the schedule.
	// The given count is the number of times the trigger has been run.
	// The last parameter is the time the trigger was last run or job created
	// time.
	// Returns nil if the schedule will never trigger again.
	Next(count uint32, last *timestamppb.Timestamp) *time.Time
}
