/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// oneshot is a scheduler that represents a one time job that is to be executed
// at a specific time.
type oneshot struct {
	dueTime time.Time
}

func (o *oneshot) Next(count uint32, _ *timestamppb.Timestamp) *time.Time {
	if count >= 1 {
		return nil
	}

	return &o.dueTime
}
