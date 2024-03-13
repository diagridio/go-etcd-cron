/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package rhythm

import "time"

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}
