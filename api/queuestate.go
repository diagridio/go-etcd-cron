/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package api

// QueueStateKeyFunc maps a job name to the namespace and app ID its queue
// state is counted under. Jobs for which ok is false are not counted.
type QueueStateKeyFunc func(jobName string) (namespace, appID string, ok bool)

// QueueState is a point in time view of the jobs this replica's queue is
// holding, counted per namespace and app ID as given by the
// QueueStateKeyFunc. Only jobs in the partition owned by this replica are
// counted.
type QueueState struct {
	// InFlight counts jobs which have been triggered and whose trigger has not
	// yet been answered. Indexed by namespace, then app ID.
	InFlight map[string]map[string]uint64

	// Staged counts jobs which are parked in the staging queue because they
	// were undeliverable when triggered. Indexed by namespace, then app ID.
	// An undeliverable job whose prefix was made deliverable just before it
	// was answered is re-triggered at once rather than staged, but is still
	// counted as staged until that re-trigger is processed.
	Staged map[string]map[string]uint64
}

// NewQueueState returns a QueueState with no counts.
func NewQueueState() *QueueState {
	return &QueueState{
		InFlight: make(map[string]map[string]uint64),
		Staged:   make(map[string]map[string]uint64),
	}
}
