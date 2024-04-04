/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"time"

	"github.com/diagridio/go-etcd-cron/storage"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

type JobStatus int

// Job Statuses
const (
	JobStatusActive JobStatus = iota
	JobStatusDeleted
)

// Job contains 3 mandatory options to define a job
type Job struct {
	// Name of the job
	Name string
	// Cron-formatted rhythm (ie. 0,10,30 1-5 0 * * *)
	Rhythm string
	// Optional metadata that the client understands.
	Metadata map[string]string
	// Optional payload containg all the information for the trigger
	Payload *anypb.Any
	// Optional start time for the first trigger of the schedule
	Repeats int32
	// Optional start time for the first trigger of the schedule
	StartTime time.Time
	// Optional time when the job must expire
	Expiration time.Time
	// Status to allow job clean up
	Status JobStatus
}

func (j *Job) expired(now time.Time) bool {
	return !j.Expiration.IsZero() && !now.Before(j.Expiration)
}

func (j *Job) toJobRecord() *storage.JobRecord {
	status := storage.JobStatus_ACTIVE
	if j.Status == JobStatusDeleted {
		status = storage.JobStatus_DELETED
	}
	return &storage.JobRecord{
		Status:              status,
		Rhythm:              j.Rhythm,
		Metadata:            j.Metadata,
		Payload:             j.Payload,
		Repeats:             j.Repeats,
		StartTimestamp:      j.StartTime.Unix(),
		ExpirationTimestamp: j.Expiration.Unix(),
	}
}

func jobFromJobRecord(name string, r *storage.JobRecord) *Job {
	if r == nil {
		return nil
	}

	status := JobStatusActive
	if r.Status == storage.JobStatus_DELETED {
		status = JobStatusDeleted
	}

	return &Job{
		Name:       name,
		Rhythm:     r.Rhythm,
		Metadata:   r.Metadata,
		Payload:    r.Payload,
		Repeats:    r.Repeats,
		StartTime:  time.Unix(r.StartTimestamp, 0),
		Expiration: time.Unix(r.ExpirationTimestamp, 0),
		Status:     status,
	}
}
