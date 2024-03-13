/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"github.com/diagridio/go-etcd-cron/storage"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

// Job contains 3 mandatory options to define a job
type Job struct {
	// Name of the job
	Name string
	// Cron-formatted rhythm (ie. 0,10,30 1-5 0 * * *)
	Rhythm string
	// The type of trigger that client undertsands
	Type string
	// The payload containg all the information for the trigger
	Payload *anypb.Any
	// Optional number of seconds until this job expires (if > 0)
	TTL int64
}

func (j *Job) clone() *Job {
	return &Job{
		Name:    j.Name,
		Rhythm:  j.Rhythm,
		Type:    j.Type,
		Payload: j.Payload,
		TTL:     j.TTL,
	}
}

func (j *Job) toJobRecord() (*storage.JobRecord, storage.JobRecordOptions) {
	return &storage.JobRecord{
			Name:    j.Name,
			Rhythm:  j.Rhythm,
			Type:    j.Type,
			Payload: j.Payload,
		}, storage.JobRecordOptions{
			TTL: j.TTL,
		}
}

func jobFromJobRecord(r *storage.JobRecord) *Job {
	if r == nil {
		return nil
	}

	return &Job{
		Name:    r.Name,
		Rhythm:  r.Rhythm,
		Type:    r.Type,
		Payload: r.Payload,
	}
}
