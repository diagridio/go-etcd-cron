/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package api

import (
	"context"
)

// TriggerFunction is the type of the function that is called when a job is
// triggered.
// Returning true will "tick" the job forward to the next scheduled time.
// Returning false will cause the job to be re-enqueued and triggered
// immediately.
type TriggerFunction func(context.Context, *TriggerRequest) bool

// API is the interface for interacting with the cron instance.
type API interface {
	// Add adds a job to the cron instance.
	Add(ctx context.Context, name string, job *Job) error

	// Get gets a job from the cron instance.
	Get(ctx context.Context, name string) (*Job, error)

	// Delete deletes a job from the cron instance.
	Delete(ctx context.Context, name string) error

	// DeletePrefixes deletes all jobs with the given prefixes from the cron
	// instance.
	DeletePrefixes(ctx context.Context, prefixes ...string) error

	// List lists all jobs under a given job name prefix.
	List(ctx context.Context, prefix string) (*ListResponse, error)
}

// Interface is a cron interface. It schedules and manages job which are stored
// and informed from ETCD. It uses a trigger function to call when a job is
// triggered.
// Jobs may be oneshot or recurring. Recurring jobs are scheduled to run at
// their next scheduled time. Oneshot jobs are scheduled to run once and are
// removed from the schedule after they are triggered.
type Interface interface {
	// Run is a blocking function that runs the cron instance. It will return an
	// error if the instance is already running.
	// Returns when the given context is cancelled, after doing all cleanup.
	Run(ctx context.Context) error

	// API implements the client API for the cron instance.
	API
}
