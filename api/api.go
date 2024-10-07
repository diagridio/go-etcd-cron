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
// The returne TriggerResponse will indicate whether the Job was successfully
// triggered, the trigger failed, or the Job need to be put into the staging
// queue.
type TriggerFunction func(context.Context, *TriggerRequest) *TriggerResponse

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

	// DeliverablePrefixes registers the given Job name prefixes as being
	// deliverable. Any Jobs that reside in the staging queue because they were
	// undeliverable at the time of trigger but whose names match these prefixes
	// will be immediately re-triggered.
	// The returned CancelFunc should be called to unregister the prefixes,
	// meaning these prefixes are no longer delivable by the caller. Duplicate
	// Prefixes may be called together and will be pooled together, meaning that
	// the prefix is still active if there is at least one DeliverablePrefixes
	// call that has not been unregistered.
	DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error)
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
