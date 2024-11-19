/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	cronapi "github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/api/validator"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type Options struct {
	Client           client.Interface
	Key              *key.Key
	SchedulerBuilder *scheduler.Builder
	Queue            *queue.Queue

	// JobNameSanitizer is a replacer that sanitizes job names before name
	// validation.
	JobNameSanitizer *strings.Replacer
}

type Interface interface {
	// Add adds a job to the cron instance.
	Add(ctx context.Context, name string, job *cronapi.Job) error

	// Get gets a job from the cron instance.
	Get(ctx context.Context, name string) (*cronapi.Job, error)

	// Delete deletes a job from the cron instance.
	Delete(ctx context.Context, name string) error

	// DeletePrefixes deletes all jobs with the given prefixes from the cron
	// instance.
	DeletePrefixes(ctx context.Context, prefixes ...string) error

	// List lists all jobs under a given job name prefix.
	List(ctx context.Context, prefix string) (*cronapi.ListResponse, error)

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

	// SetReady signals that the API is ready to accept requests.
	SetReady()

	// Close closes the API.
	Close()
}

// api implements the API interface.
type api struct {
	wg           sync.WaitGroup
	client       client.Interface
	key          *key.Key
	schedBuilder *scheduler.Builder
	validator    *validator.Validator
	queue        *queue.Queue
	readyCh      chan struct{}
	closeCh      chan struct{}
}

func New(opts Options) Interface {
	return &api{
		client:       opts.Client,
		key:          opts.Key,
		schedBuilder: opts.SchedulerBuilder,
		validator: validator.New(validator.Options{
			JobNameSanitizer: opts.JobNameSanitizer,
		}),
		queue:   opts.Queue,
		readyCh: make(chan struct{}),
		closeCh: make(chan struct{}),
	}
}

// Add adds a new cron job to the cron instance.
func (a *api) Add(ctx context.Context, name string, job *cronapi.Job) error {
	if err := a.waitReady(ctx); err != nil {
		return err
	}

	if err := a.validator.JobName(name); err != nil {
		return err
	}

	if job == nil {
		return errors.New("job cannot be nil")
	}

	storedJob, err := a.schedBuilder.Parse(job)
	if err != nil {
		return fmt.Errorf("job failed validation: %w", err)
	}

	b, err := proto.Marshal(storedJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	_, err = a.client.Put(ctx, a.key.JobKey(name), string(b))
	return err
}

// Get gets a cron job from the cron instance.
func (a *api) Get(ctx context.Context, name string) (*cronapi.Job, error) {
	if err := a.waitReady(ctx); err != nil {
		return nil, err
	}

	if err := a.validator.JobName(name); err != nil {
		return nil, err
	}

	resp, err := a.client.Get(ctx, a.key.JobKey(name))
	if err != nil {
		return nil, err
	}

	// No entry is not an error, but a nil object.
	if resp.Count == 0 {
		return nil, nil
	}

	var stored stored.Job
	if err := proto.Unmarshal(resp.Kvs[0].Value, &stored); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return stored.GetJob(), nil
}

// Delete deletes a cron job from the cron instance.
func (a *api) Delete(ctx context.Context, name string) error {
	if err := a.waitReady(ctx); err != nil {
		return err
	}

	if err := a.validator.JobName(name); err != nil {
		return err
	}

	_, err := a.client.Delete(ctx, a.key.JobKey(name))
	if err != nil {
		return err
	}

	return nil
}

// DeletePrefixes deletes cron jobs with the given prefixes from the cron
// instance.
func (a *api) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	if err := a.waitReady(ctx); err != nil {
		return err
	}

	for _, prefix := range prefixes {
		if len(prefix) == 0 {
			continue
		}

		if err := a.validator.JobName(prefix); err != nil {
			return err
		}
	}

	var errs []error
	for _, prefix := range prefixes {
		_, err := a.client.Delete(ctx, a.key.JobKey(prefix), clientv3.WithPrefix())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete jobs with prefix %q: %w", prefix, err))
		}
	}

	return errors.Join(errs...)
}

// List lists all cron jobs with the given job name prefix.
func (a *api) List(ctx context.Context, prefix string) (*cronapi.ListResponse, error) {
	if err := a.waitReady(ctx); err != nil {
		return nil, err
	}

	resp, err := a.client.Get(ctx,
		a.key.JobKey(prefix),
		clientv3.WithPrefix(),
		clientv3.WithLimit(0),
	)
	if err != nil {
		return nil, err
	}

	jobs := make([]*cronapi.NamedJob, 0, resp.Count)
	for _, kv := range resp.Kvs {
		var stored stored.Job
		if err := proto.Unmarshal(kv.Value, &stored); err != nil {
			return nil, fmt.Errorf("failed to unmarshal job from prefix %q: %w", prefix, err)
		}

		jobs = append(jobs, &cronapi.NamedJob{
			Name: string(kv.Key),
			Job:  stored.GetJob(),
		})
	}

	return &cronapi.ListResponse{
		Jobs: jobs,
	}, nil
}

// DeliverablePrefixes registers the given Job name prefixes as being
// deliverable. Calling the returned CancelFunc will de-register those
// prefixes as being deliverable.
func (a *api) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	if err := a.waitReady(ctx); err != nil {
		return nil, err
	}

	if len(prefixes) == 0 {
		return nil, errors.New("no prefixes provided")
	}

	return a.queue.DeliverablePrefixes(prefixes...), nil
}

func (a *api) Close() {
	close(a.closeCh)
}

func (a *api) SetReady() {
	close(a.readyCh)
}

func (a *api) waitReady(ctx context.Context) error {
	select {
	case <-a.readyCh:
		return nil
	case <-a.closeCh:
		return errors.New("api is closed")
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
