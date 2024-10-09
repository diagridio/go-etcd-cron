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

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	cronapi "github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/api/validator"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

var errAPIClosed = errors.New("api is closed")

type Options struct {
	Client           client.Interface
	Key              *key.Key
	SchedulerBuilder *scheduler.Builder
	Queue            *queue.Queue
	ReadyCh          chan struct{}
	CloseCh          chan struct{}

	// JobNameSanitizer is a replacer that sanitizes job names before name
	// validation.
	JobNameSanitizer *strings.Replacer
}

// api implements the API interface.
type api struct {
	client       client.Interface
	key          *key.Key
	schedBuilder *scheduler.Builder
	validator    *validator.Validator
	queue        *queue.Queue
	readyCh      chan struct{}
	closeCh      chan struct{}
}

func New(opts Options) cronapi.API {
	closeCh := opts.CloseCh
	if closeCh == nil {
		closeCh = make(chan struct{})
	}

	return &api{
		client:       opts.Client,
		key:          opts.Key,
		schedBuilder: opts.SchedulerBuilder,
		validator: validator.New(validator.Options{
			JobNameSanitizer: opts.JobNameSanitizer,
		}),
		queue:   opts.Queue,
		readyCh: opts.ReadyCh,
		closeCh: closeCh,
	}
}

// Add adds a new cron job to the cron instance.
func (a *api) Add(ctx context.Context, name string, job *cronapi.Job) error {
	select {
	case <-a.readyCh:
	case <-a.closeCh:
		return errAPIClosed
	case <-ctx.Done():
		return context.Cause(ctx)
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
	select {
	case <-a.readyCh:
	case <-a.closeCh:
		return nil, errAPIClosed
	case <-ctx.Done():
		return nil, context.Cause(ctx)
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
	select {
	case <-a.readyCh:
	case <-a.closeCh:
		return errAPIClosed
	case <-ctx.Done():
		return context.Cause(ctx)
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
	select {
	case <-a.readyCh:
	case <-a.closeCh:
		return errAPIClosed
	case <-ctx.Done():
		return context.Cause(ctx)
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
	select {
	case <-a.readyCh:
	case <-a.closeCh:
		return nil, errAPIClosed
	case <-ctx.Done():
		return nil, context.Cause(ctx)
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

// RegisterDeliverablePrefixes registers the given Job name prefixes as being
// deliverable. Calling the returned CancelFunc will de-register those
// prefixes as being deliverable.
func (a *api) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	select {
	case <-a.readyCh:
	case <-a.closeCh:
		return nil, errAPIClosed
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}

	if len(prefixes) == 0 {
		return nil, errors.New("no prefixes provided")
	}

	for _, prefix := range prefixes {
		if err := a.validator.JobName(prefix); err != nil {
			return nil, err
		}
	}

	return a.queue.DeliverablePrefixes(prefixes...), nil
}
