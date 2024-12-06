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
	"sync/atomic"

	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	cronapi "github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/api/validator"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

type Options struct {
	Log              logr.Logger
	Client           client.Interface
	Key              *key.Key
	SchedulerBuilder *scheduler.Builder
	Queue            *queue.Queue
	Informer         *informer.Informer

	// JobNameSanitizer is a replacer that sanitizes job names before name
	// validation.
	JobNameSanitizer *strings.Replacer

	Leadership *leadership.Leadership
}

// Interface is the internal API that implements the API backend.
type Interface interface {
	// Run runs the API.
	Run(ctx context.Context) error

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

	// WatchLeadership returns a channel which will receive all current leadership values when the
	// leadership keyspace changes. This function is responsible for sending the active set of
	// available replicas.
	WatchLeadership(ctx context.Context) (chan []*anypb.Any, error)
}

// api implements the API interface.
type api struct {
	log          logr.Logger
	client       client.Interface
	key          *key.Key
	schedBuilder *scheduler.Builder
	validator    *validator.Validator
	queue        *queue.Queue
	informer     *informer.Informer
	leadership   *leadership.Leadership

	wg      sync.WaitGroup
	running atomic.Bool
	readyCh chan struct{}
	closeCh chan struct{}
}

func New(opts Options) Interface {
	return &api{
		log:          opts.Log.WithName("api"),
		client:       opts.Client,
		key:          opts.Key,
		schedBuilder: opts.SchedulerBuilder,
		validator: validator.New(validator.Options{
			JobNameSanitizer: opts.JobNameSanitizer,
		}),
		queue:      opts.Queue,
		informer:   opts.Informer,
		leadership: opts.Leadership,
		wg:         sync.WaitGroup{},
		readyCh:    make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
}

func (a *api) Run(ctx context.Context) error {
	if !a.running.CompareAndSwap(false, true) {
		return errors.New("api is already running")
	}

	defer close(a.closeCh)
	defer a.wg.Wait()

	if err := a.informer.Ready(ctx); err != nil {
		return err
	}

	close(a.readyCh)

	a.log.Info("api is ready")

	<-ctx.Done()
	a.log.Info("api is shutting down")

	return nil
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

// WatchLeadership returns the dynamic, subscribed leadership replica data
func (a *api) WatchLeadership(ctx context.Context) (chan []*anypb.Any, error) {
	ch := make(chan []*anypb.Any)

	activeReplicaValues, subscribeCh := a.leadership.Subscribe(ctx)

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-subscribeCh:
			ch <- activeReplicaValues
		}
	}()

	return ch, nil
}

func (a *api) waitReady(ctx context.Context) error {
	select {
	case <-a.closeCh:
		return errors.New("api is closed")
	case <-a.readyCh:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
