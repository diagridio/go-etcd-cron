/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package handler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	cronapi "github.com/diagridio/go-etcd-cron/api"
	apierrors "github.com/diagridio/go-etcd-cron/api/errors"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/api/validator"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/engine/informer"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
)

var (
	ErrClosed = errors.New("api handler is closed")
)

type Options struct {
	Log              logr.Logger
	Client           clientapi.Interface
	Key              *key.Key
	SchedulerBuilder *scheduler.Builder
	Queue            *queue.Queue
	Informer         *informer.Informer

	// JobNameSanitizer is a replacer that sanitizes job names before name
	// validation.
	JobNameSanitizer *strings.Replacer
}

// Interface is the internal API that implements the API backend.
type Interface interface {
	// Run runs the API.
	Run(ctx context.Context) error

	// Add adds a job to the cron instance.
	Add(ctx context.Context, name string, job *cronapi.Job) error

	// AddIfNotExists adds a job to the cron instance. If the Job already exists,
	// then returns an error.
	AddIfNotExists(ctx context.Context, name string, job *cronapi.Job) error

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
	DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelCauseFunc, error)
}

// handler implements the API interface.
type handler struct {
	log          logr.Logger
	client       clientapi.Interface
	key          *key.Key
	schedBuilder *scheduler.Builder
	validator    *validator.Validator
	queue        *queue.Queue
	informer     *informer.Informer

	running atomic.Bool
	readyCh chan struct{}
	closeCh chan struct{}
}

func New(opts Options) Interface {
	return &handler{
		log:          opts.Log.WithName("api"),
		client:       opts.Client,
		key:          opts.Key,
		schedBuilder: opts.SchedulerBuilder,
		validator: validator.New(validator.Options{
			JobNameSanitizer: opts.JobNameSanitizer,
		}),
		queue:    opts.Queue,
		informer: opts.Informer,
		readyCh:  make(chan struct{}),
		closeCh:  make(chan struct{}),
	}
}

func (h *handler) Run(ctx context.Context) error {
	if !h.running.CompareAndSwap(false, true) {
		return errors.New("api is already running")
	}

	defer close(h.closeCh)

	if err := h.informer.Ready(ctx); err != nil {
		return err
	}

	close(h.readyCh)

	h.log.Info("api is ready")

	<-ctx.Done()
	h.log.Info("api is shutting down")

	return nil
}

// Add adds a new cron job to the cron instance.
func (h *handler) Add(ctx context.Context, name string, job *cronapi.Job) error {
	return h.addJob(ctx, name, job, true)
}

// AddIfNotExists adds a new cron job to the cron instance. If the Job already
// exists, an error is returned.
func (h *handler) AddIfNotExists(ctx context.Context, name string, job *cronapi.Job) error {
	return h.addJob(ctx, name, job, false)
}

func (h *handler) addJob(ctx context.Context, name string, job *cronapi.Job, upsert bool) error {
	if err := h.waitReady(ctx); err != nil {
		return err
	}

	if err := h.validator.JobName(name); err != nil {
		return err
	}

	if job == nil {
		return errors.New("job cannot be nil")
	}

	storedJob, err := h.schedBuilder.Parse(job)
	if err != nil {
		return fmt.Errorf("job failed validation: %w", err)
	}

	b, err := proto.Marshal(storedJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if upsert {
		_, err = h.client.Put(ctx, h.key.JobKey(name), string(b))
		return err
	}

	put, err := h.client.PutIfNotExists(ctx, h.key.JobKey(name), string(b))
	if err != nil {
		return err
	}

	if !put {
		return apierrors.NewJobAlreadyExists(name)
	}

	return nil
}

// Get gets a cron job from the cron instance.
func (h *handler) Get(ctx context.Context, name string) (*cronapi.Job, error) {
	if err := h.waitReady(ctx); err != nil {
		return nil, err
	}

	if err := h.validator.JobName(name); err != nil {
		return nil, err
	}

	resp, err := h.client.Get(ctx, h.key.JobKey(name))
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
func (h *handler) Delete(ctx context.Context, name string) error {
	if err := h.waitReady(ctx); err != nil {
		return err
	}

	if err := h.validator.JobName(name); err != nil {
		return err
	}

	err := h.client.DeletePair(ctx, h.key.JobKey(name), h.key.CounterKey(name))
	if err != nil {
		return err
	}

	return nil
}

// DeletePrefixes deletes cron jobs with the given prefixes from the cron
// instance.
func (h *handler) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	if err := h.waitReady(ctx); err != nil {
		return err
	}

	for _, prefix := range prefixes {
		if len(prefix) == 0 {
			continue
		}

		if err := h.validator.JobName(prefix); err != nil {
			return err
		}
	}

	txnPrefixes := make([]string, 0, len(prefixes)*2)
	for _, prefix := range prefixes {
		txnPrefixes = append(txnPrefixes, h.key.JobKey(prefix), h.key.CounterKey(prefix))
	}

	if err := h.client.DeletePrefixes(ctx, txnPrefixes...); err != nil {
		return fmt.Errorf("failed to delete prefixes %q: %w", strings.Join(prefixes, ","), err)
	}

	return nil
}

// List lists all cron jobs with the given job name prefix.
func (h *handler) List(ctx context.Context, prefix string) (*cronapi.ListResponse, error) {
	if err := h.waitReady(ctx); err != nil {
		return nil, err
	}

	resp, err := h.client.Get(ctx,
		h.key.JobKey(prefix),
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
func (h *handler) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelCauseFunc, error) {
	if err := h.waitReady(ctx); err != nil {
		return nil, err
	}

	if len(prefixes) == 0 {
		return nil, errors.New("no prefixes provided")
	}

	return h.queue.DeliverablePrefixes(ctx, prefixes...)
}

func (h *handler) waitReady(ctx context.Context) error {
	select {
	case <-h.closeCh:
		return ErrClosed
	case <-h.readyCh:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
