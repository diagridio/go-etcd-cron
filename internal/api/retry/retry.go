/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package retry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"

	"github.com/diagridio/go-etcd-cron/api"
	apierrors "github.com/diagridio/go-etcd-cron/api/errors"
	internalapi "github.com/diagridio/go-etcd-cron/internal/api"
	"github.com/diagridio/go-etcd-cron/internal/engine"
	"github.com/go-logr/logr"
)

var errClosed = errors.New("cron is closed")

type Options struct {
	Log logr.Logger
}

// Retry is a engine wrapper for executing the cron API, which will retry calls
// when the API is "closing". This ensures that caller API calls will be held
// and eventually executed during leadership reshuffles.
type Retry struct {
	log    logr.Logger
	engine atomic.Pointer[engine.Interface]

	lock    sync.RWMutex
	readyCh chan struct{}
	closeCh chan struct{}
}

func New(opts Options) *Retry {
	return &Retry{
		log:     opts.Log.WithName("retry"),
		readyCh: make(chan struct{}),
		closeCh: make(chan struct{}),
	}
}

func (r *Retry) Add(ctx context.Context, name string, job *api.Job) error {
	return r.handle(ctx, func(a internalapi.Interface) error {
		return a.Add(ctx, name, job)
	})
}

func (r *Retry) AddIfNotExists(ctx context.Context, name string, job *api.Job) error {
	return r.handle(ctx, func(a internalapi.Interface) error {
		return a.AddIfNotExists(ctx, name, job)
	})
}

func (r *Retry) Get(ctx context.Context, name string) (*api.Job, error) {
	var job *api.Job
	var err error
	err = r.handle(ctx, func(a internalapi.Interface) error {
		job, err = a.Get(ctx, name)
		return err
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (r *Retry) Delete(ctx context.Context, name string) error {
	return r.handle(ctx, func(a internalapi.Interface) error {
		return a.Delete(ctx, name)
	})
}

func (r *Retry) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	return r.handle(ctx, func(a internalapi.Interface) error {
		return a.DeletePrefixes(ctx, prefixes...)
	})
}

func (r *Retry) List(ctx context.Context, prefix string) (*api.ListResponse, error) {
	var resp *api.ListResponse
	var err error
	err = r.handle(ctx, func(a internalapi.Interface) error {
		resp, err = a.List(ctx, prefix)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (r *Retry) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	var cancel context.CancelFunc
	var err error
	err = r.handle(ctx, func(a internalapi.Interface) error {
		cancel, err = a.DeliverablePrefixes(ctx, prefixes...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return cancel, nil
}

func (r *Retry) handle(ctx context.Context, fn func(internalapi.Interface) error) error {
	for {
		a, err := r.waitAPIReady(ctx)
		if err != nil {
			return err
		}

		err = fn(a)
		if !r.handleShouldRetry(err) {
			return err
		}

		r.log.V(3).Info("retrying cron API call", "error", err)

		select {
		case <-time.After(time.Millisecond * 300):
		case <-r.closeCh:
			return errClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// handleShouldRetry returns true if the error returned from the handle
// function should be retried.
func (r *Retry) handleShouldRetry(err error) bool {
	switch {
	case err == nil, apierrors.IsJobAlreadyExists(err):
		return false
	case
		errors.Is(err, internalapi.ErrClosed),
		errors.Is(err, etcdserver.ErrTimeout),
		errors.Is(err, etcdserver.ErrTimeoutDueToLeaderFail),
		errors.Is(err, etcdserver.ErrTimeoutDueToConnectionLost),
		errors.Is(err, etcdserver.ErrTimeoutLeaderTransfer),
		errors.Is(err, etcdserver.ErrTimeoutWaitAppliedIndex),
		errors.Is(err, etcdserver.ErrLeaderChanged),
		errors.Is(err, etcdserver.ErrNotEnoughStartedMembers),
		errors.Is(err, etcdserver.ErrTooManyRequests),
		clientv3.IsConnCanceled(err):
		return true
	default:
		return false
	}
}

// Ready unblocks the Retry API calls, allowing them to be executed on the
// underlying engine.
func (r *Retry) Ready(engine engine.Interface) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.engine.Store(&engine)
	close(r.readyCh)
}

// NotReady blocks the Retry API calls, preventing them from being executed
// on the underlying engine.
func (r *Retry) NotReady() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.readyCh = make(chan struct{})
}

func (r *Retry) Close() {
	close(r.closeCh)
}

func (r *Retry) waitAPIReady(ctx context.Context) (internalapi.Interface, error) {
	r.lock.RLock()
	readyCh := r.readyCh
	r.lock.RUnlock()

	select {
	case <-readyCh:
		return (*r.engine.Load()).API(), nil
	case <-r.closeCh:
		return nil, errClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
