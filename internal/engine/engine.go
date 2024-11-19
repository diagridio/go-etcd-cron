/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package engine

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dapr/kit/concurrency"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/go-logr/logr"
)

// Options are the options for creating a new engine instance.
type Options struct {
	Log       logr.Logger
	Collector garbage.Interface
	Queue     *queue.Queue
	Informer  *informer.Informer
	API       api.API
}

type Engine struct {
	opts Options

	running atomic.Bool
	readyCh chan struct{}
	closeCh chan struct{}
}

func New(opts Options) *Engine {
	e := &Engine{
		opts:    opts,
		readyCh: make(chan struct{}),
		closeCh: make(chan struct{}),
	}
	return e
}

func (e *Engine) Run(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		return errors.New("engine is already running")
	}
	defer e.running.Store(false)

	return concurrency.NewRunnerManager(
		e.opts.Collector.Run,
		e.opts.Queue.Run,
		e.opts.Informer.Run,
		func(ctx context.Context) error {
			ev, err := e.opts.Informer.Events()
			if err != nil {
				return err
			}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case event := <-ev:
					if err := e.opts.Queue.HandleInformerEvent(ctx, event); err != nil {
						return err
					}
				}
			}
		},
		func(ctx context.Context) error {
			defer close(e.closeCh)
			if err := e.opts.Informer.Ready(ctx); err != nil {
				return err
			}

			e.opts.API.SetReady()
			e.opts.Log.Info("cron engine is ready")
			<-ctx.Done()
			e.opts.Log.Info("cron engine is shutting down")
			e.opts.API.SetUnready()

			return nil
		},
	).Run(ctx)
}
