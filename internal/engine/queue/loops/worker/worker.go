/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package worker

import (
	"context"
	"sync"

	"github.com/go-logr/logr"

	"github.com/dapr/kit/events/loop"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/counters"
)

type Options struct {
	Actioner actioner.Interface
	Log      logr.Logger
}

type worker struct {
	log logr.Logger
	act actioner.Interface

	counters map[string]*counters.Counters
	wg       sync.WaitGroup
}

func New(opts Options) loop.Interface[*queue.JobEvent] {
	return loop.New[*queue.JobEvent](1024).NewLoop(&worker{
		log:      opts.Log.WithName("worker"),
		act:      opts.Actioner,
		counters: make(map[string]*counters.Counters),
	})
}

func (w *worker) Handle(ctx context.Context, event *queue.JobEvent) error {
	switch event.GetAction().Action.(type) {
	case *queue.JobAction_CloseJob:
		w.handleCloseJob(ctx, event)
		return nil

	case *queue.JobAction_Close:
		w.handleClose()
		return nil

	default:
		return w.handleEvent(ctx, event)
	}
}

func (w *worker) handleEvent(ctx context.Context, event *queue.JobEvent) error {
	jobName := event.GetJobName()

	// If the counter doesn't exist yet, create.
	counter, ok := w.counters[jobName]
	if !ok {
		// If this is an execute response and no counter to handle, return.
		if event.Action.GetExecuteResponse() != nil {
			return nil
		}

		// If we are being informed of a counter which has already been removed
		// from the store, ignore.
		i := event.Action.GetInformed()
		if i == nil || !i.IsPut {
			return nil
		}

		counter = w.newCounter(ctx, jobName)
	}

	return counter.Handle(ctx, event.GetAction())
}

// Create a new counters loop, and start it.
func (w *worker) newCounter(ctx context.Context, jobName string) *counters.Counters {
	c := counters.New(counters.Options{
		Actioner: w.act,
		Name:     jobName,
		Log:      w.log,
	})

	w.counters[jobName] = c

	return c
}

// The inner job loop has been signalled to be closed to release its resources.
func (w *worker) handleCloseJob(ctx context.Context, event *queue.JobEvent) {
	jobName := event.GetJobName()
	counter, ok := w.counters[jobName]
	if !ok {
		return
	}
	delete(w.counters, jobName)
	counters.CountersCache.Put(counter)
}

func (w *worker) handleClose() {
	w.wg.Add(len(w.counters))

	action := &queue.JobAction{
		Action: new(queue.JobAction_Close),
	}

	for _, c := range w.counters {
		go func(c *counters.Counters) {
			_ = c.Handle(context.Background(), action)
			w.wg.Done()
		}(c)
	}

	w.wg.Wait()

	clear(w.counters)
}
