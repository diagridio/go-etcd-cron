/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package worker

import (
	"context"
	"fmt"
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

	counters map[int64]*counters.Counters
	wg       sync.WaitGroup
}

func New(opts Options) loop.Interface[*queue.JobEvent] {
	return loop.New[*queue.JobEvent](1024).NewLoop(&worker{
		log:      opts.Log.WithName("worker"),
		act:      opts.Actioner,
		counters: make(map[int64]*counters.Counters),
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
	var counter *counters.Counters
	var ok bool

	switch action := event.GetAction().Action.(type) {
	case *queue.JobAction_ExecuteRequest:
		modRevision := action.ExecuteRequest.GetModRevision()
		counter, ok = w.counters[modRevision]
		if !ok {
			panic(fmt.Sprintf("counter not found for modRevision: %d", modRevision))
		}

	case *queue.JobAction_ExecuteResponse:
		modRevision := action.ExecuteResponse.GetModRevision()
		counter, ok = w.counters[modRevision]
		if !ok {
			return nil
		}

	case *queue.JobAction_Deliverable:
		modRevision := action.Deliverable.GetModRevision()
		counter, ok = w.counters[modRevision]
		if !ok {
			return nil
		}

	case *queue.JobAction_Informed:
		modRevision := action.Informed.GetQueuedJob().GetModRevision()
		counter, ok = w.counters[modRevision]
		// If the counter doesn't exist yet, create.
		if !ok {
			// If we are being informed of a counter which has already been removed
			// from the store, ignore.
			i := action.Informed
			if i == nil || !i.IsPut {
				return nil
			}

			counter = w.newCounter(ctx, modRevision, i.GetName())
		}

	default:
		panic(fmt.Sprintf("unsupported action type: %T", event.GetAction().Action))
	}

	return counter.Handle(ctx, event.GetAction())
}

// Create a new counters loop, and start it.
func (w *worker) newCounter(ctx context.Context, modRevision int64, name string) *counters.Counters {
	c := counters.New(counters.Options{
		Actioner:    w.act,
		ModRevision: modRevision,
		Name:        name,
		Log:         w.log,
	})

	w.counters[modRevision] = c

	return c
}

// The inner job loop has been signalled to be closed to release its resources.
func (w *worker) handleCloseJob(ctx context.Context, event *queue.JobEvent) {
	modRevision := event.GetAction().GetCloseJob().GetModRevision()
	counter, ok := w.counters[modRevision]
	if !ok {
		return
	}
	delete(w.counters, modRevision)
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
