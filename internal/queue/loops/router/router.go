/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package router

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/queue/actioner"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops/counters"
)

type Options struct {
	Actioner actioner.Interface
	Log      logr.Logger
	Cancel   context.CancelFunc
}

// router is a loop to handle Job events. It routes the event to the correct
// counter loop. It creates and runs counter loops at inform instantiate time,
// and garbage collects them after close.
// TODO: @joshvanl: add sync.Pool for counters to reduce allocations.
type router struct {
	log    logr.Logger
	cancel context.CancelFunc
	act    actioner.Interface

	counters map[string]*counter
	wg       sync.WaitGroup
}

// counter is a single instance of a counter loop. Tracks the idx of the
// associated key mod revision to track if the job event is still relevant for
// the current live counter loop.
type counter struct {
	idx  *atomic.Int64
	loop loops.Interface[*queue.JobAction]
}

func New(opts Options) loops.Interface[*queue.JobEvent] {
	return loops.New(&router{
		log:      opts.Log.WithName("router"),
		cancel:   opts.Cancel,
		act:      opts.Actioner,
		counters: make(map[string]*counter),
	}, 1024)
}

func (r *router) Handle(ctx context.Context, event *queue.JobEvent) error {
	switch event.GetAction().Action.(type) {
	case *queue.JobAction_CloseJob:
		return r.handleCloseJob(event)

	case *queue.JobAction_Close:
		r.handleClose()
		return nil

	default:
		return r.handleEvent(ctx, event)
	}
}

func (r *router) handleEvent(ctx context.Context, event *queue.JobEvent) error {
	jobName := event.GetJobName()

	// If the counter doesn't exist yet, create.
	counter, ok := r.counters[jobName]
	if !ok {
		counter = r.newCounter(ctx, jobName)
	}

	counter.loop.Enqueue(event.GetAction())

	return nil
}

// Create a new counters loop, and start it.
func (r *router) newCounter(ctx context.Context, jobName string) *counter {
	var idx atomic.Int64
	loop := counters.New(counters.Options{
		IDx:      &idx,
		Actioner: r.act,
		Name:     jobName,
	})

	counter := &counter{
		loop: loop,
		idx:  &idx,
	}

	r.counters[jobName] = counter

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := loop.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.cancel()
			r.log.Error(err, "failed to run inner loop", "job", jobName)
		}
	}()

	return counter
}

// The inner job loop has been signalled to be closed to release its
// resources.
func (r *router) handleCloseJob(event *queue.JobEvent) error {
	jobName := event.GetJobName()

	counter, ok := r.counters[jobName]
	if !ok {
		return errors.New("catastrophic state machine error: lost inner loop reference")
	}

	// Ignore the close if the inner loop is now being reused.
	if counter.idx.Load() != 0 {
		return nil
	}

	// If idx is 0, then the inner loop is still not being used and resources can
	// be released.
	counter.loop.Close(&queue.JobAction{
		Action: &queue.JobAction_Close{
			Close: new(queue.Close),
		},
	})
	delete(r.counters, jobName)
	counters.LoopsCache.Put(counter.loop)

	return nil
}

func (r *router) handleClose() {
	defer r.wg.Wait()

	r.wg.Add(len(r.counters))

	action := &queue.JobAction{
		Action: new(queue.JobAction_Close),
	}

	for _, c := range r.counters {
		go func(c *counter) {
			c.loop.Close(action)
			r.wg.Done()
		}(c)
	}
}
