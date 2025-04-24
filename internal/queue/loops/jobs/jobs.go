/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dapr/kit/ptr"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/queue/actioner"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops/counters"
	"github.com/go-logr/logr"
)

type Options struct {
	Actioner actioner.Interface
	Log      logr.Logger
	Cancel   context.CancelFunc
}

type counter struct {
	idx  *atomic.Int64
	loop loops.Interface[*queue.JobAction]
}

type jobs struct {
	log    logr.Logger
	cancel context.CancelFunc
	act    actioner.Interface

	counters map[string]*counter
	wg       sync.WaitGroup
}

func New(opts Options) loops.Interface[*queue.JobEvent] {
	return loops.New(loops.Options[*queue.JobEvent]{
		BufferSize: ptr.Of(uint64(1024)),
		Handler: &jobs{
			log:      opts.Log.WithName("jobs"),
			cancel:   opts.Cancel,
			act:      opts.Actioner,
			counters: make(map[string]*counter),
		},
	})
}

func (j *jobs) Handle(ctx context.Context, event *queue.JobEvent) error {
	switch event.GetAction().Action.(type) {
	case *queue.JobAction_CloseJob:
		return j.handleCloseJob(event)

	case *queue.JobAction_Close:
		j.handleClose()
		return nil

	default:
		return j.handleEvent(ctx, event)
	}
}

func (j *jobs) handleEvent(ctx context.Context, event *queue.JobEvent) error {
	jobName := event.GetJobName()

	// If the counter doesn't exist yet, create.
	counter, ok := j.counters[jobName]
	if !ok {
		counter = j.newCounter(ctx, jobName)
	}

	counter.loop.Enqueue(event.GetAction())

	return nil
}

// Create a new counters loop, and start it.
func (j *jobs) newCounter(ctx context.Context, jobName string) *counter {
	var idx atomic.Int64
	loop := counters.New(counters.Options{
		IDx:      &idx,
		Actioner: j.act,
		Name:     jobName,
	})

	counter := &counter{
		loop: loop,
		idx:  &idx,
	}

	j.counters[jobName] = counter

	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		err := loop.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			j.cancel()
			j.log.Error(err, "failed to run inner loop", "job", jobName)
		}
	}()

	return counter
}

// The inner job loop has been signalled to be closed to release its
// resources.
func (j *jobs) handleCloseJob(event *queue.JobEvent) error {
	jobName := event.GetJobName()

	counter, ok := j.counters[jobName]
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
		Action: new(queue.JobAction_Close),
	})
	delete(j.counters, jobName)

	return nil
}

func (j *jobs) handleClose() {
	defer j.wg.Wait()

	j.wg.Add(len(j.counters))

	action := &queue.JobAction{
		Action: new(queue.JobAction_Close),
	}

	for _, c := range j.counters {
		go func(c *counter) {
			c.loop.Close(action)
			j.wg.Done()
		}(c)
	}
}
