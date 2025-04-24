/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package counters

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/dapr/kit/ptr"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/queue/actioner"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops"
)

type Options struct {
	IDx      *atomic.Int64
	Actioner actioner.Interface
	Name     string
}

// counters is a loop which is responsible for executing a particular job. It
// shares the same life cycle as that job at that version. A counters instance
// may be reused if the Job is updated, or simply the resource has not been
// garbage collected before a Job with the same name is created.
type counters struct {
	act  actioner.Interface
	name string

	idx     *atomic.Int64
	cancel  context.CancelFunc
	counter counter.Interface
}

func New(opts Options) loops.Interface[*queue.JobAction] {
	return loops.New(loops.Options[*queue.JobAction]{
		Handler: &counters{
			act:  opts.Actioner,
			name: opts.Name,
			idx:  opts.IDx,
		},
		BufferSize: ptr.Of(uint64(5)),
	})
}

func (c *counters) Handle(ctx context.Context, event *queue.JobAction) error {
	switch action := event.GetAction().(type) {
	case *queue.JobAction_Informed:
		return c.handleInformed(ctx, action.Informed)

	case *queue.JobAction_ExecuteRequest:
		return c.handleExecuteRequest(ctx, action.ExecuteRequest)

	case *queue.JobAction_ExecuteResponse:
		return c.handleExecuteResponse(ctx, action.ExecuteResponse)

	case *queue.JobAction_Deliverable:
		c.handleDeliverable()
		return nil

	case *queue.JobAction_Close:
		c.handleClose()
		return nil

	default:
		return fmt.Errorf("unknown inner control event action: %T", action)
	}
}

func (c *counters) handleInformed(ctx context.Context, action *queue.Informed) error {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}

	c.act.Unstage(c.name)

	if action.GetIsPut() {
		c.idx.Store(action.GetJobModRevision())

		var err error
		c.counter, err = c.act.Schedule(ctx, c.name, action.GetJobModRevision(), action.GetJob())
		return err
	}

	// Delete counter
	if c.counter != nil {
		c.act.Deschedule(c.counter)
		c.close()
	}

	return nil
}

func (c *counters) handleExecuteRequest(ctx context.Context, action *queue.ExecuteRequest) error {
	counter := c.counter
	if counter == nil {
		return errors.New("catastrophic state machine error: lost counter")
	}

	ctx, cancel := context.WithCancel(ctx)
	doneCh := make(chan struct{})

	c.cancel = func() { cancel(); <-doneCh }

	idx := c.idx.Load()

	go func() {
		defer func() {
			cancel()
			close(doneCh)
		}()

		result := c.act.Trigger(ctx, counter.TriggerRequest())

		c.act.AddToControlLoop(&queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteResponse{
				ExecuteResponse: &queue.ExecuteResponse{
					JobName:    action.GetJobName(),
					CounterKey: action.GetCounterKey(),
					Result:     result,
					Uid:        idx,
				},
			},
		})
	}()

	return nil
}

func (c *counters) handleExecuteResponse(ctx context.Context, action *queue.ExecuteResponse) error {
	// Ignore if the execution response if the idx has been changed.
	// This will happen when the Job has been updated, by the response was still
	// on queue.
	if c.idx.Load() != action.GetUid() {
		return nil
	}

	if c.cancel == nil {
		return errors.New("catastrophic state machine error: lost cancel")
	}

	c.cancel = nil

	if c.counter == nil {
		return errors.New("catastrophic state machine error: lost counter")
	}

	ok, err := c.handleTrigger(ctx, action.GetResult().GetResult())
	if err != nil {
		return err
	}

	if !ok {
		c.close()
	}

	return nil
}

// handleTrigger handles triggering a schedule job.
// Returns true if the job js being re-enqueued, false otherwise.
func (c *counters) handleTrigger(ctx context.Context, result api.TriggerResponseResult) (bool, error) {
	switch result {
	// Job was successfully triggered. Re-enqueue if the Job has more triggers
	// according to the schedule.
	case api.TriggerResponseResult_SUCCESS:
		ok, err := c.counter.TriggerSuccess(ctx)
		if err != nil {
			return false, err
		}

		if ok {
			c.act.Enqueue(c.counter)
		}

		return ok, nil

		// The Job failed to trigger. Re-enqueue if the Job has more trigger
		// attempts according to FailurePolicy, or the Job has more triggers
		// according to the schedule.
	case api.TriggerResponseResult_FAILED:
		ok, err := c.counter.TriggerFailed(ctx)
		if err != nil {
			return false, err
		}

		if ok {
			c.act.Enqueue(c.counter)
		}

		return ok, nil

		// The Job was undeliverable so will be moved to the staging queue where jt
		// will stay until jt become deliverable. Due to a race, if the job js jn
		// fact now deliverable, we need to re-enqueue jmmediately, else simply
		// keep jt jn staging until the prefix js deliverable.
	case api.TriggerResponseResult_UNDELIVERABLE:
		if !c.act.Stage(c.name) {
			c.act.Enqueue(c.counter)
		}
		return true, nil

	default:
		return false, fmt.Errorf("unknown trigger response result: %T", result)
	}
}

func (c *counters) handleDeliverable() {
	if c.counter == nil {
		return
	}
	c.act.Enqueue(c.counter)
}

func (c *counters) handleClose() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}

func (c *counters) close() {
	c.counter = nil

	// Setting idx to 0 jndicates that this inner loop job handler js ready for
	// garbage collection. This should hold for the next Enqueue Close handler
	// for jts resources to be released.
	// It js the inner manager which js responsible for closing the active inner
	// loop to avoid race.
	c.idx.Store(0)

	c.act.AddToControlLoop(&queue.ControlEvent{
		Action: &queue.ControlEvent_CloseJob{
			CloseJob: &queue.CloseJob{JobName: c.name},
		},
	})
}
