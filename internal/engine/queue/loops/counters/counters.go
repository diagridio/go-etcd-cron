/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package counters

import (
	"context"
	"fmt"
	"sync"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner"
	"github.com/go-logr/logr"
)

type Options struct {
	Actioner actioner.Interface
	Name     string
	Log      logr.Logger
}

// Counters is a loop which is responsible for executing a particular job. It
// shares the same life cycle as that job at that version. A Counters instance
// may be reused if the Job is updated, or simply the resource has not been
// garbage collected before a Job with the same name is created.
type Counters struct {
	act  actioner.Interface
	name string
	log  logr.Logger

	jobVersion int64
	counter    counter.Interface
}

// LoopsFacory and CountersCache are used to cache the loops and Counters
// structs. Used to reduce memory allocations of these highly used structs,
// improving performance.
var (
	CountersCache = sync.Pool{
		New: func() any {
			return new(Counters)
		},
	}
)

func New(opts Options) *Counters {
	c := CountersCache.Get().(*Counters)
	c.name = opts.Name
	c.act = opts.Actioner
	c.log = opts.Log

	return c
}

func (c *Counters) Handle(ctx context.Context, event *queue.JobAction) error {
	switch action := event.GetAction().(type) {
	case *queue.JobAction_Informed:
		return c.handleInformed(ctx, action.Informed)

	case *queue.JobAction_ExecuteRequest:
		c.handleExecuteRequest(action.ExecuteRequest)
		return nil

	case *queue.JobAction_ExecuteResponse:
		return c.handleExecuteResponse(ctx, action.ExecuteResponse)

	case *queue.JobAction_Deliverable:
		c.handleDeliverable()
		return nil

	case *queue.JobAction_Close:
		return nil

	default:
		return fmt.Errorf("unknown inner control event action: %T", action)
	}
}

func (c *Counters) handleInformed(ctx context.Context, action *queue.Informed) error {
	c.act.Unstage(c.name)

	if action.GetIsPut() {
		c.jobVersion = action.GetJobModRevision()

		var err error
		c.counter, err = c.act.Schedule(ctx, c.name, c.jobVersion, action.GetJob())
		return err
	}

	// Delete counter
	if c.counter != nil {
		c.act.Deschedule(c.counter)
		c.close()
	}

	return nil
}

func (c *Counters) handleExecuteRequest(action *queue.ExecuteRequest) {
	counter := c.counter
	if counter == nil {
		c.log.WithName("counters").WithValues("job", c.name).Info(
			"dropped ExecuteRequest due to missing counter")
		return
	}

	jobVersion := c.jobVersion

	c.act.Trigger(counter.TriggerRequest(), func(result *api.TriggerResponse) {
		c.act.AddToControlLoop(&queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteResponse{
				ExecuteResponse: &queue.ExecuteResponse{
					JobName:    action.GetJobName(),
					CounterKey: action.GetCounterKey(),
					Result:     result,
					Uid:        jobVersion,
				},
			},
		})
	})
}

func (c *Counters) handleExecuteResponse(ctx context.Context, action *queue.ExecuteResponse) error {
	// Ignore if the execution response if the jobVersion has been changed. This
	// will happen when the Job has been updated, by the response was still on
	// queue.
	if c.jobVersion != action.GetUid() || c.counter == nil {
		c.log.WithName("counters").WithValues("job", c.name, "uid", action.GetUid()).Info(
			"dropped ExecuteResponse for old job version",
		)
		return nil
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

// handleTrigger handles triggering a scheduled job counter.
// Returns true if the job is being re-enqueued, false otherwise.
func (c *Counters) handleTrigger(ctx context.Context, result api.TriggerResponseResult) (bool, error) {
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
		// will stay until jt become deliverable. Due to a race, if the job is jn
		// fact now deliverable, we need to re-enqueue jmmediately, else simply
		// keep jt jn staging until the prefix is deliverable.
	case api.TriggerResponseResult_UNDELIVERABLE:
		if !c.act.Stage(c.name) {
			c.act.Enqueue(c.counter)
		}
		return true, nil

	default:
		return false, fmt.Errorf("unknown trigger response result: %T", result)
	}
}

func (c *Counters) handleDeliverable() {
	if c.counter == nil {
		return
	}
	c.act.Enqueue(c.counter)
}

func (c *Counters) close() {
	// Setting jobVersion to 0 indicates that this inner loop job counter handler
	// is ready for garbage collection. It is the inner manager which is
	// responsible for closing the active inner loop to avoid race.
	c.jobVersion = 0

	c.act.RemoveConsumer(c.counter)
	c.counter = nil

	c.act.AddToControlLoop(&queue.ControlEvent{
		Action: &queue.ControlEvent_CloseJob{
			CloseJob: &queue.CloseJob{JobName: c.name},
		},
	})
}
