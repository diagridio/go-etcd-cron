/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package control

import (
	"context"
	"fmt"

	"github.com/dapr/kit/events/loop"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner"
)

var factory = loop.New[*queue.ControlEvent](1024)

type Options struct {
	Actioner actioner.Interface
	Jobs     loop.Interface[*queue.JobEvent]
}

// control is the main outer control loop responsible for handling all control
// signals, such as inform, execute, deliverable etc. Appropriate events are
// routed to the jobs loop for processing.
type control struct {
	jobs loop.Interface[*queue.JobEvent]
	act  actioner.Interface
}

func New(opts Options) loop.Interface[*queue.ControlEvent] {
	return factory.NewLoop(&control{
		jobs: opts.Jobs,
		act:  opts.Actioner,
	})
}

func (c *control) Handle(_ context.Context, event *queue.ControlEvent) error {
	switch action := event.GetAction().(type) {
	case *queue.ControlEvent_Informed:
		c.handleInformed(action.Informed)

	case *queue.ControlEvent_ExecuteRequest:
		c.handleExecuteRequest(action.ExecuteRequest)

	case *queue.ControlEvent_ExecuteResponse:
		c.handleExecuteResponse(action.ExecuteResponse)

	case *queue.ControlEvent_DeliverablePrefixes:
		c.handleDeliverablePrefixes(action.DeliverablePrefixes)

	case *queue.ControlEvent_UndeliverablePrefixes:
		c.handleUndeliverablePrefixes(action.UndeliverablePrefixes)

	case *queue.ControlEvent_CloseJob:
		c.handleCloseJob(action.CloseJob)

	case *queue.ControlEvent_Close:
		c.handleClose()

	default:
		return fmt.Errorf("unknown event type %T", action)
	}

	return nil
}

func (c *control) handleInformed(action *queue.Informed) {
	c.jobs.Enqueue(&queue.JobEvent{
		JobName: action.GetName(),
		Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{Informed: action},
		},
	})
}

func (c *control) handleExecuteRequest(action *queue.ExecuteRequest) {
	c.jobs.Enqueue(&queue.JobEvent{
		JobName: action.GetJobName(),
		Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{ExecuteRequest: action},
		},
	})
}

func (c *control) handleExecuteResponse(action *queue.ExecuteResponse) {
	c.jobs.Enqueue(&queue.JobEvent{
		JobName: action.GetJobName(),
		Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteResponse{ExecuteResponse: action},
		},
	})
}

func (c *control) handleDeliverablePrefixes(action *queue.DeliverablePrefixes) {
	jobNames := c.act.DeliverablePrefixes(action.GetPrefixes()...)

	for _, jobName := range jobNames {
		c.jobs.Enqueue(&queue.JobEvent{
			JobName: jobName,
			Action:  &queue.JobAction{Action: new(queue.JobAction_Deliverable)},
		})
	}
}

func (c *control) handleUndeliverablePrefixes(action *queue.UndeliverablePrefixes) {
	c.act.UnDeliverablePrefixes(action.GetPrefixes()...)
}

func (c *control) handleCloseJob(action *queue.CloseJob) {
	c.jobs.Enqueue(&queue.JobEvent{
		JobName: action.GetJobName(),
		Action: &queue.JobAction{Action: &queue.JobAction_CloseJob{
			CloseJob: new(queue.CloseJob),
		}},
	})
}

func (c *control) handleClose() {
	c.jobs.Close(&queue.JobEvent{
		Action: &queue.JobAction{
			Action: &queue.JobAction_Close{
				Close: new(queue.Close),
			},
		},
	})
}
