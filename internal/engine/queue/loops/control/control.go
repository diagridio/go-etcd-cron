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
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/queuestate"
)

type Options struct {
	Actioner actioner.Interface
	Router   loop.Interface[*queue.JobEvent]
	State    *queuestate.State
}

// control is the main outer control loop responsible for handling all control
// signals, such as inform, execute, deliverable etc. Appropriate events are
// routed to the router loop for processing. It is the only writer of the
// queue state, since it sees every job event in order.
type control struct {
	router loop.Interface[*queue.JobEvent]
	act    actioner.Interface
	state  *queuestate.State
}

func New(opts Options) loop.Interface[*queue.ControlEvent] {
	return loop.New[*queue.ControlEvent](1024).NewLoop(&control{
		router: opts.Router,
		act:    opts.Actioner,
		state:  opts.State,
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
	c.state.Informed(action.GetQueuedJob().GetModRevision(), action.GetName(), action.GetIsPut())
	c.router.Enqueue(&queue.JobEvent{
		Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{Informed: action},
		},
	})
}

func (c *control) handleExecuteRequest(action *queue.ExecuteRequest) {
	c.state.ExecuteRequest(action.GetModRevision())
	c.router.Enqueue(&queue.JobEvent{
		Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{ExecuteRequest: action},
		},
	})
}

func (c *control) handleExecuteResponse(action *queue.ExecuteResponse) {
	c.state.ExecuteResponse(action.GetModRevision(), action.GetResult().GetResult())
	c.router.Enqueue(&queue.JobEvent{
		Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteResponse{ExecuteResponse: action},
		},
	})
}

func (c *control) handleDeliverablePrefixes(action *queue.DeliverablePrefixes) {
	modRevisions := c.act.DeliverablePrefixes(action.GetPrefixes()...)

	for _, modRevision := range modRevisions {
		c.router.Enqueue(&queue.JobEvent{
			Action: &queue.JobAction{Action: &queue.JobAction_Deliverable{
				Deliverable: &queue.DeliverableJob{
					ModRevision: modRevision,
				},
			}},
		})
	}
}

func (c *control) handleUndeliverablePrefixes(action *queue.UndeliverablePrefixes) {
	c.act.UnDeliverablePrefixes(action.GetPrefixes()...)
}

func (c *control) handleCloseJob(action *queue.CloseJob) {
	c.state.CloseJob(action.GetModRevision())
	c.router.Enqueue(&queue.JobEvent{
		Action: &queue.JobAction{Action: &queue.JobAction_CloseJob{
			CloseJob: &queue.CloseJob{
				ModRevision: action.GetModRevision(),
			},
		}},
	})
}

func (c *control) handleClose() {
	c.router.Close(&queue.JobEvent{
		Action: &queue.JobAction{
			Action: &queue.JobAction_Close{
				Close: new(queue.Close),
			},
		},
	})
}
