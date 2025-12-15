/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package router

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"

	"github.com/dapr/kit/events/loop"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
)

type Options struct {
	Log     logr.Logger
	Workers []loop.Interface[*queue.JobEvent]
}

// router is a loop to handle Job events. It routes the event to the correct
// counter loop. It creates and runs counter loops at inform instantiate time,
// and garbage collects them after close.
type router struct {
	log     logr.Logger
	workers []loop.Interface[*queue.JobEvent]
}

func New(opts Options) loop.Interface[*queue.JobEvent] {
	return loop.New[*queue.JobEvent](1024).NewLoop(&router{
		log:     opts.Log.WithName("router"),
		workers: opts.Workers,
	})
}

func (r *router) Handle(ctx context.Context, event *queue.JobEvent) error {
	switch event.GetAction().Action.(type) {
	case *queue.JobAction_Close:
		r.handleClose()

	default:
		r.workers[r.modRevisionFromEvent(event)%int64(len(r.workers))].Enqueue(event)
	}

	return nil
}

func (r *router) modRevisionFromEvent(event *queue.JobEvent) int64 {
	switch action := event.GetAction().Action.(type) {
	case *queue.JobAction_Informed:
		return action.Informed.GetQueuedJob().GetModRevision()
	case *queue.JobAction_ExecuteRequest:
		return action.ExecuteRequest.GetModRevision()
	case *queue.JobAction_ExecuteResponse:
		return action.ExecuteResponse.GetModRevision()
	case *queue.JobAction_Deliverable:
		return action.Deliverable.GetModRevision()
	case *queue.JobAction_CloseJob:
		return action.CloseJob.GetModRevision()
	default:
		panic(fmt.Sprintf("unknown action type: %T", event.GetAction().Action))
	}
}

func (r *router) handleClose() {
	action := &queue.JobEvent{
		Action: &queue.JobAction{
			Action: new(queue.JobAction_Close),
		},
	}
	for _, w := range r.workers {
		w.Close(action)
	}
}
