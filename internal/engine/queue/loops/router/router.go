/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package router

import (
	"context"
	"hash/fnv"

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
		h := fnv.New32a()
		h.Write([]byte(event.GetJobName()))
		r.workers[int(h.Sum32())%len(r.workers)].Enqueue(event)
	}

	return nil
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
