/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package consumer

import (
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
)

type Options struct {
	Sink chan<- *api.InformerEvent
}

// Consumer is responsible for optionally sending DB informer and trigger
// events to the Consumer Informer Sink.
type Consumer struct {
	sink chan<- *api.InformerEvent
}

func New(opts Options) *Consumer {
	return &Consumer{
		sink: opts.Sink,
	}
}

func (c *Consumer) Put(name string, job *queue.QueuedJob) {
	if c.sink == nil {
		return
	}

	c.sink <- &api.InformerEvent{
		Event: &api.InformerEvent_Put{
			Put: &api.InformerEventJobPut{
				Name:        name,
				ModRevision: job.GetModRevision(),
				Metadata:    job.GetStored().GetJob().GetMetadata(),
				Payload:     job.GetStored().GetJob().GetPayload(),
			},
		},
	}
}

func (c *Consumer) Delete(modRevision int64) {
	if c.sink == nil {
		return
	}

	c.sink <- &api.InformerEvent{
		Event: &api.InformerEvent_Delete{
			Delete: &api.InformerEventJobDelete{
				ModRevision: modRevision,
			},
		},
	}
}

func (c *Consumer) DropAll() {
	if c.sink == nil {
		return
	}

	c.sink <- &api.InformerEvent{
		Event: &api.InformerEvent_DropAll{
			DropAll: new(api.InformerEventDropAll),
		},
	}
}
