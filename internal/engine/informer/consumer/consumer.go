/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package consumer

import (
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
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

func (c *Consumer) Put(name string, job *stored.Job) {
	if c.sink == nil {
		return
	}

	c.sink <- &api.InformerEvent{
		Event: &api.InformerEvent_Put{
			Put: &api.InformerEventJob{
				Name:     name,
				Metadata: job.GetJob().GetMetadata(),
				Payload:  job.GetJob().GetPayload(),
			},
		},
	}
}

func (c *Consumer) Delete(name string, job *stored.Job) {
	if c.sink == nil {
		return
	}

	c.sink <- &api.InformerEvent{
		Event: &api.InformerEvent_Delete{
			Delete: &api.InformerEventJob{
				Name:     name,
				Metadata: job.GetJob().GetMetadata(),
				Payload:  job.GetJob().GetPayload(),
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
