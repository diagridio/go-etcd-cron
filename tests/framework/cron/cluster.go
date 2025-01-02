/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

type Cluster struct {
	*Cron
	Crons [3]*Cron
}

func TripplePartition(t *testing.T) *Cluster {
	t.Helper()

	client := etcd.EmbeddedBareClient(t)

	cr1 := newCron(t, client, "0")
	cr2 := newCron(t, client, "1")
	cr3 := newCron(t, client, "2")
	return &Cluster{
		Cron:  cr1,
		Crons: [3]*Cron{cr1, cr2, cr3},
	}
}

func TripplePartitionRun(t *testing.T) *Cluster {
	t.Helper()
	crs := TripplePartition(t)
	for _, cr := range crs.Crons {
		cr.run(t)
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, cr := range crs.Crons {
			assert.True(c, cr.IsElected())
		}
	}, time.Second*5, time.Millisecond)

	return crs
}

func (c *Cluster) Stop(t *testing.T) {
	t.Helper()

	for _, cr := range c.Crons {
		if cr != nil {
			cr.Stop(t)
		}
	}
}
