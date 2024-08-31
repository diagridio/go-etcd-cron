/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/tests"
)

type Cluster struct {
	*Cron
	crons [3]*Cron
}

func TripplePartition(t *testing.T) *Cluster {
	t.Helper()
	client := tests.EmbeddedETCDBareClient(t)
	cr1 := newCron(t, client, 3, 0)
	cr2 := newCron(t, client, 3, 1)
	cr3 := newCron(t, client, 3, 2)
	return &Cluster{
		Cron:  cr1,
		crons: [3]*Cron{cr1, cr2, cr3},
	}
}

func TripplePartitionRun(t *testing.T) *Cluster {
	t.Helper()
	crs := TripplePartition(t)
	for _, cr := range crs.crons {
		cr.run(t)
	}
	return crs
}
