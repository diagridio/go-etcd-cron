/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func SinglePartition(t *testing.T) *Cron {
	t.Helper()
	return newCron(t, etcd.EmbeddedBareClient(t), "0")
}

func SinglePartitionRun(t *testing.T) *Cron {
	t.Helper()
	return SinglePartition(t).run(t)
}
