/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/tests"
)

func SinglePartition(t *testing.T) *Cron {
	t.Helper()
	return newCron(t, tests.EmbeddedETCDBareClient(t), 1, 0)
}

func SinglePartitionRun(t *testing.T) *Cron {
	t.Helper()
	return SinglePartition(t).run(t)
}
