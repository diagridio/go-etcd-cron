/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
)

func Test_cron_starting(t *testing.T) {
	t.Parallel()

	cr := cron.SinglePartitionRun(t)
	defer cr.Stop(t)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cr.KV.Get(context.Background(), "abc/leadership/", clientv3.WithPrefix())
		assert.NoError(c, err, "unexpected error querying etcd")
		assert.Equal(c, 1, len(resp.Kvs), "expected 1 leadership key, but got %d", len(resp.Kvs))
	}, 3*time.Second, 100*time.Millisecond)
}

func Test_cron_cluster_starting(t *testing.T) {
	t.Parallel()

	cr := cron.TripplePartitionRun(t)
	defer cr.Stop(t)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cr.KV.Get(context.Background(), "abc/leadership/", clientv3.WithPrefix())
		assert.NoError(c, err, "unexpected error querying etcd")
		assert.Equal(c, 3, len(resp.Kvs), "expected 3 leadership keys, but got %d", len(resp.Kvs))
	}, 5*time.Second, 100*time.Millisecond)
}
