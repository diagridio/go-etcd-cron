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
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
)

func Test_cron_stopping(t *testing.T) {
	t.Parallel()

	cr := cron.SinglePartitionRun(t)
	cr.Stop(t)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cr.KV.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Empty(c, resp.Kvs)
		assert.Equal(c, 0, int(resp.Count))
	}, time.Second*5, time.Millisecond*10)
}

func Test_cron_cluster_stopping(t *testing.T) {
	t.Parallel()

	cr := cron.TripplePartitionRun(t)
	cr.Stop(t)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cr.KV.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Empty(c, resp.Kvs)
		assert.Equal(c, 0, int(resp.Count))
	}, time.Second*5, time.Millisecond*10)
}
