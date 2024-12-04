/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func Test_cron_stopping(t *testing.T) {
	t.Parallel()

	cr := cron.SinglePartitionRun(t)
	defer cr.Stop(t)

	cr.Stop(t)
	time.Sleep(time.Second)

	resp, err := cr.KV.Do(context.Background(), clientv3.OpGet("abc/leadership/0"))
	require.Empty(t, resp.Get().Kvs)
	require.Equal(t, 0, int(resp.Get().Count))
	require.NoError(t, err)
}

func Test_cron_cluster_stopping(t *testing.T) {
	t.Parallel()

	cr := cron.TripplePartitionRun(t)

	defer cr.Stop(t)

	cr.Stop(t)
	time.Sleep(time.Second)

	resp, err := cr.Cron.KV.Do(context.Background(), clientv3.OpGet("abc/leadership/0"))
	require.Empty(t, resp.Get().Kvs)
	require.Equal(t, 0, int(resp.Get().Count))
	require.NoError(t, err)
}
