/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
)

func Test_single_instance_leadership(t *testing.T) {
	t.Parallel()

	cr := cron.SinglePartitionRun(t)
	defer cr.Stop(t)

	leaderBytes, err := proto.Marshal(&stored.Leadership{
		Total: 1,
	})
	require.NoError(t, err)

	_, err = cr.KV.Do(context.Background(), clientv3.OpPut("abc/leadership/0", string(leaderBytes)))
	require.NoError(t, err, "Failed to write leadership key with one instance")

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cr.KV.Do(context.Background(), clientv3.OpGet("abc/leadership/0"))
		require.NoError(t, err)
		require.NotNil(t, resp)
	}, 4*time.Second, 10*time.Millisecond)
}

func Test_three_instances_leadership(t *testing.T) {
	t.Parallel()

	cluster := cron.TripplePartitionRun(t)
	defer func() {
		for _, cr := range cluster.Crons {
			cr.Stop(t)
		}
	}()

	leaderBytes, err := proto.Marshal(&stored.Leadership{
		Total: 3,
	})
	require.NoError(t, err)

	for i, cr := range cluster.Crons {
		_, err := cr.KV.Do(context.Background(), clientv3.OpPut(fmt.Sprintf("abc/leadership/%d", i), string(leaderBytes)))
		require.NoError(t, err, "Failed to write leadership key for instance %d", i)
	}

	// Ensure leadership key is active
	for i, cr := range cluster.Crons {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cr.KV.Do(context.Background(), clientv3.OpGet(fmt.Sprintf("abc/leadership/%d", i)))
			require.NoError(t, err, "Failed to retrieve leadership key for instance %d", i)
			require.NotNil(t, resp, "Leadership key for instance %d should exist", i)
		}, 4*time.Second, 10*time.Millisecond)
	}
}
