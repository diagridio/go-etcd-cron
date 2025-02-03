/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_schedule(t *testing.T) {
	t.Parallel()

	t.Run("if no counter, job should not be deleted and no counter created", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		now := time.Now().UTC()
		jobBytes1, err := proto.Marshal(&stored.Job{
			Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(now.Add(time.Hour))},
			PartitionId: 123,
			Job:         &api.Job{DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes1))
		require.NoError(t, err)

		jobBytes2, err := proto.Marshal(&stored.Job{
			Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(now)},
			PartitionId: 123,
			Job:         &api.Job{DueTime: ptr.Of(now.Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/jobs/2", string(jobBytes2))
		require.NoError(t, err)

		resp, err := client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(t, resp.Kvs, 2)

		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    client,
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, cron.Triggered())
		}, 5*time.Second, 10*time.Millisecond)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err = client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
			require.NoError(t, err)
			assert.Len(c, resp.Kvs, 1)
		}, 5*time.Second, 10*time.Millisecond)

		cron.Close()

		resp, err = client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(jobBytes1), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/counters", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		assert.Equal(t, 1, cron.Triggered())
	})

	t.Run("if schedule is not done, job and counter should not be deleted", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		future := time.Now().UTC().Add(time.Hour)
		jobBytes, err := proto.Marshal(&stored.Job{
			Begin: &stored.Job_DueTime{
				DueTime: timestamppb.New(future),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(future.Format(time.RFC3339)),
			},
		})
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(&stored.Counter{
			LastTrigger:    nil,
			Count:          0,
			JobPartitionId: 123,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		now := time.Now().UTC()
		jobBytes2, err := proto.Marshal(&stored.Job{
			Begin: &stored.Job_DueTime{DueTime: timestamppb.New(now)},
			Job:   &api.Job{DueTime: ptr.Of(now.Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/jobs/2", string(jobBytes2))
		require.NoError(t, err)

		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    client,
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, cron.Triggered())
		}, 5*time.Second, 10*time.Millisecond)

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(jobBytes), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(counterBytes), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(t, resp.Kvs, 1)
	})

	t.Run("if schedule is done, expect job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		now := time.Now().UTC()
		jobBytes, err := proto.Marshal(&stored.Job{
			Begin: &stored.Job_DueTime{
				DueTime: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		})
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(&stored.Counter{
			LastTrigger:    timestamppb.New(now),
			Count:          1,
			JobPartitionId: 123,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    client,
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Get(context.Background(), "abc/jobs/1")
			require.NoError(t, err)
			assert.Empty(c, resp.Kvs)
			resp, err = client.Get(context.Background(), "abc/counters/1")
			require.NoError(t, err)
			assert.Empty(c, resp.Kvs)
		}, 5*time.Second, 10*time.Millisecond)

		assert.Equal(t, 0, cron.Triggered())
	})
}

func Test_schedule_six(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		Schedule: ptr.Of("1 2 3 4 5 6"),
	}

	require.NoError(t, cron.API().Add(cron.Context(), "def", job))
}
