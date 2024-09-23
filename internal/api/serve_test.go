/*v3Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package api_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/dapr/kit/ptr"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/cron"
)

func Test_Add(t *testing.T) {
	t.Parallel()

	c := cron.SinglePartitionRun(t)

	resp, err := c.KV.Get(context.Background(), "abc", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)

	require.NoError(t, c.Add(context.Background(), "xxx", &api.Job{
		Schedule: ptr.Of("@every 1s"),
	}))

	resp, err = c.KV.Get(context.Background(), "abc/jobs/xxx")
	require.NoError(t, err)
	assert.Len(t, resp.Kvs, 1)
}

func Test_Get(t *testing.T) {
	t.Parallel()

	c := cron.SinglePartitionRun(t)

	resp, err := c.Get(context.Background(), "xxx")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, c.Add(context.Background(), "xxx", &api.Job{
		Schedule: ptr.Of("@every 1s"),
	}))

	resp, err = c.Get(context.Background(), "xxx")
	require.NoError(t, err)
	assert.Equal(t, ptr.Of("@every 1s"), resp.Schedule)
}

func Test_Delete(t *testing.T) {
	t.Parallel()

	t.Run("deleting a job should dequeue it and delete counter", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartitionRun(t)

		require.NoError(t, c.Add(context.Background(), "abc", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.Eventually(t, func() bool {
			return c.Calls.Load() > 0
		}, time.Second*3, time.Millisecond*10)

		assert.EventuallyWithT(t, func(co *assert.CollectT) {
			assert.Len(co, c.Counters(t).Kvs, 1)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, c.Delete(context.Background(), "abc"))

		assert.EventuallyWithT(t, func(co *assert.CollectT) {
			assert.Len(co, c.Counters(t).Kvs, 0)
		}, time.Second*3, time.Millisecond*10)

		current := c.Calls.Load()

		time.Sleep(time.Second * 2)
		assert.Equal(t, current, c.Calls.Load())
	})

	t.Run("deleting jobs in cluster should delete jobs", func(t *testing.T) {
		t.Parallel()

		cr := cron.TripplePartitionRun(t)

		for i := 0; i < 100; i++ {
			require.NoError(t, cr.Add(context.Background(), "a"+strconv.Itoa(i), &api.Job{Schedule: ptr.Of("@every 1s")}))
		}

		assert.Len(t, cr.Jobs(t).Kvs, 100)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 100)
		}, time.Second*10, time.Millisecond*10)

		for i := 0; i < 100; i++ {
			require.NoError(t, cr.Delete(context.Background(), "a"+strconv.Itoa(i)))
		}

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*10, time.Millisecond*10)

		calls := cr.Calls.Load()
		time.Sleep(time.Second * 2)
		assert.Equal(t, calls, cr.Calls.Load())
	})
}

func Test_DeletePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("deleting a job should dequeue it and delete counter", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(context.Background(), "a1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))
		require.NoError(t, cr.Add(context.Background(), "b2", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.Len(t, cr.Jobs(t).Kvs, 2)

		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			assert.Greater(t, cr.Calls.Load(), int64(0))
		}, time.Second*3, time.Millisecond*10)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			counters := cr.Counters(t)
			if assert.Len(c, counters.Kvs, 2) {
				assert.ElementsMatch(t,
					[]string{"abc/counters/a1", "abc/counters/b2"},
					[]string{string(counters.Kvs[0].Key), string(counters.Kvs[1].Key)},
				)
			}
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background(), "a"))

		assert.Len(t, cr.Jobs(t).Kvs, 1)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 1)
		}, time.Second*3, time.Millisecond*10)
	})

	t.Run("deleting with empty string should delete all jobs", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(context.Background(), "a1", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "b2", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "c3", &api.Job{Schedule: ptr.Of("@every 1s")}))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 3)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background(), ""))

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*3, time.Millisecond*10)
	})

	t.Run("deleting all with prefix in cluster should delete all jobs with prefix", func(t *testing.T) {
		t.Parallel()

		cr := cron.TripplePartitionRun(t)

		for i := 0; i < 100; i++ {
			require.NoError(t, cr.Add(context.Background(), "a"+strconv.Itoa(i), &api.Job{Schedule: ptr.Of("@every 1s")}))
		}

		assert.Len(t, cr.Jobs(t).Kvs, 100)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 100)
		}, time.Second*10, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background(), "a"))

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*10, time.Millisecond*10)

		calls := cr.Calls.Load()
		time.Sleep(time.Second * 2)
		assert.Equal(t, calls, cr.Calls.Load())
	})

	t.Run("deleting no prefixes should delete nothing", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(context.Background(), "a1", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "b2", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "c3", &api.Job{Schedule: ptr.Of("@every 1s")}))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 3)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background()))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.Len(t, cr.Counters(t).Kvs, 3)
	})
}

func Test_List(t *testing.T) {
	t.Parallel()

	t.Run("List with no jobs should return empty", func(t *testing.T) {
		t.Parallel()

		cron := cron.SinglePartitionRun(t)

		resp, err := cron.List(context.Background(), "")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs)
	})

	t.Run("List should return jobs which are in the namespace", func(t *testing.T) {
		t.Parallel()

		cron := cron.SinglePartitionRun(t)

		resp, err := cron.List(context.Background(), "")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs)

		now := time.Now()
		require.NoError(t, cron.Add(context.Background(), "a123", &api.Job{
			DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		}))
		require.NoError(t, cron.Add(context.Background(), "a345", &api.Job{
			DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		}))

		resp, err = cron.List(context.Background(), "")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 2)
		resp, err = cron.List(context.Background(), "a")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 2)
		resp, err = cron.List(context.Background(), "a1")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 1)
		resp, err = cron.List(context.Background(), "a123")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 1)
		resp, err = cron.List(context.Background(), "a345")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 1)
		resp, err = cron.List(context.Background(), "1")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs)
		resp, err = cron.List(context.Background(), "b123")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs)

		require.NoError(t, cron.Delete(context.Background(), "a123"))
		resp, err = cron.List(context.Background(), "a")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 1)
		resp, err = cron.List(context.Background(), "a123")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs, 0)
		resp, err = cron.List(context.Background(), "a345")
		require.NoError(t, err)
		assert.Len(t, resp.Jobs, 1)

		require.NoError(t, cron.Delete(context.Background(), "a345"))
		resp, err = cron.List(context.Background(), "a")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs, 0)
		resp, err = cron.List(context.Background(), "a123")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs, 0)
		resp, err = cron.List(context.Background(), "a345")
		require.NoError(t, err)
		assert.Empty(t, resp.Jobs, 0)
	})
}
