/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package api_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
)

func Test_Add(t *testing.T) {
	t.Parallel()

	c := cron.SinglePartitionRun(t)

	resp, err := c.KV.Get(t.Context(), "abc", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)

	require.NoError(t, c.Add(t.Context(), "xxx", &api.Job{
		Schedule: ptr.Of("@every 1s"),
	}))

	resp, err = c.KV.Get(t.Context(), "abc/jobs/xxx")
	require.NoError(t, err)
	assert.Len(t, resp.Kvs, 1)
}

func Test_Get(t *testing.T) {
	t.Parallel()

	c := cron.SinglePartitionRun(t)

	resp, err := c.Get(t.Context(), "xxx")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, c.Add(t.Context(), "xxx", &api.Job{
		Schedule: ptr.Of("@every 1s"),
	}))

	resp, err = c.Get(t.Context(), "xxx")
	require.NoError(t, err)
	assert.Equal(t, "@every 1s", resp.GetSchedule())
}

func Test_Delete(t *testing.T) {
	t.Parallel()

	t.Run("deleting a job should dequeue it and delete counter", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartitionRun(t)

		require.NoError(t, c.Add(t.Context(), "abc", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.Eventually(t, func() bool {
			return c.Calls.Load() > 0
		}, time.Second*3, time.Millisecond*10)

		assert.EventuallyWithT(t, func(co *assert.CollectT) {
			assert.Len(co, c.Counters(t).Kvs, 1)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, c.Delete(t.Context(), "abc"))

		assert.EventuallyWithT(t, func(co *assert.CollectT) {
			assert.Empty(co, c.Counters(t).Kvs)
		}, time.Second*3, time.Millisecond*10)

		current := c.Calls.Load()

		time.Sleep(time.Second * 2)
		assert.Equal(t, current, c.Calls.Load())
	})

	t.Run("deleting jobs in cluster should delete jobs", func(t *testing.T) {
		t.Parallel()

		cr := cron.TripplePartitionRun(t)

		for i := range 100 {
			require.NoError(t, cr.Add(t.Context(), "a"+strconv.Itoa(i), &api.Job{Schedule: ptr.Of("@every 1s")}))
		}

		assert.Len(t, cr.Jobs(t).Kvs, 100)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 100)
		}, time.Second*10, time.Millisecond*10)

		for i := range 100 {
			require.NoError(t, cr.Delete(t.Context(), "a"+strconv.Itoa(i)))
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

		require.NoError(t, cr.Add(t.Context(), "a1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))
		require.NoError(t, cr.Add(t.Context(), "b2", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.Len(t, cr.Jobs(t).Kvs, 2)

		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			assert.Positive(t, cr.Calls.Load())
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

		require.NoError(t, cr.DeletePrefixes(t.Context(), "a"))

		assert.Len(t, cr.Jobs(t).Kvs, 1)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 1)
		}, time.Second*3, time.Millisecond*10)
	})

	t.Run("deleting with empty string should delete all jobs", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(t.Context(), "a1", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(t.Context(), "b2", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(t.Context(), "c3", &api.Job{Schedule: ptr.Of("@every 1s")}))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 3)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(t.Context(), ""))

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*3, time.Millisecond*10)
	})

	t.Run("deleting all with prefix in cluster should delete all jobs with prefix", func(t *testing.T) {
		t.Parallel()

		cr := cron.TripplePartitionRun(t)

		for i := range 100 {
			require.NoError(t, cr.Add(t.Context(), "a"+strconv.Itoa(i), &api.Job{Schedule: ptr.Of("@every 1s")}))
		}

		assert.Len(t, cr.Jobs(t).Kvs, 100)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 100)
		}, time.Second*10, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(t.Context(), "a"))

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

		require.NoError(t, cr.Add(t.Context(), "a1", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(t.Context(), "b2", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(t.Context(), "c3", &api.Job{Schedule: ptr.Of("@every 1s")}))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 3)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(t.Context()))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.Len(t, cr.Counters(t).Kvs, 3)
	})
}

func Test_List(t *testing.T) {
	t.Parallel()

	t.Run("List with no jobs should return empty", func(t *testing.T) {
		t.Parallel()

		cron := cron.SinglePartitionRun(t)

		resp, err := cron.List(t.Context(), "")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())
	})

	t.Run("List should return jobs which are in the namespace", func(t *testing.T) {
		t.Parallel()

		cron := cron.SinglePartitionRun(t)

		resp, err := cron.List(t.Context(), "")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())

		now := time.Now()
		require.NoError(t, cron.Add(t.Context(), "a123", &api.Job{
			DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		}))
		require.NoError(t, cron.Add(t.Context(), "a345", &api.Job{
			DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		}))

		resp, err = cron.List(t.Context(), "")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 2)
		resp, err = cron.List(t.Context(), "a")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 2)
		resp, err = cron.List(t.Context(), "a1")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 1)
		resp, err = cron.List(t.Context(), "a123")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 1)
		resp, err = cron.List(t.Context(), "a345")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 1)
		resp, err = cron.List(t.Context(), "1")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())
		resp, err = cron.List(t.Context(), "b123")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())

		require.NoError(t, cron.Delete(t.Context(), "a123"))
		resp, err = cron.List(t.Context(), "a")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 1)
		resp, err = cron.List(t.Context(), "a123")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())
		resp, err = cron.List(t.Context(), "a345")
		require.NoError(t, err)
		assert.Len(t, resp.GetJobs(), 1)

		require.NoError(t, cron.Delete(t.Context(), "a345"))
		resp, err = cron.List(t.Context(), "a")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())
		resp, err = cron.List(t.Context(), "a123")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())
		resp, err = cron.List(t.Context(), "a345")
		require.NoError(t, err)
		assert.Empty(t, resp.GetJobs())
	})
}
