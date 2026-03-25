/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_closejob_rapid_add_delete(t *testing.T) {
	t.Parallel()

	t.Run("add then immediately delete many jobs does not crash", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		for i := range 100 {
			name := "rapid-" + strconv.Itoa(i)
			job := &api.Job{
				DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
			}
			require.NoError(t, cron.API().Add(cron.Context(), name, job))
			require.NoError(t, cron.API().Delete(cron.Context(), name))
		}

		job := &api.Job{
			DueTime: ptr.Of("0s"),
		}
		require.NoError(t, cron.API().Add(cron.Context(), "health-check", job))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, cron.Triggered(), 1)
		}, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("rapid upsert same job name does not crash", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		for i := range 50 {
			job := &api.Job{
				DueTime: ptr.Of(time.Now().Add(time.Duration(i+1) * time.Minute).Format(time.RFC3339)),
			}
			require.NoError(t, cron.API().Add(cron.Context(), "upsert-same", job))
		}

		job := &api.Job{
			DueTime: ptr.Of("0s"),
		}
		require.NoError(t, cron.API().Add(cron.Context(), "health-after-upsert", job))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, cron.Triggered(), 1)
		}, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("delete non-existent job does not crash", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		require.NoError(t, cron.API().Delete(cron.Context(), "never-existed"))

		job := &api.Job{
			DueTime: ptr.Of("0s"),
		}
		require.NoError(t, cron.API().Add(cron.Context(), "still-works", job))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 1, cron.Triggered())
		}, 5*time.Second, 10*time.Millisecond)
	})
}

func Test_closejob_prefix_delete_does_not_crash(t *testing.T) {
	t.Parallel()

	t.Run("delete prefixes for jobs that were already individually deleted", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		for i := range 10 {
			job := &api.Job{
				DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
			}
			require.NoError(t, cron.API().Add(cron.Context(), "ns||"+strconv.Itoa(i), job))
		}

		for i := range 10 {
			require.NoError(t, cron.API().Delete(cron.Context(), "ns||"+strconv.Itoa(i)))
		}

		require.NoError(t, cron.API().DeletePrefixes(cron.Context(), "ns||"))

		job := &api.Job{
			DueTime: ptr.Of("0s"),
		}
		require.NoError(t, cron.API().Add(cron.Context(), "still-alive", job))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, cron.Triggered(), 1)
		}, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("delete prefixes while jobs are being added under same prefix", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		for i := range 20 {
			job := &api.Job{
				DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
			}
			require.NoError(t, cron.API().Add(cron.Context(), "churn||"+strconv.Itoa(i), job))
		}

		require.NoError(t, cron.API().DeletePrefixes(cron.Context(), "churn||"))

		for i := range 20 {
			job := &api.Job{
				DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
			}
			require.NoError(t, cron.API().Add(cron.Context(), "churn||new-"+strconv.Itoa(i), job))
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().List(cron.Context(), "churn||")
			require.NoError(t, err)
			assert.Len(c, resp.GetJobs(), 20)
		}, 5*time.Second, 10*time.Millisecond)
	})
}

func Test_closejob_scheduler_remains_healthy_after_churn(t *testing.T) {
	t.Parallel()

	t.Run("high volume add-delete-add cycle with triggers", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		const n = 50
		for i := range n {
			job := &api.Job{
				DueTime: ptr.Of("0s"),
			}
			require.NoError(t, cron.API().Add(cron.Context(), "lifecycle-"+strconv.Itoa(i), job))
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, n, cron.Triggered())
		}, 10*time.Second, 10*time.Millisecond)

		for i := range n {
			job := &api.Job{
				DueTime: ptr.Of("0s"),
			}
			require.NoError(t, cron.API().Add(cron.Context(), "lifecycle-round2-"+strconv.Itoa(i), job))
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, n*2, cron.Triggered())
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("multi-instance cluster survives rapid churn", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 3)

		for i := range 30 {
			name := "cluster-churn-" + strconv.Itoa(i)
			job := &api.Job{
				DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
			}
			require.NoError(t, cron.API().Add(cron.Context(), name, job))
			require.NoError(t, cron.API().Delete(cron.Context(), name))
		}

		require.NoError(t, cron.API().Add(cron.Context(), "cluster-ok", &api.Job{
			DueTime: ptr.Of("0s"),
		}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, cron.Triggered(), 1)
		}, 5*time.Second, 10*time.Millisecond)
	})
}
