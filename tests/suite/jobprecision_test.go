/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/ptr"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_precision(t *testing.T) {
	t.Parallel()

	t.Run("Running jobs with second precision", func(t *testing.T) {
		cron := integration.NewBase(t, 1)

		job := &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(3)),
		}

		require.NoError(t, cron.API().Add(cron.Context(), "def", job))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 3, cron.Triggered())
		}, 5*time.Second, 1*time.Second)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.Client().Get(t.Context(), "abc/jobs/def")
			require.NoError(t, err)
			assert.Empty(c, resp.Kvs)
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("Running jobs with millisecond precision", func(t *testing.T) {
		cron := integration.NewBase(t, 1)

		job := &api.Job{
			Schedule: ptr.Of("@every 100ms"),
			Repeats:  ptr.Of(uint32(3)),
		}

		require.NoError(t, cron.API().Add(cron.Context(), "def", job))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 3, cron.Triggered())
		}, 500*time.Millisecond, 10*time.Millisecond)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.Client().Get(t.Context(), "abc/jobs/def")
			require.NoError(t, err)
			assert.Empty(c, resp.Kvs)
		}, 2*time.Second, 10*time.Millisecond)
	})
}
