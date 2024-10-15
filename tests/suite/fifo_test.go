/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_fifo(t *testing.T) {
	t.Parallel()

	t.Run("single: should execute job on Add based on FIFO", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)
		job := &api.Job{DueTime: ptr.Of(time.Now().Format(time.RFC3339))}

		require.NoError(t, cron.API().Add(cron.Context(), "def", job))
		require.NoError(t, cron.API().Delete(cron.Context(), "def"))
		time.Sleep(time.Second * 2)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, 1, cron.Triggered())
		}, 10*time.Second, time.Millisecond*10)
	})

	t.Run("multiple: should execute job on Add based on FIFO", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 10)
		var wg sync.WaitGroup
		wg.Add(10)
		for i := range 10 {
			go func(i int) {
				defer wg.Done()
				assert.NoError(t, cron.API().Add(
					cron.Context(),
					strconv.Itoa(i),
					&api.Job{DueTime: ptr.Of(time.Now().Format(time.RFC3339))},
				))
				assert.NoError(t, cron.API().Delete(cron.Context(), strconv.Itoa(i)))
			}(i)
		}

		wg.Wait()
		time.Sleep(time.Second * 2)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, 10, cron.Triggered())
		}, 10*time.Second, time.Millisecond*10)
	})
}
