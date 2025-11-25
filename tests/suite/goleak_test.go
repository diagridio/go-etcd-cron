/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

// Test cannot be done in parallel to ensure runtime go routines are correct
// because we are running in the same process as all the other tests.
func Test_goleak(t *testing.T) {
	runtime.GC()
	cron := integration.NewBase(t, 1)

	// Create a job and wait for it to trigger to settle the number of
	// goroutines.
	require.NoError(t, cron.API().Add(cron.Context(), "abc", &api.Job{
		DueTime: ptr.Of("0s"),
	}))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, cron.Triggered())
	}, 10*time.Second, time.Millisecond*10)

	const n = 10000
	for i := range n {
		require.NoError(t, cron.API().Add(cron.Context(), strconv.Itoa(i), &api.Job{
			DueTime: ptr.Of("0s"),
		}))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, n+1, cron.Triggered())
	}, 10*time.Second, time.Millisecond*10)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// We have a stable number of go routines.
		assert.InDelta(c, 133, runtime.NumGoroutine(), 10)
	}, 10*time.Second, time.Millisecond*10)
}

func Test_goleak_3(t *testing.T) {
	runtime.GC()
	cron := integration.NewBase(t, 3)

	assert.Eventually(t, cron.API().IsElected, time.Second*10, time.Millisecond*10)

	require.NoError(t, cron.API().Add(cron.Context(), "abc", &api.Job{
		DueTime: ptr.Of("0s"),
	}))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, cron.Triggered())
	}, 10*time.Second, time.Millisecond*10)

	const n = 10000
	for i := range n {
		require.NoError(t, cron.API().Add(t.Context(), strconv.Itoa(i), &api.Job{
			DueTime: ptr.Of("0s"),
		}))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, n+1, cron.Triggered())
	}, 10*time.Second, time.Millisecond*10)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// We have a stable number of go routines.
		assert.InDelta(c, 289, runtime.NumGoroutine(), 10)
	}, 10*time.Second, time.Millisecond*10)
}
