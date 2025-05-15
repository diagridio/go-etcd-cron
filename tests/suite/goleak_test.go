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

// Test cannot be done in parallel to ensure runtime go routines are correct.
func Test_goleak(t *testing.T) {
	cron := integration.NewBase(t, 1)

	startGoN := runtime.NumGoroutine()

	const n = 10000
	for i := range n {
		require.NoError(t, cron.API().Add(cron.Context(), strconv.Itoa(i), &api.Job{
			DueTime: ptr.Of("0s"),
		}))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, n, cron.Triggered())
	}, 10*time.Second, time.Millisecond*10)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.InDelta(c, startGoN, runtime.NumGoroutine(), 10)
	}, 10*time.Second, time.Millisecond*10)
}
