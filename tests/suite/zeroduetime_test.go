/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_zeroDueTime(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	require.NoError(t, cron.API().Add(cron.Context(), "yoyo", &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of("0s"),
	}))
	assert.Eventually(t, func() bool {
		return cron.Triggered() == 1
	}, 3*time.Second, time.Millisecond*10)

	require.NoError(t, cron.API().Add(cron.Context(), "yoyo2", &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of("1s"),
	}))
	assert.Eventually(t, func() bool {
		return cron.Triggered() == 2
	}, 3*time.Second, time.Millisecond*10)

	require.NoError(t, cron.API().Add(cron.Context(), "yoyo3", &api.Job{
		Schedule: ptr.Of("@every 1h"),
	}))
	<-time.After(2 * time.Second)
	assert.Equal(t, 2, cron.Triggered())
}
