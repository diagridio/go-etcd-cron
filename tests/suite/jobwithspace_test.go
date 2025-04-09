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

func Test_jobWithSpace(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	require.NoError(t, cron.API().Add(t.Context(), "hello world", &api.Job{
		DueTime: ptr.Of(time.Now().Add(2).Format(time.RFC3339)),
	}))
	resp, err := cron.API().Get(t.Context(), "hello world")
	require.NoError(t, err)
	assert.NotNil(t, resp)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, cron.Triggered())
		resp, err = cron.API().Get(t.Context(), "hello world")
		assert.NoError(c, err)
		assert.Nil(c, resp)
	}, time.Second*10, time.Millisecond*10)

	require.NoError(t, cron.API().Add(t.Context(), "another hello world", &api.Job{
		Schedule: ptr.Of("@every 1s"),
	}))
	resp, err = cron.API().Get(t.Context(), "another hello world")
	require.NoError(t, err)
	assert.NotNil(t, resp)
	listresp, err := cron.API().List(t.Context(), "")
	require.NoError(t, err)
	assert.Len(t, listresp.GetJobs(), 1)
	require.NoError(t, cron.API().Delete(t.Context(), "another hello world"))
	resp, err = cron.API().Get(t.Context(), "another hello world")
	require.NoError(t, err)
	assert.Nil(t, resp)
	listresp, err = cron.API().List(t.Context(), "")
	require.NoError(t, err)
	assert.Empty(t, listresp.GetJobs())
}
