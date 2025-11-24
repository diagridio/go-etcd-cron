/*
Copyright (c) 2025 Diagrid Inc.
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

func Test_deleteprefixes(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second * 2).Format(time.RFC3339)),
	}

	require.NoError(t, cron.API().Add(cron.Context(), "def||abc", job))
	got, err := cron.API().Get(cron.Context(), "def||abc")
	require.NoError(t, err)
	assert.NotNil(t, got)

	require.NoError(t, cron.API().DeletePrefixes(cron.Context(), "def"))
	got, err = cron.API().Get(cron.Context(), "def||abc")
	require.NoError(t, err)
	assert.Nil(t, got)

	require.NoError(t, cron.API().Add(cron.Context(), "def||abc", job))
	got, err = cron.API().Get(cron.Context(), "def||abc")
	require.NoError(t, err)
	assert.NotNil(t, got)

	require.NoError(t, cron.API().DeletePrefixes(cron.Context(), "def||"))
	got, err = cron.API().Get(cron.Context(), "def||abc")
	require.NoError(t, err)
	assert.Nil(t, got)
}
