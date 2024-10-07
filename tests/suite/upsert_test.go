/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_upsert(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))
	job = &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))

	assert.Eventually(t, func() bool {
		return cron.Triggered() == 1
	}, 5*time.Second, 1*time.Second)

	resp, err := cron.Client().Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}
