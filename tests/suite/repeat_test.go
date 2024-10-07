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

func Test_repeat(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		Schedule: ptr.Of("@every 10ms"),
		Repeats:  ptr.Of(uint32(3)),
	}

	require.NoError(t, cron.API().Add(cron.Context(), "def", job))

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 3, cron.Triggered())
	}, 5*time.Second, 1*time.Second)

	resp, err := cron.Client().Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}
