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

func Test_remove(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second * 2).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))
	require.NoError(t, cron.API().Delete(cron.Context(), "def"))

	<-time.After(3 * time.Second)

	assert.Equal(t, 0, cron.Triggered())
}
