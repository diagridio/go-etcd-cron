/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_patition(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 100)

	for i := range 100 {
		job := &api.Job{
			DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
		}
		require.NoError(t, cron.AllCrons()[i].Add(cron.Context(), "test-"+strconv.Itoa(i), job))
	}

	assert.Eventually(t, func() bool {
		return cron.Triggered() == 100
	}, 5*time.Second, 1*time.Second)

	resp, err := cron.Client().Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}
