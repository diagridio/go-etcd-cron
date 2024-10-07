/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"sync"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_retry(t *testing.T) {
	t.Parallel()

	ok := api.TriggerResponseResult_FAILED
	var lock sync.Mutex
	cron := integration.New(t, integration.Options{
		PartitionTotal: 1,
		TriggerFn: func(*api.TriggerRequest) *api.TriggerResponse {
			lock.Lock()
			defer lock.Unlock()
			return &api.TriggerResponse{Result: ok}
		},
	})

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "yoyo", job))

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Greater(c, cron.Triggered(), 1)
	}, 5*time.Second, 10*time.Millisecond)
	lock.Lock()
	triggered := cron.Triggered()
	triggered++
	ok = api.TriggerResponseResult_SUCCESS
	lock.Unlock()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, triggered, cron.Triggered())
	}, time.Second*10, time.Millisecond*10)
	<-time.After(3 * time.Second)
	assert.Equal(t, triggered, cron.Triggered())
}
