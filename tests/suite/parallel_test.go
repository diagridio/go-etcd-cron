/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_parallel(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		total uint32
	}{
		{"1 queue", 1},
		{"multi queue", 50},
	} {
		total := test.total
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			releaseCh := make(chan struct{})
			var waiting atomic.Int32
			var done atomic.Int32
			cron := integration.New(t, integration.Options{
				PartitionTotal: total,
				TriggerFn: func(*api.TriggerRequest) *api.TriggerResponse {
					waiting.Add(1)
					<-releaseCh
					done.Add(1)
					return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
				},
			})

			for i := range 100 {
				require.NoError(t, cron.API().Add(cron.Context(), strconv.Itoa(i), &api.Job{
					DueTime: ptr.Of("0s"),
				}))
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, int32(100), waiting.Load())
			}, 5*time.Second, 10*time.Millisecond)
			close(releaseCh)
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, int32(100), done.Load())
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
