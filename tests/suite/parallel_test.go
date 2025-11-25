/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/concurrency/slice"
	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_parallel(t *testing.T) {
	t.Parallel()

	const jobsN = 100

	for _, test := range []struct {
		name  string
		total uint64
	}{
		{"1 queue", 1},
		{"multi queue", 50},
	} {
		total := test.total
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fns := slice.New[func(*api.TriggerResponse)]()
			cron := integration.New(t, integration.Options{
				Instances: total,
				TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
					fns.Append(fn)
				},
			})

			for i := range jobsN {
				require.NoError(t, cron.API().Add(cron.Context(), strconv.Itoa(i), &api.Job{
					DueTime: ptr.Of("0s"),
				}))
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, jobsN, fns.Len())
			}, 5*time.Second, 10*time.Millisecond)

			resp, err := cron.API().List(t.Context(), "")
			require.NoError(t, err)
			assert.Len(t, resp.Jobs, jobsN)

			for _, fn := range fns.Slice() {
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				resp, err = cron.API().List(t.Context(), "")
				require.NoError(t, err)
				assert.Empty(c, resp.Jobs)
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
