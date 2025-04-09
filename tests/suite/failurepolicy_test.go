/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_FailurePolicy(t *testing.T) {
	t.Parallel()

	t.Run("default policy should retry 3 times with a 1sec delay", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    etcd.EmbeddedBareClient(t),
			TriggerFn: func(*api.TriggerRequest) *api.TriggerResponse {
				assert.GreaterOrEqual(t, uint32(8), got.Add(1))
				return &api.TriggerResponse{Result: api.TriggerResponseResult_FAILED}
			},
			GotCh: gotCh,
		})

		require.NoError(t, cron.API().Add(t.Context(), "test", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(2)),
		}))

		for range 8 {
			resp, err := cron.API().Get(t.Context(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(t.Context(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("drop policy should not retry triggering", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    etcd.EmbeddedBareClient(t),
			TriggerFn: func(*api.TriggerRequest) *api.TriggerResponse {
				assert.GreaterOrEqual(t, uint32(2), got.Add(1))
				return &api.TriggerResponse{Result: api.TriggerResponseResult_FAILED}
			},
			GotCh: gotCh,
		})

		require.NoError(t, cron.API().Add(t.Context(), "test", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(2)),
			FailurePolicy: &api.FailurePolicy{
				Policy: new(api.FailurePolicy_Drop),
			},
		}))

		for range 2 {
			resp, err := cron.API().Get(t.Context(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(t.Context(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("constant policy should only retry when it fails ", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    etcd.EmbeddedBareClient(t),
			TriggerFn: func(*api.TriggerRequest) *api.TriggerResponse {
				assert.GreaterOrEqual(t, uint32(5), got.Add(1))
				if got.Load() == 3 {
					return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
				}
				return &api.TriggerResponse{Result: api.TriggerResponseResult_FAILED}
			},
			GotCh: gotCh,
		})

		require.NoError(t, cron.API().Add(t.Context(), "test", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(3)),
			FailurePolicy: &api.FailurePolicy{
				Policy: &api.FailurePolicy_Constant{
					Constant: &api.FailurePolicyConstant{
						Interval: durationpb.New(time.Millisecond), MaxRetries: ptr.Of(uint32(1)),
					},
				},
			},
		}))

		for range 5 {
			resp, err := cron.API().Get(t.Context(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(t.Context(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("constant policy can retry forever until it succeeds", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    etcd.EmbeddedBareClient(t),
			TriggerFn: func(*api.TriggerRequest) *api.TriggerResponse {
				assert.GreaterOrEqual(t, uint32(100), got.Add(1))
				if got.Load() == 100 {
					return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
				}
				return &api.TriggerResponse{Result: api.TriggerResponseResult_FAILED}
			},
			GotCh: gotCh,
		})

		require.NoError(t, cron.API().Add(t.Context(), "test", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
			FailurePolicy: &api.FailurePolicy{
				Policy: &api.FailurePolicy_Constant{
					Constant: &api.FailurePolicyConstant{
						Interval: durationpb.New(time.Millisecond),
					},
				},
			},
		}))

		for range 100 {
			resp, err := cron.API().Get(t.Context(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(t.Context(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})
}
