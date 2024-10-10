/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	cronapi "github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/queue"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

var errCancel = errors.New("custom cancel")

func Test_CRUD(t *testing.T) {
	t.Parallel()

	api := newAPI(t)

	now := time.Now()

	resp, err := api.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, api.Add(context.Background(), "def", &cronapi.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}))

	resp, err = api.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &cronapi.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		FailurePolicy: &cronapi.FailurePolicy{Policy: &cronapi.FailurePolicy_Constant{
			Constant: &cronapi.FailurePolicyConstant{
				Delay: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
			},
		}},
	}, resp)

	newNow := time.Now()
	require.NoError(t, api.Add(context.Background(), "def", &cronapi.Job{
		DueTime: ptr.Of(newNow.Add(time.Hour).Format(time.RFC3339)),
		FailurePolicy: &cronapi.FailurePolicy{Policy: &cronapi.FailurePolicy_Constant{
			Constant: &cronapi.FailurePolicyConstant{
				Delay: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
			},
		}},
	}))
	resp, err = api.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &cronapi.Job{
		DueTime: ptr.Of(newNow.Add(time.Hour).Format(time.RFC3339)),
		FailurePolicy: &cronapi.FailurePolicy{Policy: &cronapi.FailurePolicy_Constant{
			Constant: &cronapi.FailurePolicyConstant{
				Delay: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
			},
		}},
	}, resp)

	require.NoError(t, api.Delete(context.Background(), "def"))

	resp, err = api.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, api.Add(context.Background(), "def", &cronapi.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		FailurePolicy: &cronapi.FailurePolicy{Policy: &cronapi.FailurePolicy_Constant{
			Constant: &cronapi.FailurePolicyConstant{
				Delay: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
			},
		}},
	}))

	resp, err = api.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &cronapi.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
		FailurePolicy: &cronapi.FailurePolicy{Policy: &cronapi.FailurePolicy_Constant{
			Constant: &cronapi.FailurePolicyConstant{
				Delay: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
			},
		}},
	}, resp)
}

func Test_Add(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if api not ready in time", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errCancel)
		assert.Equal(t, errCancel, newAPINotReady(t).Add(ctx, "def", &cronapi.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		api := newAPINotReady(t)
		close(api.closeCh)
		assert.Equal(t, errors.New("api is closed"), api.Add(context.Background(), "def", &cronapi.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		api := newAPI(t)

		require.Error(t, api.Add(context.Background(), "./.", &cronapi.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("empty job should error", func(t *testing.T) {
		t.Parallel()

		api := newAPI(t)

		require.Error(t, api.Add(context.Background(), "def", nil))
	})
}

func Test_Get(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errCancel)
		resp, err := newAPINotReady(t).Get(ctx, "def")
		assert.Equal(t, errCancel, err)
		assert.Nil(t, resp)
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		api := newAPINotReady(t)
		close(api.closeCh)
		resp, err := api.Get(context.Background(), "def")
		assert.Equal(t, errors.New("api is closed"), err)
		assert.Nil(t, resp)
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		api := newAPI(t)

		resp, err := api.Get(context.Background(), "./.")
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func Test_Delete(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errCancel)
		assert.Equal(t, errCancel, newAPINotReady(t).Delete(ctx, "def"))
		require.Error(t, newAPINotReady(t).Delete(ctx, "def"))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		api := newAPINotReady(t)
		close(api.closeCh)
		assert.Equal(t, errors.New("api is closed"), api.Delete(context.Background(), "def"))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()
		api := newAPI(t)
		require.Error(t, api.Delete(context.Background(), "./."))
	})
}

func Test_DeletePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errCancel)
		require.Error(t, newAPINotReady(t).DeletePrefixes(ctx, "foobar"))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		api := newAPINotReady(t)
		close(api.closeCh)
		assert.Equal(t, errors.New("api is closed"), api.DeletePrefixes(context.Background(), "foobar"))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		api := newAPI(t)
		require.Error(t, api.DeletePrefixes(context.Background(), "./."))
	})
}

func Test_List(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errCancel)
		resp, err := newAPINotReady(t).List(ctx, "")
		assert.Equal(t, errCancel, err)
		assert.Nil(t, resp)
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		api := newAPINotReady(t)
		close(api.closeCh)
		resp, err := api.List(context.Background(), "")
		assert.Equal(t, errors.New("api is closed"), err)
		assert.Nil(t, resp)
	})
}

func Test_DeliverablePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errCancel)
		dcancel, err := newAPINotReady(t).DeliverablePrefixes(ctx, "helloworld")
		assert.Equal(t, errCancel, err)
		assert.Nil(t, dcancel)
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		api := newAPINotReady(t)
		close(api.closeCh)
		cancel, err := api.DeliverablePrefixes(context.Background(), "hello world")
		assert.Equal(t, errors.New("api is closed"), err)
		assert.Nil(t, cancel)
	})
}

func newAPI(t *testing.T) *api {
	t.Helper()
	api := newAPINotReady(t)
	close(api.readyCh)
	return api
}

func newAPINotReady(t *testing.T) *api {
	t.Helper()

	client := etcd.Embedded(t)

	collector, err := garbage.New(garbage.Options{
		Log:                logr.Discard(),
		Client:             client,
		CollectionInterval: ptr.Of(time.Second),
	})
	require.NoError(t, err)

	key := key.New(key.Options{})
	schedulerBuilder := scheduler.NewBuilder()
	queue := queue.New(queue.Options{
		Log:              logr.Discard(),
		Client:           client,
		Key:              key,
		SchedulerBuilder: schedulerBuilder,
		TriggerFn: func(context.Context, *cronapi.TriggerRequest) *cronapi.TriggerResponse {
			return &cronapi.TriggerResponse{Result: cronapi.TriggerResponseResult_SUCCESS}
		},
		Collector: collector,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for cron to stop")
		}
	})
	go func() {
		done <- queue.Run(ctx)
	}()

	return New(Options{
		Client:           client,
		Key:              key,
		SchedulerBuilder: schedulerBuilder,
		Queue:            queue,
		ReadyCh:          make(chan struct{}),
		CloseCh:          make(chan struct{}),
	}).(*api)
}
