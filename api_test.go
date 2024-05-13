/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/tests"
)

func Test_CRUD(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
	})
	require.NoError(t, err)

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for cron to stop")
		}
	})
	go func() {
		errCh <- cron.Run(ctx)
	}()

	now := time.Now()

	resp, err := cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, cron.Add(context.Background(), "def", &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}))

	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}, resp)

	newNow := time.Now()
	require.NoError(t, cron.Add(context.Background(), "def", &api.Job{
		DueTime: ptr.Of(newNow.Add(time.Hour).Format(time.RFC3339)),
	}))
	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &api.Job{
		DueTime: ptr.Of(newNow.Add(time.Hour).Format(time.RFC3339)),
	}, resp)

	require.NoError(t, cron.Delete(context.Background(), "def"))

	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, cron.Add(context.Background(), "def", &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}))

	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}, resp)
}

func Test_Add(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, cron.Add(ctx, "def", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, cron.Run(ctx))

		require.Error(t, cron.Add(context.Background(), "def", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		require.Error(t, cron.Add(context.Background(), "./.", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("empty job should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		require.Error(t, cron.Add(context.Background(), "def", nil))
	})
}

func Test_Get(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp, err := cron.Get(ctx, "def")
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, cron.Run(ctx))

		resp, err := cron.Get(context.Background(), "def")
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		resp, err := cron.Get(context.Background(), "./.")
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func Test_Delete(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, cron.Delete(ctx, "def"))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, cron.Run(ctx))

		require.Error(t, cron.Delete(context.Background(), "def"))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		require.Error(t, cron.Delete(context.Background(), "./."))
	})
}

func Test_validateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		expErr bool
	}{
		{
			name:   "",
			expErr: true,
		},
		{
			name:   "/",
			expErr: true,
		},
		{
			name:   "/foo/",
			expErr: true,
		},
		{
			name:   "foo/",
			expErr: true,
		},
		{
			name:   ".",
			expErr: true,
		},
		{
			name:   "..",
			expErr: true,
		},
		{
			name:   "./.",
			expErr: true,
		},
		{
			name:   "fo.o",
			expErr: true,
		},
		{
			name:   "fo...o",
			expErr: true,
		},
		{
			name:   "valid",
			expErr: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateName(test.name)
			assert.Equal(t, test.expErr, err != nil, "%v", err)
		})
	}
}
