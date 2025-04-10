/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package retry

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/internal/api"
	clienterrors "github.com/diagridio/go-etcd-cron/internal/client/errors"
	"github.com/diagridio/go-etcd-cron/internal/engine/fake"
)

func Test_handle(t *testing.T) {
	t.Parallel()

	t.Run("if the given context is cancelled, should return error", func(t *testing.T) {
		t.Parallel()

		r := New()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.Error(t, r.handle(ctx, nil))
	})

	t.Run("if retry has been closed, then should return error", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Close()
		require.Error(t, r.handle(t.Context(), nil))
	})

	t.Run("when retry ready, should call given func", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Ready(fake.New())

		var called atomic.Bool
		require.NoError(t, r.handle(t.Context(), func(a api.Interface) error {
			called.Store(true)
			return nil
		}))

		assert.True(t, called.Load())
	})

	t.Run("if handle func returns error, expect error", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Ready(fake.New())

		var called atomic.Bool
		require.Error(t, r.handle(t.Context(), func(a api.Interface) error {
			called.Store(true)
			return errors.New("this is an error")
		}))

		assert.True(t, called.Load())
	})

	t.Run("if error api closed, expect multiple calls till it is not", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Ready(fake.New())

		var called atomic.Int64
		require.NoError(t, r.handle(t.Context(), func(a api.Interface) error {
			if called.Add(1) < 4 {
				return api.ErrClosed
			}
			return nil
		}))

		assert.Equal(t, int64(4), called.Load())
	})

	t.Run("if context cancelled during retry loop, expect context error", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Ready(fake.New())

		var called atomic.Int64
		ctx, cancel := context.WithCancel(t.Context())
		err := r.handle(ctx, func(a api.Interface) error {
			if called.Add(1) > 3 {
				cancel()
			}
			return api.ErrClosed
		})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("if closed during retry loop, expect closed error", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Ready(fake.New())

		var called atomic.Int64
		err := r.handle(t.Context(), func(a api.Interface) error {
			if called.Add(1) > 3 {
				r.Close()
			}
			return api.ErrClosed
		})
		require.ErrorIs(t, err, errClosed)
	})

	t.Run("if the key already exists error, then should not retry", func(t *testing.T) {
		t.Parallel()

		r := New()
		r.Ready(fake.New())

		var called atomic.Int64
		err := r.handle(context.Background(), func(a api.Interface) error {
			called.Add(1)
			return clienterrors.NewKeyAlreadyExists("foo")
		})
		require.Error(t, err)
		assert.Equal(t, int64(1), called.Load())
		assert.True(t, clienterrors.IsKeyAlreadyExists(err))
	})
}
