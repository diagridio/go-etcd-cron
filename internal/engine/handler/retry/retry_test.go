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

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierrors "github.com/diagridio/go-etcd-cron/api/errors"
	"github.com/diagridio/go-etcd-cron/internal/engine/fake"
	"github.com/diagridio/go-etcd-cron/internal/engine/handler"
)

func Test_handle(t *testing.T) {
	t.Parallel()

	t.Run("if the given context is cancelled, should return error", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.Error(t, r.handle(ctx, nil))
	})

	t.Run("if retry has been closed, then should return error", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Close()
		require.Error(t, r.handle(t.Context(), nil))
	})

	t.Run("when retry ready, should call given func", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Ready(fake.New())

		var called atomic.Bool
		require.NoError(t, r.handle(t.Context(), func(a handler.Interface) error {
			called.Store(true)
			return nil
		}))

		assert.True(t, called.Load())
	})

	t.Run("if handle func returns error, expect error", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Ready(fake.New())

		var called atomic.Bool
		require.Error(t, r.handle(t.Context(), func(a handler.Interface) error {
			called.Store(true)
			return errors.New("this is an error")
		}))

		assert.True(t, called.Load())
	})

	t.Run("if error api closed, expect multiple calls till it is not", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Ready(fake.New())

		var called atomic.Int64
		require.NoError(t, r.handle(t.Context(), func(a handler.Interface) error {
			if called.Add(1) < 4 {
				return handler.ErrClosed
			}
			return nil
		}))

		assert.Equal(t, int64(4), called.Load())
	})

	t.Run("if context cancelled during retry loop, expect context error", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Ready(fake.New())

		var called atomic.Int64
		ctx, cancel := context.WithCancel(t.Context())
		err := r.handle(ctx, func(a handler.Interface) error {
			if called.Add(1) > 3 {
				cancel()
			}
			return handler.ErrClosed
		})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("if closed during retry loop, expect closed error", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Ready(fake.New())

		var called atomic.Int64
		err := r.handle(t.Context(), func(a handler.Interface) error {
			if called.Add(1) > 3 {
				r.Close()
			}
			return handler.ErrClosed
		})
		require.ErrorIs(t, err, errClosed)
	})

	t.Run("if the key already exists error, then should not retry", func(t *testing.T) {
		t.Parallel()

		r := New(Options{Log: logr.Discard()})
		r.Ready(fake.New())

		var called atomic.Int64
		err := r.handle(context.Background(), func(a handler.Interface) error {
			called.Add(1)
			return apierrors.NewJobAlreadyExists("foo")
		})
		require.Error(t, err)
		assert.Equal(t, int64(1), called.Load())
		assert.True(t, apierrors.IsJobAlreadyExists(err))
	})
}
