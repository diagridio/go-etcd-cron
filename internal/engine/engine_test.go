/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	internalapi "github.com/diagridio/go-etcd-cron/internal/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_New(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		client := etcd.Embedded(t)
		key := key.New(key.Options{
			Namespace:   "test-ns",
			PartitionID: 1,
		})

		part, err := partitioner.New(partitioner.Options{
			ID:    1,
			Total: 1,
		})
		require.NoError(t, err)

		engine, err := New(Options{
			Key:         key,
			Partitioner: part,
			Client:      client,
			TriggerFn: func(ctx context.Context, request *api.TriggerRequest) *api.TriggerResponse {
				return nil
			},
		})

		require.NoError(t, err)
		require.NotNil(t, engine)
	})
}

func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key := key.New(key.Options{
			Namespace:   "test-ns",
			PartitionID: 1,
		})

		part, err := partitioner.New(partitioner.Options{
			ID:    1,
			Total: 1,
		})
		require.NoError(t, err)

		engine, err := New(Options{
			Key:         key,
			Partitioner: part,
			Client:      client,
			TriggerFn: func(ctx context.Context, request *api.TriggerRequest) *api.TriggerResponse {
				return nil
			},
		})

		require.NoError(t, err)
		require.NotNil(t, engine)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error)
		go func() { errCh <- engine.Run(ctx) }()

		// Allow time for the engine to start
		time.Sleep(100 * time.Millisecond)
		cancel()

		err = <-errCh

		// runner manager returns nil here
		assert.NoError(t, err)
	})

	t.Run("multiple starts", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key := key.New(key.Options{
			Namespace:   "test-ns",
			PartitionID: 1,
		})

		part, err := partitioner.New(partitioner.Options{
			ID:    1,
			Total: 1,
		})
		require.NoError(t, err)

		engine, err := New(Options{
			Key:         key,
			Partitioner: part,
			Client:      client,
			TriggerFn: func(ctx context.Context, request *api.TriggerRequest) *api.TriggerResponse {
				return nil
			},
		})

		require.NoError(t, err)
		require.NotNil(t, engine)

		ctx1, cancel1 := context.WithCancel(context.Background())
		defer cancel1()

		startedCh := make(chan struct{})
		go func() {
			close(startedCh)
			_ = engine.Run(ctx1)
		}()
		<-startedCh // ensure engine starts

		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()

		err = engine.Run(ctx2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "engine is already running")

		// Cancel the first Run to allow cleanup
		cancel1()
	})
}

func Test_API(t *testing.T) {
	t.Parallel()

	client := etcd.Embedded(t)
	key := key.New(key.Options{
		Namespace:   "test-ns",
		PartitionID: 1,
	})

	part, err := partitioner.New(partitioner.Options{
		ID:    1,
		Total: 1,
	})
	require.NoError(t, err)

	engine, err := New(Options{
		Key:         key,
		Partitioner: part,
		Client:      client,
		TriggerFn: func(ctx context.Context, request *api.TriggerRequest) *api.TriggerResponse {
			return nil
		},
	})

	require.NoError(t, err)
	require.NotNil(t, engine)

	api := engine.API()
	require.NotNil(t, api)
	assert.Implements(t, (*internalapi.Interface)(nil), api)
}
