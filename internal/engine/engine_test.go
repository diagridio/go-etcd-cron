/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/diagridio/go-etcd-cron/api"
	internalapi "github.com/diagridio/go-etcd-cron/internal/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/partitioner"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_New(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "test-ns",
			ID:        "0",
		})
		require.NoError(t, err)

		part, err := partitioner.New(partitioner.Options{
			Key:     key,
			Leaders: []*mvccpb.KeyValue{{Key: []byte("test-ns/leader/0")}},
		})
		require.NoError(t, err)

		engine, err := New(Options{
			Key:         key,
			Partitioner: part,
			Client:      client,
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
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
		key, err := key.New(key.Options{
			Namespace: "test-ns",
			ID:        "0",
		})
		require.NoError(t, err)

		part, err := partitioner.New(partitioner.Options{
			Key:     key,
			Leaders: []*mvccpb.KeyValue{{Key: []byte("test-ns/leader/0")}},
		})
		require.NoError(t, err)

		engine, err := New(Options{
			Key:         key,
			Partitioner: part,
			Client:      client,
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
				return nil
			},
		})

		require.NoError(t, err)
		require.NotNil(t, engine)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error)
		go func() { errCh <- engine.Run(ctx) }()

		cancel()

		// runner manager returns nil here
		assert.NoError(t, <-errCh)
	})

	t.Run("multiple starts", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "test-ns",
			ID:        "0",
		})
		require.NoError(t, err)

		part, err := partitioner.New(partitioner.Options{
			Key:     key,
			Leaders: []*mvccpb.KeyValue{{Key: []byte("test-ns/leader/0")}},
		})
		require.NoError(t, err)

		engine, err := New(Options{
			Key:         key,
			Partitioner: part,
			Client:      client,
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
				return nil
			},
		})

		require.NoError(t, err)
		require.NotNil(t, engine)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		errCh := make(chan error)
		go func() { errCh <- engine.Run(ctx) }()
		go func() { errCh <- engine.Run(ctx) }()
		require.Error(t, <-errCh)
		cancel()
		require.NoError(t, <-errCh)
	})
}

func Test_API(t *testing.T) {
	t.Parallel()

	client := etcd.Embedded(t)
	key, err := key.New(key.Options{
		Namespace: "test-ns",
		ID:        "0",
	})
	require.NoError(t, err)

	part, err := partitioner.New(partitioner.Options{
		Key:     key,
		Leaders: []*mvccpb.KeyValue{{Key: []byte("test-ns/leader/0")}},
	})
	require.NoError(t, err)

	engine, err := New(Options{
		Key:         key,
		Partitioner: part,
		Client:      client,
		TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
			return nil
		},
	})

	require.NoError(t, err)
	require.NotNil(t, engine)

	api := engine.API()
	require.NotNil(t, api)
	assert.Implements(t, (*internalapi.Interface)(nil), api)
}
