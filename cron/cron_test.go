/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("Running multiple times should error", func(t *testing.T) {
		t.Parallel()
		replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)
		client := etcd.EmbeddedBareClient(t)
		var triggered atomic.Int64
		cronI, err := New(Options{
			Log:       logr.Discard(),
			Client:    client,
			Namespace: "abc",
			ID:        "0",
			TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				triggered.Add(1)
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			},
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(t.Context())
		errCh1 := make(chan error)
		errCh2 := make(chan error)

		go func() {
			errCh1 <- cronI.Run(ctx)
		}()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, cron.IsElected())
		}, time.Second*5, 100*time.Millisecond)

		go func() {
			errCh2 <- cronI.Run(ctx)
		}()

		select {
		case err := <-errCh2:
			require.Error(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}

		cancel()
		select {
		case err := <-errCh1:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})

	t.Run("cron instance fatal errors when leadership is overwritten", func(t *testing.T) {
		t.Parallel()

		replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)
		client := etcd.EmbeddedBareClient(t)

		cronI, err := New(Options{
			Log:       logr.Discard(),
			Client:    client,
			Namespace: "abc",
			ID:        "0",
			TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			},
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		errCh := make(chan error)

		go func() {
			errCh <- cronI.Run(ctx)
		}()

		// wait until ready
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, cron.IsElected())
		}, time.Second*5, 100*time.Millisecond)

		leadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       10,
			ReplicaData: replicaData,
		})
		require.NoError(t, err, "failed to marshal leadership data")

		_, err = client.Put(t.Context(), "abc/leadership/1", string(leadershipData))
		require.NoError(t, err, "failed to insert leadership data into etcd")

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.False(c, cron.IsElected())
		}, time.Second*5, 100*time.Millisecond)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})
}
