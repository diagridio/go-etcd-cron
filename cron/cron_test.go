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

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
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
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
				triggered.Add(1)
				return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
			},
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(context.Background())
		errCh1 := make(chan error)
		errCh2 := make(chan error)

		go func() {
			errCh1 <- cronI.Run(ctx)
		}()

		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

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

	t.Run("cron engine remains ready after leadership change that keeps partition total the same", func(t *testing.T) {
		t.Parallel()

		replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)
		client := etcd.EmbeddedBareClient(t)

		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 10,
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
				return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
			},
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error)

		go func() {
			errCh <- cronI.Run(ctx)
		}()

		// wait until ready
		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		leadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       10,
			ReplicaData: replicaData,
		})
		require.NoError(t, err, "failed to marshal leadership data")

		_, err = client.Put(context.Background(), "abc/leadership/1", string(leadershipData))
		require.NoError(t, err, "failed to insert leadership data into etcd")

		// wait until ready
		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		// confirm cron is ready
		err = cronI.Add(context.Background(), "a123", &api.Job{
			DueTime: ptr.Of("10s"),
		})
		require.NoError(t, err)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})
}
