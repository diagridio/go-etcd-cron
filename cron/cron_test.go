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
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/diagridio/go-etcd-cron/api"
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
}
