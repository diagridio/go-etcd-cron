/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"sync"
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

	t.Run("cron engine should show restarting when leadership changes, ensure proper close", func(t *testing.T) {
		t.Parallel()

		replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)
		client := etcd.EmbeddedBareClient(t)

		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abcd",
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
			close(errCh)
		}()

		cron.lock.RLock()
		readyCh := cron.readyCh
		cron.lock.RUnlock()

		// wait until ready
		select {
		case <-readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		leadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       5,
			ReplicaData: replicaData,
		})
		require.NoError(t, err, "failed to marshal leadership data")

		childCtx, childCancel := context.WithCancel(ctx)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		cron.lock.RLock()
		restartingCh := cron.restartingCh
		cron.lock.RUnlock()

		restartingDetected := make(chan struct{})
		// Monitor cron restarting
		go func(ctx context.Context) {
			defer childCancel()
			defer wg.Done()

			select {
			case <-restartingCh:
				close(restartingDetected)
			case <-ctx.Done():
			}
		}(childCtx)

		_, err = client.Put(context.Background(), "abcd/leadership/1", string(leadershipData))
		require.NoError(t, err, "failed to insert leadership data into etcd")

		select {
		case <-restartingCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})

	t.Run("cron should not be ready while restarting the engine", func(t *testing.T) {
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
			close(errCh)
		}()

		cron.lock.RLock()
		readyCh := cron.readyCh
		cron.lock.RUnlock()

		// wait until ready
		select {
		case <-readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		leadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       5,
			ReplicaData: replicaData,
		})
		require.NoError(t, err, "failed to marshal leadership data")

		childCtx, childCancel := context.WithCancel(ctx)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		cron.lock.RLock()
		restartingCh := cron.restartingCh
		cron.lock.RUnlock()

		restartingDetected := make(chan struct{})
		// Monitor cron restarting
		go func(ctx context.Context) {
			defer childCancel()
			defer wg.Done()

			select {
			case <-restartingCh:
				close(restartingDetected)
			case <-ctx.Done():
			}
		}(childCtx)

		_, err = client.Put(context.Background(), "abc/leadership/1", string(leadershipData))
		require.NoError(t, err, "failed to insert leadership data into etcd")

		select {
		case <-restartingCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for cron to be restarting")
		}

		cron.lock.RLock()
		newReadyCh := cron.readyCh
		cron.lock.RUnlock()

		select {
		case <-newReadyCh:
			t.Fatal("cron should not be ready while restarting")
		case <-time.After(5 * time.Second):
		}

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})

	t.Run("cron engine should gate until leadership ready", func(t *testing.T) {
		t.Parallel()

		replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)
		client := etcd.EmbeddedBareClient(t)

		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abcde",
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
			close(errCh)
		}()

		cron.lock.RLock()
		readyCh := cron.readyCh
		cron.lock.RUnlock()

		// wait until ready
		select {
		case <-readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		// intentionally incorrect total
		leadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       5,
			ReplicaData: replicaData,
		})
		require.NoError(t, err, "failed to marshal leadership data")

		childCtx, childCancel := context.WithCancel(ctx)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		cron.lock.RLock()
		restartingCh := cron.restartingCh
		cron.lock.RUnlock()

		restartingDetected := make(chan struct{})
		// Monitor cron restarting
		go func(ctx context.Context) {
			defer childCancel()
			defer wg.Done()

			select {
			case <-restartingCh:
				close(restartingDetected)
			case <-ctx.Done():
			}
		}(childCtx)

		_, err = client.Put(context.Background(), "abcde/leadership/1", string(leadershipData))
		require.NoError(t, err, "failed to insert leadership data into etcd")

		// cron engine restarting
		select {
		case <-restartingCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		cron.lock.RLock()
		newReadyCh := cron.readyCh
		cron.lock.RUnlock()

		select {
		case <-newReadyCh:
			t.Fatal("cron should not be ready while restarting")
		case <-time.After(2 * time.Second):
		}

		// Ensure restartingCh is reset eventually
		require.Eventually(t, func() bool {
			cron.lock.RLock()
			defer cron.lock.RUnlock()
			return cron.restartingCh != nil
		}, 1*time.Second, 100*time.Millisecond, "cron restartingCh was not reset")

		// double check cron does not become ready
		cron.lock.RLock()
		finalReadyCh := cron.readyCh
		cron.lock.RUnlock()

		select {
		case <-finalReadyCh:
			t.Fatal("cron became ready unexpectedly after invalid leadership data change. leadership should not be ready and should gate cron being ready")
		case <-time.After(2 * time.Second):
		}

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})

	t.Run("cron engine should become ready again after proper leadership change", func(t *testing.T) {
		t.Parallel()

		replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)
		client := etcd.EmbeddedBareClient(t)

		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abcdef",
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
			close(errCh)
		}()

		cron.lock.RLock()
		readyCh := cron.readyCh
		cron.lock.RUnlock()

		// wait until ready
		select {
		case <-readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		childCtx, childCancel := context.WithCancel(ctx)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		cron.lock.RLock()
		restartingCh := cron.restartingCh
		cron.lock.RUnlock()

		restartingDetected := make(chan struct{})
		// Monitor cron restarting
		go func(ctx context.Context) {
			defer childCancel()
			defer wg.Done()

			select {
			case <-restartingCh:
				close(restartingDetected)
			case <-ctx.Done():
			}
		}(childCtx)

		// wait until ready
		select {
		case <-readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		// intentionally incorrect total
		leadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       5,
			ReplicaData: replicaData,
		})
		require.NoError(t, err, "failed to marshal leadership data")

		_, err = client.Put(context.Background(), "abcdef/leadership/1", string(leadershipData))
		require.NoError(t, err, "failed to insert leadership data into etcd")

		select {
		case <-restartingCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		cron.lock.RLock()
		newReadyCh := cron.readyCh
		cron.lock.RUnlock()

		select {
		case <-newReadyCh:
			t.Fatal("cron should not be ready while restarting")
		case <-time.After(2 * time.Second):
		}

		// Ensure restartingCh is reset eventually
		require.Eventually(t, func() bool {
			cron.lock.RLock()
			defer cron.lock.RUnlock()
			return cron.restartingCh != nil
		}, 1*time.Second, 100*time.Millisecond, "cron restartingCh was not reset")

		// Update leadership data to match partition total
		correctLeadershipData, err := proto.Marshal(&stored.Leadership{
			Total:       10,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abcdef/leadership/1", string(correctLeadershipData))
		require.NoError(t, err, "failed to update leadership data in etcd")

		// Wait for the cron to become ready again
		cron.lock.RLock()
		finalReadyCh := cron.readyCh
		cron.lock.RUnlock()

		select {
		case <-finalReadyCh:
		case <-time.After(5 * time.Second):
			t.Fatal("cron did not become ready again after restart")
		}

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})
}
