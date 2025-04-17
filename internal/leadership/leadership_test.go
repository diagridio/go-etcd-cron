/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Relect(t *testing.T) {
	t.Parallel()

	t.Run("returns error when closed", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())

		errCh := make(chan error)
		go func() { errCh <- leader.Run(ctx) }()

		_, _, err = leader.Elect(t.Context())
		require.NoError(t, err)

		cancel()
		select {
		case <-time.After(5 * time.Second):
			require.Fail(t, "timed out waiting for leader to stop")
		case err := <-errCh:
			require.NoError(t, err)
		}

		_, _, err = leader.Reelect(t.Context())
		require.Error(t, err)
	})

	t.Run("returns error if not ready", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
		})

		_, _, err = leader.Reelect(t.Context())
		require.Error(t, err)
	})

	t.Run("reelects leadership", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		errCh := make(chan error)
		go func() { errCh <- leader.Run(ctx) }()

		_, elected, err := leader.Elect(ctx)
		require.NoError(t, err)
		assert.Len(t, elected.LeadershipData, 1)

		_, _, err = leader.Reelect(ctx)
		require.NoError(t, err)

		cancel()
		select {
		case <-time.After(5 * time.Second):
			require.Fail(t, "timed out waiting for leader to stop")
		case err := <-errCh:
			require.NoError(t, err)
		}
	})

	t.Run("reelects leadership after losing leadership quorum", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key1, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key1,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		errCh := make(chan error)
		go func() { errCh <- leader.Run(ctx) }()

		gotCtx, elected, err := leader.Elect(ctx)
		require.NoError(t, err)
		assert.Len(t, elected.LeadershipData, 1)

		key2, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld2",
		})
		require.NoError(t, err)
		leader2 := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key2,
			ReplicaData: replicaData,
		})
		go func() { errCh <- leader2.Run(ctx) }()

		go func() {
			_, _, err2 := leader2.Elect(ctx)
			errCh <- err2
		}()

		select {
		case <-time.After(5 * time.Second):
			require.Fail(t, "timed out waiting for leader to stop")
		case <-gotCtx.Done():
		}

		_, _, err = leader.Reelect(ctx)
		require.NoError(t, err)

		cancel()
		for range 3 {
			select {
			case <-time.After(5 * time.Second):
				require.Fail(t, "timed out waiting for leader to stop")
			case <-errCh:
			}
		}
	})
}

func Test_Elect(t *testing.T) {
	t.Parallel()

	t.Run("returns no error when closed", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.Error(t, leader.Run(ctx))

		_, _, err = leader.Elect(t.Context())
		require.NoError(t, err)
	})

	t.Run("returns error context cancelled", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, err = leader.Elect(ctx)
		require.Error(t, err)
	})

	t.Run("elects leadership", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		errCh := make(chan error)
		go func() { errCh <- leader.Run(ctx) }()

		_, elected, err := leader.Elect(ctx)
		require.NoError(t, err)
		assert.Len(t, elected.LeadershipData, 1)

		cancel()
		select {
		case <-time.After(5 * time.Second):
			require.Fail(t, "timed out waiting for leader to stop")
		case err := <-errCh:
			require.NoError(t, err)
		}
	})

	t.Run("cancels context on leadership event", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key1, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)
		replicaData := &anypb.Any{Value: []byte("hello")}

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key1,
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		errCh := make(chan error)
		go func() { errCh <- leader.Run(ctx) }()

		gotCtx, elected, err := leader.Elect(ctx)
		require.NoError(t, err)
		assert.Len(t, elected.LeadershipData, 1)

		key2, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld2",
		})
		require.NoError(t, err)
		leader2 := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key2,
			ReplicaData: replicaData,
		})
		go func() { errCh <- leader2.Run(ctx) }()

		go func() {
			_, _, err2 := leader2.Elect(ctx)
			errCh <- err2
		}()

		select {
		case <-time.After(5 * time.Second):
			require.Fail(t, "timed out waiting for leader to stop")
		case <-gotCtx.Done():
		}

		cancel()
		for range 3 {
			select {
			case <-time.After(5 * time.Second):
				require.Fail(t, "timed out waiting for leader to stop")
			case <-errCh:
			}
		}
	})
}
