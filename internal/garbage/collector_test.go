/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package garbage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/diagridio/go-etcd-cron/internal/tests"
)

func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("doubling Run should return error", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		c.clock = clocktesting.NewFakeClock(time.Now())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		errCh := make(chan error, 1)

		go func() {
			errCh <- c.Run(ctx)
		}()
		assert.Eventually(t, c.running.Load, time.Second, time.Millisecond*1)

		require.Error(t, c.Run(ctx))
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("expected error")
		}
	})

	t.Run("pushing after 500k runs after Run has returned should not error", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		c.clock = clocktesting.NewFakeClock(time.Now())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, c.Run(ctx))

		for i := 0; i < 500000; i++ {
			c.Push(fmt.Sprintf("test-%d", i))
		}

		assert.Len(t, c.keys, 500000)
	})

	t.Run("closing the collector should result in the remaining keys to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		c := New(Options{
			Client: client,
		}).(*collector)
		c.clock = clocktesting.NewFakeClock(time.Now())

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Run(ctx)
		}()

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test-%d", i)
			_, err := client.Put(context.Background(), key, "value")
			require.NoError(t, err)
			c.Push(key)
		}

		resp, err := client.Get(context.Background(), "test", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(t, resp.Kvs, 100)
		assert.Equal(t, int64(100), resp.Count)

		cancel()
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("expected collector to return")
		case err := <-errCh:
			require.NoError(t, err)
		}

		resp, err = client.Get(context.Background(), "test", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Empty(t, resp.Kvs)
		assert.Equal(t, int64(0), resp.Count)
	})

	t.Run("reaching max garbage limit (500k) should cause all keys to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		c := New(Options{
			Client: client,
		}).(*collector)
		c.clock = clocktesting.NewFakeClock(time.Now())
		c.garbageLimit = 100

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Run(ctx)
		}()

		for i := 0; i < 100-1; i++ {
			key := fmt.Sprintf("test-%d", i)
			_, err := client.Put(context.Background(), key, "value")
			require.NoError(t, err)
			c.Push(key)
		}

		key := "test-100"
		_, err := client.Put(context.Background(), key, "value")
		require.NoError(t, err)
		c.Push(key)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Get(context.Background(), "test", clientv3.WithPrefix())
			if assert.NoError(c, err) {
				assert.Empty(c, resp.Kvs)
				assert.Equal(c, int64(0), resp.Count)
			}
		}, time.Second*5, time.Millisecond*10)

		c.lock.Lock()
		assert.Empty(t, c.keys)
		c.lock.Unlock()

		cancel()
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("expected collector to return")
		case err := <-errCh:
			require.NoError(t, err)
		}
	})

	t.Run("if ticks past 180 seconds, then should delete all garbage keys", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		clock := clocktesting.NewFakeClock(time.Now())
		c := New(Options{
			Client: client,
		}).(*collector)
		c.clock = clock

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Run(ctx)
		}()

		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("test-%d", i)
			_, err := client.Put(context.Background(), key, "value")
			require.NoError(t, err)
			c.Push(key)
		}

		assert.True(t, clock.HasWaiters())
		clock.Step(time.Second*180 - 1)
		assert.True(t, clock.HasWaiters())
		clock.Step(1)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Get(context.Background(), "test", clientv3.WithPrefix())
			if assert.NoError(c, err) {
				assert.Empty(c, resp.Kvs)
				assert.Equal(c, int64(0), resp.Count)
			}
		}, time.Second*5, time.Millisecond*10)

		cancel()
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("expected collector to return")
		case err := <-errCh:
			require.NoError(t, err)
		}
	})
}

func Test_Push(t *testing.T) {
	t.Parallel()

	t.Run("pushing a key should add it to the list", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		assert.Empty(t, c.keys)
		c.Push("test")
		assert.Len(t, c.keys, 1)
	})

	t.Run("double pushing a key does nothing", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		assert.Empty(t, c.keys)
		c.Push("test")
		assert.Len(t, c.keys, 1)
		c.Push("test")
		assert.Len(t, c.keys, 1)
		c.Push("test2")
		assert.Len(t, c.keys, 2)
	})

	t.Run("popping more than 500k keys should trigger the sooner channel", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)

		select {
		case <-c.soonerCh:
			t.Fatal("should not have triggered sooner channel")
		default:
		}

		for i := 0; i < 500000-1; i++ {
			c.Push(fmt.Sprintf("test-%d", i))
		}

		select {
		case <-c.soonerCh:
			t.Fatal("should not have triggered sooner channel")
		default:
		}

		c.Push("test-500000")

		select {
		case <-c.soonerCh:
		default:
			t.Fatal("should have triggered sooner channel")
		}
	})
}

func Test_Pop(t *testing.T) {
	t.Parallel()

	t.Run("popping a key should remove it from the list", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		c.keys["test"] = struct{}{}
		assert.Len(t, c.keys, 1)
		c.Pop("test")
		assert.Empty(t, c.keys)
	})

	t.Run("popping a key which doesn't exist should not panic", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		c.keys["not-test"] = struct{}{}
		assert.Len(t, c.keys, 1)
		c.Pop("test")
		assert.Len(t, c.keys, 1)
	})
}

func Test_collect(t *testing.T) {
	t.Parallel()

	t.Run("if there are no keys to delete, then expect no call", func(t *testing.T) {
		t.Parallel()

		c := New(Options{}).(*collector)
		assert.Empty(t, c.keys)
		require.NoError(t, c.collect())
	})

	t.Run("if there are keys to delete, expect them to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		c := New(Options{
			Client: client,
		}).(*collector)

		for i := 0; i < 10; i++ {
			_, err := client.Put(context.Background(), fmt.Sprintf("/test/%d", i), "value")
			require.NoError(t, err)
			c.keys[fmt.Sprintf("/test/%d", i)] = struct{}{}
		}

		for i := 0; i < 10; i++ {
			resp, err := client.Get(context.Background(), fmt.Sprintf("/test/%d", i))
			require.NoError(t, err)
			require.Len(t, resp.Kvs, 1)
			assert.Equal(t, fmt.Sprintf("/test/%d", i), string(resp.Kvs[0].Key))
		}

		assert.Len(t, c.keys, 10)
		require.NoError(t, c.collect())
		assert.Empty(t, c.keys)

		for i := 0; i < 10; i++ {
			resp, err := client.Get(context.Background(), fmt.Sprintf("/test/%d", i))
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
		}
	})

	t.Run("should no delete other keys which are not marked for deletion", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		c := New(Options{
			Client: client,
		}).(*collector)

		for i := 0; i < 10; i++ {
			_, err := client.Put(context.Background(), fmt.Sprintf("/test/%d", i), "value")
			require.NoError(t, err)
			c.keys[fmt.Sprintf("/test/%d", i)] = struct{}{}
		}

		for i := 10; i < 20; i++ {
			_, err := client.Put(context.Background(), fmt.Sprintf("/test/%d", i), "value")
			require.NoError(t, err)
		}

		for i := 0; i < 20; i++ {
			resp, err := client.Get(context.Background(), fmt.Sprintf("/test/%d", i))
			require.NoError(t, err)
			require.Len(t, resp.Kvs, 1)
			assert.Equal(t, fmt.Sprintf("/test/%d", i), string(resp.Kvs[0].Key))
		}

		assert.Len(t, c.keys, 10)
		require.NoError(t, c.collect())
		assert.Empty(t, c.keys)

		for i := 0; i < 10; i++ {
			resp, err := client.Get(context.Background(), fmt.Sprintf("/test/%d", i))
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
		}
		for i := 10; i < 20; i++ {
			resp, err := client.Get(context.Background(), fmt.Sprintf("/test/%d", i))
			require.NoError(t, err)
			require.Len(t, resp.Kvs, 1)
			assert.Equal(t, fmt.Sprintf("/test/%d", i), string(resp.Kvs[0].Key))
		}
	})
}
