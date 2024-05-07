/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/tests"
)

func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("Leadership should become leader and become ready", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		t.Cleanup(func() {
			cancel()
			select {
			case <-errCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for error")
			}
		})

		go func() { errCh <- l.Run(ctx) }()

		require.NoError(t, l.WaitForLeadership(context.Background()))
	})

	t.Run("Running leadership multiple times should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		t.Cleanup(func() {
			cancel()
			select {
			case <-errCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for error")
			}
		})

		go func() { errCh <- l.Run(ctx) }()

		require.NoError(t, l.WaitForLeadership(context.Background()))
		require.Error(t, l.Run(ctx))
	})

	t.Run("Closing the leadership should delete the accosted partition leader key", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()

		require.NoError(t, l.WaitForLeadership(context.Background()))

		resp, err := client.KV.Get(ctx, "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)
		assert.Equal(t, []byte("10"), resp.Kvs[0].Value)
		leaseID := resp.Kvs[0].Lease
		assert.NotEqual(t, int64(0), leaseID)

		lresp, err := client.Lease.Leases(ctx)
		require.NoError(t, err)
		assert.Len(t, lresp.Leases, 1)
		assert.Equal(t, leaseID, int64(lresp.Leases[0].ID))

		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		resp, err = client.KV.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(0), resp.Count)
		assert.Empty(t, resp.Kvs)

		lresp, err = client.Lease.Leases(context.Background())
		require.NoError(t, err)
		assert.Empty(t, lresp.Leases)
	})

	t.Run("Closing the leadership should not delete the other partition keys", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		_, err := l.kv.Put(context.Background(), "abc/leadership/1", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/2", "10")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()

		require.NoError(t, l.WaitForLeadership(context.Background()))

		resp, err := client.KV.Get(ctx, "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)
		assert.Equal(t, []byte("10"), resp.Kvs[0].Value)
		leaseID := resp.Kvs[0].Lease
		assert.NotEqual(t, int64(0), leaseID)

		lresp, err := client.Lease.Leases(ctx)
		require.NoError(t, err)
		assert.Len(t, lresp.Leases, 1)
		assert.Equal(t, leaseID, int64(lresp.Leases[0].ID))

		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		resp, err = client.KV.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(0), resp.Count)
		assert.Empty(t, resp.Kvs)

		lresp, err = client.Lease.Leases(context.Background())
		require.NoError(t, err)
		assert.Empty(t, lresp.Leases)

		resp, err = client.KV.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Equal(t, int64(2), resp.Count)
		require.Len(t, resp.Kvs, 2)
		assert.Equal(t, []byte("abc/leadership/1"), resp.Kvs[0].Key)
		assert.Equal(t, []byte("abc/leadership/2"), resp.Kvs[1].Key)
		assert.Equal(t, []byte("10"), resp.Kvs[0].Value)
		assert.Equal(t, []byte("10"), resp.Kvs[1].Value)
	})

	t.Run("An existing key will gate becoming ready until deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		_, err := l.kv.Put(context.Background(), "abc/leadership/0", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/2", "10")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-errCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for error")
			}
		})

		lerrCh := make(chan error)
		go func() {
			lerrCh <- l.WaitForLeadership(ctx)
		}()

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}

		_, err = l.kv.Delete(context.Background(), "abc/leadership/0")
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
			t.Fatal("expected WaitForLeadership to return ready")
		case err := <-lerrCh:
			require.NoError(t, err)
		}
	})

	t.Run("Leadership will gate until all partition keys have the same total", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		_, err := l.kv.Put(context.Background(), "abc/leadership/2", "7")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/8", "9")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-errCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for error")
			}
		})

		lerrCh := make(chan error)
		go func() {
			lerrCh <- l.WaitForLeadership(ctx)
		}()

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		assert.Equal(t, []byte("10"), resp.Kvs[0].Value)

		_, err = l.kv.Put(context.Background(), "abc/leadership/2", "10")
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}

		_, err = l.kv.Put(context.Background(), "abc/leadership/8", "10")
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
			t.Fatal("expected WaitForLeadership to return ready")
		case err := <-lerrCh:
			require.NoError(t, err)
		}
	})

	t.Run("Leadership of different partition IDs should all become leader", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l1 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})
		l2 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 1,
			}),
		})
		l3 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 2,
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)

		go func() { errCh <- l1.Run(ctx) }()
		go func() { errCh <- l2.Run(ctx) }()
		go func() { errCh <- l3.Run(ctx) }()

		require.NoError(t, l1.WaitForLeadership(ctx))
		require.NoError(t, l2.WaitForLeadership(ctx))
		require.NoError(t, l3.WaitForLeadership(ctx))

		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Equal(t, int64(3), resp.Count)
		for _, kv := range resp.Kvs {
			assert.Equal(t, []byte("3"), kv.Value)
		}

		cancel()

		for i := 0; i < 3; i++ {
			select {
			case <-errCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for error")
			}
		}

		resp, err = client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Equal(t, int64(0), resp.Count)
	})
}

func Test_checkLeadershipKeys(t *testing.T) {
	t.Parallel()

	t.Run("if no leadership keys, return error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		ok, err := l.checkLeadershipKeys(context.Background())
		assert.False(t, ok)
		require.Error(t, err)
	})

	t.Run("if all keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		for i := 0; i < 10; i++ {
			_, err := l.kv.Put(context.Background(), "abc/leadership/"+strconv.Itoa(i), "10")
			require.NoError(t, err)
		}

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		_, err := l.kv.Put(context.Background(), "abc/leadership/0", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/3", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/5", "10")
		require.NoError(t, err)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total but this partition doesn't exist, return error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		_, err := l.kv.Put(context.Background(), "abc/leadership/3", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/5", "10")
		require.NoError(t, err)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.Error(t, err)
		assert.False(t, ok)
	})

	t.Run("if some keys have the same partition total but some don't, return error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		_, err := l.kv.Put(context.Background(), "abc/leadership/0", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/3", "5")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/5", "10")
		require.NoError(t, err)
		_, err = l.kv.Put(context.Background(), "abc/leadership/8", "8")
		require.NoError(t, err)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func Test_attemptPartitionLeadership(t *testing.T) {
	t.Parallel()

	t.Run("no previous leader, expect to become leader", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		lease, err := l.lease.Grant(context.Background(), 20)
		require.NoError(t, err)

		ok, err := l.attemptPartitionLeadership(context.Background(), lease.ID)
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		assert.Equal(t, int64(lease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, []byte("10"), resp.Kvs[0].Value)
	})

	t.Run("previous leader, expect not to become leader", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		prevlease, err := l.lease.Grant(context.Background(), 20)
		require.NoError(t, err)

		_, err = l.kv.Put(context.Background(), "abc/leadership/0", "10", clientv3.WithLease(prevlease.ID))
		require.NoError(t, err)

		lease, err := l.lease.Grant(context.Background(), 20)
		require.NoError(t, err)

		ok, err := l.attemptPartitionLeadership(context.Background(), lease.ID)
		require.NoError(t, err)
		assert.False(t, ok)

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		assert.NotEqual(t, int64(lease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, int64(prevlease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, []byte("10"), resp.Kvs[0].Value)
	})
}

func Test_WaitForLeadership(t *testing.T) {
	t.Parallel()

	t.Run("if context has been cancelled, expect context error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		leadership := New(Options{Client: new(clientv3.Client)})
		cancel()
		assert.Equal(t, context.Canceled, leadership.WaitForLeadership(ctx))
	})

	t.Run("if leadership is ready, expect nil", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		leadership := New(Options{Client: new(clientv3.Client)})
		close(leadership.readyCh)
		require.NoError(t, leadership.WaitForLeadership(ctx))
	})
}
