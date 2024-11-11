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

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

//nolint:gocyclo
func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("Leadership should become leader and become ready", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
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

		client := etcd.Embedded(t)
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

		client := etcd.Embedded(t)
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

		resp, err := client.Get(ctx, "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)
		leaseID := resp.Kvs[0].Lease
		assert.NotEqual(t, int64(0), leaseID)

		lresp, err := client.Leases(ctx)
		require.NoError(t, err)
		assert.Len(t, lresp.Leases, 1)
		assert.Equal(t, leaseID, int64(lresp.Leases[0].ID))

		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		resp, err = client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(0), resp.Count)
		assert.Empty(t, resp.Kvs)

		lresp, err = client.Leases(context.Background())
		require.NoError(t, err)
		assert.Empty(t, lresp.Leases)
	})

	t.Run("Closing the leadership should not delete the other partition keys", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/1", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/2", string(leadershipData))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()

		require.NoError(t, l.WaitForLeadership(context.Background()))

		resp, err := client.Get(ctx, "abc/leadership/0")
		assert.NotNil(t, resp)
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)
		leaseID := resp.Kvs[0].Lease
		assert.NotEqual(t, int64(0), leaseID)

		lresp, err := client.Leases(ctx)
		require.NoError(t, err)
		assert.Len(t, lresp.Leases, 1)
		assert.Equal(t, leaseID, int64(lresp.Leases[0].ID))

		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		resp, err = client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(0), resp.Count)
		assert.Empty(t, resp.Kvs)

		lresp, err = client.Leases(context.Background())
		require.NoError(t, err)
		assert.Empty(t, lresp.Leases)

		resp, err = client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Equal(t, int64(2), resp.Count)
		require.Len(t, resp.Kvs, 2)
		assert.Equal(t, []byte("abc/leadership/1"), resp.Kvs[0].Key)
		assert.Equal(t, []byte("abc/leadership/2"), resp.Kvs[1].Key)

		var leader1 stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader1))
		assert.Equal(t, uint32(10), leader1.Total)
		var leader2 stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[1].Value, &leader2))
		assert.Equal(t, uint32(10), leader2.Total)
	})

	t.Run("An existing key will gate becoming ready until deleted", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/0", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/2", string(leadershipData))
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

		_, err = client.Delete(context.Background(), "abc/leadership/0")
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

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 7}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/2", string(leadershipData))
		require.NoError(t, err)

		leadershipProto = &stored.Leadership{Total: 9}
		leadershipData, err = proto.Marshal(leadershipProto)
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/8", string(leadershipData))
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
		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)

		leadershipProto = &stored.Leadership{Total: 10}
		leadershipData, err = proto.Marshal(leadershipProto)
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/2", string(leadershipData))
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}

		leadershipProto = &stored.Leadership{Total: 10}
		leadershipData, err = proto.Marshal(leadershipProto)
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/8", string(leadershipData))
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

		client := etcd.Embedded(t)
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
			var leader stored.Leadership
			assert.NoError(t, proto.Unmarshal(kv.Value, &leader))
			assert.Equal(t, uint32(3), leader.Total)
		}

		cancel()

		for range 3 {
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

	t.Run("Two leaders of the same partition should make one passive until the other is closed", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l1 := New(Options{
			Client:         client,
			PartitionTotal: 1,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})
		l2 := New(Options{
			Client:         client,
			PartitionTotal: 1,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())

		errCh := make(chan error)

		go func() { errCh <- l1.Run(ctx1) }()
		require.NoError(t, l1.WaitForLeadership(ctx1))
		resp, err := client.Leases(context.Background())
		oldLease := resp.Leases
		require.NoError(t, err)
		assert.Len(t, resp.Leases, 1)

		resp1, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp1.Count)
		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp1.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(1), leader.Total)

		go func() { errCh <- l2.Run(ctx2) }()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Leases(context.Background())
			require.NoError(t, err)
			assert.Len(c, resp.Leases, 2)
		}, time.Second*5, time.Millisecond*10)

		cancel1()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		resp, err = client.Leases(context.Background())
		require.NoError(t, err)
		assert.Len(t, resp.Leases, 1)
		assert.NotEqual(t, oldLease, resp.Leases)

		require.NoError(t, l2.WaitForLeadership(ctx2))

		resp2, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp2.Count)
		var leader1 stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp2.Kvs[0].Value, &leader1))
		assert.Equal(t, uint32(1), leader1.Total)

		cancel2()

		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}
	})
}

func Test_checkLeadershipKeys(t *testing.T) {
	t.Parallel()

	t.Run("if no leadership keys, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
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

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abcdef",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)
		for i := range 10 {
			_, err := client.Put(context.Background(), "abcdef/leadership/"+strconv.Itoa(i), string(leadershipData))
			require.NoError(t, err)
		}

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/0", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/3", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/5", string(leadershipData))
		require.NoError(t, err)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total but this partition doesn't exist, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/3", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/5", string(leadershipData))
		require.NoError(t, err)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.Error(t, err)
		assert.False(t, ok)
	})

	t.Run("if some keys have the same partition total but some don't, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		leadershipProto5 := &stored.Leadership{Total: 5}
		leadershipData5, err := proto.Marshal(leadershipProto5)
		require.NoError(t, err)

		leadershipProto8 := &stored.Leadership{Total: 8}
		leadershipData8, err := proto.Marshal(leadershipProto8)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/0", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/3", string(leadershipData5))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/5", string(leadershipData))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/8", string(leadershipData8))
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

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		ok, err := l.attemptPartitionLeadership(context.Background(), lease.ID)
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, int64(lease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, uint32(10), leader.Total)
	})

	t.Run("previous leader, expect not to become leader", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		prevlease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/0", string(leadershipData), clientv3.WithLease(prevlease.ID))
		require.NoError(t, err)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		ok, err := l.attemptPartitionLeadership(context.Background(), lease.ID)
		require.NoError(t, err)
		assert.False(t, ok)

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.NotEqual(t, int64(lease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, int64(prevlease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, uint32(10), leader.Total)
	})
}

func Test_WaitForLeadership(t *testing.T) {
	t.Parallel()

	t.Run("if context has been cancelled, expect context error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		leadership := New(Options{Client: client.New(client.Options{})})
		cancel()
		assert.Equal(t, context.Canceled, leadership.WaitForLeadership(ctx))
	})

	t.Run("if leadership is ready, expect nil", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		leadership := New(Options{Client: client.New(client.Options{})})
		close(leadership.readyCh)
		require.NoError(t, leadership.WaitForLeadership(ctx))
	})
}
