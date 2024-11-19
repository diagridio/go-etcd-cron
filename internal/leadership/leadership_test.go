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
	"google.golang.org/protobuf/types/known/anypb"
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
			Key: *key.New(key.Options{
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

		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		defer leaderCancel()
		defer l.Close()
		_, err := l.WaitForLeadership(leaderCtx)
		require.NoError(t, err)
	})

	t.Run("Running leadership multiple times should error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
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

		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		defer leaderCancel()
		defer l.Close()
		_, err := l.WaitForLeadership(leaderCtx)
		require.NoError(t, err)
		require.Error(t, l.Run(ctx))
	})
	t.Run("Closing the leadership should delete the accosted partition leader key", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()
		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		defer leaderCancel()
		_, err := l.WaitForLeadership(leaderCtx)
		require.NoError(t, err)

		resp, err := client.Get(ctx, "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)

		assert.NotNil(t, resp.Kvs[0].Value)
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
		close(l.changeCh)
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

	t.Run("Closing leadership should release the leader key", func(t *testing.T) {
		t.Parallel()

		leadershipProto1 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l10-initial-replica-data")},
		}
		anyLeadershipData1, err := anypb.New(leadershipProto1)
		require.NoError(t, err)

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
				Namespace:   "abcd",
				PartitionID: 0,
			}),
			ReplicaData: anyLeadershipData1,
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- l.Run(ctx)
		}()

		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		defer leaderCancel()

		_, err = l.WaitForLeadership(leaderCtx)
		require.NoError(t, err)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			// ensure leadership is active and key exists in etcd
			resp, err := client.Get(context.Background(), "abcd/leadership/0")
			require.NoError(t, err)
			assert.Equal(t, int64(1), resp.Count)
		}, time.Second*5, time.Millisecond*10)

		l.Close()
		cancel()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		// ensure the partition leader key is removed
		resp, err := client.Get(context.Background(), "abcd/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(0), resp.Count)
	})

	t.Run("Closing the leadership should not delete the other partition keys", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
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

		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		defer leaderCancel()
		_, err = l.WaitForLeadership(leaderCtx)
		require.NoError(t, err)

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
		l.Close()
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
			Key: *key.New(key.Options{
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
			leaderCtx, leaderCancel := context.WithCancel(context.Background())
			defer leaderCancel()
			defer l.Close()
			_, err := l.WaitForLeadership(leaderCtx)
			lerrCh <- err
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

		leadershipProto1 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l10-initial-replica-data")},
		}
		anyLeadershipData1, err := anypb.New(leadershipProto1)
		require.NoError(t, err)

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
				Namespace:   "cas",
				PartitionID: 0,
			}),
			ReplicaData: anyLeadershipData1,
		})

		leadershipProto := &stored.Leadership{Total: 7}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)
		putCtx, putCancel := context.WithCancel(context.Background())
		_, err = client.Put(putCtx, "cas/leadership/2", string(leadershipData))
		putCancel()
		require.NoError(t, err)

		leadershipProto = &stored.Leadership{Total: 9}
		leadershipData, err = proto.Marshal(leadershipProto)
		require.NoError(t, err)
		putCtx, putCancel = context.WithCancel(context.Background())
		_, err = client.Put(putCtx, "cas/leadership/8", string(leadershipData))
		putCancel()
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
		leaderCtx, leaderCancel := context.WithCancel(context.Background())
		defer leaderCancel()
		go func() {
			_, err := l.WaitForLeadership(leaderCtx)
			lerrCh <- err
		}()
		t.Cleanup(func() {
			leaderCancel()
			l.Close()
		})

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}
		getCtx, getCancel := context.WithCancel(context.Background())
		_, err = client.Get(getCtx, "cas/leadership/", clientv3.WithPrefix())
		getCancel()
		getCtx, getCancel = context.WithCancel(context.Background())
		resp, err := client.Get(context.Background(), "cas/leadership/0")
		getCancel()
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)

		leadershipProto = &stored.Leadership{Total: 10}
		leadershipData, err = proto.Marshal(leadershipProto)
		require.NoError(t, err)
		putCtx, putCancel = context.WithCancel(context.Background())
		_, err = client.Put(putCtx, "cas/leadership/2", string(leadershipData))
		putCancel()
		require.NoError(t, err)

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}

		leadershipProto = &stored.Leadership{Total: 10}
		leadershipData, err = proto.Marshal(leadershipProto)
		require.NoError(t, err)
		putCtx, putCancel = context.WithCancel(context.Background())
		_, err = client.Put(putCtx, "cas/leadership/8", string(leadershipData))
		putCancel()
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
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})
		l2 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 1,
			}),
		})
		l3 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 2,
			}),
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)

		go func() { errCh <- l1.Run(ctx) }()
		go func() { errCh <- l2.Run(ctx) }()
		go func() { errCh <- l3.Run(ctx) }()

		_, l1err := l1.WaitForLeadership(ctx)
		_, l2err := l2.WaitForLeadership(ctx)
		_, l3err := l3.WaitForLeadership(ctx)
		require.NoError(t, l1err)
		require.NoError(t, l2err)
		require.NoError(t, l3err)

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
			Key: *key.New(key.Options{
				Namespace:   "abce",
				PartitionID: 0,
			}),
		})
		l2 := New(Options{
			Client:         client,
			PartitionTotal: 1,
			Key: *key.New(key.Options{
				Namespace:   "abce",
				PartitionID: 0,
			}),
		})

		ctx1, cancel1 := context.WithCancel(context.Background())

		errCh := make(chan error)

		go func() { errCh <- l1.Run(ctx1) }()
		waitCtx, err := l1.WaitForLeadership(ctx1)
		require.NoError(t, err)
		resp, err := client.Leases(context.Background())
		oldLease := resp.Leases
		require.NoError(t, err)
		assert.Len(t, resp.Leases, 1)

		resp1, err := client.Get(context.Background(), "abce/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp1.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp1.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(1), leader.Total)

		ctx2, cancel2 := context.WithCancel(context.Background())

		go func() { errCh <- l2.Run(ctx2) }()

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Leases(context.Background())
			require.NoError(t, err)
			assert.Len(c, resp.Leases, 2)
		}, time.Second*5, time.Millisecond*10)

		cancel1()
		l1.Close()
		<-waitCtx.Done()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}

		resp, err = client.Leases(context.Background())
		require.NoError(t, err)
		assert.Len(t, resp.Leases, 1)
		assert.NotEqual(t, oldLease, resp.Leases)

		_, err = l2.WaitForLeadership(ctx2)
		require.NoError(t, err)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {

			resp2, err := client.Get(context.Background(), "abce/leadership/0")
			require.NoError(t, err)
			require.Equal(t, int64(1), resp2.Count)
			var leader1 stored.Leadership
			assert.NoError(t, proto.Unmarshal(resp2.Kvs[0].Value, &leader1))
			assert.Equal(t, uint32(1), leader1.Total)
		}, time.Second*5, time.Millisecond*100)

		cancel2()
		l2.Close()
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
			Key: *key.New(key.Options{
				Namespace:   "ced",
				PartitionID: 0,
			}),
		})
		ctx, cancel := context.WithCancel(context.Background())
		ok, err := l.checkLeadershipKeys(ctx)
		cancel()
		assert.False(t, ok)
		require.Error(t, err)
	})

	t.Run("if all keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		leadershipProto1 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l10-initial-replica-data")},
		}
		anyLeadershipData1, err := anypb.New(leadershipProto1)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
				Namespace:   "abcdef",
				PartitionID: 0,
			}),
			ReplicaData: anyLeadershipData1,
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)
		for i := range 10 {
			ctx, cancel := context.WithCancel(context.Background())
			_, err := client.Put(ctx, "abcdef/leadership/"+strconv.Itoa(i), string(leadershipData))
			cancel()
			require.NoError(t, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		ok, err := l.checkLeadershipKeys(ctx)
		cancel()
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/0", string(leadershipData))
		cancel()
		require.NoError(t, err)

		ctx, cancel = context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/3", string(leadershipData))
		cancel()
		require.NoError(t, err)

		ctx, cancel = context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/5", string(leadershipData))
		cancel()
		require.NoError(t, err)

		ctx, cancel = context.WithCancel(context.Background())
		ok, err := l.checkLeadershipKeys(ctx)
		cancel()

		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total but this partition doesn't exist, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
		})

		leadershipProto := &stored.Leadership{Total: 10}
		leadershipData, err := proto.Marshal(leadershipProto)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/3", string(leadershipData))
		cancel()
		require.NoError(t, err)
		ctx, cancel = context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/5", string(leadershipData))
		cancel()
		require.NoError(t, err)

		ctx, cancel = context.WithCancel(context.Background())
		ok, err := l.checkLeadershipKeys(ctx)
		cancel()
		require.Error(t, err)
		assert.False(t, ok)
	})

	t.Run("if some keys have the same partition total but some don't, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: *key.New(key.Options{
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

		ctx, cancel := context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/0", string(leadershipData))
		cancel()
		require.NoError(t, err)
		ctx, cancel = context.WithCancel(context.Background())
		_, err = client.Put(ctx, "abc/leadership/3", string(leadershipData5))
		cancel()
		require.NoError(t, err)
		ctx, cancel = context.WithCancel(context.Background())

		_, err = client.Put(ctx, "abc/leadership/5", string(leadershipData))
		cancel()

		require.NoError(t, err)
		ctx, cancel = context.WithCancel(context.Background())

		_, err = client.Put(ctx, "abc/leadership/8", string(leadershipData8))
		cancel()

		require.NoError(t, err)

		keyctx, keycancel := context.WithCancel(context.Background())
		ok, err := l.checkLeadershipKeys(keyctx)
		t.Cleanup(func() {
			keycancel()
		})
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
			Key: *key.New(key.Options{
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
			Key: *key.New(key.Options{
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
		_, err := leadership.WaitForLeadership(ctx)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("if leadership is ready, expect nil", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		leadership := New(Options{Client: client.New(client.Options{})})
		close(leadership.readyCh)
		_, err := leadership.WaitForLeadership(ctx)
		require.NoError(t, err)
	})

	t.Run("WaitForLeadership should context cancel on leadership change", func(t *testing.T) {
		t.Parallel()

		leadership := &Leadership{
			readyCh:  make(chan struct{}),
			changeCh: make(chan struct{}),
		}

		close(leadership.readyCh)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		leadCtx, err := leadership.WaitForLeadership(ctx)
		require.NoError(t, err)

		select {
		case <-leadCtx.Done():
			t.Fatal("leadCtx should not be canceled initially")
		default:
		}

		leadership.changeCh <- struct{}{}

		// ensure leadCtx is canceled
		select {
		case <-leadCtx.Done():
			assert.ErrorIs(t, leadCtx.Err(), context.Canceled, "leadCtx should be canceled after leadership change")
		case <-time.After(1 * time.Second):
			t.Fatal("leadCtx was not canceled after leadership change")
		}
	})
}
func Test_LeadershipSubscribe(t *testing.T) {
	t.Parallel()

	t.Run("Subscribe should show the total leadership replicas", func(t *testing.T) {
		t.Parallel()

		leadershipProto1 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l1-initial-replica-data")},
		}
		anyLeadershipData1, err := anypb.New(leadershipProto1)
		require.NoError(t, err)

		leadershipProto2 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l2-initial-replica-data")},
		}
		anyLeadershipData2, err := anypb.New(leadershipProto2)
		require.NoError(t, err)

		client := etcd.Embedded(t)

		l1 := New(Options{
			Client:         client,
			PartitionTotal: 2,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: anyLeadershipData1,
		})

		l2 := New(Options{
			Client:         client,
			PartitionTotal: 2,
			Key: *key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 1,
			}),
			ReplicaData: anyLeadershipData2,
		})

		errCh := make(chan error)

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel1()
		defer cancel2()

		go func() { errCh <- l1.Run(ctx1) }()
		go func() { errCh <- l2.Run(ctx2) }()

		t.Cleanup(func() {
			l1.Close()
			l2.Close()
		})

		// Wait for leadership for both instances
		_, l1err := l1.WaitForLeadership(ctx1)
		require.NoError(t, l1err)

		_, l2err := l2.WaitForLeadership(ctx2)
		require.NoError(t, l2err)

		// Subscribe to leadership events
		activeReplicaValues, _ := l1.Subscribe(ctx1)
		activeReplicaValuesL2, _ := l2.Subscribe(ctx2)

		require.Len(t, activeReplicaValues, 2)
		require.Len(t, activeReplicaValuesL2, 2)

		// l1
		require.NotEmpty(t, activeReplicaValues, "expected to get active replica values for l1 upon subscription")
		require.NotNil(t, activeReplicaValues[0])
		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(activeReplicaValues[0].Value, &leader))
		replicaData := string(leader.ReplicaData.Value)
		require.Equal(t, "l1-initial-replica-data", replicaData)
		require.NotNil(t, activeReplicaValues[1])
		assert.NoError(t, proto.Unmarshal(activeReplicaValues[1].Value, &leader))
		replicaData = string(leader.ReplicaData.Value)
		require.Equal(t, "l2-initial-replica-data", replicaData)

		// l2
		require.NotEmpty(t, activeReplicaValuesL2, "expected to get active replica values for l2 upon subscription")
		require.NotNil(t, activeReplicaValuesL2[0])
		var leaderL2 stored.Leadership
		assert.NoError(t, proto.Unmarshal(activeReplicaValuesL2[0].Value, &leaderL2))
		replicaDataL2 := string(leaderL2.ReplicaData.Value)
		require.Equal(t, "l1-initial-replica-data", replicaDataL2)
		assert.NoError(t, proto.Unmarshal(activeReplicaValuesL2[1].Value, &leaderL2))
		replicaDataL2 = string(leaderL2.ReplicaData.Value)
		require.Equal(t, "l2-initial-replica-data", replicaDataL2)
	})

	t.Run("Subscribe should notify when a leader goes up or down", func(t *testing.T) {
		t.Parallel()

		leadershipProto1 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l1-initial-replica-data")},
		}
		anyLeadershipData1, err := anypb.New(leadershipProto1)
		require.NoError(t, err)

		leadershipProto2 := &stored.Leadership{
			Total:       10,
			ReplicaData: &anypb.Any{Value: []byte("l2-initial-replica-data")},
		}
		anyLeadershipData2, err := anypb.New(leadershipProto2)
		require.NoError(t, err)

		client := etcd.Embedded(t)

		l1 := New(Options{
			Client:         client,
			PartitionTotal: 2,
			Key: *key.New(key.Options{
				Namespace:   "abcde",
				PartitionID: 0,
			}),
			ReplicaData: anyLeadershipData1,
		})

		l2 := New(Options{
			Client:         client,
			PartitionTotal: 2,
			Key: *key.New(key.Options{
				Namespace:   "abcde",
				PartitionID: 1,
			}),
			ReplicaData: anyLeadershipData2,
		})

		errCh := make(chan error)

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()

		go func() { errCh <- l1.Run(ctx1) }()
		go func() { errCh <- l2.Run(ctx2) }()

		_, l1err := l1.WaitForLeadership(ctx1)
		require.NoError(t, l1err)

		_, l2err := l2.WaitForLeadership(ctx2)
		require.NoError(t, l2err)

		var subscribeChL2 chan struct{}

		// Subscribe to leadership events
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			activeReplicaValues, _ := l1.Subscribe(ctx1)
			var activeReplicaValuesL2 []*anypb.Any
			activeReplicaValuesL2, subscribeChL2 = l2.Subscribe(ctx2)

			// Ensure leadership keys exist initially
			require.Len(t, activeReplicaValues, 2)
			require.Len(t, activeReplicaValuesL2, 2)
		}, time.Second*10, time.Millisecond*10)

		// leadership change
		cancel1()
		l1.Close()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}
		//_, err = client.Delete(context.Background(), "abcde/leadership/0")
		//require.NoError(t, err)
		//
		//l2.partitionTotal = 1
		_, l2err = l2.WaitForLeadership(ctx2)
		require.NoError(t, l2err)

		// only 1 leader now
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Get(context.Background(), "abcde/leadership", clientv3.WithPrefix())
			require.NoError(t, err)
			require.Equal(t, int64(1), resp.Count)
		}, time.Second*10, time.Millisecond*10)

		select {
		case <-subscribeChL2:
			updatedReplicaValuesL2 := l2.allReplicaDatas
			// l2 is the only leader now
			require.Len(t, updatedReplicaValuesL2, 1)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for leadership update for l2")
		}
		l2.Close()
		cancel2()
	})
}
