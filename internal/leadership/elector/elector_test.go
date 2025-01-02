/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package elector

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/informer"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Relect(t *testing.T) {
	t.Parallel()

	replicaData := &anypb.Any{Value: []byte("hello")}

	client := etcd.Embedded(t)
	lease, err := client.Grant(context.Background(), 20)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	key1, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "helloworld1",
	})
	require.NoError(t, err)
	inf1, err := informer.New(ctx, informer.Options{
		Client: client,
		Key:    key1,
	})
	require.NoError(t, err)
	e1 := New(Options{
		Log:         logr.Discard(),
		Client:      client,
		Key:         key1,
		ReplicaData: replicaData,
		LeaseID:     lease.ID,
		Informer:    inf1,
	})

	key2, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "helloworld2",
	})
	require.NoError(t, err)
	inf2, err := informer.New(ctx, informer.Options{
		Client: client,
		Key:    key2,
	})
	require.NoError(t, err)
	e2 := New(Options{
		Log:         logr.Discard(),
		Client:      client,
		Key:         key2,
		ReplicaData: replicaData,
		LeaseID:     lease.ID,
		Informer:    inf2,
	})

	errCh := make(chan error, 2)

	go func() {
		_, _, err := e1.Elect(ctx)
		errCh <- err
	}()
	go func() {
		_, _, err := e2.Elect(ctx)
		errCh <- err
	}()

	for range 2 {
		select {
		case <-time.After(time.Second * 10):
			require.Fail(t, "timed out")
		case err := <-errCh:
			require.NoError(t, err)
		}
	}

	key3, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "helloworld3",
	})
	require.NoError(t, err)
	inf3, err := informer.New(ctx, informer.Options{
		Client: client,
		Key:    key3,
	})
	require.NoError(t, err)
	e3 := New(Options{
		Log:         logr.Discard(),
		Client:      client,
		Key:         key3,
		ReplicaData: replicaData,
		LeaseID:     lease.ID,
		Informer:    inf3,
	})

	electedCh := make(chan *Elected)

	go func() {
		_, elected, err := e3.Elect(ctx)
		errCh <- err
		electedCh <- elected
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(c, resp.Kvs, 3)
	}, time.Second*10, time.Millisecond*10)

	go func() {
		_, elected, err := e1.Reelect(ctx)
		errCh <- err
		electedCh <- elected
	}()
	go func() {
		_, elected, err := e2.Reelect(ctx)
		errCh <- err
		electedCh <- elected
	}()

	for range 3 {
		select {
		case <-time.After(time.Second * 10):
			require.Fail(t, "timed out")
		case err := <-errCh:
			require.NoError(t, err)
		}
		select {
		case <-time.After(time.Second * 10):
			require.Fail(t, "timed out")
		case el := <-electedCh:
			assert.Len(t, el.LeadershipData, 3)
		}
	}
}

func Test_Elect(t *testing.T) {
	t.Parallel()

	replicaData := &anypb.Any{Value: []byte("hello")}

	t.Run("if no existing leadership, expect to be elected leader", func(t *testing.T) {
		t.Parallel()

		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)

		client := etcd.Embedded(t)
		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		inf, err := informer.New(ctx, informer.Options{
			Client: client,
			Key:    key,
		})
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
			Informer:    inf,
		})

		_, el, err := e.Elect(ctx)
		require.NoError(t, err)
		require.Len(t, el.LeadershipData, 1)
		assert.True(t, proto.Equal(el.LeadershipData[0], replicaData))
	})

	t.Run("if multiple leaders, expect all elect", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		key1, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld1",
		})
		require.NoError(t, err)
		inf1, err := informer.New(ctx, informer.Options{
			Client: client,
			Key:    key1,
		})
		require.NoError(t, err)
		e1 := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key1,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
			Informer:    inf1,
		})

		key2, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld2",
		})
		require.NoError(t, err)
		inf2, err := informer.New(ctx, informer.Options{
			Client: client,
			Key:    key2,
		})
		require.NoError(t, err)
		e2 := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key2,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
			Informer:    inf2,
		})

		errCh := make(chan error, 2)

		go func() {
			_, _, err := e1.Elect(ctx)
			errCh <- err
		}()
		go func() {
			_, _, err := e2.Elect(ctx)
			errCh <- err
		}()

		for range 2 {
			select {
			case <-time.After(time.Second * 10):
				require.Fail(t, "timed out")
			case err := <-errCh:
				require.NoError(t, err)
			}
		}
	})
}

func Test_haveQuorum(t *testing.T) {
	t.Parallel()

	key, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "helloworld",
	})
	require.NoError(t, err)
	replicaData := &anypb.Any{Value: []byte("hello")}

	t.Run("if the expTotal and resp count is different, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)
		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		require.Error(t, e.haveQuorum(resp, 3))
	})

	t.Run("if the expTotal and resp count is same, expect no error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		require.NoError(t, e.haveQuorum(resp, 2))
	})

	t.Run("if leader thinks total is different, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       3,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		require.Error(t, e.haveQuorum(resp, 2))
	})

	t.Run("if we don't find outselves, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l2, err := proto.Marshal(&stored.Leadership{
			Total:       1,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		require.Error(t, e.haveQuorum(resp, 1))
	})

	t.Run("if our uid has changed, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         123,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		require.Error(t, e.haveQuorum(resp, 2))
	})

	t.Run("if our replica data has changed, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		require.Error(t, e.haveQuorum(resp, 2))
	})
}

func Test_quorumReconcile(t *testing.T) {
	t.Parallel()

	key, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "helloworld",
	})
	require.NoError(t, err)

	replicaData := &anypb.Any{Value: []byte("hello")}

	t.Run("if self isn't in resp, ecpect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		a, ok, err := e.quorumReconcile(context.Background(), new(clientv3.GetResponse))
		require.Error(t, err)
		assert.False(t, ok)
		assert.Nil(t, a)
	})

	t.Run("if self exists with 1, expect no write with ok", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       1,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		a, ok, err := e.quorumReconcile(context.Background(), resp)
		require.NoError(t, err)
		assert.True(t, ok)
		require.Len(t, a, 1)
		assert.True(t, proto.Equal(a[0], replicaData))
	})

	t.Run("if self exists with 2, expect ok with write 1", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		a, ok, err := e.quorumReconcile(context.Background(), resp)
		require.NoError(t, err)
		assert.True(t, ok)
		require.Len(t, a, 1)
		assert.True(t, proto.Equal(a[0], replicaData))

		resp, err = client.Get(context.Background(), "abc/leadership/helloworld")
		require.NoError(t, err)
		exp, err := proto.Marshal(&stored.Leadership{
			Total:       1,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		assert.Equal(t, exp, resp.Kvs[0].Value)
	})

	t.Run("if self exists with 2, but other thinks 1, expect false", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       1,
			Uid:         123,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/1", string(l2))
		require.NoError(t, err)
		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		a, ok, err := e.quorumReconcile(context.Background(), resp)
		require.NoError(t, err)
		assert.False(t, ok)
		require.Len(t, a, 2)
		assert.True(t, proto.Equal(a[1], replicaData))
		assert.True(t, proto.Equal(a[0], &anypb.Any{Value: []byte("world")}))

		resp, err = client.Get(context.Background(), "abc/leadership/helloworld")
		require.NoError(t, err)
		exp, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		assert.Equal(t, exp, resp.Kvs[0].Value)
	})
}

func Test_reconcileSelf(t *testing.T) {
	t.Parallel()

	key, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "helloworld",
	})
	require.NoError(t, err)

	replicaData := &anypb.Any{Value: []byte("hello")}

	t.Run("if the self leadership uid has changed, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		selfLeader := &stored.Leadership{
			Total:       1,
			Uid:         123,
			ReplicaData: replicaData,
		}
		self := &mvccpb.KeyValue{Value: []byte("123")}

		err = e.reconcileSelf(context.Background(), self, selfLeader, 1)
		require.Error(t, err)
	})

	t.Run("if the self leadership replica data has changed, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		selfLeader := &stored.Leadership{
			Total:       1,
			Uid:         e.uid,
			ReplicaData: &anypb.Any{Value: []byte("world")},
		}
		selfValue, err := proto.Marshal(selfLeader)
		require.NoError(t, err)
		self := &mvccpb.KeyValue{Value: selfValue}

		err = e.reconcileSelf(context.Background(), self, selfLeader, 1)
		require.Error(t, err)
	})

	t.Run("if partition total has not changed, expect no error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		selfLeader := &stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		}
		selfValue, err := proto.Marshal(selfLeader)
		require.NoError(t, err)
		self := &mvccpb.KeyValue{Value: selfValue}

		err = e.reconcileSelf(context.Background(), self, selfLeader, 2)
		require.NoError(t, err)
	})

	t.Run("if partition total has changed, but the written key value is different, expect error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		selfLeader := &stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		}
		selfValue, err := proto.Marshal(selfLeader)
		require.NoError(t, err)
		self := &mvccpb.KeyValue{Value: selfValue}

		wproto, err := proto.Marshal(&stored.Leadership{
			Total:       5,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(wproto))
		require.NoError(t, err)

		err = e.reconcileSelf(context.Background(), self, selfLeader, 3)
		require.Error(t, err)
	})

	t.Run("if partition total has changed, expect write new total and expect no error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		selfLeader := &stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		}
		selfValue, err := proto.Marshal(selfLeader)
		require.NoError(t, err)
		self := &mvccpb.KeyValue{Value: selfValue}

		wproto, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(wproto))
		require.NoError(t, err)

		err = e.reconcileSelf(context.Background(), self, selfLeader, 3)
		require.NoError(t, err)

		resp, err := client.Get(context.Background(), "abc/leadership/helloworld")
		require.NoError(t, err)
		assert.Len(t, resp.Kvs, 1)

		var gproto stored.Leadership
		require.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &gproto))
		assert.True(t, proto.Equal(&gproto, &stored.Leadership{
			Total:       3,
			Uid:         e.uid,
			ReplicaData: replicaData,
		}))
	})
}

func Test_attemptNewLeadership(t *testing.T) {
	t.Parallel()

	t.Run("non-existing key should write leadership key", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)

		replicaData := &anypb.Any{Value: []byte("hello")}

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		ok, err := e.attemptNewLeadership(context.Background(), resp)
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err = client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, "abc/leadership/helloworld", string(resp.Kvs[0].Key))

		exp, err := proto.Marshal(&stored.Leadership{
			Total:       1,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		assert.Equal(t, exp, resp.Kvs[0].Value)
	})

	t.Run("non-existing key should write leadership key with other leaders", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)

		replicaData := &anypb.Any{Value: []byte("hello")}

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		l1, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         123,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		l2, err := proto.Marshal(&stored.Leadership{
			Total:       2,
			Uid:         456,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/1", string(l1))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/2", string(l2))
		require.NoError(t, err)

		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		ok, err := e.attemptNewLeadership(context.Background(), resp)
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err = client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 3)
		require.Equal(t, "abc/leadership/helloworld", string(resp.Kvs[2].Key))

		exp, err := proto.Marshal(&stored.Leadership{
			Total:       3,
			Uid:         e.uid,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)
		assert.Equal(t, exp, resp.Kvs[2].Value)
	})

	t.Run("existing key should not be written", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "helloworld",
		})
		require.NoError(t, err)

		replicaData := &anypb.Any{Value: []byte("hello")}
		exp, err := proto.Marshal(&stored.Leadership{
			Total:       1,
			Uid:         123,
			ReplicaData: replicaData,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/leadership/helloworld", string(exp))
		require.NoError(t, err)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)

		e := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         key,
			ReplicaData: replicaData,
			LeaseID:     lease.ID,
		})

		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)

		ok, err := e.attemptNewLeadership(context.Background(), resp)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
