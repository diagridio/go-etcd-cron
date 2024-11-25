/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

//nolint:gocyclo
func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("Leadership should error if ctx canceled", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)

		go func() { errCh <- l.Run(ctx) }()
		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, context.Canceled, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}
	})

	t.Run("Leadership should become leader and become ready", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
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

		_, err = l.WaitForLeadership(ctx)
		require.NoError(t, err)
	})

	t.Run("Leadership should error if ctx canceled after being ready", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)

		go func() { errCh <- l.Run(ctx) }()

		_, err = l.WaitForLeadership(ctx)
		require.NoError(t, err)

		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, context.Canceled, err)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for error")
		}
	})

	t.Run("Running leadership multiple times should error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
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

		_, err = l.WaitForLeadership(ctx)
		require.NoError(t, err)
		require.Error(t, l.Run(ctx))
	})

	t.Run("Closing the leadership should delete the accosted partition leader key", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()

		_, err = l.WaitForLeadership(ctx)
		require.NoError(t, err)

		resp, err := client.Get(ctx, "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)

		var retrievedExpr expr.Expr
		assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))

		assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr.Location)

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

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		putLeadershipData(t, client, 1, 10, replicaData)
		putLeadershipData(t, client, 2, 10, replicaData)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() { errCh <- l.Run(ctx) }()

		_, err = l.WaitForLeadership(ctx)
		require.NoError(t, err)

		resp, err := client.Get(ctx, "abc/leadership/0")
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)

		var retrievedExpr expr.Expr
		assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))

		assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr.Location)

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

		var leader0 stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader0))
		assert.Equal(t, uint32(10), leader0.Total)

		var retrievedExpr0 expr.Expr
		assert.NoError(t, leader0.ReplicaData.UnmarshalTo(&retrievedExpr0))

		assert.Equal(t, exprMessage.Expression, retrievedExpr0.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr0.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr0.Location)

		var leader1 stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[1].Value, &leader1))
		assert.Equal(t, uint32(10), leader1.Total)

		var retrievedExpr1 expr.Expr
		assert.NoError(t, leader1.ReplicaData.UnmarshalTo(&retrievedExpr1))

		assert.Equal(t, exprMessage.Expression, retrievedExpr1.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr1.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr1.Location)
	})

	t.Run("An existing key will gate becoming ready until deleted", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		putLeadershipData(t, client, 0, 10, replicaData)
		putLeadershipData(t, client, 2, 10, replicaData)

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
			_, err := l.WaitForLeadership(ctx)
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

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		putLeadershipData(t, client, 2, 10, replicaData)
		putLeadershipData(t, client, 8, 9, replicaData)

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
			_, err = l.WaitForLeadership(ctx)
			lerrCh <- err
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

		var retrievedExpr expr.Expr
		assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))
		assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr.Location)

		putLeadershipData(t, client, 2, 10, replicaData)

		select {
		case <-time.After(time.Second):
		case <-lerrCh:
			t.Fatal("expected WaitForLeadership to block")
		}

		putLeadershipData(t, client, 8, 10, replicaData)

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

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l1 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})
		l2 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 1,
			}),
			ReplicaData: replicaData,
		})
		l3 := New(Options{
			Client:         client,
			PartitionTotal: 3,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 2,
			}),
			ReplicaData: replicaData,
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)

		go func() { errCh <- l1.Run(ctx) }()
		go func() { errCh <- l2.Run(ctx) }()
		go func() { errCh <- l3.Run(ctx) }()

		_, err1 := l1.WaitForLeadership(ctx)
		_, err2 := l2.WaitForLeadership(ctx)
		_, err3 := l3.WaitForLeadership(ctx)

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)

		resp, err := client.Get(context.Background(), "abc/leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Equal(t, int64(3), resp.Count)
		for _, kv := range resp.Kvs {

			var leader stored.Leadership
			assert.NoError(t, proto.Unmarshal(kv.Value, &leader))
			assert.Equal(t, uint32(3), leader.Total)

			var retrievedExpr expr.Expr
			assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))

			assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
			assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
			assert.Equal(t, exprMessage.Location, retrievedExpr.Location)
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

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l1 := New(Options{
			Client:         client,
			PartitionTotal: 1,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})
		l2 := New(Options{
			Client:         client,
			PartitionTotal: 1,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())

		errCh := make(chan error)

		go func() { errCh <- l1.Run(ctx1) }()
		_, err1 := l1.WaitForLeadership(ctx1)
		require.NoError(t, err1)

		resp, err := client.Leases(context.Background())
		require.NoError(t, err)
		assert.Len(t, resp.Leases, 1)

		resp1, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp1.Count)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp1.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(1), leader.Total)

		var retrievedExpr expr.Expr
		assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))

		assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr.Location)

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

		_, err2 := l2.WaitForLeadership(ctx2)
		require.NoError(t, err2)

		resp2, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp2.Count)

		var leader2 stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp2.Kvs[0].Value, &leader2))
		assert.Equal(t, uint32(1), leader2.Total)

		var retrievedExpr2 expr.Expr
		assert.NoError(t, leader2.ReplicaData.UnmarshalTo(&retrievedExpr2))

		assert.Equal(t, exprMessage.Expression, retrievedExpr2.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr2.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr2.Location)

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

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		ok, err := l.checkLeadershipKeys(context.Background())
		assert.False(t, ok)
		require.Error(t, err)
	})

	t.Run("if all keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		require.NoError(t, err)
		for i := range 10 {
			putLeadershipData(t, client, uint32(i), 10, replicaData)
		}

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total, return true", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		putLeadershipData(t, client, 0, 10, replicaData)
		putLeadershipData(t, client, 3, 10, replicaData)
		putLeadershipData(t, client, 5, 10, replicaData)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("if some keys have the same partition total but this partition doesn't exist, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		putLeadershipData(t, client, 3, 10, replicaData)
		putLeadershipData(t, client, 5, 10, replicaData)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.Error(t, err)
		assert.False(t, ok)
	})

	t.Run("if some keys have the same partition total but some don't, return error", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		putLeadershipData(t, client, 0, 10, replicaData)
		putLeadershipData(t, client, 3, 5, replicaData)
		putLeadershipData(t, client, 5, 10, replicaData)
		putLeadershipData(t, client, 8, 8, replicaData)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("old version of leadership value format (non-protobuf), return err", func(t *testing.T) {
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

		_, err := client.Put(context.Background(), "abc/leadership/0", "10")
		require.NoError(t, err)

		ok, err := l.checkLeadershipKeys(context.Background())
		require.Error(t, err)

		assert.ErrorContains(t, err, "failed to unmarshal leadership data: proto:")
		assert.ErrorContains(t, err, "cannot parse invalid wire-format data")
		assert.False(t, ok)
	})
}

func Test_attemptPartitionLeadership(t *testing.T) {
	t.Parallel()

	t.Run("no previous leader, expect to become leader", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := client.Revoke(context.Background(), lease.ID)
			assert.NoError(t, err, "failed to revoke lease")
		})

		ok, err := l.attemptPartitionLeadership(context.Background(), lease.ID)
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		assert.Equal(t, int64(lease.ID), resp.Kvs[0].Lease)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)

		var retrievedExpr expr.Expr
		assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))

		assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr.Location)

	})

	t.Run("previous leader, expect not to become leader", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		exprMessage := &expr.Expr{
			Expression:  "cron-test-expression",
			Description: "this is dummy cron test data. ooo lala",
			Location:    "home",
		}

		replicaData, err := anypb.New(exprMessage)
		require.NoError(t, err)

		l := New(Options{
			Client:         client,
			PartitionTotal: 10,
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			ReplicaData: replicaData,
		})

		prevlease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := client.Revoke(context.Background(), prevlease.ID)
			assert.NoError(t, err, "failed to revoke prevlease")
		})

		replicaDataBytes, err := proto.Marshal(&stored.Leadership{
			Total:       l.partitionTotal,
			ReplicaData: l.replicaData,
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/leadership/0", string(replicaDataBytes), clientv3.WithLease(prevlease.ID))
		require.NoError(t, err)

		lease, err := client.Grant(context.Background(), 20)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := client.Revoke(context.Background(), lease.ID)
			assert.NoError(t, err, "failed to revoke lease")
		})

		ok, err := l.attemptPartitionLeadership(context.Background(), lease.ID)
		require.NoError(t, err)
		assert.False(t, ok)

		resp, err := client.Get(context.Background(), "abc/leadership/0")
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.Count)
		assert.NotEqual(t, int64(lease.ID), resp.Kvs[0].Lease)
		assert.Equal(t, int64(prevlease.ID), resp.Kvs[0].Lease)

		var leader stored.Leadership
		assert.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &leader))
		assert.Equal(t, uint32(10), leader.Total)

		var retrievedExpr expr.Expr
		assert.NoError(t, leader.ReplicaData.UnmarshalTo(&retrievedExpr))

		assert.Equal(t, exprMessage.Expression, retrievedExpr.Expression)
		assert.Equal(t, exprMessage.Description, retrievedExpr.Description)
		assert.Equal(t, exprMessage.Location, retrievedExpr.Location)

	})
}

func Test_WaitForLeadership(t *testing.T) {
	t.Parallel()

	t.Run("if context has been canceled, expect context error", func(t *testing.T) {
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

	t.Run("if leadership is closed, expect error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		leadership := New(Options{Client: client.New(client.Options{})})
		close(leadership.closeCh)

		_, err := leadership.WaitForLeadership(ctx)
		assert.EqualError(t, err, "leadership closed")
	})

	t.Run("if leadership is closed after being ready, expect nil", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		leadership := New(Options{Client: client.New(client.Options{})})
		close(leadership.readyCh)

		leaderCtx, err := leadership.WaitForLeadership(ctx)
		assert.NoError(t, err)

		close(leadership.closeCh)

		// allow time for go routine to execute and cancel the ctx
		time.Sleep(10 * time.Millisecond)

		// Verify that leaderCtx is done
		select {
		case <-leaderCtx.Done():
			assert.ErrorIs(t, leaderCtx.Err(), context.Canceled)
		default:
			t.Fatal("expected leaderCtx to be done after closeCh was closed")
		}
	})

	t.Run("ensure leadershipCtx is canceled with parent ctx after leadership is ready", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		leadership := New(Options{Client: client.New(client.Options{})})
		close(leadership.readyCh)

		leaderCtx, err := leadership.WaitForLeadership(ctx)
		assert.NoError(t, err)

		// check leadershipCtx
		select {
		case <-leaderCtx.Done():
		default:
			cancel()
		}

		assert.ErrorIs(t, leaderCtx.Err(), context.Canceled)
	})

	t.Run("when a leadership change occurs, expect ctx canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		leadership := New(Options{Client: client.New(client.Options{})})
		close(leadership.readyCh)

		leaderCtx, err := leadership.WaitForLeadership(ctx)
		require.NoError(t, err)

		close(leadership.changeCh)

		select {
		case <-leaderCtx.Done():
			assert.ErrorIs(t, leaderCtx.Err(), context.Canceled)
		case <-time.After(1 * time.Second):
			t.Fatal("expected leadership context to be canceled but it was not")
		}
	})

	t.Run("when leadership becomes ready after a delay, expect nil", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		leadership := New(Options{Client: client.New(client.Options{})})

		go func() {
			time.Sleep(100 * time.Millisecond)
			close(leadership.readyCh)
		}()

		_, err := leadership.WaitForLeadership(ctx)
		require.NoError(t, err)
	})
}
func putLeadershipData(t *testing.T, client client.Interface, partitionID, total uint32, replicaData *anypb.Any) {
	t.Helper()

	leadershipData, err := proto.Marshal(&stored.Leadership{
		Total:       total,
		ReplicaData: replicaData,
	})
	require.NoError(t, err, "failed to marshal leadership data")

	_, err = client.Put(context.Background(), fmt.Sprintf("abc/leadership/%d", partitionID), string(leadershipData))
	require.NoError(t, err, "failed to insert leadership data into etcd")
}
