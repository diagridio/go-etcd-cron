/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/anypb"
)

func Test_watch_leadership_with_leadership_key_update(t *testing.T) {
	t.Parallel()

	cluster := cron.TripplePartitionRun(t)
	defer func() {
		for _, cr := range cluster.Crons {
			cr.Stop(t)
		}
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cluster.Cron.KV.Get(context.Background(), "abc/leadership/", clientv3.WithPrefix(), clientv3.WithSerializable())
		assert.NoError(t, err, "unexpected error querying etcd")
		assert.Equal(t, 3, len(resp.Kvs), "expected 3 leadership keys but got %d", len(resp.Kvs))
	}, 3*time.Second, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Watch leadership updates for all instances concurrently
	instanceUpdatesChs := make([]chan []*anypb.Any, len(cluster.Crons))
	for i := 0; i < len(cluster.Crons); i++ {
		instanceUpdatesCh, err := cluster.Crons[i].WatchLeadership(ctx)
		require.NoError(t, err)
		require.NotNil(t, instanceUpdatesCh)
		instanceUpdatesChs[i] = instanceUpdatesCh
	}

	var updatesReceived atomic.Int64
	var wg sync.WaitGroup

	wg.Add(2)
	// watch for updates on instance 0
	go func() {
		fmt.Printf("in go routine 0\n")
		defer wg.Done()
		select {
		case <-instanceUpdatesChs[0]:
			updatesReceived.Add(1)
		case <-time.After(time.Second * 10):
			t.Errorf("Timed out waiting for update from instance 0")
		}
	}()

	// watch for updates on instance 2
	go func() {
		fmt.Printf("in go routine 2\n")
		defer wg.Done()
		select {
		case <-instanceUpdatesChs[2]:
			updatesReceived.Add(1)
		case <-time.After(time.Second * 10):

			t.Errorf("Timed out waiting for update from instance 2")
		}
	}()

	// update leadership table for instance 1
	_, err := cluster.Crons[1].KV.Do(context.Background(), clientv3.OpPut("abc/leadership/1", ""))
	require.NoError(t, err)

	// other 2 instances should get notified of the new total and send that on the channel
	wg.Wait()
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(t, int64(2), updatesReceived.Load())
	}, 10*time.Second, 10*time.Millisecond)

	cancel()
}

// TODO: cancel a random instance, not hard coded
// TODO: add check for updated data here
func Test_watch_leadership_with_instance_down(t *testing.T) {
	t.Parallel()

	cluster := cron.TripplePartitionRun(t)
	defer func() {
		for _, cr := range cluster.Crons {
			cr.Stop(t)
		}
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cluster.Cron.KV.Get(context.Background(), "abc/leadership/", clientv3.WithPrefix())
		assert.NoError(c, err, "unexpected error querying etcd")
		assert.Equal(c, 3, len(resp.Kvs), "expected 3 leadership keys but got %d", len(resp.Kvs))
	}, 20*time.Second, 100*time.Millisecond)

	// Watch leadership updates for all instances concurrently
	instanceUpdatesChs := make([]chan []*anypb.Any, 3)
	contexts := make([]context.Context, 3)
	cancels := make([]context.CancelFunc, 3)

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		contexts[i] = ctx
		cancels[i] = cancel

		instanceUpdatesCh, err := cluster.Crons[i].WatchLeadership(ctx)
		require.NoError(t, err)
		require.NotNil(t, instanceUpdatesCh)
		instanceUpdatesChs[i] = instanceUpdatesCh
	}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	// watch for updates on instance 0 and 2
	var updatesReceived atomic.Int64
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		select {
		case <-instanceUpdatesChs[0]:
			updatesReceived.Add(1)
		case <-time.After(30 * time.Second):
			t.Errorf("Timed out waiting for update from instance 0")
		}
	}()
	go func() {
		defer wg.Done()
		select {
		case <-instanceUpdatesChs[2]:
			updatesReceived.Add(1)
		case <-time.After(30 * time.Second):
			t.Errorf("Timed out waiting for update from instance 2")
		}
	}()

	log.Println("Stopping instance 1")
	cluster.Crons[1].Stop(t)
	cancels[1]()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cluster.Crons[0].KV.Get(context.Background(), "abc/leadership/", clientv3.WithPrefix(), clientv3.WithSerializable())
		assert.NoError(t, err, "unexpected error querying etcd")
		assert.Equal(t, 2, len(resp.Kvs), "expected 2 leadership keys but got %d", len(resp.Kvs))
	}, 10*time.Second, 100*time.Millisecond)

	// other 2 instances should get notified of the new total and send that on the channel
	wg.Wait()
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Equal(t, int64(2), updatesReceived.Load())
	}, 10*time.Second, 10*time.Millisecond)
}
