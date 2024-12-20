/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_active_passive(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	var lock sync.Mutex
	var called int
	instanceCalled := make(map[int]bool)
	ch := make(chan []*anypb.Any)

	opts := cron.Options{Client: client}

	crs := make([]api.Interface, 6)
	var err error
	for i := range 6 {
		if i%3 == 0 {
			opts.WatchLeadership = ch
		}

		opts.TriggerFn = func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
			lock.Lock()
			instanceCalled[i] = true
			called++
			lock.Unlock()
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		}
		opts.ID = strconv.Itoa(i % 3)
		crs[i], err = cron.New(opts)
		require.NoError(t, err)
		opts.WatchLeadership = nil
	}

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	for i := range 6 {
		go func() { errCh <- crs[i].Run(ctx) }()
	}

	t.Cleanup(func() {
		cancel()
		for range 6 {
			require.NoError(t, <-errCh)
		}
	})

	var d []*anypb.Any
	for len(d) != 3 {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for leadership quroum")
		case d = <-ch:
		}
	}

	var cr api.Interface
	for _, c := range crs {
		if c.IsElected() {
			cr = c
			break
		}
	}

	for i := range 100 {
		require.NoError(t, cr.Add(ctx, strconv.Itoa(i), &api.Job{
			DueTime: ptr.Of("0s"),
		}))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		assert.Equal(c, 100, called)
		lock.Unlock()
	}, time.Second*5, time.Millisecond*10)

	assert.Len(t, instanceCalled, 3)
}

func Test_passive_active(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	var lock sync.Mutex
	var called int
	instanceCalled := make(map[int]bool)
	ch := make(chan []*anypb.Any)

	opts := cron.Options{Client: client}

	crs := make([]api.Interface, 6)
	var err error
	for i := range 6 {
		if i%3 == 0 {
			opts.WatchLeadership = ch
		}

		opts.TriggerFn = func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
			lock.Lock()
			instanceCalled[i] = true
			called++
			lock.Unlock()
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		}
		opts.ID = strconv.Itoa(i % 3)
		crs[i], err = cron.New(opts)
		require.NoError(t, err)
		opts.WatchLeadership = nil
	}

	errCh := make(chan error)
	ctxs := make([]context.Context, 6)
	cancels := make([]context.CancelFunc, 6)

	for i := range 6 {
		//nolint:fatcontext
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
		go func(i int) { errCh <- crs[i].Run(ctxs[i]) }(i)
	}

	t.Cleanup(func() {
		for i := range 6 {
			cancels[i]()
		}
		for range 6 {
			require.NoError(t, <-errCh)
		}
		close(ch)
	})

	var d []*anypb.Any
	for len(d) != 3 {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for leadership quroum")
		case d = <-ch:
		}
	}

	var cr api.Interface
	for _, c := range crs {
		if c.IsElected() {
			cr = c
			break
		}
	}

	for i := range 100 {
		require.NoError(t, cr.Add(context.Background(), strconv.Itoa(i), &api.Job{
			DueTime: ptr.Of("0s"),
		}))
	}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		assert.Equal(c, 100, called)
		lock.Unlock()
	}, time.Second*5, time.Millisecond*10)
	assert.Len(t, instanceCalled, 3)

	tocancel := make([]int, 0, 3)
	for i, c := range crs {
		if c.IsElected() {
			tocancel = append(tocancel, i)
		} else {
			cr = c
		}
	}

	uids := make([]uint64, 3)
	resp, err := client.Get(context.Background(), "leadership", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 3)
	for i, kv := range resp.Kvs {
		var lead stored.Leadership
		require.NoError(t, proto.Unmarshal(kv.Value, &lead))
		uids[i] = lead.GetUid()
	}

	for _, c := range tocancel {
		cancels[c]()
	}

	go func() {
		for {
			_, ok := <-ch
			if !ok {
				return
			}
		}
	}()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err = client.Get(context.Background(), "leadership", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(c, resp.Kvs, 3)
		for _, kv := range resp.Kvs {
			var lead stored.Leadership
			require.NoError(t, proto.Unmarshal(kv.Value, &lead))
			assert.NotContains(c, uids, lead.GetUid())
		}
	}, time.Second*5, time.Millisecond*10)

	for _, c := range crs {
		if c.IsElected() {
			cr = c
			break
		}
	}

	for i := range 100 {
		require.NoError(t, cr.Add(context.Background(), strconv.Itoa(i+100), &api.Job{
			DueTime: ptr.Of("0s"),
		}))
	}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		assert.Equal(c, 200, called)
		assert.Len(c, instanceCalled, 6)
		lock.Unlock()
	}, time.Second*5, time.Millisecond*10)
}
