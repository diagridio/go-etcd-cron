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
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_leadership_scaleup(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	var lock sync.Mutex
	called := make(map[string]int)
	instanceCalled := make(map[int]bool)
	ch := make(chan []*anypb.Any)

	opts := cron.Options{Client: client}

	crs := make([]api.Interface, 5)
	var err error
	for i := range 5 {
		if i == 0 {
			opts.WatchLeadership = ch
		}

		opts.TriggerFn = func(_ context.Context, req *api.TriggerRequest) *api.TriggerResponse {
			lock.Lock()
			instanceCalled[i] = true
			called[req.GetName()]++
			lock.Unlock()
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		}
		opts.ID = strconv.Itoa(i)
		crs[i], err = cron.New(opts)
		require.NoError(t, err)
		opts.WatchLeadership = nil
	}

	errCh := make(chan error)
	ctx1, cancel1 := context.WithCancel(t.Context())
	for i := range 3 {
		go func() { errCh <- crs[i].Run(ctx1) }()
	}

	t.Cleanup(func() {
		cancel1()
		for range 3 {
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

	for i := range 100 {
		require.NoError(t, crs[0].Add(ctx1, strconv.Itoa(i), &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(5)),
			DueTime:  ptr.Of("0s"),
		}))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		assert.Len(c, called, 100)
		lock.Unlock()
	}, time.Second*5, time.Millisecond*10)

	lock.Lock()
	assert.Len(t, instanceCalled, 3)
	lock.Unlock()

	ctx2, cancel2 := context.WithCancel(t.Context())
	for i := range 2 {
		go func() { errCh <- crs[i+3].Run(ctx2) }()
	}

	t.Cleanup(func() {
		cancel2()
		for range 2 {
			require.NoError(t, <-errCh)
		}
	})

	for len(d) != 5 {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for leadership quroum")
		case d = <-ch:
		}
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		for _, k := range called {
			assert.Equal(c, 5, k)
		}
		lock.Unlock()
	}, time.Second*10, time.Millisecond*10)

	assert.Len(t, instanceCalled, 5)
}

func Test_leadership_scaledown(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	var lock sync.Mutex
	called := make(map[string]int)
	instanceCalled := make(map[int]bool)
	ch := make(chan []*anypb.Any)

	opts := cron.Options{Client: client}

	crs := make([]api.Interface, 5)
	var err error
	for i := range 5 {
		if i == 0 {
			opts.WatchLeadership = ch
		}

		opts.TriggerFn = func(_ context.Context, req *api.TriggerRequest) *api.TriggerResponse {
			lock.Lock()
			instanceCalled[i] = true
			called[req.GetName()]++
			lock.Unlock()
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		}
		opts.ID = strconv.Itoa(i)
		crs[i], err = cron.New(opts)
		require.NoError(t, err)
		opts.WatchLeadership = nil
	}

	errCh := make(chan error)
	ctx1, cancel1 := context.WithCancel(t.Context())
	for i := range 3 {
		go func() { errCh <- crs[i].Run(ctx1) }()
	}

	ctx2, cancel2 := context.WithCancel(t.Context())
	for i := range 2 {
		go func() { errCh <- crs[i+3].Run(ctx2) }()
	}

	t.Cleanup(func() {
		cancel1()
		cancel2()
		for range 5 {
			require.NoError(t, <-errCh)
		}
	})

	var d []*anypb.Any
	for len(d) != 5 {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for leadership quroum")
		case d = <-ch:
		}
	}

	for i := range 100 {
		require.NoError(t, crs[0].Add(ctx1, strconv.Itoa(i), &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(5)),
			DueTime:  ptr.Of("0s"),
		}))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		assert.Len(c, called, 100)
		lock.Unlock()
	}, time.Second*5, time.Millisecond*10)

	lock.Lock()
	assert.Len(t, instanceCalled, 5)
	lock.Unlock()

	cancel2()
	for len(d) != 3 {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for leadership quroum")
		case d = <-ch:
		}
	}

	lock.Lock()
	clear(instanceCalled)
	lock.Unlock()

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lock.Lock()
		for _, k := range called {
			assert.Equal(c, 5, k)
		}
		lock.Unlock()
	}, time.Second*10, time.Millisecond*10)

	assert.Len(t, instanceCalled, 3)
}

func Test_leadership_wait_free(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)
	opts := cron.Options{
		Client: client,
		Log:    logr.Discard(),
		ID:     "123",
		TriggerFn: func(_ context.Context, req *api.TriggerRequest) *api.TriggerResponse {
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		},
	}

	ctx, cancel := context.WithCancel(t.Context())

	_, err := client.Put(ctx, "leadership/456", "123")
	require.NoError(t, err)

	cr, err := cron.New(opts)
	require.NoError(t, err)

	errCh := make(chan error)
	go func() { errCh <- cr.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timeout waiting for cron return")
		case err := <-errCh:
			require.NoError(t, err)
		}
	})

	select {
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(time.Second * 5):
	}

	_, err = client.Delete(ctx, "leadership/456")
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, cr.IsElected())
	}, time.Second*10, time.Millisecond*10)

	resp, err := client.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
}
