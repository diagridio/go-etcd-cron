/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

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

// A leader whose key is rewritten in place, as when its delete and the put of
// a replacement under the same ID land in one informer read, is still sent to
// WatchLeadership with its new replica data.
func Test_leaderreplaced(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	crs := make([]api.Interface, 3)
	ch := make(chan []*anypb.Any)
	opts := cron.Options{Client: client}
	var err error
	for i := range 3 {
		if i == 0 {
			opts.WatchLeadership = ch
		}
		opts.TriggerFn = func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
			fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
		}
		opts.ID = strconv.Itoa(i)
		opts.ReplicaData = &anypb.Any{Value: []byte(opts.ID)}
		crs[i], err = cron.New(opts)
		require.NoError(t, err)
		opts.WatchLeadership = nil
	}

	var lock sync.Mutex
	var latest []*anypb.Any
	go func() {
		for ds := range ch {
			lock.Lock()
			latest = ds
			lock.Unlock()
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	errChs := make([]chan error, 3)
	for i := range 3 {
		errChs[i] = make(chan error, 1)
		go func() { errChs[i] <- crs[i].Run(ctx) }()
	}
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-errChs[0])
		require.NoError(t, <-errChs[1])
		// The rewritten leader can no longer reconcile its own key and is
		// stuck re-electing until shutdown.
		if err := <-errChs[2]; err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		close(ch)
	})

	expect := func(exp ...string) {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			lock.Lock()
			defer lock.Unlock()
			if !assert.Len(c, latest, len(exp)) {
				return
			}
			for i, d := range exp {
				assert.True(c, proto.Equal(&anypb.Any{Value: []byte(d)}, latest[i]), latest)
			}
		}, 10*time.Second, 10*time.Millisecond)
	}
	expect("0", "1", "2")

	lease, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	value, err := proto.Marshal(&stored.Leadership{
		Total:       3,
		Uid:         1,
		ReplicaData: &anypb.Any{Value: []byte("moved")},
	})
	require.NoError(t, err)
	_, err = client.Put(ctx, "leadership/2", string(value), clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	expect("0", "1", "moved")
}
