/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/elector"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Reelect_transientEtcdError(t *testing.T) {
	t.Parallel()

	endpoint := etcd.EmbeddedServer(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	type replica struct {
		l      *Leadership
		faults *etcd.TxnFaultInjector
		cancel context.CancelFunc
		errCh  chan error
	}

	rs := make([]*replica, 3)
	for i, id := range []string{"one", "two", "three"} {
		faults := etcd.NewTxnFaultInjector("abc/leadership/")
		k, err := key.New(key.Options{Namespace: "abc", ID: id})
		require.NoError(t, err)
		rctx, rcancel := context.WithCancel(ctx)
		r := &replica{
			l: New(Options{
				Log:         logr.Discard(),
				Client:      etcd.Client(t, endpoint, faults.DialOption()),
				Key:         k,
				ReplicaData: &anypb.Any{Value: []byte(id)},
			}),
			faults: faults,
			cancel: rcancel,
			errCh:  make(chan error, 1),
		}
		go func() { r.errCh <- r.l.Run(rctx) }()
		rs[i] = r
	}

	type result struct {
		ctx context.Context
		el  *elector.Elected
		err error
	}
	elected := make(chan result, 3)
	for _, r := range rs {
		go func() {
			c, el, err := r.l.Elect(ctx)
			for err == nil && len(el.LeadershipData) != 3 {
				<-c.Done()
				c, el, err = r.l.Reelect(ctx)
			}
			elected <- result{c, el, err}
		}()
	}
	var survivors []context.Context
	for range 3 {
		select {
		case res := <-elected:
			require.NoError(t, res.err)
			survivors = append(survivors, res.ctx)
		case <-time.After(20 * time.Second):
			require.FailNow(t, "timed out electing")
		}
	}

	// Let quorum watchers drain leftover election events, which would
	// otherwise wake a stuck survivor.
	time.Sleep(time.Second)

	rs[0].faults.FailNext(1, rpctypes.ErrGRPCTimeout)
	rs[1].faults.FailNext(1, rpctypes.ErrGRPCTimeout)
	rs[2].cancel()
	require.NoError(t, <-rs[2].errCh)

	for _, c := range survivors {
		select {
		case <-c.Done():
		case <-time.After(10 * time.Second):
		}
	}

	reelected := make(chan result, 2)
	for _, r := range rs[:2] {
		go func() {
			c, el, err := r.l.Reelect(ctx)
			reelected <- result{c, el, err}
		}()
	}
	for range 2 {
		select {
		case res := <-reelected:
			require.NoError(t, res.err)
			assert.Len(t, res.el.LeadershipData, 2)
		case <-time.After(10 * time.Second):
			require.FailNow(t, "survivors did not re-elect")
		}
	}
	assert.Equal(t, 0, rs[0].faults.Pending())
	assert.Equal(t, 0, rs[1].faults.Pending())

	cancel()
	for _, r := range rs[:2] {
		require.NoError(t, <-r.errCh)
	}
}
