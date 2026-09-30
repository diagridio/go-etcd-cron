/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

const leadershipPrefix = "abc/leadership/"

type retryCron struct {
	api.Interface
	id     string
	faults *etcd.TxnFaultInjector
	latest atomic.Pointer[[]*anypb.Any]
	cancel context.CancelFunc
	errCh  chan error
}

func newRetryCron(t *testing.T, endpoint, id string, triggered *atomic.Int64) *retryCron {
	t.Helper()

	faults := etcd.NewTxnFaultInjector(leadershipPrefix)
	client := etcd.BareClient(t, endpoint, faults.DialOption())

	ch := make(chan []*anypb.Any)
	c, err := cron.New(cron.Options{
		Client:          client,
		Namespace:       "abc",
		ID:              id,
		ReplicaData:     &anypb.Any{Value: []byte(id)},
		WatchLeadership: ch,
		TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
			triggered.Add(1)
			fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
		},
	})
	require.NoError(t, err)

	rc := &retryCron{
		Interface: c,
		id:        id,
		faults:    faults,
		errCh:     make(chan error, 1),
	}

	ctx, cancel := context.WithCancel(t.Context())
	rc.cancel = cancel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ds := <-ch:
				rc.latest.Store(&ds)
			}
		}
	}()

	return rc
}

func (r *retryCron) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	prev := r.cancel
	r.cancel = func() { cancel(); prev() }
	go func() { r.errCh <- r.Run(ctx) }()
	t.Cleanup(func() { r.stop(t) })
}

func (r *retryCron) stop(t *testing.T) {
	t.Helper()
	if r.cancel == nil {
		return
	}
	r.cancel()
	r.cancel = nil
	select {
	case err := <-r.errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting for cron "+r.id+" to stop")
	}
}

func (r *retryCron) leaders() []string {
	ds := r.latest.Load()
	if ds == nil {
		return nil
	}
	ids := make([]string, len(*ds))
	for i, d := range *ds {
		ids[i] = string(d.GetValue())
	}
	return ids
}

func (r *retryCron) assertRunning(t *testing.T) {
	t.Helper()
	select {
	case err := <-r.errCh:
		r.errCh <- err
		require.FailNow(t, "cron "+r.id+" stopped unexpectedly", "error: %v", err)
	default:
	}
}

func Test_leadershipRetry_failover(t *testing.T) {
	t.Parallel()

	tests := map[string]func(f *etcd.TxnFaultInjector){
		"write fails once": func(f *etcd.TxnFaultInjector) {
			f.FailNext(1, rpctypes.ErrGRPCTimeout)
		},
		"write applied but response lost": func(f *etcd.TxnFaultInjector) {
			f.FailNextAfterApply(1, rpctypes.ErrGRPCTimeout)
		},
		"write fails repeatedly": func(f *etcd.TxnFaultInjector) {
			f.FailNext(3, rpctypes.ErrGRPCTimeout)
		},
		"leader changed error": func(f *etcd.TxnFaultInjector) {
			f.FailNext(1, rpctypes.ErrGRPCLeaderChanged)
		},
	}

	for name, arm := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			endpoint := etcd.EmbeddedServer(t)
			var triggered atomic.Int64

			crs := make([]*retryCron, 3)
			for i := range crs {
				crs[i] = newRetryCron(t, endpoint, strconv.Itoa(i), &triggered)
				crs[i].run(t)
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				for _, cr := range crs {
					assert.True(c, cr.IsElected())
					assert.ElementsMatch(c, []string{"0", "1", "2"}, cr.leaders())
				}
			}, 20*time.Second, 10*time.Millisecond)

			// Let quorum watchers drain leftover election events.
			time.Sleep(time.Second)

			arm(crs[0].faults)
			arm(crs[1].faults)
			hits0, hits1 := crs[0].faults.Hits(), crs[1].faults.Hits()

			crs[2].stop(t)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				for _, cr := range crs[:2] {
					assert.True(c, cr.IsElected())
					assert.ElementsMatch(c, []string{"0", "1"}, cr.leaders())
				}
			}, 20*time.Second, 10*time.Millisecond,
				"survivors did not publish the new leadership table")

			assert.Equal(t, 0, crs[0].faults.Pending())
			assert.Equal(t, 0, crs[1].faults.Pending())
			assert.Greater(t, crs[0].faults.Hits(), hits0)
			assert.Greater(t, crs[1].faults.Hits(), hits1)

			const jobs = 10
			addCtx, addCancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer addCancel()
			for i := range jobs {
				require.NoError(t, crs[i%2].Add(addCtx, "job-"+strconv.Itoa(i), &api.Job{
					DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
				}))
			}
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, int64(jobs), triggered.Load())
			}, 20*time.Second, 10*time.Millisecond)

			crs[0].assertRunning(t)
			crs[1].assertRunning(t)
		})
	}
}
