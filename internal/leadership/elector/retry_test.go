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
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/informer"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

type harness struct {
	client  api.Interface
	faults  *etcd.TxnFaultInjector
	clock   *clocktesting.FakeClock
	e       *Elector
	key     *key.Key
	leaseID clientv3.LeaseID
	ctx     context.Context
	cancel  context.CancelFunc
}

func newHarness(t *testing.T, endpoint, id string) *harness {
	t.Helper()

	faults := etcd.NewTxnFaultInjector("abc/leadership/")
	client := etcd.Client(t, endpoint, faults.DialOption())

	k, err := key.New(key.Options{Namespace: "abc", ID: id})
	require.NoError(t, err)
	lease, err := client.Grant(t.Context(), 20)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	inf, err := informer.New(ctx, informer.Options{Client: client, Key: k})
	require.NoError(t, err)

	e := New(Options{
		Log:         logr.Discard(),
		Client:      client,
		Key:         k,
		ReplicaData: &anypb.Any{Value: []byte(id)},
		LeaseID:     lease.ID,
		Informer:    inf,
	})
	clock := clocktesting.NewFakeClock(time.Now())
	e.clock = clock

	return &harness{
		client:  client,
		faults:  faults,
		clock:   clock,
		e:       e,
		key:     k,
		leaseID: lease.ID,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (h *harness) put(t *testing.T, k string, l *stored.Leadership) {
	t.Helper()
	b, err := proto.Marshal(l)
	require.NoError(t, err)
	_, err = h.client.Put(t.Context(), k, string(b))
	require.NoError(t, err)
}

// seed writes our key with total, and another leader's key with other.
func (h *harness) seed(t *testing.T, total, other uint64) {
	t.Helper()
	h.put(t, h.e.leaderKey, &stored.Leadership{Total: total, Uid: h.e.uid, ReplicaData: h.e.replicaData})
	h.put(t, "abc/leadership/other", &stored.Leadership{Total: other, Uid: 1})
}

func (h *harness) total(t *testing.T) uint64 {
	t.Helper()
	resp, err := h.client.Get(t.Context(), h.e.leaderKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	var l stored.Leadership
	require.NoError(t, proto.Unmarshal(resp.Kvs[0].Value, &l))
	return l.GetTotal()
}

// rewatch restarts the informer after seeding, so seeded keys do not wake the
// elector. The informer watches from the current revision, inclusive.
func (h *harness) rewatch(t *testing.T) {
	t.Helper()
	_, err := h.client.Put(t.Context(), "zzz", "")
	require.NoError(t, err)
	inf, err := informer.New(h.ctx, informer.Options{Client: h.client, Key: h.key})
	require.NoError(t, err)
	h.e.informer = inf
}

type result struct {
	ctx context.Context
	el  *Elected
	err error
}

func (h *harness) reelect(t *testing.T) <-chan result {
	t.Helper()
	h.rewatch(t)
	ch := make(chan result, 1)
	go func() {
		c, el, err := h.e.Reelect(h.ctx)
		ch <- result{c, el, err}
	}()
	return ch
}

func (h *harness) waitHits(t *testing.T, n int64) {
	t.Helper()
	require.Eventually(t, func() bool { return h.faults.Hits() == n }, 10*time.Second, time.Millisecond)
}

// fire asserts the elector waits within [d, d+jitter), then fires the wait.
func (h *harness) fire(t *testing.T, d time.Duration) {
	t.Helper()
	require.Eventually(t, h.clock.HasWaiters, 10*time.Second, time.Millisecond)
	h.clock.Step(d - time.Millisecond)
	require.True(t, h.clock.HasWaiters(), "fired before %s", d)
	h.clock.Step(500*time.Millisecond + time.Millisecond)
	require.False(t, h.clock.HasWaiters(), "did not fire by %s", d+500*time.Millisecond)
}

func wait(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out")
		return result{}
	}
}

func Test_Reelect_retry(t *testing.T) {
	t.Parallel()

	t.Run("transient write error with no other writes is retried", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, etcd.EmbeddedServer(t), "self")
		h.seed(t, 1, 2)
		h.faults.FailNext(3, rpctypes.ErrGRPCTimeout)
		ch := h.reelect(t)

		for i, d := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second} {
			h.waitHits(t, int64(i+1))
			h.fire(t, d)
		}

		r := wait(t, ch)
		require.NoError(t, r.err)
		assert.Len(t, r.el.LeadershipData, 2)
		assert.Equal(t, int64(4), h.faults.Hits())
		assert.Equal(t, uint64(2), h.total(t))
	})

	t.Run("write applied but response lost is not re-written", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, etcd.EmbeddedServer(t), "self")
		h.seed(t, 1, 2)
		h.faults.FailNextAfterApply(1, rpctypes.ErrGRPCTimeout)
		ch := h.reelect(t)

		h.waitHits(t, 1)
		h.fire(t, 500*time.Millisecond)

		r := wait(t, ch)
		require.NoError(t, r.err)
		assert.Len(t, r.el.LeadershipData, 2)
		assert.Equal(t, int64(1), h.faults.Hits())
		assert.Equal(t, uint64(2), h.total(t))
	})

	t.Run("backoff doubles up to 5s", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, etcd.EmbeddedServer(t), "self")
		h.seed(t, 1, 2)
		backoffs := []time.Duration{
			500 * time.Millisecond, time.Second, 2 * time.Second,
			4 * time.Second, 5 * time.Second, 5 * time.Second,
		}
		h.faults.FailNext(len(backoffs), rpctypes.ErrGRPCLeaderChanged)
		ch := h.reelect(t)

		for i, d := range backoffs {
			h.waitHits(t, int64(i+1))
			h.fire(t, d)
		}

		r := wait(t, ch)
		require.NoError(t, r.err)
		assert.Len(t, r.el.LeadershipData, 2)
	})

	t.Run("context cancelled during backoff returns", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, etcd.EmbeddedServer(t), "self")
		h.seed(t, 1, 2)
		h.faults.FailNext(1, rpctypes.ErrGRPCTimeout)
		ch := h.reelect(t)

		require.Eventually(t, h.clock.HasWaiters, 10*time.Second, time.Millisecond)
		h.cancel()

		r := wait(t, ch)
		require.ErrorIs(t, r.err, context.Canceled)
		assert.Nil(t, r.el)
		assert.Equal(t, int64(1), h.faults.Hits())
	})

	t.Run("non-transient error waits for the informer", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t, etcd.EmbeddedServer(t), "self")
		h.seed(t, 1, 2)
		h.faults.FailNext(1, status.Error(codes.PermissionDenied, "boom"))
		ch := h.reelect(t)

		h.waitHits(t, 1)
		assert.Never(t, func() bool {
			return h.clock.HasWaiters() || len(ch) > 0 || h.faults.Hits() != 1
		}, 500*time.Millisecond, 10*time.Millisecond)

		h.put(t, "abc/leadership/other", &stored.Leadership{Total: 2, Uid: 1})

		r := wait(t, ch)
		require.NoError(t, r.err)
		assert.Len(t, r.el.LeadershipData, 2)
		assert.Equal(t, int64(2), h.faults.Hits())
	})

	t.Run("both survivors hit transient errors after a leader leaves", func(t *testing.T) {
		t.Parallel()

		endpoint := etcd.EmbeddedServer(t)
		hs := []*harness{
			newHarness(t, endpoint, "one"),
			newHarness(t, endpoint, "two"),
			newHarness(t, endpoint, "three"),
		}
		results := electAll(t, hs)

		hs[0].faults.FailNext(1, rpctypes.ErrGRPCTimeout)
		hs[1].faults.FailNext(1, rpctypes.ErrGRPCTimeout)
		_, err := hs[2].client.Revoke(t.Context(), hs[2].leaseID)
		require.NoError(t, err)

		chs := make([]<-chan result, 2)
		for i, h := range hs[:2] {
			select {
			case <-results[i].ctx.Done():
			case <-time.After(10 * time.Second):
				require.FailNow(t, "quorum not lost")
			}
			chs[i] = h.reelect(t)
		}

		for _, h := range hs[:2] {
			require.Eventually(t, func() bool { return h.faults.Pending() == 0 }, 10*time.Second, time.Millisecond)
			h.fire(t, 500*time.Millisecond)
		}

		for i, ch := range chs {
			r := wait(t, ch)
			require.NoError(t, r.err)
			assert.Len(t, r.el.LeadershipData, 2)
			assert.Equal(t, uint64(2), hs[i].total(t))
		}
	})
}

func electAll(t *testing.T, hs []*harness) []result {
	t.Helper()

	results := make([]result, len(hs))
	errCh := make(chan error, len(hs))
	for i, h := range hs {
		go func() {
			c, el, err := h.e.Elect(h.ctx)
			for err == nil && len(el.LeadershipData) != len(hs) {
				<-c.Done()
				c, el, err = h.e.Reelect(h.ctx)
			}
			results[i] = result{c, el, err}
			errCh <- err
		}()
	}
	for range hs {
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(20 * time.Second):
			require.FailNow(t, "timed out electing")
		}
	}
	return results
}
