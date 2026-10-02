/*
Copyright (c) 2026 Diagrid Inc.
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Run_revokeRetry(t *testing.T) {
	t.Parallel()

	type result struct {
		dropper *etcd.LeaseRevokeDropper
		err     error
		took    time.Duration
		leases  int
	}

	run := func(t *testing.T, fault func(*etcd.LeaseRevokeDropper)) result {
		t.Helper()

		endpoint := etcd.EmbeddedServer(t)
		dropper := etcd.NewLeaseRevokeDropper()
		client := etcd.Client(t, endpoint, dropper.DialOption())
		k, err := key.New(key.Options{Namespace: "abc", ID: "helloworld"})
		require.NoError(t, err)

		leader := New(Options{
			Log:         logr.Discard(),
			Client:      client,
			Key:         k,
			ReplicaData: &anypb.Any{Value: []byte("hello")},
		})

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() { errCh <- leader.Run(ctx) }()

		_, _, err = leader.Elect(ctx)
		require.NoError(t, err)

		fault(dropper)
		start := time.Now()
		cancel()

		var res result
		select {
		case res.err = <-errCh:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "timed out waiting for leader to stop")
		}
		res.took = time.Since(start)

		leases, err := client.Leases(t.Context())
		require.NoError(t, err)

		res.dropper = dropper
		res.leases = len(leases.Leases)

		return res
	}

	t.Run("a dropped revoke is retried and the lease is released", func(t *testing.T) {
		t.Parallel()

		res := run(t, func(d *etcd.LeaseRevokeDropper) { d.DropNext(1) })
		require.NoError(t, res.err)
		assert.Equal(t, int64(2), res.dropper.Hits())
		assert.Zero(t, res.leases)
		assert.Less(t, res.took, revokeBudget)
	})

	t.Run("gives up after the budget and leaves the lease to its TTL", func(t *testing.T) {
		t.Parallel()

		res := run(t, func(d *etcd.LeaseRevokeDropper) { d.DropNext(100) })
		require.NoError(t, res.err)
		assert.GreaterOrEqual(t, res.dropper.Hits(), int64(2))
		assert.Equal(t, 1, res.leases)
		assert.GreaterOrEqual(t, res.took, revokeBudget)
		assert.Less(t, res.took, revokeBudget+2*revokeAttempt)
	})

	t.Run("a hard error is returned without retrying", func(t *testing.T) {
		t.Parallel()

		hard := status.Error(codes.PermissionDenied, "no")
		res := run(t, func(d *etcd.LeaseRevokeDropper) { d.FailNext(hard) })
		require.ErrorIs(t, res.err, hard)
		assert.Equal(t, int64(1), res.dropper.Hits())
		assert.Equal(t, 1, res.leases)
		assert.Less(t, res.took, revokeBudget)
	})
}
