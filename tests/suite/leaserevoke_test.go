/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

// Every member of a cluster stops at once while the etcd leader hands off,
// which is when follower lease revokes are dropped. Leases must still be
// released so the same ids can take leadership again without waiting out the
// lease TTL.
func Test_leadership_leaseRevokedOnClusterShutdown(t *testing.T) {
	t.Parallel()

	const members = 3

	cluster := etcd.EmbeddedCluster(t, members)
	bare := etcd.BareClient(t, cluster.Endpoint(0))

	var triggered atomic.Int64

	for round := range 3 {
		crs := make([]*retryCron, members)
		for i := range crs {
			crs[i] = newRetryCron(t, cluster.Endpoint(i), strconv.Itoa(i), &triggered)
			crs[i].run(t)
		}

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, cr := range crs {
				assert.True(c, cr.IsElected())
				assert.ElementsMatch(c, []string{"0", "1", "2"}, cr.leaders())
			}
		}, 10*time.Second, 10*time.Millisecond, "round %d: members did not elect", round)

		// Let quorum watchers drain leftover election events.
		time.Sleep(time.Second)

		leader := cluster.Leader()
		require.NotNil(t, leader)

		var wg sync.WaitGroup
		wg.Add(1 + members)
		go func() {
			defer wg.Done()
			_ = leader.Server.TransferLeadership()
		}()

		stops := make([]time.Duration, members)
		for i, cr := range crs {
			go func() {
				defer wg.Done()
				start := time.Now()
				cr.stop(t)
				stops[i] = time.Since(start)
			}()
		}
		wg.Wait()

		t.Logf("round %d: stop durations %v", round, stops)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			leases, err := bare.Leases(t.Context())
			if assert.NoError(c, err) {
				assert.Empty(c, leases.Leases)
			}
		}, 8*time.Second, 50*time.Millisecond, "round %d: leases were not revoked", round)
	}
}
