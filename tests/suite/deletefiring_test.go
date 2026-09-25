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

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

// Test_deletefiring deletes or overwrites jobs while they are continuously
// firing. Both close the job's counter (an overwrite is informed as a delete
// of the old revision plus a put of the new one), so a trigger whose
// ExecuteRequest reaches the worker after that close must be dropped, not
// fail the queue and shut down the cron instance.
func Test_deletefiring(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		// overwrite re-adds the job under the same name instead of deleting it.
		overwrite bool
	}{
		"delete":    {overwrite: false},
		"overwrite": {overwrite: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testDeleteFiring(t, test.overwrite)
		})
	}
}

func testDeleteFiring(t *testing.T, overwrite bool) {
	t.Helper()

	const (
		workers   = 8
		perWorker = 125
	)

	var triggered atomic.Int64
	cron := integration.New(t, integration.Options{
		Instances: 1,
		TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
			triggered.Add(1)
			fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
		},
	})

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func() {
			defer wg.Done()
			for i := range perWorker {
				name := "job-" + strconv.Itoa(w) + "-" + strconv.Itoa(i)
				if overwrite {
					// All iterations of a worker share one name.
					name = "job-" + strconv.Itoa(w)
				}

				// A fresh Job per Add: Add sets defaults on the Job it is given.
				if err := cron.API().Add(cron.Context(), name, &api.Job{
					DueTime:  ptr.Of("0s"),
					Schedule: ptr.Of("@every 1ms"),
				}); err != nil {
					errCh <- err
					return
				}

				// Stagger the next write so it lands at varying points of the
				// job's trigger cycle.
				time.Sleep(time.Duration(i%5) * time.Millisecond)

				if overwrite {
					continue
				}
				if err := cron.API().Delete(cron.Context(), name); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err, "cron must stay running while replacing firing jobs")
	}
	assert.Positive(t, triggered.Load(), "jobs must have fired for the race to be exercised")

	resp, err := cron.API().List(cron.Context(), "")
	require.NoError(t, err)
	if overwrite {
		assert.Len(t, resp.GetJobs(), workers)
		require.NoError(t, cron.API().DeletePrefixes(cron.Context(), "job-"))
	} else {
		assert.Empty(t, resp.GetJobs())
	}

	// The same cron instance must still schedule and trigger jobs.
	before := triggered.Load()
	require.NoError(t, cron.API().Add(cron.Context(), "after", &api.Job{
		DueTime: ptr.Of("0s"),
	}))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Greater(c, triggered.Load(), before)
	}, time.Second*10, time.Millisecond*10)

	// Close asserts Run returned no error.
	cron.Close()
}
