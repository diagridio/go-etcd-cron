/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package router

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/events/loop/fake"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/fake"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/counters"
)

// drainLoopsCache removes any fake loops that handleCloseJob put into the
// package-level counters.LoopsCache sync.Pool. Without this, integration
// tests that create real routers can pull stale fakes from the pool,
// causing data races on fake callback captures.
func drainLoopsCache() {
	for range 100 {
		counters.LoopsCache.Get()
	}
}

// Tests in this package must NOT use t.Parallel() at the top level.
// handleCloseJob puts fake loops into counters.LoopsCache (a package-level
// sync.Pool). The integration tests use real routers that pull from the same
// pool. Running them concurrently causes data races on the fake callbacks.
func Test_router(t *testing.T) {
	t.Cleanup(drainLoopsCache)

	t.Run("if handle close job but no counter, should return nil and not error", func(t *testing.T) {

		r := &router{
			counters: make(map[string]*counter),
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: new(queue.CloseJob),
				},
			},
		}))
	})

	t.Run("if handle close job and counter is set to non-0 (not closed), then shouldn't take any action", func(t *testing.T) {

		var idx atomic.Int64
		idx.Store(123)
		r := &router{
			counters: map[string]*counter{
				"test": {
					idx: &idx,
				},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: new(queue.CloseJob),
				},
			},
		}))
	})

	t.Run("if handle job close then should close loop and delete from map", func(t *testing.T) {

		var called int
		var gotAction *queue.JobAction
		loop := fake.New[*queue.JobAction]().WithClose(func(action *queue.JobAction) {
			gotAction = action
			called++
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"test":   {idx: &idx, loop: loop},
				"test-2": {},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: new(queue.CloseJob),
				},
			},
		}))

		assert.Equal(t, 1, called)
		assert.Equal(t, &queue.JobAction{
			Action: &queue.JobAction_Close{
				Close: new(queue.Close),
			},
		}, gotAction)
		assert.Equal(t, map[string]*counter{
			"test-2": {},
		}, r.counters)
	})

	t.Run("if handle close, expect all loops to be closed", func(t *testing.T) {

		var called atomic.Uint64
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			// Don't use t inside this callback — handleClose runs it in a
			// goroutine, and using t from a goroutine that outlives the
			// parent test causes "panic: Fail in goroutine after Test has
			// completed". We verify the call count after Handle returns.
			called.Add(1)
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"test1": {idx: &idx, loop: loop},
				"test2": {idx: &idx, loop: loop},
				"test3": {idx: &idx, loop: loop},
				"test4": {idx: &idx, loop: loop},
				"test5": {idx: &idx, loop: loop},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			Action: &queue.JobAction{
				Action: &queue.JobAction_Close{
					Close: new(queue.Close),
				},
			},
		}))

		assert.Equal(t, uint64(5), called.Load())
	})

	t.Run("if handle event with existing counter, expect enqueue", func(t *testing.T) {

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: false,
			},
		}}

		var called int
		loop := fake.New[*queue.JobAction]().WithEnqueue(func(action *queue.JobAction) {
			assert.Equal(t, exp, action)
			called++
		})

		r := &router{
			counters: map[string]*counter{
				"test-job": {loop: loop},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test-job",
			Action:  exp,
		}))
		assert.Equal(t, 1, called)
		assert.Equal(t, map[string]*counter{
			"test-job": {loop: loop},
		}, r.counters)
	})

	t.Run("if handle event with non-existing counter, expect create and enqueue", func(t *testing.T) {

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: true,
			},
		}}

		r := &router{
			act: actionerfake.New(),
			counters: map[string]*counter{
				"test-job-2": {},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test-job",
			Action:  exp,
		}))

		assert.Len(t, r.counters, 2)
	})

	t.Run("if handle event for delete with non-existing counter, expect no create or enqueue", func(t *testing.T) {

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: false,
			},
		}}

		r := &router{
			act: actionerfake.New(),
			counters: map[string]*counter{
				"test-job-2": {},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test-job",
			Action:  exp,
		}))

		assert.Len(t, r.counters, 1)
	})
}

func Test_handleCloseJob(t *testing.T) {
	t.Cleanup(drainLoopsCache)

	t.Run("missing counter returns nil, not error", func(t *testing.T) {

		r := &router{
			counters: make(map[string]*counter),
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "nonexistent",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
	})

	t.Run("missing counter does not modify counters map", func(t *testing.T) {

		r := &router{
			counters: map[string]*counter{
				"other-job": {},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "nonexistent",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
		assert.Len(t, r.counters, 1)
		assert.Contains(t, r.counters, "other-job")
	})

	t.Run("missing counter with empty job name returns nil", func(t *testing.T) {

		r := &router{
			counters: make(map[string]*counter),
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
	})

	t.Run("counter with non-zero idx is not closed (reused)", func(t *testing.T) {

		var closeCalled int
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx atomic.Int64
		idx.Store(42)
		r := &router{
			counters: map[string]*counter{
				"reused-job": {idx: &idx, loop: loop},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "reused-job",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 0, closeCalled, "loop should not be closed when idx != 0")
		assert.Contains(t, r.counters, "reused-job", "counter should remain in map")
	})

	t.Run("counter with zero idx is closed and removed", func(t *testing.T) {

		var closeCalled int
		var closeAction *queue.JobAction
		loop := fake.New[*queue.JobAction]().WithClose(func(action *queue.JobAction) {
			closeCalled++
			closeAction = action
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"idle-job": {idx: &idx, loop: loop},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "idle-job",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, closeCalled)
		assert.NotNil(t, closeAction.GetClose())
		assert.NotContains(t, r.counters, "idle-job")
	})

	t.Run("closing one counter does not affect others", func(t *testing.T) {

		closedLoop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {})

		otherCloseCalled := false
		otherLoop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			otherCloseCalled = true
		})

		var idxZero atomic.Int64
		var idxOther atomic.Int64
		idxOther.Store(10)

		r := &router{
			counters: map[string]*counter{
				"close-me": {idx: &idxZero, loop: closedLoop},
				"keep-me":  {idx: &idxOther, loop: otherLoop},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "close-me",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
		assert.NotContains(t, r.counters, "close-me")
		assert.Contains(t, r.counters, "keep-me")
		assert.False(t, otherCloseCalled)
	})

	t.Run("close same job twice, second is no-op", func(t *testing.T) {

		var closeCalled int
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"once-job": {idx: &idx, loop: loop},
			},
		}

		event := &queue.JobEvent{
			JobName: "once-job",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		}

		require.NoError(t, r.handleCloseJob(event))
		assert.Equal(t, 1, closeCalled)
		assert.NotContains(t, r.counters, "once-job")

		// Second call — counter is gone, should return nil (not error).
		require.NoError(t, r.handleCloseJob(event))
		assert.Equal(t, 1, closeCalled, "should not call close again")
	})

	t.Run("close then re-add then close again", func(t *testing.T) {

		var closeCalled int
		loop1 := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})
		loop2 := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx1 atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"recycled": {idx: &idx1, loop: loop1},
			},
		}

		event := &queue.JobEvent{
			JobName: "recycled",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		}

		// First close.
		require.NoError(t, r.handleCloseJob(event))
		assert.Equal(t, 1, closeCalled)
		assert.NotContains(t, r.counters, "recycled")

		// Re-add with new counter, idx=0 (ready for GC).
		var idx2 atomic.Int64
		r.counters["recycled"] = &counter{idx: &idx2, loop: loop2}

		// Second close on new counter.
		require.NoError(t, r.handleCloseJob(event))
		assert.Equal(t, 2, closeCalled)
		assert.NotContains(t, r.counters, "recycled")
	})

	t.Run("idx transitions from non-zero to zero between calls allows close", func(t *testing.T) {

		var closeCalled int
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx atomic.Int64
		idx.Store(99)
		r := &router{
			counters: map[string]*counter{
				"transition": {idx: &idx, loop: loop},
			},
		}

		event := &queue.JobEvent{
			JobName: "transition",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		}

		// While idx != 0, close is skipped.
		require.NoError(t, r.handleCloseJob(event))
		assert.Equal(t, 0, closeCalled)
		assert.Contains(t, r.counters, "transition")

		// Now idx goes to 0, close should proceed.
		idx.Store(0)
		require.NoError(t, r.handleCloseJob(event))
		assert.Equal(t, 1, closeCalled)
		assert.NotContains(t, r.counters, "transition")
	})

	t.Run("many counters, close only the targeted one", func(t *testing.T) {

		closedJobs := make(map[string]bool)
		makeLoop := func(name string) *fake.Fake[*queue.JobAction] {
			return fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
				closedJobs[name] = true
			})
		}

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"job-a": {idx: &idx, loop: makeLoop("job-a")},
				"job-b": {idx: &idx, loop: makeLoop("job-b")},
				"job-c": {idx: &idx, loop: makeLoop("job-c")},
				"job-d": {idx: &idx, loop: makeLoop("job-d")},
				"job-e": {idx: &idx, loop: makeLoop("job-e")},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "job-c",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)

		assert.Len(t, r.counters, 4)
		assert.NotContains(t, r.counters, "job-c")
		assert.True(t, closedJobs["job-c"])
		assert.False(t, closedJobs["job-a"])
		assert.False(t, closedJobs["job-b"])
		assert.False(t, closedJobs["job-d"])
		assert.False(t, closedJobs["job-e"])
	})

	t.Run("close all counters one by one via handleCloseJob leaves empty map", func(t *testing.T) {

		var idx atomic.Int64
		names := []string{"a", "b", "c", "d"}
		r := &router{
			counters: make(map[string]*counter, len(names)),
		}
		for _, name := range names {
			r.counters[name] = &counter{
				idx:  &idx,
				loop: fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {}),
			}
		}

		for _, name := range names {
			err := r.handleCloseJob(&queue.JobEvent{
				JobName: name,
				Action: &queue.JobAction{
					Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
				},
			})
			require.NoError(t, err)
		}

		assert.Empty(t, r.counters)
	})

	t.Run("sequential close-job events for different missing counters all return nil", func(t *testing.T) {

		r := &router{
			counters: make(map[string]*counter),
		}

		for _, name := range []string{"ghost-1", "ghost-2", "ghost-3"} {
			err := r.handleCloseJob(&queue.JobEvent{
				JobName: name,
				Action: &queue.JobAction{
					Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
				},
			})
			require.NoError(t, err, "close of missing counter %q should not error", name)
		}

		assert.Empty(t, r.counters)
	})
}

func Test_handleCloseJob_race_scenarios(t *testing.T) {
	t.Cleanup(drainLoopsCache)

	t.Run("close arrives after job already deleted by prefix delete", func(t *testing.T) {

		// Simulates the production scenario: a namespace is deleted via
		// DeletePrefixes which removes the job from etcd. The counter's inner
		// loop handles the delete inform and sets idx=0, then sends a CloseJob
		// event. But by the time the CloseJob event is processed by the router,
		// the counter has already been cleaned up by a prior CloseJob or
		// re-inform cycle. The router must not error.
		r := &router{
			counters: make(map[string]*counter),
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "app||deleted-ns||myapp||myjob",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
	})

	t.Run("rapid upsert: close arrives for old version, new version already running", func(t *testing.T) {

		// Job is rapidly upserted. The old counter sends a CloseJob, but
		// by the time it arrives, a new counter with the same name has been
		// created (idx != 0). The close should be ignored.
		var closeCalled int
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx atomic.Int64
		idx.Store(200) // New version is active.
		r := &router{
			counters: map[string]*counter{
				"rapidly-upserted": {idx: &idx, loop: loop},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "rapidly-upserted",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 0, closeCalled)
		assert.Contains(t, r.counters, "rapidly-upserted")
	})

	t.Run("delete then re-create: stale close arrives after new counter created at idx 0", func(t *testing.T) {

		// Job deleted, then immediately re-created. A stale CloseJob from the
		// old lifecycle arrives. New counter is at idx=0 (just created, not yet
		// informed). This is the edge case — the close will actually close the
		// new counter. This is acceptable because the new counter will be
		// re-created on the next inform event.
		var closeCalled int
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"recreated": {idx: &idx, loop: loop},
			},
		}

		err := r.handleCloseJob(&queue.JobEvent{
			JobName: "recreated",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, closeCalled)
		assert.NotContains(t, r.counters, "recreated")
	})

	t.Run("multiple close events queued for same job, all safe", func(t *testing.T) {

		// Due to race conditions, multiple CloseJob events can be enqueued for
		// the same job. Only the first should close; subsequent should be no-ops.
		var closeCalled int
		loop := fake.New[*queue.JobAction]().WithClose(func(_ *queue.JobAction) {
			closeCalled++
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"multi-close": {idx: &idx, loop: loop},
			},
		}

		event := &queue.JobEvent{
			JobName: "multi-close",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
			},
		}

		for i := range 5 {
			err := r.handleCloseJob(event)
			require.NoError(t, err, "call %d should not error", i)
		}

		assert.Equal(t, 1, closeCalled, "close should only be called once")
		assert.NotContains(t, r.counters, "multi-close")
	})
}

func Test_handleCloseJob_does_not_propagate_error(t *testing.T) {
	t.Cleanup(drainLoopsCache)

	// This is the core safety test. Before the fix, a missing counter would
	// return an error that propagated through the loop → router → cancel chain,
	// killing the entire scheduler state machine. This test verifies that the
	// fix prevents that cascading failure.

	t.Run("missing counter must never return error to prevent cascading failure", func(t *testing.T) {

		cancelCalled := false
		r := &router{
			cancel: func(err error) {
				cancelCalled = true
			},
			counters: make(map[string]*counter),
		}

		// Simulate multiple missing counter close events (as would happen
		// during a namespace deletion with many jobs).
		for _, name := range []string{
			"actorreminder||prod-ns||type1||id1||job1",
			"app||prod-ns||myapp||critical-workflow",
			"actorreminder||prod-ns||type2||id2||job2",
			"app||prod-ns||another-app||scheduler-job",
		} {
			err := r.handleCloseJob(&queue.JobEvent{
				JobName: name,
				Action: &queue.JobAction{
					Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
				},
			})
			require.NoError(t, err, "missing counter for %q must not error", name)
		}

		assert.False(t, cancelCalled, "cancel must never be called for missing counters")
	})
}
