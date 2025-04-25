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

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/queue/actioner/fake"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops/fake"
)

func Test_router(t *testing.T) {
	t.Parallel()

	t.Run("if handle close job but no counter, then should error", func(t *testing.T) {
		t.Parallel()

		r := &router{
			counters: make(map[string]*counter),
		}

		require.Error(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: new(queue.CloseJob),
				},
			},
		}))
	})

	t.Run("if handle close job and counter is set to non-0 (not closed), then shouldn't take any action", func(t *testing.T) {
		t.Parallel()

		var idx atomic.Int64
		idx.Store(123)
		r := &router{
			counters: map[string]*counter{
				"test": &counter{
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
		t.Parallel()

		var called int
		loop := fake.New[*queue.JobAction]().WithClose(func(action *queue.JobAction) {
			assert.Equal(t, &queue.JobAction{
				Action: &queue.JobAction_Close{
					Close: new(queue.Close),
				},
			}, action)
			called++
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"test":   &counter{idx: &idx, loop: loop},
				"test-2": &counter{},
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
		assert.Equal(t, map[string]*counter{
			"test-2": &counter{},
		}, r.counters)
	})

	t.Run("if handle close, expect all loops to be closed", func(t *testing.T) {
		t.Parallel()

		exp := &queue.JobAction{
			Action: new(queue.JobAction_Close),
		}

		var called atomic.Uint64
		loop := fake.New[*queue.JobAction]().WithClose(func(action *queue.JobAction) {
			assert.Equal(t, exp, action)
			called.Add(1)
		})

		var idx atomic.Int64
		r := &router{
			counters: map[string]*counter{
				"test1": &counter{idx: &idx, loop: loop},
				"test2": &counter{idx: &idx, loop: loop},
				"test3": &counter{idx: &idx, loop: loop},
				"test4": &counter{idx: &idx, loop: loop},
				"test5": &counter{idx: &idx, loop: loop},
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
		t.Parallel()

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
				"test-job": &counter{loop: loop},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test-job",
			Action:  exp,
		}))
		assert.Equal(t, 1, called)
		assert.Equal(t, map[string]*counter{
			"test-job": &counter{loop: loop},
		}, r.counters)
	})

	t.Run("if handle event with non-existing counter, expect create and enqueue", func(t *testing.T) {
		t.Parallel()

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: false,
			},
		}}

		r := &router{
			act: actionerfake.New(),
			counters: map[string]*counter{
				"test-job-2": &counter{},
			},
		}

		require.NoError(t, r.Handle(t.Context(), &queue.JobEvent{
			JobName: "test-job",
			Action:  exp,
		}))

		assert.Len(t, r.counters, 2)
	})
}
