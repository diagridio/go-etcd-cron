/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/fake"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/counters"
)

func Test_worker(t *testing.T) {
	t.Parallel()

	t.Run("if handle close job but no counter, then no error", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			counters: make(map[string]*counters.Counters),
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			JobName: "123",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: new(queue.CloseJob),
				},
			},
		}))
	})

	t.Run("if handle job close then should close delete from map", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			counters: map[string]*counters.Counters{
				"1": counters.New(counters.Options{}),
				"2": counters.New(counters.Options{}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			JobName: "1",
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: new(queue.CloseJob),
				},
			},
		}))

		assert.Equal(t, map[string]*counters.Counters{
			"2": counters.New(counters.Options{}),
		}, w.counters)
	})

	t.Run("if handle close, expect all to be closed", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			counters: map[string]*counters.Counters{
				"1": counters.New(counters.Options{}),
				"2": counters.New(counters.Options{}),
				"3": counters.New(counters.Options{}),
				"4": counters.New(counters.Options{}),
				"5": counters.New(counters.Options{}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			Action: &queue.JobAction{
				Action: &queue.JobAction_Close{
					Close: new(queue.Close),
				},
			},
		}))

		assert.Empty(t, w.counters)
	})

	t.Run("if handle event with non-existing counter, expect create and enqueue", func(t *testing.T) {
		t.Parallel()

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: true,
				Job: &stored.Job{
					PartitionId: 1,
				},
			},
		}}

		w := &worker{
			act: actionerfake.New(),
			counters: map[string]*counters.Counters{
				"2": counters.New(counters.Options{
					Actioner: actionerfake.New(),
				}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			JobName: "1",
			Action:  exp,
		}))

		assert.Len(t, w.counters, 2)
	})

	t.Run("if handle event for delete with non-existing counter, expect no create or enqueue", func(t *testing.T) {
		t.Parallel()

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: false,
				Job: &stored.Job{
					PartitionId: 1,
				},
			},
		}}

		w := &worker{
			act: actionerfake.New(),
			counters: map[string]*counters.Counters{
				"test-job": counters.New(counters.Options{
					Actioner: actionerfake.New(),
				}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			JobName: "1",
			Action:  exp,
		}))

		assert.Len(t, w.counters, 1)
	})
}
