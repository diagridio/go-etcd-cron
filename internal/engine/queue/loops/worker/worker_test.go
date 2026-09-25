/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package worker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	counterfake "github.com/diagridio/go-etcd-cron/internal/counter/fake"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/fake"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/loops/counters"
)

func Test_worker(t *testing.T) {
	t.Parallel()

	t.Run("if handle close job but no counter, then no error", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			counters: make(map[int64]*counters.Counters),
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: &queue.CloseJob{
						ModRevision: 1,
					},
				},
			},
		}))
	})

	t.Run("if handle job close then should close delete from map", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			counters: map[int64]*counters.Counters{
				1: counters.New(counters.Options{}),
				2: counters.New(counters.Options{}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			Action: &queue.JobAction{
				Action: &queue.JobAction_CloseJob{
					CloseJob: &queue.CloseJob{
						ModRevision: 1,
					},
				},
			},
		}))

		assert.Equal(t, map[int64]*counters.Counters{
			2: counters.New(counters.Options{}),
		}, w.counters)
	})

	t.Run("if handle close, expect all to be closed", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			counters: map[int64]*counters.Counters{
				1: counters.New(counters.Options{}),
				2: counters.New(counters.Options{}),
				3: counters.New(counters.Options{}),
				4: counters.New(counters.Options{}),
				5: counters.New(counters.Options{}),
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
				Name: "test-job",
				QueuedJob: &queue.QueuedJob{
					ModRevision: 1,
				},
				IsPut: true,
			},
		}}

		w := &worker{
			act: actionerfake.New(),
			counters: map[int64]*counters.Counters{
				2: counters.New(counters.Options{
					Actioner: actionerfake.New(),
				}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			Action: exp,
		}))

		assert.Len(t, w.counters, 2)
	})

	t.Run("if handle event for delete with non-existing counter, expect no create or enqueue", func(t *testing.T) {
		t.Parallel()

		exp := &queue.JobAction{Action: &queue.JobAction_Informed{
			Informed: &queue.Informed{
				Name:  "test-job",
				IsPut: false,
				QueuedJob: &queue.QueuedJob{
					ModRevision: 1,
				},
			},
		}}

		w := &worker{
			act: actionerfake.New(),
			counters: map[int64]*counters.Counters{
				1: counters.New(counters.Options{
					Actioner: actionerfake.New(),
				}),
			},
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			Action: exp,
		}))

		assert.Len(t, w.counters, 1)
	})

	t.Run("if handle ExecuteRequest with non-existing counter, expect dropped without error", func(t *testing.T) {
		t.Parallel()

		var triggered atomic.Int64
		w := &worker{
			act: actionerfake.New().WithTrigger(func(*api.TriggerRequest, func(*api.TriggerResponse)) {
				triggered.Add(1)
			}),
			counters: make(map[int64]*counters.Counters),
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
			Action: &queue.JobAction{
				Action: &queue.JobAction_ExecuteRequest{
					ExecuteRequest: &queue.ExecuteRequest{
						ModRevision: 42,
					},
				},
			},
		}))
		assert.Empty(t, w.counters)
		assert.Zero(t, triggered.Load())
	})

	t.Run("if handle ExecuteRequest with non-existing counter, other counters are preserved and not panicked over", func(t *testing.T) {
		t.Parallel()

		w := &worker{
			act: actionerfake.New(),
			counters: map[int64]*counters.Counters{
				7: counters.New(counters.Options{
					Actioner: actionerfake.New(),
				}),
			},
		}

		assert.NotPanics(t, func() {
			require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
				Action: &queue.JobAction{
					Action: &queue.JobAction_ExecuteRequest{
						ExecuteRequest: &queue.ExecuteRequest{
							ModRevision: 1,
						},
					},
				},
			}))
		})

		assert.Len(t, w.counters, 1)
		assert.Contains(t, w.counters, int64(7))
	})

	// A job's trigger is fired by the queue asynchronously to the job's
	// lifecycle, so its ExecuteRequest can reach the worker at any point while
	// the job is being deleted. Every interleaving must be tolerated: an error
	// here tears down the whole cron instance.
	t.Run("ExecuteRequest racing a job delete never errors or triggers", func(t *testing.T) {
		t.Parallel()

		executeRequest := &queue.JobEvent{Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{ModRevision: 5},
			},
		}}
		put := &queue.JobEvent{Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{Informed: &queue.Informed{
				Name:      "test-job",
				IsPut:     true,
				QueuedJob: &queue.QueuedJob{ModRevision: 5},
			}},
		}}
		del := &queue.JobEvent{Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{Informed: &queue.Informed{
				Name:      "test-job",
				IsPut:     false,
				QueuedJob: &queue.QueuedJob{ModRevision: 5},
			}},
		}}

		tests := map[string]struct {
			// executeAfterCloseJob delivers the ExecuteRequest after the
			// CloseJob the delete emits has been handled, i.e. the counter is
			// gone from the worker. Otherwise it is delivered between the delete
			// and its CloseJob, while the counter is closed but still mapped.
			executeAfterCloseJob bool
		}{
			"ExecuteRequest between delete and CloseJob": {executeAfterCloseJob: false},
			"ExecuteRequest after CloseJob":              {executeAfterCloseJob: true},
		}

		for name, test := range tests {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				var triggered atomic.Int64
				var closeJobs []*queue.ControlEvent
				act := actionerfake.New().
					WithSchedule(func(context.Context, string, *queue.QueuedJob) (counter.Interface, error) {
						return counterfake.New().WithKey(5), nil
					}).
					WithTrigger(func(*api.TriggerRequest, func(*api.TriggerResponse)) {
						triggered.Add(1)
					}).
					WithAddToControlLoop(func(e *queue.ControlEvent) {
						closeJobs = append(closeJobs, e)
					})

				w := &worker{
					act:      act,
					counters: make(map[int64]*counters.Counters),
				}

				require.NoError(t, w.Handle(t.Context(), put))
				require.Contains(t, w.counters, int64(5))

				require.NoError(t, w.Handle(t.Context(), del))
				require.Len(t, closeJobs, 1)
				closeJob := closeJobs[0].GetCloseJob()
				require.NotNil(t, closeJob)
				assert.Equal(t, int64(5), closeJob.GetModRevision())

				if !test.executeAfterCloseJob {
					require.NoError(t, w.Handle(t.Context(), executeRequest))
				}

				require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{
					Action: &queue.JobAction{
						Action: &queue.JobAction_CloseJob{CloseJob: closeJob},
					},
				}))
				assert.Empty(t, w.counters)

				if test.executeAfterCloseJob {
					require.NoError(t, w.Handle(t.Context(), executeRequest))
				}

				assert.Empty(t, w.counters)
				assert.Zero(t, triggered.Load(), "a deleted job must not be triggered")
			})
		}
	})

	t.Run("ExecuteRequest for a closed job does not affect a live job", func(t *testing.T) {
		t.Parallel()

		var triggered []string
		act := actionerfake.New().
			WithSchedule(func(_ context.Context, name string, job *queue.QueuedJob) (counter.Interface, error) {
				return counterfake.New().
					WithKey(job.GetModRevision()).
					WithTriggerRequest(func() *api.TriggerRequest {
						return &api.TriggerRequest{Name: name}
					}), nil
			}).
			WithTrigger(func(req *api.TriggerRequest, _ func(*api.TriggerResponse)) {
				triggered = append(triggered, req.GetName())
			})

		w := &worker{
			act:      act,
			counters: make(map[int64]*counters.Counters),
		}

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{Informed: &queue.Informed{
				Name:      "live",
				IsPut:     true,
				QueuedJob: &queue.QueuedJob{ModRevision: 9},
			}},
		}}))

		// ModRevision 3 was deleted and fully closed before its trigger landed.
		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{ModRevision: 3},
			},
		}}))

		require.NoError(t, w.Handle(t.Context(), &queue.JobEvent{Action: &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{ModRevision: 9},
			},
		}}))

		assert.Len(t, w.counters, 1)
		assert.Equal(t, []string{"live"}, triggered, "only the live job must be triggered")
	})
}
