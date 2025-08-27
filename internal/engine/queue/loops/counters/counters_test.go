/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package counters

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	counterfake "github.com/diagridio/go-etcd-cron/internal/counter/fake"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/fake"
)

func Test_counters(t *testing.T) {
	t.Parallel()

	t.Run("unknown event type should error", func(t *testing.T) {
		t.Parallel()

		c := new(counters)
		require.Error(t, c.Handle(t.Context(), new(queue.JobAction)))
	})

	t.Run("a delete informer event should call cancel, unstage, dechedule, set counter to nil, set idx to 0, queue the job to be closed", func(t *testing.T) {
		cnter := counterfake.New()
		var called int

		act := fake.New().
			WithUnstage(func(jobName string) {
				assert.Equal(t, "test-job", jobName)
				called++
			}).
			WithAddToControlLoop(func(event *queue.ControlEvent) {
				assert.Equal(t, &queue.ControlEvent{
					Action: &queue.ControlEvent_CloseJob{
						CloseJob: &queue.CloseJob{JobName: "test-job"},
					},
				}, event)
				called++
			}).
			WithDeschedule(func(counter counter.Interface) {
				assert.Equal(t, cnter, counter)
				called++
			})

		var idx atomic.Int64
		idx.Store(123)
		c := &counters{
			act:     act,
			name:    "test-job",
			idx:     &idx,
			cancel:  func(error) { called++ },
			counter: cnter,
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{
					Name:  "test-job",
					IsPut: false,
				},
			},
		}))

		assert.Equal(t, int64(0), c.idx.Load())
		assert.Nil(t, c.counter)
		assert.Equal(t, 4, called)
	})

	t.Run("a put event should increase the idx and schedule with a new counter", func(t *testing.T) {
		t.Parallel()

		var called int

		act := fake.New().WithSchedule(func(_ context.Context, jobName string, mod int64, job *stored.Job) (counter.Interface, error) {
			called++
			assert.Equal(t, "test-job", jobName)
			assert.Equal(t, int64(456), mod)
			assert.Equal(t, &stored.Job{PartitionId: 987}, job)
			return counterfake.New(), nil
		})

		var idx atomic.Int64
		c := &counters{
			act:    act,
			name:   "test-job",
			idx:    &idx,
			cancel: func(error) { called++ },
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{
					Name:           "test-job",
					IsPut:          true,
					JobModRevision: 456,
					Job:            &stored.Job{PartitionId: 987},
				},
			},
		}))

		assert.Equal(t, int64(456), c.idx.Load())
		assert.NotNil(t, c.counter)
		assert.Equal(t, 2, called)
	})

	t.Run("a put event where the scheduler returns an error should return an error", func(t *testing.T) {
		t.Parallel()

		var called int
		act := fake.New().WithSchedule(func(_ context.Context, jobName string, mod int64, job *stored.Job) (counter.Interface, error) {
			called++
			return nil, assert.AnError
		})

		var idx atomic.Int64
		c := &counters{
			act:    act,
			name:   "test-job",
			idx:    &idx,
			cancel: func(error) { called++ },
		}

		require.Error(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{
					Name:           "test-job",
					IsPut:          true,
					JobModRevision: 456,
					Job:            &stored.Job{PartitionId: 987},
				},
			},
		}))

		assert.Equal(t, int64(456), c.idx.Load())
		assert.Nil(t, c.counter)
		assert.Equal(t, 2, called)
	})

	t.Run("an execute request should nil if there is no counter", func(t *testing.T) {
		t.Parallel()

		c := &counters{}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{
					JobName:    "test-job",
					CounterKey: "test-key",
				},
			},
		}))
	})

	t.Run("an execute request should trigger, and enqueue the result", func(t *testing.T) {
		t.Parallel()

		var called atomic.Uint64
		act := fake.New().WithTrigger(func(_ context.Context, req *api.TriggerRequest) *api.TriggerResponse {
			called.Add(1)
			assert.Equal(t, &api.TriggerRequest{
				Name: "test-job",
			}, req)
			return &api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE}
		}).WithAddToControlLoop(func(event *queue.ControlEvent) {
			called.Add(1)
			assert.Equal(t, &queue.ControlEvent{
				Action: &queue.ControlEvent_ExecuteResponse{
					ExecuteResponse: &queue.ExecuteResponse{
						JobName:    "test-job",
						CounterKey: "test-key",
						Uid:        1234,
						Result: &api.TriggerResponse{
							Result: api.TriggerResponseResult_UNDELIVERABLE,
						},
					},
				},
			}, event)
		})

		var idx atomic.Int64
		idx.Store(1234)
		c := &counters{
			act:  act,
			name: "test-job",
			idx:  &idx,
			counter: counterfake.New().WithTriggerRequest(func() *api.TriggerRequest {
				return &api.TriggerRequest{Name: "test-job"}
			}),
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{
					JobName:    "test-job",
					CounterKey: "test-key",
				},
			},
		}))

		assert.Equal(t, int64(1234), c.idx.Load())
		assert.NotNil(t, c.counter)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint64(2), called.Load())
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("an execute request which has been cancelled, should return the result and enqueue", func(t *testing.T) {
		t.Parallel()

		var called atomic.Uint64
		act := fake.New().WithTrigger(func(ctx context.Context, req *api.TriggerRequest) *api.TriggerResponse {
			<-ctx.Done()
			called.Add(1)
			assert.Equal(t, &api.TriggerRequest{
				Name: "test-job",
			}, req)
			return &api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE}
		}).WithAddToControlLoop(func(event *queue.ControlEvent) {
			called.Add(1)
			assert.Equal(t, &queue.ControlEvent{
				Action: &queue.ControlEvent_ExecuteResponse{
					ExecuteResponse: &queue.ExecuteResponse{
						JobName:    "test-job",
						CounterKey: "test-key",
						Uid:        1234,
						Result: &api.TriggerResponse{
							Result: api.TriggerResponseResult_UNDELIVERABLE,
						},
					},
				},
			}, event)
		})

		var idx atomic.Int64
		idx.Store(1234)
		c := &counters{
			act:  act,
			name: "test-job",
			idx:  &idx,
			counter: counterfake.New().WithTriggerRequest(func() *api.TriggerRequest {
				return &api.TriggerRequest{Name: "test-job"}
			}),
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{
					JobName:    "test-job",
					CounterKey: "test-key",
				},
			},
		}))

		assert.Equal(t, int64(1234), c.idx.Load())
		assert.NotNil(t, c.counter)

		c.cancel(assert.AnError)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint64(2), called.Load())
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("a handle deliverable with no counter should do nothing", func(t *testing.T) {
		t.Parallel()
		c := &counters{}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Deliverable{
				Deliverable: new(queue.DeliverableJob),
			},
		}))
	})

	t.Run("a handle deliverable with counter should enqueue the counter", func(t *testing.T) {
		t.Parallel()

		cnter := counterfake.New()
		var called int
		act := fake.New().WithEnqueue(func(counter counter.Interface) {
			assert.Equal(t, cnter, counter)
			called++
		})

		c := &counters{
			counter: cnter,
			act:     act,
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Deliverable{
				Deliverable: new(queue.DeliverableJob),
			},
		}))

		assert.Equal(t, 1, called)
	})

	t.Run("a handle close should call cancel", func(t *testing.T) {
		t.Parallel()

		var called int
		c := &counters{
			idx: new(atomic.Int64),
			cancel: func(error) {
				called++
			},
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Close{
				Close: new(queue.Close),
			},
		}))

		assert.Equal(t, 1, called)
	})

	t.Run("a stale execute response with matching uid and nil cancel should be ignored", func(t *testing.T) {
		t.Parallel()

		var idx atomic.Int64
		idx.Store(999)

		c := &counters{
			name:    "test-job",
			idx:     &idx,
			counter: counterfake.New(),
		}

		assert.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteResponse{
				ExecuteResponse: &queue.ExecuteResponse{
					JobName:    "test-job",
					CounterKey: "test-key",
					Uid:        999,
					Result: &api.TriggerResponse{
						Result: api.TriggerResponseResult_SUCCESS,
					},
				},
			},
		}))
	})
}
