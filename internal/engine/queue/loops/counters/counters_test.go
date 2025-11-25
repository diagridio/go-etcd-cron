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

func Test_Counters(t *testing.T) {
	t.Parallel()

	t.Run("unknown event type should error", func(t *testing.T) {
		t.Parallel()

		c := new(Counters)
		require.Error(t, c.Handle(t.Context(), new(queue.JobAction)))
	})

	t.Run("a delete informer event should unstage, dechedule, set counter to nil, set jobVersion to 0, queue the job to be closed", func(t *testing.T) {
		cnter := counterfake.New()
		var called int

		act := fake.New().
			WithUnstage(func(name string) {
				assert.Equal(t, "test-job", name)
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

		c := &Counters{
			act:     act,
			name:    "test-job",
			counter: cnter,
		}
		c.jobVersion = 123

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{
					Name:  "test-job",
					Job:   &stored.Job{},
					IsPut: false,
				},
			},
		}))

		assert.Equal(t, int64(0), c.jobVersion)
		assert.Nil(t, c.counter)
		assert.Equal(t, 3, called)
	})

	t.Run("a put event should increase the jobVersion and schedule with a new counter", func(t *testing.T) {
		t.Parallel()

		var called int

		act := fake.New().WithSchedule(func(_ context.Context, name string, id int64, _ *stored.Job) (counter.Interface, error) {
			called++
			assert.Equal(t, "test-job", name)
			assert.Equal(t, int64(456), id)
			return counterfake.New(), nil
		})

		c := &Counters{
			act:  act,
			name: "test-job",
		}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{
					Name:           "test-job",
					JobModRevision: 456,
					IsPut:          true,
					Job:            &stored.Job{},
				},
			},
		}))

		assert.NotEqual(t, c.jobVersion, 0)
		assert.NotNil(t, c.counter)
		assert.Equal(t, 1, called)
	})

	t.Run("a put event where the scheduler returns an error should return an error", func(t *testing.T) {
		t.Parallel()

		var called int
		act := fake.New().WithSchedule(func(_ context.Context, name string, id int64, _ *stored.Job) (counter.Interface, error) {
			called++
			return nil, assert.AnError
		})

		c := &Counters{
			act:  act,
			name: "test-job",
		}

		require.Error(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{
					Name:           "test-job",
					JobModRevision: 456,
					IsPut:          true,
					Job:            &stored.Job{},
				},
			},
		}))

		assert.NotEqual(t, c.jobVersion, 0)
		assert.Nil(t, c.counter)
		assert.Equal(t, 1, called)
	})

	t.Run("an execute request should not error if there is no counter", func(t *testing.T) {
		t.Parallel()

		c := &Counters{}

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{
					JobName: "test-job",
				},
			},
		}))
	})

	t.Run("an execute request should trigger, and enqueue the result", func(t *testing.T) {
		t.Parallel()

		var called atomic.Uint64
		act := fake.New().WithTrigger(func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
			called.Add(1)
			assert.Equal(t, &api.TriggerRequest{
				Name: "test-job",
			}, req)
			fn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
		}).WithAddToControlLoop(func(event *queue.ControlEvent) {
			called.Add(1)
			assert.Equal(t, &queue.ControlEvent{
				Action: &queue.ControlEvent_ExecuteResponse{
					ExecuteResponse: &queue.ExecuteResponse{
						JobName: "test-job",
						Uid:     1234,
						Result: &api.TriggerResponse{
							Result: api.TriggerResponseResult_UNDELIVERABLE,
						},
					},
				},
			}, event)
		})

		c := &Counters{
			act:  act,
			name: "test-job",
			counter: counterfake.New().WithTriggerRequest(func() *api.TriggerRequest {
				return &api.TriggerRequest{Name: "test-job"}
			}),
		}
		c.jobVersion = 1234

		require.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteRequest{
				ExecuteRequest: &queue.ExecuteRequest{
					JobName: "test-job",
				},
			},
		}))

		assert.Equal(t, int64(1234), c.jobVersion)
		assert.NotNil(t, c.counter)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint64(2), called.Load())
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("a handle deliverable with no counter should do nothing", func(t *testing.T) {
		t.Parallel()
		c := &Counters{}

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

		c := &Counters{
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

	t.Run("a stale execute response with matching uid and nil counter should be ignored", func(t *testing.T) {
		t.Parallel()

		c := &Counters{
			name: "test-job",
		}
		c.jobVersion = 999

		assert.NoError(t, c.Handle(t.Context(), &queue.JobAction{
			Action: &queue.JobAction_ExecuteResponse{
				ExecuteResponse: &queue.ExecuteResponse{
					JobName: "test-job",
					Uid:     999,
					Result: &api.TriggerResponse{
						Result: api.TriggerResponseResult_SUCCESS,
					},
				},
			},
		}))
	})
}
