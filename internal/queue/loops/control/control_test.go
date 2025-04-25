/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package control

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/queue/actioner/fake"
	"github.com/diagridio/go-etcd-cron/internal/queue/loops/fake"
)

func Test_control(t *testing.T) {
	t.Parallel()

	t.Run("receive informer event should send informer to jobs loop", func(t *testing.T) {
		t.Parallel()

		var called int
		jobs := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			called++
			assert.Equal(t, &queue.JobEvent{
				JobName: "test",
				Action: &queue.JobAction{
					Action: &queue.JobAction_Informed{
						Informed: &queue.Informed{
							Name:  "test",
							IsPut: true,
						},
					},
				},
			}, action)
		})

		c := &control{
			jobs: jobs,
			act:  actionerfake.New(),
		}

		require.NoError(t, c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_Informed{
				Informed: &queue.Informed{
					Name:  "test",
					IsPut: true,
				},
			},
		}))

		assert.Equal(t, 1, called)
	})

	t.Run("ExecuteRequest should enqueue to jobs loop", func(t *testing.T) {
		t.Parallel()

		execute := &queue.ExecuteRequest{JobName: "job1"}
		jobs := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			assert.Equal(t, "job1", action.JobName)
			assert.Equal(t, execute, action.GetAction().GetExecuteRequest())
		})

		c := &control{jobs: jobs, act: actionerfake.New()}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteRequest{ExecuteRequest: execute},
		})
		require.NoError(t, err)
	})

	t.Run("ExecuteResponse should enqueue to jobs loop", func(t *testing.T) {
		t.Parallel()

		response := &queue.ExecuteResponse{JobName: "job2"}
		jobs := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			assert.Equal(t, "job2", action.JobName)
			assert.Equal(t, response, action.GetAction().GetExecuteResponse())
		})

		c := &control{jobs: jobs, act: actionerfake.New()}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteResponse{ExecuteResponse: response},
		})
		require.NoError(t, err)
	})

	t.Run("DeliverablePrefixes should call actioner and enqueue deliverable jobs", func(t *testing.T) {
		t.Parallel()

		act := actionerfake.New().WithDeliverablePrefixes(func(prefixes ...string) []string {
			assert.ElementsMatch(t, []string{"prefix1", "prefix2"}, prefixes)
			return []string{"jobA", "jobB"}
		})

		var enqueued []string
		jobs := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			enqueued = append(enqueued, action.JobName)
		})

		c := &control{jobs: jobs, act: act}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_DeliverablePrefixes{
				DeliverablePrefixes: &queue.DeliverablePrefixes{Prefixes: []string{"prefix1", "prefix2"}},
			},
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"jobA", "jobB"}, enqueued)
	})

	t.Run("UndeliverablePrefixes should call UnDeliverablePrefixes on actioner", func(t *testing.T) {
		t.Parallel()

		called := false
		act := actionerfake.New().WithUnDeliverablePrefixes(func(prefixes ...string) {
			called = true
			assert.ElementsMatch(t, []string{"badPrefix"}, prefixes)
		})

		c := &control{jobs: fake.New[*queue.JobEvent](), act: act}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_UndeliverablePrefixes{
				UndeliverablePrefixes: &queue.UndeliverablePrefixes{Prefixes: []string{"badPrefix"}},
			},
		})
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("CloseJob should enqueue close job action", func(t *testing.T) {
		t.Parallel()

		var received *queue.JobEvent
		jobs := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			received = action
		})

		c := &control{jobs: jobs, act: actionerfake.New()}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_CloseJob{
				CloseJob: &queue.CloseJob{JobName: "closeme"},
			},
		})
		require.NoError(t, err)

		assert.Equal(t, "closeme", received.JobName)
		assert.NotNil(t, received.Action.GetCloseJob())
	})

	t.Run("Close should trigger close on jobs loop", func(t *testing.T) {
		t.Parallel()

		closed := false
		jobs := fake.New[*queue.JobEvent]().WithClose(func(event *queue.JobEvent) {
			closed = true
			assert.NotNil(t, event.Action.GetClose())
		})

		c := &control{jobs: jobs, act: actionerfake.New()}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_Close{},
		})
		require.NoError(t, err)
		assert.True(t, closed)
	})

	t.Run("unknown event should return error", func(t *testing.T) {
		t.Parallel()

		c := &control{jobs: fake.New[*queue.JobEvent](), act: actionerfake.New()}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: nil,
		})
		assert.Error(t, err)
	})
}
