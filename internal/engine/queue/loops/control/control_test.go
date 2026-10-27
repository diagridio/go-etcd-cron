/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package control

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/events/loop/fake"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/fake"
	"github.com/diagridio/go-etcd-cron/internal/engine/queue/queuestate"
)

func Test_control(t *testing.T) {
	t.Parallel()

	t.Run("receive informer event should send informer to jobs loop", func(t *testing.T) {
		t.Parallel()

		var called int
		router := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			called++
			assert.Equal(t, &queue.JobEvent{
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
			router: router,
			act:    actionerfake.New(),
			state:  queuestate.New(nil),
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

	t.Run("ExecuteRequest should enqueue to router loop", func(t *testing.T) {
		t.Parallel()

		execute := &queue.ExecuteRequest{ModRevision: 42}
		router := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			assert.Equal(t, execute, action.GetAction().GetExecuteRequest())
		})

		c := &control{router: router, act: actionerfake.New(), state: queuestate.New(nil)}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteRequest{ExecuteRequest: execute},
		})
		require.NoError(t, err)
	})

	t.Run("ExecuteResponse should enqueue to router loop", func(t *testing.T) {
		t.Parallel()

		response := &queue.ExecuteResponse{ModRevision: 84}
		router := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			assert.Equal(t, response, action.GetAction().GetExecuteResponse())
		})

		c := &control{router: router, act: actionerfake.New(), state: queuestate.New(nil)}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteResponse{ExecuteResponse: response},
		})
		require.NoError(t, err)
	})

	t.Run("DeliverablePrefixes should call actioner and enqueue deliverable router", func(t *testing.T) {
		t.Parallel()

		act := actionerfake.New().WithDeliverablePrefixes(func(prefixes ...string) []int64 {
			assert.ElementsMatch(t, []string{"prefix1", "prefix2"}, prefixes)
			return []int64{42, 84}
		})

		var enqueued []int64
		router := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			enqueued = append(enqueued, action.GetAction().GetDeliverable().ModRevision)
		})

		c := &control{router: router, act: act, state: queuestate.New(nil)}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_DeliverablePrefixes{
				DeliverablePrefixes: &queue.DeliverablePrefixes{Prefixes: []string{"prefix1", "prefix2"}},
			},
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []int64{42, 84}, enqueued)
	})

	t.Run("UndeliverablePrefixes should call UnDeliverablePrefixes on actioner", func(t *testing.T) {
		t.Parallel()

		called := false
		act := actionerfake.New().WithUnDeliverablePrefixes(func(prefixes ...string) {
			called = true
			assert.ElementsMatch(t, []string{"badPrefix"}, prefixes)
		})

		c := &control{router: fake.New[*queue.JobEvent](), act: act, state: queuestate.New(nil)}

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
		router := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			received = action
		})

		c := &control{router: router, act: actionerfake.New(), state: queuestate.New(nil)}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_CloseJob{
				CloseJob: &queue.CloseJob{ModRevision: 100},
			},
		})
		require.NoError(t, err)

		assert.Equal(t, int64(100), received.Action.GetCloseJob().ModRevision)
		assert.NotNil(t, received.Action.GetCloseJob())
	})

	t.Run("Close should trigger close on router loop", func(t *testing.T) {
		t.Parallel()

		closed := false
		router := fake.New[*queue.JobEvent]().WithClose(func(event *queue.JobEvent) {
			closed = true
			assert.NotNil(t, event.Action.GetClose())
		})

		c := &control{router: router, act: actionerfake.New(), state: queuestate.New(nil)}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_Close{},
		})
		require.NoError(t, err)
		assert.True(t, closed)
	})

	t.Run("Idle should move the job to idle in the state without routing", func(t *testing.T) {
		t.Parallel()

		router := fake.New[*queue.JobEvent]().WithEnqueue(func(action *queue.JobEvent) {
			assert.Fail(t, "Idle should not be routed", action)
		})

		state := queuestate.New(func(string) (string, string, bool) {
			return "ns1", "app1", true
		})
		state.Informed(42, "job", true)
		state.ExecuteRequest(42)
		require.NotEmpty(t, state.Get().InFlight)

		c := &control{router: router, act: actionerfake.New(), state: state}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: &queue.ControlEvent_Idle{Idle: &queue.Idle{ModRevision: 42}},
		})
		require.NoError(t, err)
		assert.Empty(t, state.Get().InFlight)
	})

	t.Run("unknown event should return error", func(t *testing.T) {
		t.Parallel()

		c := &control{router: fake.New[*queue.JobEvent](), act: actionerfake.New(), state: queuestate.New(nil)}

		err := c.Handle(t.Context(), &queue.ControlEvent{
			Action: nil,
		})
		assert.Error(t, err)
	})
}
