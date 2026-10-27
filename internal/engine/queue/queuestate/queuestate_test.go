/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package queuestate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/diagridio/go-etcd-cron/api"
)

func Test_State(t *testing.T) {
	t.Parallel()

	newState := func() *State {
		return New(func(jobName string) (string, string, bool) {
			return strings.Cut(jobName, "/")
		})
	}

	counts := func(c map[string]uint64) map[string]map[string]uint64 {
		return map[string]map[string]uint64{"ns1": c}
	}

	t.Run("a job is in flight from request to response, then dropped", func(t *testing.T) {
		t.Parallel()

		s := newState()
		s.Informed(1, true)
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.jobs)

		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)

		s.ExecuteResponse(1, api.TriggerResponseResult_SUCCESS)
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.Get().Staged)
		assert.Empty(t, s.jobs)
	})

	t.Run("a repeated request for a job in flight is counted once", func(t *testing.T) {
		t.Parallel()

		s := newState()
		s.ExecuteRequest(1, "ns1/app1")
		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)
	})

	t.Run("an undeliverable response moves the job from in flight to staged in one step", func(t *testing.T) {
		t.Parallel()

		s := newState()
		s.ExecuteRequest(1, "ns1/app1")
		s.ExecuteResponse(1, api.TriggerResponseResult_UNDELIVERABLE)
		assert.Empty(t, s.Get().InFlight)
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().Staged)

		// Released by a deliverable prefix, the job stays staged until its next
		// trigger.
		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)
		assert.Empty(t, s.Get().Staged)
	})

	t.Run("a job deleted while in flight is counted until its trigger is answered", func(t *testing.T) {
		t.Parallel()

		s := newState()
		s.ExecuteRequest(1, "ns1/app1")
		s.Informed(1, false)
		s.CloseJob(1)
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)

		// A late request for the deleted job is not counted again.
		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)

		s.ExecuteResponse(1, api.TriggerResponseResult_UNDELIVERABLE)
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.Get().Staged)
		assert.Empty(t, s.jobs)
	})

	t.Run("an undeliverable job the worker re-enqueued rather than staged is idle", func(t *testing.T) {
		t.Parallel()

		s := newState()
		s.ExecuteRequest(1, "ns1/app1")
		s.ExecuteResponse(1, api.TriggerResponseResult_UNDELIVERABLE)
		s.Idle(1)
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.Get().Staged)
		assert.Empty(t, s.jobs)

		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)
	})

	t.Run("a request the worker dropped is counted until reported idle", func(t *testing.T) {
		t.Parallel()

		// The state does not remember idle jobs, so a request for a job which
		// was deleted meanwhile is counted until the worker reports it idle.
		s := newState()
		s.Informed(1, true)
		s.Informed(1, false)
		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)

		s.Idle(1)
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.jobs)
	})

	t.Run("a job closed while in flight is dropped once answered or reported idle", func(t *testing.T) {
		t.Parallel()

		for name, finish := range map[string]func(*State){
			"answered": func(s *State) { s.ExecuteResponse(1, api.TriggerResponseResult_SUCCESS) },
			"idle":     func(s *State) { s.Idle(1) },
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				s := newState()
				s.ExecuteRequest(1, "ns1/app1")
				s.CloseJob(1)
				assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)

				finish(s)
				assert.Empty(t, s.Get().InFlight)
				assert.Empty(t, s.jobs)
			})
		}
	})

	t.Run("a staged job is removed when deleted, updated or closed", func(t *testing.T) {
		t.Parallel()

		s := newState()
		for _, id := range []int64{1, 2, 3} {
			s.ExecuteRequest(id, "ns1/app1")
			s.ExecuteResponse(id, api.TriggerResponseResult_UNDELIVERABLE)
		}
		assert.Equal(t, counts(map[string]uint64{"app1": 3}), s.Get().Staged)

		s.Informed(1, false)
		assert.Equal(t, counts(map[string]uint64{"app1": 2}), s.Get().Staged)

		// An update deletes the old version and puts the new one.
		s.Informed(2, false)
		s.Informed(4, true)
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().Staged)

		s.CloseJob(3)
		assert.Empty(t, s.Get().Staged)
		assert.Empty(t, s.jobs)

		s.ExecuteRequest(4, "ns1/app1")
		s.ExecuteResponse(4, api.TriggerResponseResult_UNDELIVERABLE)
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().Staged)
	})

	t.Run("jobs are counted per namespace and app ID", func(t *testing.T) {
		t.Parallel()

		s := newState()
		s.ExecuteRequest(1, "ns1/app1")
		s.ExecuteRequest(2, "ns1/app1")
		s.ExecuteRequest(3, "ns2/app1")
		s.ExecuteRequest(4, "not-counted")
		assert.Equal(t, map[string]map[string]uint64{
			"ns1": {"app1": 2},
			"ns2": {"app1": 1},
		}, s.Get().InFlight)
		assert.Len(t, s.jobs, 3)
	})

	t.Run("no key function counts nothing", func(t *testing.T) {
		t.Parallel()

		s := New(nil)
		s.ExecuteRequest(1, "ns1/app1")
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.jobs)
	})

	t.Run("a panicking key function leaves the job uncounted", func(t *testing.T) {
		t.Parallel()

		s := New(func(string) (string, string, bool) {
			panic("bad job name")
		})
		assert.NotPanics(t, func() { s.ExecuteRequest(1, "ns1/app1") })
		assert.Empty(t, s.Get().InFlight)
		assert.Empty(t, s.jobs)
	})

	t.Run("the key function is called without holding the lock", func(t *testing.T) {
		t.Parallel()

		var s *State
		s = New(func(jobName string) (string, string, bool) {
			// Would deadlock if called while holding the lock.
			s.Get()
			return strings.Cut(jobName, "/")
		})
		s.ExecuteRequest(1, "ns1/app1")
		assert.Equal(t, counts(map[string]uint64{"app1": 1}), s.Get().InFlight)
	})
}
