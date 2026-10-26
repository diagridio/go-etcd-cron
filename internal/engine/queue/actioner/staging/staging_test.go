/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/diagridio/go-etcd-cron/internal/engine/queue/queuestate"
)

func Test_DeliverablePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("registering empty prefixes should add nothing", func(t *testing.T) {
		t.Parallel()

		s := New(nil)
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes())
		assert.Empty(t, s.deliverablePrefixes)
		s.UnDeliverablePrefixes()
		assert.Empty(t, s.DeliverablePrefixes())
	})

	t.Run("registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New(nil)
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("foo")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("multiple: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New(nil)
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)

		s.UnDeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("multiple with diff prefixes: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New(nil)
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		assert.Empty(t, s.DeliverablePrefixes("def"))
		assert.Len(t, s.deliverablePrefixes, 2)
		assert.Empty(t, s.DeliverablePrefixes("def"))
		assert.Len(t, s.deliverablePrefixes, 2)

		s.UnDeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 2)
		s.UnDeliverablePrefixes("def")
		assert.Len(t, s.deliverablePrefixes, 2)
		s.UnDeliverablePrefixes("def")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("staged counters should be enqueued if they match an added prefix", func(t *testing.T) {
		t.Parallel()

		s := New(nil)
		s.staged = map[int64]string{
			123: "abc123", 234: "abc234",
			124: "def123", 235: "def234",
			125: "xyz123", 236: "xyz234",
		}

		assert.ElementsMatch(t,
			[]int64{123, 234, 125, 236},
			s.DeliverablePrefixes("abc", "xyz"),
		)
		assert.Equal(t,
			map[int64]string{124: "def123", 235: "def234"},
			s.staged,
		)

		assert.ElementsMatch(t,
			[]int64{124, 235},
			s.DeliverablePrefixes("d"),
		)
		assert.Empty(t, s.staged)
	})
}

func Test_Stage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		id                  int64
		jobName             string
		deliverablePrefixes []string
		expStaged           bool
	}{
		"no deliverable prefixes, should stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{},
			expStaged:           true,
		},
		"deliverable prefixes but different, should stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba"},
			expStaged:           true,
		},
		"deliverable prefixes and matches, should not stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"abc123"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and matches, should not stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "abc123", "cba"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and matches on prefix, should not stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba", "abc"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and not matches on prefix, should stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba", "abc1234"},
			expStaged:           true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := New(nil)
			s.DeliverablePrefixes(test.deliverablePrefixes...)

			got := s.Stage(test.id, test.jobName, nil)

			assert.Equal(t, test.expStaged, got)
			assert.Equal(t, test.expStaged, len(s.staged) == 1)
		})
	}
}

func Test_QueueState(t *testing.T) {
	t.Parallel()

	newStaging := func() (*Staging, *queuestate.State) {
		state := queuestate.New(func(jobName string) (string, string, bool) {
			ns, app, ok := strings.Cut(jobName, "/")
			return ns, app, ok
		})
		return New(state), state
	}

	staged := func(state *queuestate.State) map[string]map[string]uint64 {
		return state.Get().Staged
	}

	t.Run("staging counts the job, unstaging removes it", func(t *testing.T) {
		t.Parallel()

		s, state := newStaging()
		assert.True(t, s.Stage(1, "ns1/app1", state.Key("ns1/app1")))
		assert.True(t, s.Stage(2, "ns1/app1", state.Key("ns1/app1")))
		assert.True(t, s.Stage(3, "ns2/app2", state.Key("ns2/app2")))
		assert.Equal(t, map[string]map[string]uint64{
			"ns1": {"app1": 2},
			"ns2": {"app2": 1},
		}, staged(state))

		s.Unstage(1)
		s.Unstage(3)
		assert.Equal(t, map[string]map[string]uint64{"ns1": {"app1": 1}}, staged(state))
	})

	t.Run("staging the same job version twice counts it once", func(t *testing.T) {
		t.Parallel()

		s, state := newStaging()
		assert.True(t, s.Stage(1, "ns1/app1", state.Key("ns1/app1")))
		assert.True(t, s.Stage(1, "ns1/app1", state.Key("ns1/app1")))
		assert.Equal(t, map[string]map[string]uint64{"ns1": {"app1": 1}}, staged(state))
	})

	t.Run("unstaging a job which is not staged changes nothing", func(t *testing.T) {
		t.Parallel()

		s, state := newStaging()
		assert.True(t, s.Stage(1, "ns1/app1", state.Key("ns1/app1")))
		s.Unstage(2)
		s.Unstage(1)
		s.Unstage(1)
		assert.Empty(t, staged(state))
	})

	t.Run("a job which is delivered straight away is not counted", func(t *testing.T) {
		t.Parallel()

		s, state := newStaging()
		s.DeliverablePrefixes("ns1/")
		assert.False(t, s.Stage(1, "ns1/app1", state.Key("ns1/app1")))
		assert.Empty(t, staged(state))
	})

	t.Run("jobs released by a deliverable prefix are removed", func(t *testing.T) {
		t.Parallel()

		s, state := newStaging()
		assert.True(t, s.Stage(1, "ns1/app1", state.Key("ns1/app1")))
		assert.True(t, s.Stage(2, "ns1/app2", state.Key("ns1/app2")))
		assert.ElementsMatch(t, []int64{1}, s.DeliverablePrefixes("ns1/app1"))
		assert.Equal(t, map[string]map[string]uint64{"ns1": {"app2": 1}}, staged(state))
	})

	t.Run("jobs without a key are staged but not counted", func(t *testing.T) {
		t.Parallel()

		s, state := newStaging()
		assert.Nil(t, state.Key("no-namespace"))
		assert.True(t, s.Stage(1, "no-namespace", state.Key("no-namespace")))
		assert.Len(t, s.staged, 1)
		assert.Empty(t, staged(state))
		s.Unstage(1)
		assert.Empty(t, staged(state))
	})
}
