/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_DeliverablePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("registering empty prefixes should add nothing", func(t *testing.T) {
		t.Parallel()

		s := New()
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes())
		assert.Empty(t, s.deliverablePrefixes)
		s.UnDeliverablePrefixes()
		assert.Empty(t, s.DeliverablePrefixes())
	})

	t.Run("registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New()
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

		s := New()
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

		s := New()
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

		s := New()
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

			s := New()
			for _, prefix := range test.deliverablePrefixes {
				s.deliverablePrefixes[prefix] = new(uint64)
				(*s.deliverablePrefixes[prefix])++
			}

			got := s.Stage(test.id, test.jobName)

			assert.Equal(t, test.expStaged, got)
			assert.Equal(t, test.expStaged, len(s.staged) == 1)
		})
	}
}
