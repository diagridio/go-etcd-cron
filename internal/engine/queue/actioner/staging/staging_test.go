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
		s.staged = map[string]struct{}{
			"abc123": struct{}{}, "abc234": struct{}{},
			"def123": struct{}{}, "def234": struct{}{},
			"xyz123": struct{}{}, "xyz234": struct{}{},
		}

		assert.ElementsMatch(t,
			[]string{"abc123", "abc234", "xyz123", "xyz234"},
			s.DeliverablePrefixes("abc", "xyz"),
		)
		assert.Equal(t,
			map[string]struct{}{"def123": struct{}{}, "def234": struct{}{}},
			s.staged,
		)

		assert.ElementsMatch(t,
			[]string{"def123", "def234"},
			s.DeliverablePrefixes("d"),
		)
		assert.Empty(t, s.staged)
	})
}

func Test_Stage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		jobName             string
		deliverablePrefixes []string
		expStaged           bool
	}{
		"no deliverable prefixes, should stage": {
			jobName:             "abc123",
			deliverablePrefixes: []string{},
			expStaged:           true,
		},
		"deliverable prefixes but different, should stage": {
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba"},
			expStaged:           true,
		},
		"deliverable prefixes and matches, should not stage": {
			jobName:             "abc123",
			deliverablePrefixes: []string{"abc123"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and matches, should not stage": {
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "abc123", "cba"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and matches on prefix, should not stage": {
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba", "abc"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and not matches on prefix, should stage": {
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

			got := s.Stage(test.jobName)

			assert.Equal(t, test.expStaged, got)
			assert.Equal(t, test.expStaged, len(s.staged) == 1)
		})
	}
}
