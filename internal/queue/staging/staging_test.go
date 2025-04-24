/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"testing"
	"time"

	"github.com/dapr/kit/concurrency/lock"
	"github.com/dapr/kit/events/queue"
	"github.com/stretchr/testify/assert"

	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/counter/fake"
)

func Test_DeliverablePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("registering empty prefixes should add nothing", func(t *testing.T) {
		t.Parallel()

		s := &Staging{
			deliverablePrefixes: make(map[string]*uint64),
			queue:               new(queue.Processor[string, counter.Interface]),
		}
		assert.Empty(t, s.deliverablePrefixes)

		s.DeliverablePrefixes()
		assert.Empty(t, s.deliverablePrefixes)
		s.UnDeliverablePrefixes()
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := &Staging{
			deliverablePrefixes: make(map[string]*uint64),
			queue:               new(queue.Processor[string, counter.Interface]),
		}
		assert.Empty(t, s.deliverablePrefixes)

		s.DeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("foo")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("multiple: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := &Staging{
			deliverablePrefixes: make(map[string]*uint64),
			queue:               new(queue.Processor[string, counter.Interface]),
		}
		assert.Empty(t, s.deliverablePrefixes)

		s.DeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.DeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)

		s.UnDeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("multiple with diff prefixes: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := &Staging{
			deliverablePrefixes: make(map[string]*uint64),
			queue:               new(queue.Processor[string, counter.Interface]),
		}
		assert.Empty(t, s.deliverablePrefixes)

		s.DeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.DeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.DeliverablePrefixes("def")
		assert.Len(t, s.deliverablePrefixes, 2)
		s.DeliverablePrefixes("def")
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

		lock := lock.NewContext()
		var triggered []string
		s := &Staging{
			deliverablePrefixes: make(map[string]*uint64),
			staged:              make(map[string]counter.Interface),
			queue: queue.NewProcessor[string, counter.Interface](queue.Options[string, counter.Interface]{
				ExecuteFn: func(counter counter.Interface) {
					//nolint:errcheck
					lock.Lock(t.Context())
					defer lock.Unlock()
					triggered = append(triggered, counter.JobName())
				},
			}),
		}

		counter1 := fake.New().WithJobName("abc123").WithKey("abc123")
		counter2 := fake.New().WithJobName("abc234").WithKey("abc234")
		counter3 := fake.New().WithJobName("def123").WithKey("def123")
		counter4 := fake.New().WithJobName("def234").WithKey("def234")
		counter5 := fake.New().WithJobName("xyz123").WithKey("xyz123")
		counter6 := fake.New().WithJobName("xyz234").WithKey("xyz234")
		s.staged = map[string]counter.Interface{
			"abc123": counter1, "abc234": counter2,
			"def123": counter3, "def234": counter4,
			"xyz123": counter5, "xyz234": counter6,
		}

		s.DeliverablePrefixes("abc", "xyz")
		assert.Equal(t, map[string]counter.Interface{"def123": counter3, "def234": counter4}, s.staged)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			//nolint:errcheck
			lock.Lock(t.Context())
			defer lock.Unlock()
			assert.ElementsMatch(c, []string{"abc123", "abc234", "xyz123", "xyz234"}, triggered)
		}, time.Second*10, time.Millisecond*10)

		s.DeliverablePrefixes("d")
		assert.Empty(t, s.staged)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			//nolint:errcheck
			lock.Lock(t.Context())
			defer lock.Unlock()
			assert.ElementsMatch(c, []string{"abc123", "abc234", "xyz123", "xyz234", "def123", "def234"}, triggered)
		}, time.Second*10, time.Millisecond*10)
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

			s := &Staging{
				deliverablePrefixes: make(map[string]*uint64),
				staged:              make(map[string]counter.Interface),
			}

			for _, prefix := range test.deliverablePrefixes {
				s.deliverablePrefixes[prefix] = new(uint64)
				(*s.deliverablePrefixes[prefix])++
			}

			got := s.Stage(fake.New().WithJobName(test.jobName))

			assert.Equal(t, test.expStaged, got)
			assert.Equal(t, test.expStaged, len(s.staged) == 1)
		})
	}
}
