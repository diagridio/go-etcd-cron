/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/concurrency/lock"
	"github.com/dapr/kit/events/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/counter/fake"
)

func Test_DeliverablePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("registering empty prefixes should add nothing", func(t *testing.T) {
		t.Parallel()

		q := &Queue{
			deliverablePrefixes: make(map[string]*atomic.Int32),
			eventsLock:          lock.NewContext(),
		}
		assert.Empty(t, q.deliverablePrefixes)

		cancel, err := q.DeliverablePrefixes(context.Background())
		require.NoError(t, err)
		assert.Empty(t, q.deliverablePrefixes)
		cancel()
		assert.Empty(t, q.deliverablePrefixes)
	})

	t.Run("registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		q := &Queue{
			deliverablePrefixes: make(map[string]*atomic.Int32),
			eventsLock:          lock.NewContext(),
		}
		assert.Empty(t, q.deliverablePrefixes)

		cancel, err := q.DeliverablePrefixes(context.Background(), "abc")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 1)
		cancel()
		assert.Empty(t, q.deliverablePrefixes)
	})

	t.Run("multiple: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		q := &Queue{
			deliverablePrefixes: make(map[string]*atomic.Int32),
			eventsLock:          lock.NewContext(),
		}
		assert.Empty(t, q.deliverablePrefixes)

		cancel1, err := q.DeliverablePrefixes(context.Background(), "abc")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 1)
		cancel2, err := q.DeliverablePrefixes(context.Background(), "abc")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 1)

		cancel1()
		assert.Len(t, q.deliverablePrefixes, 1)
		cancel2()
		assert.Empty(t, q.deliverablePrefixes)
	})

	t.Run("multiple with diff prefixes: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		q := &Queue{
			deliverablePrefixes: make(map[string]*atomic.Int32),
			eventsLock:          lock.NewContext(),
		}
		assert.Empty(t, q.deliverablePrefixes)

		cancel1, err := q.DeliverablePrefixes(context.Background(), "abc")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 1)
		cancel2, err := q.DeliverablePrefixes(context.Background(), "abc")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 1)
		cancel3, err := q.DeliverablePrefixes(context.Background(), "def")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 2)
		cancel4, err := q.DeliverablePrefixes(context.Background(), "def")
		require.NoError(t, err)
		assert.Len(t, q.deliverablePrefixes, 2)

		cancel1()
		assert.Len(t, q.deliverablePrefixes, 2)
		cancel4()
		assert.Len(t, q.deliverablePrefixes, 2)
		cancel2()
		assert.Len(t, q.deliverablePrefixes, 1)
		cancel3()
		assert.Empty(t, q.deliverablePrefixes)
	})

	t.Run("staged counters should be enqueued if they match an added prefix", func(t *testing.T) {
		t.Parallel()

		lock := lock.NewContext()
		var triggered []string
		q := &Queue{
			deliverablePrefixes: make(map[string]*atomic.Int32),
			eventsLock:          lock,
			staged:              make(map[string]counter.Interface),
			queue: queue.NewProcessor[string, counter.Interface](
				func(counter counter.Interface) {
					lock.Lock(context.Background())
					defer lock.Unlock()
					triggered = append(triggered, counter.JobName())
				},
			),
		}

		counter1 := fake.New().WithJobName("abc123").WithKey("abc123")
		counter2 := fake.New().WithJobName("abc234").WithKey("abc234")
		counter3 := fake.New().WithJobName("def123").WithKey("def123")
		counter4 := fake.New().WithJobName("def234").WithKey("def234")
		counter5 := fake.New().WithJobName("xyz123").WithKey("xyz123")
		counter6 := fake.New().WithJobName("xyz234").WithKey("xyz234")
		q.staged = map[string]counter.Interface{
			"abc123": counter1, "abc234": counter2,
			"def123": counter3, "def234": counter4,
			"xyz123": counter5, "xyz234": counter6,
		}

		cancel, err := q.DeliverablePrefixes(context.Background(), "abc", "xyz")
		require.NoError(t, err)
		t.Cleanup(cancel)
		assert.Equal(t, map[string]counter.Interface{"def123": counter3, "def234": counter4}, q.staged)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			lock.Lock(context.Background())
			defer lock.Unlock()
			assert.ElementsMatch(c, []string{"abc123", "abc234", "xyz123", "xyz234"}, triggered)
		}, time.Second*10, time.Millisecond*10)

		cancel, err = q.DeliverablePrefixes(context.Background(), "d")
		require.NoError(t, err)
		t.Cleanup(cancel)
		assert.Empty(t, q.staged)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			lock.Lock(context.Background())
			defer lock.Unlock()
			assert.ElementsMatch(c, []string{"abc123", "abc234", "xyz123", "xyz234", "def123", "def234"}, triggered)
		}, time.Second*10, time.Millisecond*10)
	})
}

func Test_stage(t *testing.T) {
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

			q := &Queue{
				deliverablePrefixes: make(map[string]*atomic.Int32),
				eventsLock:          lock.NewContext(),
				staged:              make(map[string]counter.Interface),
			}

			for _, prefix := range test.deliverablePrefixes {
				q.deliverablePrefixes[prefix] = new(atomic.Int32)
				q.deliverablePrefixes[prefix].Add(1)
			}

			got := q.stage(fake.New().WithJobName(test.jobName))

			assert.Equal(t, test.expStaged, got)
			assert.Equal(t, test.expStaged, len(q.staged) == 1)
		})
	}
}
