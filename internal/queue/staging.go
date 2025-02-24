/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/diagridio/go-etcd-cron/internal/counter"
)

// DeliverablePrefixes adds the job name prefixes that can currently be
// delivered by the consumer. When the returned `CancelFunc` is called, the
// prefixes registered are released indicating that these prefixes can no
// longer be delivered. Multiple of the same prefix can be added and are
// tracked as a pool, meaning the prefix is still active if at least one
// instance is still registered.
func (q *Queue) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	// Attempt lock until context cancel.
	if err := q.eventsLock.Lock(ctx); err != nil {
		return nil, err
	}
	defer q.eventsLock.Unlock()

	var toEnqueue []counter.Interface
	for _, prefix := range prefixes {
		if _, ok := q.deliverablePrefixes[prefix]; !ok {
			q.deliverablePrefixes[prefix] = new(atomic.Int32)

			for jobName, stage := range q.staged {
				if strings.HasPrefix(jobName, prefix) {
					toEnqueue = append(toEnqueue, stage)
					delete(q.staged, jobName)
				}
			}
		}

		q.deliverablePrefixes[prefix].Add(1)
	}

	for _, counter := range toEnqueue {
		q.queue.Enqueue(counter)
	}

	return func() {
		q.eventsLock.Lock(context.Background())
		defer q.eventsLock.Unlock()

		for _, prefix := range prefixes {
			if i, ok := q.deliverablePrefixes[prefix]; ok {
				if i.Add(-1) <= 0 {
					delete(q.deliverablePrefixes, prefix)
				}
			}
		}
	}, nil
}

// stage adds the counter (job) to the staging queue. Accounting for race
// conditions, returns false if the counter can actually be delivered now based
// on the current deliverable prefixes and should be immediately re-queued at
// the current count.
func (q *Queue) stage(counter counter.Interface) bool {
	// Must lock.
	q.eventsLock.Lock(context.Background())
	defer q.eventsLock.Unlock()

	jobName := counter.JobName()

	// Check if the job is actually now deliverable.
	for prefix := range q.deliverablePrefixes {
		if strings.HasPrefix(jobName, prefix) {
			return false
		}
	}

	q.staged[jobName] = counter

	return true
}
