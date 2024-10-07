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
func (q *Queue) DeliverablePrefixes(prefixes ...string) context.CancelFunc {
	q.stagedLock.Lock()
	defer q.stagedLock.Unlock()

	var toEnqueue []counter.Interface
	for _, prefix := range prefixes {
		if _, ok := q.deliverablePrefixes[prefix]; !ok {
			q.deliverablePrefixes[prefix] = new(atomic.Int32)

			for i := 0; i < len(q.staged); i++ {
				if strings.HasPrefix(q.staged[i].JobName(), prefix) {
					toEnqueue = append(toEnqueue, q.staged[i])
					q.staged = append(q.staged[:i], q.staged[i+1:]...)
					i--
				}
			}
		}

		q.deliverablePrefixes[prefix].Add(1)
	}

	for _, counter := range toEnqueue {
		q.queue.Enqueue(counter)
	}

	return func() {
		q.stagedLock.Lock()
		defer q.stagedLock.Unlock()

		for _, prefix := range prefixes {
			if i, ok := q.deliverablePrefixes[prefix]; ok {
				if i.Add(-1) <= 0 {
					delete(q.deliverablePrefixes, prefix)
				}
			}
		}
	}
}

// stage adds the counter (job) to the staging queue. Accounting for race
// conditions, returns false if the counter can actually be delivered now based
// on the current deliverable prefixes and should be immediately re-queued at
// the current count.
func (q *Queue) stage(counter counter.Interface) bool {
	q.stagedLock.Lock()
	defer q.stagedLock.Unlock()

	jobName := counter.JobName()

	// Check if the job is actually now deliverable.
	for prefix := range q.deliverablePrefixes {
		if strings.HasPrefix(jobName, prefix) {
			return false
		}
	}

	q.staged = append(q.staged, counter)

	return true
}
