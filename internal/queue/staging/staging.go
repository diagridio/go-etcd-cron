/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"strings"

	"github.com/dapr/kit/events/queue"
	"github.com/diagridio/go-etcd-cron/internal/counter"
)

type Options struct {
	// Queue is the actual queue that schedules jobs.
	Queue *queue.Processor[string, counter.Interface]
}

// Staging handles the staging queue for the Job scheduler. It tracks the
// currently deliverable job name prefixes and the counters that have been
// staged for later triggering. The consumer can signal that a job name
// prefix is deliverable, and the staging queue will enqueue all counters that
// match that prefix.
type Staging struct {
	// queue is the real Job scheduling queue.
	queue *queue.Processor[string, counter.Interface]

	// staged are the counters that have been staged for later triggering as the
	// consumer has signalled that the job is current undeliverable. When the
	// consumer signals a prefix has become deliverable, counters in that prefix
	// will be enqueued. Indexed by the counters Job Name.
	staged map[string]counter.Interface

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0. Indexed by the prefix strings.
	deliverablePrefixes map[string]*uint64
}

func New(opts Options) *Staging {
	return &Staging{
		queue:               opts.Queue,
		staged:              make(map[string]counter.Interface),
		deliverablePrefixes: make(map[string]*uint64),
	}
}

// DeliverablePrefixes adds the job name prefixes that can currently be
// delivered by the consumer. When the returned `CancelFunc` is called, the
// prefixes registered are released indicating that these prefixes can no
// longer be delivered. Multiple of the same prefix can be added and are
// tracked as a pool, meaning the prefix is still active if at least one
// instance is still registered.
func (s *Staging) DeliverablePrefixes(prefixes ...string) {
	var toEnqueue []counter.Interface
	for _, prefix := range prefixes {
		if _, ok := s.deliverablePrefixes[prefix]; !ok {
			s.deliverablePrefixes[prefix] = new(uint64)

			for jobName, stage := range s.staged {
				if strings.HasPrefix(jobName, prefix) {
					toEnqueue = append(toEnqueue, stage)
					delete(s.staged, jobName)
				}
			}
		}

		(*s.deliverablePrefixes[prefix])++
	}

	s.queue.Enqueue(toEnqueue...)
}

// UnDeliverablePrefixes removes the job name prefixes that can no longer be
// delivered by the job handler.
func (s *Staging) UnDeliverablePrefixes(prefixes ...string) {
	for _, prefix := range prefixes {
		if i, ok := s.deliverablePrefixes[prefix]; ok {
			// If the number of active consumers drops to 0, remove the prefix from
			// the deliverable prefixes set.
			(*i)--
			if *i <= 0 {
				delete(s.deliverablePrefixes, prefix)
			}
		}
	}
}

// Stage adds the counter (job) to the staging queue. Accounting for control
// loop ordering returns false if the counter can actually be delivered now
// based on the current deliverable prefixes and should be immediately
// re-queued at the current count.
func (s *Staging) Stage(counter counter.Interface) bool {
	jobName := counter.JobName()

	// Check if the job is actually now deliverable.
	for prefix := range s.deliverablePrefixes {
		if strings.HasPrefix(jobName, prefix) {
			return false
		}
	}

	s.staged[jobName] = counter

	return true
}

// Unstage removes the counter (job) from the staging queue.
func (s *Staging) Unstage(jobName string) {
	delete(s.staged, jobName)
}
