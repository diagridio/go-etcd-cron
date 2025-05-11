/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"strings"
	"sync"
)

// Staging handles the staging queue for the Job scheduler. It tracks the
// currently deliverable job name prefixes and the counters that have been
// staged for later triggering. The consumer can signal that a job name
// prefix is deliverable, and the staging queue will enqueue all counters that
// match that prefix.
type Staging struct {
	// consumer has signalled that the job is currently undeliverable. When the
	// consumer signals a prefix has become deliverable, counters in that prefix
	// will be enqueued. Indexed by the Job Name.
	staged map[string]struct{}

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0. Indexed by the prefix strings.
	deliverablePrefixes map[string]*uint64

	lock sync.Mutex
}

func New() *Staging {
	return &Staging{
		deliverablePrefixes: make(map[string]*uint64),
		staged:              make(map[string]struct{}),
	}
}

// DeliverablePrefixes adds the job name prefixes that can currently be
// delivered by the consumer. When the returned `CancelFunc` is called, the
// prefixes registered are released indicating that these prefixes can no
// longer be delivered. Multiple of the same prefix can be added and are
// tracked as a pool, meaning the prefix is still active if at least one
// instance is still registered.
func (s *Staging) DeliverablePrefixes(prefixes ...string) []string {
	s.lock.Lock()
	defer s.lock.Unlock()

	var toEnqueue []string
	for _, prefix := range prefixes {
		if _, ok := s.deliverablePrefixes[prefix]; !ok {
			s.deliverablePrefixes[prefix] = new(uint64)

			for jobName := range s.staged {
				if strings.HasPrefix(jobName, prefix) {
					delete(s.staged, jobName)
					toEnqueue = append(toEnqueue, jobName)
				}
			}
		}

		(*s.deliverablePrefixes[prefix])++
	}

	return toEnqueue
}

// UnDeliverablePrefixes removes the job name prefixes that can no longer be
// delivered by the job handler.
func (s *Staging) UnDeliverablePrefixes(prefixes ...string) {
	s.lock.Lock()
	defer s.lock.Unlock()

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
func (s *Staging) Stage(jobName string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the job is actually now deliverable.
	for prefix := range s.deliverablePrefixes {
		if strings.HasPrefix(jobName, prefix) {
			return false
		}
	}

	s.staged[jobName] = struct{}{}

	return true
}

// Unstage removes the job from the staging queue as it can now be delivered.
func (s *Staging) Unstage(jobName string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.staged, jobName)
}
