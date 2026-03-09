/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"sort"
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
	staged map[int64]string

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0. Indexed by the prefix strings.
	deliverablePrefixes map[string]*uint64

	// sortedPrefixes maintains a sorted slice of the currently deliverable
	// prefixes for efficient binary search in Stage().
	sortedPrefixes []string

	lock sync.Mutex
}

func New() *Staging {
	return &Staging{
		deliverablePrefixes: make(map[string]*uint64),
		staged:              make(map[int64]string),
	}
}

// DeliverablePrefixes adds the job name prefixes that can currently be
// delivered by the consumer. When the returned `CancelCauseFunc` is called,
// the prefixes registered are released indicating that these prefixes can no
// longer be delivered. Multiple of the same prefix can be added and are
// tracked as a pool, meaning the prefix is still active if at least one
// instance is still registered.
func (s *Staging) DeliverablePrefixes(prefixes ...string) []int64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	var toEnqueue []int64
	for _, prefix := range prefixes {
		if _, ok := s.deliverablePrefixes[prefix]; !ok {
			s.deliverablePrefixes[prefix] = new(uint64)
			s.sortedPrefixes = append(s.sortedPrefixes, prefix)
			sort.Strings(s.sortedPrefixes)

			for modRevision, jobName := range s.staged {
				if strings.HasPrefix(jobName, prefix) {
					delete(s.staged, modRevision)
					toEnqueue = append(toEnqueue, modRevision)
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
				idx := sort.SearchStrings(s.sortedPrefixes, prefix)
				if idx < len(s.sortedPrefixes) && s.sortedPrefixes[idx] == prefix {
					s.sortedPrefixes = append(s.sortedPrefixes[:idx], s.sortedPrefixes[idx+1:]...)
				}
			}
		}
	}
}

// hasMatchingPrefix checks if any registered deliverable prefix matches the
// given job name using binary search on the sorted prefix list.
func (s *Staging) hasMatchingPrefix(jobName string) bool {
	if len(s.sortedPrefixes) == 0 {
		return false
	}

	// Find the position where jobName would be inserted.
	idx := sort.SearchStrings(s.sortedPrefixes, jobName)

	// Check the entry at idx (if equal or a prefix starting after).
	if idx < len(s.sortedPrefixes) && strings.HasPrefix(jobName, s.sortedPrefixes[idx]) {
		return true
	}

	// Check the entry just before idx (the largest prefix <= jobName).
	if idx > 0 && strings.HasPrefix(jobName, s.sortedPrefixes[idx-1]) {
		return true
	}

	return false
}

// Stage adds the counter (job) to the staging queue. Accounting for control
// loop ordering returns false if the counter can actually be delivered now
// based on the current deliverable prefixes and should be immediately
// re-queued at the current count.
func (s *Staging) Stage(modRevision int64, jobName string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the job is actually now deliverable.
	if s.hasMatchingPrefix(jobName) {
		return false
	}

	s.staged[modRevision] = jobName

	return true
}

// Unstage removes the job from the staging queue as it can now be delivered.
func (s *Staging) Unstage(modRevision int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.staged, modRevision)
}
