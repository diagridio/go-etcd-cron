/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"strings"
	"sync"

	"github.com/diagridio/go-etcd-cron/internal/engine/queue/queuestate"
)

// Staging handles the staging queue for the Job scheduler. It tracks the
// currently deliverable job name prefixes and the counters that have been
// staged for later triggering. The consumer can signal that a job name
// prefix is deliverable, and the staging queue will enqueue all counters that
// match that prefix.
type Staging struct {
	// consumer has signalled that the job is currently undeliverable. When the
	// consumer signals a prefix has become deliverable, counters in that prefix
	// will be enqueued. Indexed by the Job mod revision.
	staged map[int64]stagedJob

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0. Indexed by the prefix strings.
	deliverablePrefixes map[string]*uint64

	// prefixLengths tracks the count of distinct registered prefixes for each
	// prefix length. Stage() iterates this set rather than the full prefix map
	// and probes deliverablePrefixes with jobName[:length] for each length.
	prefixLengths map[int]int

	// state counts the staged jobs.
	state *queuestate.State

	lock sync.Mutex
}

type stagedJob struct {
	name string
	key  *queuestate.Key
}

func New(state *queuestate.State) *Staging {
	return &Staging{
		deliverablePrefixes: make(map[string]*uint64),
		staged:              make(map[int64]stagedJob),
		prefixLengths:       make(map[int]int),
		state:               state,
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
			s.prefixLengths[len(prefix)]++

			for modRevision, job := range s.staged {
				if strings.HasPrefix(job.name, prefix) {
					delete(s.staged, modRevision)
					s.state.RemoveStaged(job.key)
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
				s.prefixLengths[len(prefix)]--
				if s.prefixLengths[len(prefix)] == 0 {
					delete(s.prefixLengths, len(prefix))
				}
			}
		}
	}
}

// hasMatchingPrefix reports whether any registered deliverable prefix is a
// prefix of jobName. It probes deliverablePrefixes with jobName[:length] for
// every distinct registered prefix length, giving O(L) lookups where L is
// the number of distinct prefix lengths currently registered.
func (s *Staging) hasMatchingPrefix(jobName string) bool {
	for length := range s.prefixLengths {
		if length > len(jobName) {
			continue
		}
		if _, ok := s.deliverablePrefixes[jobName[:length]]; ok {
			return true
		}
	}

	return false
}

// Stage adds the counter (job) to the staging queue. Accounting for control
// loop ordering returns false if the counter can actually be delivered now
// based on the current deliverable prefixes and should be immediately
// re-queued at the current count. key is what the job is counted under in the
// queue state, nil if it is not counted.
func (s *Staging) Stage(modRevision int64, jobName string, key *queuestate.Key) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the job is actually now deliverable.
	if s.hasMatchingPrefix(jobName) {
		return false
	}

	if _, ok := s.staged[modRevision]; !ok {
		s.state.AddStaged(key)
	}
	s.staged[modRevision] = stagedJob{name: jobName, key: key}

	return true
}

// Unstage removes the job from the staging queue as it can now be delivered.
func (s *Staging) Unstage(modRevision int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if job, ok := s.staged[modRevision]; ok {
		delete(s.staged, modRevision)
		s.state.RemoveStaged(job.key)
	}
}
