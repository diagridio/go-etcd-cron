/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package queuestate

import (
	"sync"

	"github.com/diagridio/go-etcd-cron/api"
)

// Key is the namespace and app ID a job is counted under.
type Key struct {
	namespace string
	appID     string
}

// State holds the in-flight and staged job counts of a queue. Only the queue's
// state machine loops write to it, so the counts track exactly what the queue
// holds. A nil *State counts nothing.
type State struct {
	keyFn api.QueueStateKeyFunc

	lock     sync.Mutex
	inFlight map[Key]uint64
	staged   map[Key]uint64
}

func New(keyFn api.QueueStateKeyFunc) *State {
	return &State{
		keyFn:    keyFn,
		inFlight: make(map[Key]uint64),
		staged:   make(map[Key]uint64),
	}
}

// Key returns the key the given job is counted under, or nil if the job is
// not counted.
func (s *State) Key(jobName string) *Key {
	if s == nil || s.keyFn == nil {
		return nil
	}

	namespace, appID, ok := s.keyFn(jobName)
	if !ok {
		return nil
	}

	return &Key{namespace: namespace, appID: appID}
}

// AddInFlight records that a job has been triggered. A nil key, which is all
// a nil *State hands out, is not counted.
func (s *State) AddInFlight(key *Key) {
	if key != nil {
		s.add(s.inFlight, *key)
	}
}

// RemoveInFlight records that a job's trigger has been answered.
func (s *State) RemoveInFlight(key *Key) {
	if key != nil {
		s.remove(s.inFlight, *key)
	}
}

// AddStaged records that a job has entered the staging queue.
func (s *State) AddStaged(key *Key) {
	if key != nil {
		s.add(s.staged, *key)
	}
}

// RemoveStaged records that a job has left the staging queue.
func (s *State) RemoveStaged(key *Key) {
	if key != nil {
		s.remove(s.staged, *key)
	}
}

// Get returns a copy of the current counts.
func (s *State) Get() *api.QueueState {
	state := &api.QueueState{
		InFlight: make(map[string]map[string]uint64),
		Staged:   make(map[string]map[string]uint64),
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	copyCounts(state.InFlight, s.inFlight)
	copyCounts(state.Staged, s.staged)

	return state
}

func (s *State) add(counts map[Key]uint64, key Key) {
	s.lock.Lock()
	defer s.lock.Unlock()
	counts[key]++
}

func (s *State) remove(counts map[Key]uint64, key Key) {
	s.lock.Lock()
	defer s.lock.Unlock()

	switch counts[key] {
	case 0:
	case 1:
		delete(counts, key)
	default:
		counts[key]--
	}
}

func copyCounts(dst map[string]map[string]uint64, src map[Key]uint64) {
	for key, count := range src {
		apps, ok := dst[key.namespace]
		if !ok {
			apps = make(map[string]uint64)
			dst[key.namespace] = apps
		}
		apps[key.appID] = count
	}
}
