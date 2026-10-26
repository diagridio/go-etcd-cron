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

	// stagedIDs is the key each staged job is counted under, indexed by the
	// job's ID, so that staging is idempotent.
	stagedIDs map[int64]Key
}

func New(keyFn api.QueueStateKeyFunc) *State {
	return &State{
		keyFn:    keyFn,
		inFlight:  make(map[Key]uint64),
		staged:    make(map[Key]uint64),
		stagedIDs: make(map[int64]Key),
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

// Stage records that the job with the given ID has entered the staging queue,
// counted under key. Staging a job which is already staged does nothing.
func (s *State) Stage(id int64, key *Key) {
	if key == nil {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.stagedIDs[id]; ok {
		return
	}

	s.stagedIDs[id] = *key
	s.staged[*key]++
}

// Unstage records that the job with the given ID has left the staging queue.
// Unstaging a job which is not staged does nothing.
func (s *State) Unstage(id int64) {
	if s == nil {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	key, ok := s.stagedIDs[id]
	if !ok {
		return
	}

	delete(s.stagedIDs, id)
	decrement(s.staged, key)
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
	decrement(counts, key)
}

func decrement(counts map[Key]uint64, key Key) {
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
