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

type phase uint8

const (
	phaseIdle phase = iota
	phaseInFlight
	phaseStaged
)

// job is the phase of one counted job version.
type job struct {
	key   Key
	phase phase

	// deleted is set when the job version is deleted while its trigger is in
	// flight. The entry is dropped once the trigger is answered.
	deleted bool
}

// State holds the in-flight and staged job counts of a queue. It is written
// only by the queue's control loop, which sees every job event in order before
// they are fanned out to the workers, so each job version moves from one phase
// to the next in a single step.
type State struct {
	keyFn api.QueueStateKeyFunc

	lock     sync.Mutex
	jobs     map[int64]*job
	inFlight map[Key]uint64
	staged   map[Key]uint64
}

func New(keyFn api.QueueStateKeyFunc) *State {
	return &State{
		keyFn:    keyFn,
		jobs:     make(map[int64]*job),
		inFlight: make(map[Key]uint64),
		staged:   make(map[Key]uint64),
	}
}

// Informed records a job version being put or deleted. A put which is counted
// by the key function starts the version idle. Mirroring the counter, which
// unstages the job on any inform, a staged job is no longer counted as staged.
// A deleted job is dropped, unless its trigger is in flight, in which case it
// is dropped once the trigger is answered.
func (s *State) Informed(modRevision int64, jobName string, isPut bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	j, ok := s.jobs[modRevision]
	if !ok {
		if !isPut || s.keyFn == nil {
			return
		}

		namespace, appID, ok := s.keyFn(jobName)
		if !ok {
			return
		}

		s.jobs[modRevision] = &job{key: Key{namespace: namespace, appID: appID}}
		return
	}

	if j.phase == phaseStaged {
		s.setPhase(j, phaseIdle)
	}

	if !isPut {
		if j.phase == phaseInFlight {
			j.deleted = true
		} else {
			delete(s.jobs, modRevision)
		}
	}
}

// ExecuteRequest records a job version being triggered. Requests for deleted
// job versions are dropped by the worker, so are not counted.
func (s *State) ExecuteRequest(modRevision int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if j, ok := s.jobs[modRevision]; ok && !j.deleted {
		s.setPhase(j, phaseInFlight)
	}
}

// ExecuteResponse records a job version's trigger being answered. An
// undeliverable job is staged, and a job deleted while in flight is dropped.
func (s *State) ExecuteResponse(modRevision int64, result api.TriggerResponseResult) {
	s.lock.Lock()
	defer s.lock.Unlock()

	j, ok := s.jobs[modRevision]
	if !ok || j.phase != phaseInFlight {
		return
	}

	switch {
	case j.deleted:
		s.setPhase(j, phaseIdle)
		delete(s.jobs, modRevision)
	case result == api.TriggerResponseResult_UNDELIVERABLE:
		s.setPhase(j, phaseStaged)
	default:
		s.setPhase(j, phaseIdle)
	}
}

// CloseJob records a job version having no more triggers. A job whose trigger
// is in flight was deleted, and is dropped once the trigger is answered.
func (s *State) CloseJob(modRevision int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	j, ok := s.jobs[modRevision]
	if !ok || j.phase == phaseInFlight {
		return
	}

	s.setPhase(j, phaseIdle)
	delete(s.jobs, modRevision)
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

func (s *State) setPhase(j *job, p phase) {
	switch j.phase {
	case phaseInFlight:
		decrement(s.inFlight, j.key)
	case phaseStaged:
		decrement(s.staged, j.key)
	}

	switch p {
	case phaseInFlight:
		s.inFlight[j.key]++
	case phaseStaged:
		s.staged[j.key]++
	}

	j.phase = p
}

func decrement(counts map[Key]uint64, key Key) {
	if counts[key] <= 1 {
		delete(counts, key)
		return
	}
	counts[key]--
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
