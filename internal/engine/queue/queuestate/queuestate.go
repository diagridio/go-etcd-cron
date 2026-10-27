/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package queuestate

import (
	"sync"

	"github.com/diagridio/go-etcd-cron/api"
)

// key is the namespace and app ID a job is counted under.
type key struct {
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
	key   key
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
	jobs     map[int64]job
	inFlight map[key]uint64
	staged   map[key]uint64
}

func New(keyFn api.QueueStateKeyFunc) *State {
	return &State{
		keyFn:    keyFn,
		jobs:     make(map[int64]job),
		inFlight: make(map[key]uint64),
		staged:   make(map[key]uint64),
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

		s.jobs[modRevision] = job{key: key{namespace: namespace, appID: appID}}
		return
	}

	if j.phase == phaseStaged {
		s.setPhase(modRevision, &j, phaseIdle)
	}

	if !isPut {
		if j.phase == phaseInFlight {
			j.deleted = true
			s.jobs[modRevision] = j
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
		s.setPhase(modRevision, &j, phaseInFlight)
	}
}

// ExecuteResponse records a job version's trigger being answered. An
// undeliverable job is staged, and a job deleted while in flight is dropped.
// The worker re-triggers an undeliverable job at once, rather than staging it,
// if a matching prefix is already deliverable. That job is counted as staged
// until its re-trigger request is recorded, a brief over-count.
func (s *State) ExecuteResponse(modRevision int64, result api.TriggerResponseResult) {
	s.lock.Lock()
	defer s.lock.Unlock()

	j, ok := s.jobs[modRevision]
	if !ok || j.phase != phaseInFlight {
		return
	}

	switch {
	case j.deleted:
		s.setPhase(modRevision, &j, phaseIdle)
		delete(s.jobs, modRevision)
	case result == api.TriggerResponseResult_UNDELIVERABLE:
		s.setPhase(modRevision, &j, phaseStaged)
	default:
		s.setPhase(modRevision, &j, phaseIdle)
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

	s.setPhase(modRevision, &j, phaseIdle)
	delete(s.jobs, modRevision)
}

// Get returns a copy of the current counts.
func (s *State) Get() *api.QueueState {
	state := api.NewQueueState()

	s.lock.Lock()
	defer s.lock.Unlock()

	copyCounts(state.InFlight, s.inFlight)
	copyCounts(state.Staged, s.staged)

	return state
}

// setPhase moves the job version to the given phase, updating the counts, and
// stores it.
func (s *State) setPhase(modRevision int64, j *job, p phase) {
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
	s.jobs[modRevision] = *j
}

func decrement(counts map[key]uint64, k key) {
	if counts[k] <= 1 {
		delete(counts, k)
		return
	}
	counts[k]--
}

func copyCounts(dst map[string]map[string]uint64, src map[key]uint64) {
	for k, count := range src {
		apps, ok := dst[k.namespace]
		if !ok {
			apps = make(map[string]uint64)
			dst[k.namespace] = apps
		}
		apps[k.appID] = count
	}
}
