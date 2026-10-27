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
	// phaseIdle is the phase of a job which is not counted. Idle jobs are not
	// stored.
	phaseIdle phase = iota
	phaseInFlight
	phaseStaged
)

// job is the phase of one counted job version.
type job struct {
	key   key
	phase phase

	// deleted is set when the job version is deleted or closed while its
	// trigger is in flight. The entry is dropped once the trigger is answered
	// or reported idle.
	deleted bool
}

// State holds the in-flight and staged job counts of a queue. It is written
// only by the queue's control loop, which sees every job event in order before
// they are fanned out to the workers, so each job version moves from one phase
// to the next in a single step.
type State struct {
	keyFn api.QueueStateKeyFunc

	// jobs holds the in-flight and staged job versions. It is only accessed by
	// the control loop, so is not guarded by lock.
	jobs map[int64]job

	lock     sync.Mutex
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

// Informed records a job version being put or deleted. Mirroring the counter,
// which unstages the job on any inform, a staged job is no longer counted. A
// job deleted while its trigger is in flight is dropped once the trigger is
// answered.
func (s *State) Informed(modRevision int64, isPut bool) {
	j, ok := s.jobs[modRevision]
	if !ok {
		return
	}

	switch {
	case j.phase == phaseStaged:
		s.drop(modRevision, j)
	case !isPut:
		j.deleted = true
		s.jobs[modRevision] = j
	}
}

// ExecuteRequest records a job version being triggered, counted under the key
// of its name. If the worker drops the request, including because the job was
// deleted meanwhile, it reports the job Idle.
func (s *State) ExecuteRequest(modRevision int64, jobName string) {
	j, ok := s.jobs[modRevision]
	if !ok {
		k, ok := s.key(jobName)
		if !ok {
			return
		}
		j = job{key: k}
	}

	if j.deleted || j.phase == phaseInFlight {
		return
	}

	s.move(&j, phaseInFlight)
	s.jobs[modRevision] = j
}

// ExecuteResponse records a job version's trigger being answered. An
// undeliverable job is staged, and a job deleted while in flight is dropped.
// If the worker re-enqueues an undeliverable job rather than staging it, it
// reports the job Idle.
func (s *State) ExecuteResponse(modRevision int64, result api.TriggerResponseResult) {
	j, ok := s.jobs[modRevision]
	if !ok || j.phase != phaseInFlight {
		return
	}

	if !j.deleted && result == api.TriggerResponseResult_UNDELIVERABLE {
		s.move(&j, phaseStaged)
		s.jobs[modRevision] = j
		return
	}

	s.drop(modRevision, j)
}

// CloseJob records a job version having no more triggers. A job whose trigger
// is in flight is dropped once the trigger is answered, or reported Idle.
func (s *State) CloseJob(modRevision int64) {
	j, ok := s.jobs[modRevision]
	if !ok {
		return
	}

	if j.phase == phaseInFlight {
		j.deleted = true
		s.jobs[modRevision] = j
		return
	}

	s.drop(modRevision, j)
}

// Idle records the worker reporting that a job version counted as in flight
// or staged is neither: it dropped the execute request or response, or
// re-enqueued an undeliverable job rather than staging it.
func (s *State) Idle(modRevision int64) {
	if j, ok := s.jobs[modRevision]; ok {
		s.drop(modRevision, j)
	}
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

// key returns the key the job is counted under. It is called outside of lock,
// so a slow key function does not block Get, and a panicking one leaves the
// job uncounted rather than crashing the control loop.
func (s *State) key(jobName string) (k key, ok bool) {
	if s.keyFn == nil {
		return key{}, false
	}

	defer func() {
		if recover() != nil {
			ok = false
		}
	}()

	k.namespace, k.appID, ok = s.keyFn(jobName)
	return k, ok
}

// move moves the job version to the given phase, updating the counts.
func (s *State) move(j *job, p phase) {
	s.lock.Lock()
	defer s.lock.Unlock()

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

// drop stops counting the job version.
func (s *State) drop(modRevision int64, j job) {
	s.move(&j, phaseIdle)
	delete(s.jobs, modRevision)
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
