/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"sync"
)

// TODO: Add clean up routine to delete old multexes
type MutexStore struct {
	lock         sync.RWMutex
	cache        map[string]DistributedMutex
	mutexBuilder EtcdMutexBuilder
}

func NewMutexStore(mutexBuilder EtcdMutexBuilder) *MutexStore {
	return &MutexStore{
		cache:        map[string]DistributedMutex{},
		mutexBuilder: mutexBuilder,
	}
}

func (m *MutexStore) Get(key string) (DistributedMutex, error) {
	m.lock.RLock()
	mutex := m.cache[key]
	m.lock.RUnlock()
	if mutex != nil {
		return mutex, nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	mutex = m.cache[key]
	if mutex != nil {
		return mutex, nil
	}

	mutex, err := m.mutexBuilder.NewMutex(key)
	if err != nil {
		return nil, err
	}
	m.cache[key] = mutex
	return mutex, nil
}
