/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"sync"
)

// MutexStore allows reuse of the same dist mutex in Etcd for a given key.
type MutexStore struct {
	lock         sync.RWMutex
	cache        map[string]DistributedMutex
	mutexBuilder EtcdMutexBuilder
	collector    *Collector
}

func NewMutexStore(mutexBuilder EtcdMutexBuilder, collector *Collector) *MutexStore {
	return &MutexStore{
		cache:        map[string]DistributedMutex{},
		mutexBuilder: mutexBuilder,
		collector:    collector,
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
	m.collector.Add(func(ctx context.Context) {
		m.Delete(key)
	})
	m.cache[key] = mutex
	return mutex, nil
}

func (m *MutexStore) Delete(keys ...string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, key := range keys {
		_, ok := m.cache[key]
		if ok {
			delete(m.cache, key)
		}
	}
}
