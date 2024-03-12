/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"sync"
)

// Mutexer locks and unlocks mutexes locally based on key, with garbage collection method.
type Mutexer struct {
	mutex sync.RWMutex

	mutexes   map[string]*sync.RWMutex
	collector *Collector
}

func NewMutexer(collector *Collector) *Mutexer {
	return &Mutexer{
		mutexes:   map[string]*sync.RWMutex{},
		collector: collector,
	}
}

func (o *Mutexer) Lock(key string) {
	// Optimistic read lock.
	o.mutex.RLock()
	m, ok := o.mutexes[key]
	o.mutex.RUnlock()
	if !ok {
		// Now we need to check again after getting lock, common pattern.
		o.mutex.Lock()
		m, ok = o.mutexes[key]
		if !ok {
			m = &sync.RWMutex{}
			o.collector.Add(func(ctx context.Context) {
				o.Delete(key)
			})
			o.mutexes[key] = m
		}
		o.mutex.Unlock()
	}

	m.Lock()
}

func (o *Mutexer) Unlock(key string) {
	// Optimistic read lock.
	o.mutex.RLock()
	m, ok := o.mutexes[key]
	o.mutex.RUnlock()
	if !ok {
		// Nothing to do since lock does not exist.
		return
	}

	m.Unlock()
}

func (o *Mutexer) Delete(keys ...string) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	for _, key := range keys {
		_, ok := o.mutexes[key]
		if ok {
			delete(o.mutexes, key)
		}
	}
}
