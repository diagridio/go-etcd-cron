/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package locking

import (
	"context"
	"sync"

	"github.com/diagridio/go-etcd-cron/collector"
)

// Mutexer locks and unlocks mutexes locally based on key, with garbage collection method.
type Mutexer struct {
	mutex sync.RWMutex

	mutexes   map[string]*sync.RWMutex
	collector collector.Collector
}

func NewMutexer(collector collector.Collector) *Mutexer {
	return &Mutexer{
		mutexes:   map[string]*sync.RWMutex{},
		collector: collector,
	}
}

func (o *Mutexer) Get(key string) *sync.RWMutex {
	// Optimistic read lock.
	o.mutex.RLock()
	m, ok := o.mutexes[key]
	o.mutex.RUnlock()
	if !ok {
		// Now we need to check again after getting lock, common pattern.
		o.mutex.Lock()
		defer o.mutex.Unlock()
		m, ok = o.mutexes[key]
		if !ok {
			m = &sync.RWMutex{}
			o.collector.Add(func(ctx context.Context) error {
				o.Delete(key)
				return nil
			})
			o.mutexes[key] = m
			return m
		}
	}

	return m
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
