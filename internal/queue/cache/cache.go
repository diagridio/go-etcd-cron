/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package cache

import (
	"github.com/diagridio/go-etcd-cron/internal/counter"
)

// Cache is an inmemory cache storage of the counters of Jobs which are
// currently active in the Jobs queue.
// Used to fetch counters, and ensure there is no race condition whereby a
// Delete operation on a job which was mid execution on the same scheduler
// instance, would not see that job as not deleted from the in-memory queue.
// Used to back out of an execution if it is no longer in that cache (deleted).
type Cache struct {
	store map[string]counter.Interface
}

func New() *Cache {
	return &Cache{
		store: make(map[string]counter.Interface),
	}
}

func (c *Cache) Load(key string) (counter.Interface, bool) {
	counter, ok := c.store[key]
	return counter, ok
}

func (c *Cache) LoadAndDelete(key string) (counter.Interface, bool) {
	v, ok := c.store[key]
	delete(c.store, key)
	return v, ok
}

func (c *Cache) Delete(key string) {
	delete(c.store, key)
}

func (c *Cache) Store(key string, counter counter.Interface) {
	c.store[key] = counter
}
