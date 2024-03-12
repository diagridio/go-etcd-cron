/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"sync"
	"time"
)

// collector garbage collects items after a globally configured TTL.
type Collector struct {
	ttl        int64 // time to wait to perform collection
	bufferTime int64 // arbitrary delay to allow buffering of operations

	running         bool
	mutex           sync.RWMutex
	operations      []*collectorEntry
	changed         chan bool
	runWaitingGroup sync.WaitGroup
}

type collectorEntry struct {
	expiration int64
	op         func(ctx context.Context)
}

func NewCollector(ttl int64, bufferTime int64) *Collector {
	return &Collector{
		ttl:        ttl,
		bufferTime: bufferTime,
		running:    false,
		changed:    make(chan bool),
		operations: []*collectorEntry{},
	}
}

func (c *Collector) Start(ctx context.Context) {
	if c.running {
		return
	}

	c.running = true

	doIt := func(ctx context.Context) {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		now := time.Now().Unix()
		nextStartIndex := -1
		for i, o := range c.operations {
			if o.expiration <= now {
				o.op(ctx)
			} else {
				nextStartIndex = i
				break
			}
		}

		if nextStartIndex >= 0 {
			c.operations = c.operations[nextStartIndex:]
			return
		}
		c.operations = []*collectorEntry{}
	}

	waitTimeForNext := func() time.Duration {
		c.mutex.RLock()
		defer c.mutex.RUnlock()

		now := time.Now().Unix()
		if len(c.operations) > 0 {
			diff := c.operations[0].expiration - now
			if diff <= 0 {
				return 0
			}

			return time.Duration(diff)
		}

		// Some arbitrarily large number that gives us certainty that some record will be added.
		return 24 * time.Hour
	}

	c.runWaitingGroup.Add(1)
	go func(ctx context.Context) {
		for {
			doIt(ctx)
			select {
			case <-time.After(waitTimeForNext() + time.Duration(c.bufferTime)):
				continue
			case <-c.changed:
				continue
			case <-ctx.Done():
				c.runWaitingGroup.Done()
				return
			}
		}
	}(ctx)
}

func (c *Collector) Add(op func(ctx context.Context)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.operations = append(c.operations, &collectorEntry{
		expiration: time.Now().Unix() + c.ttl,
	})
}

func (c *Collector) Wait() {
	c.runWaitingGroup.Wait()
}
