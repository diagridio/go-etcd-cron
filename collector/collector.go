/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package collector

import (
	"context"
	"sync"
	"time"
)

// Collector garbage collects items after a globally configured TTL.
type Collector struct {
	ttl        time.Duration // time to wait to perform collection
	bufferTime time.Duration // arbitrary delay to allow buffering of operations

	running         bool
	mutex           sync.RWMutex
	operations      []*collectorEntry
	changed         chan bool
	runWaitingGroup sync.WaitGroup
}

type collectorEntry struct {
	expiration time.Time
	op         func(ctx context.Context)
}

func New(ttl time.Duration, bufferTime time.Duration) *Collector {
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

		now := time.Now()
		nextStartIndex := -1
		for i, o := range c.operations {
			if o.expiration.Before(now) {
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

		now := time.Now()
		if len(c.operations) > 0 {
			op := c.operations[0]
			if op.expiration.Before(now) {
				return 0
			}

			return now.Sub(op.expiration)
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
		expiration: time.Now().Add(c.ttl),
	})
}

func (c *Collector) Wait() {
	c.runWaitingGroup.Wait()
}
