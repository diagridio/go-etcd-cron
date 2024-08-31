/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package garbage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/internal/client"
)

// Options is the configuration for the garbage collector.
type Options struct {
	// Log is the logger for the collector to use.
	Log logr.Logger

	// Client is the ETCD client to use for deleting keys.
	Client client.Interface

	// CollectionInterval is the interval at which the garbage collector runs.
	// When nil, defaults to 180 seconds.
	CollectionInterval *time.Duration
}

// Interface is a garbage collector. It is used to queue-up deletion of ETCD
// keys so that they can be deleted in bulk and not interrupt triggering. Keys
// can be pulled from garbage collection queue.
// Runs every 30s.
type Interface interface {
	// Run runs the garbage collector. Collects every 30 seconds, or when the
	// collector is shutting down.
	Run(ctx context.Context) error

	// Push pushes a key to the garbage collector to be deleted.
	Push(key string)

	// Pop removes a key from the garbage collector.
	Pop(key string)
}

// collector is the implementation of the garbage collector.
type collector struct {
	log                logr.Logger
	client             client.Interface
	clock              clock.Clock
	keys               map[string]struct{}
	soonerCh           chan struct{}
	lock               sync.Mutex
	running            atomic.Bool
	closed             atomic.Bool
	collectionInterval time.Duration
	garbageLimit       int
}

// garbageLimit is the maximum number of keys to queue up for deletion before
// triggering a collection sooner.
const garbageLimit = 500000

// New creates a new garbage collector.
func New(opts Options) (Interface, error) {
	collectionInterval := 180 * time.Second
	if opts.CollectionInterval != nil {
		if *opts.CollectionInterval <= 0 {
			return nil, errors.New("collection interval must be greater than 0")
		}
		collectionInterval = *opts.CollectionInterval
	}
	return &collector{
		log:                opts.Log.WithName("garbage-collector"),
		client:             opts.Client,
		clock:              clock.RealClock{},
		soonerCh:           make(chan struct{}, 100),
		keys:               make(map[string]struct{}),
		garbageLimit:       garbageLimit,
		collectionInterval: collectionInterval,
	}, nil
}

func (c *collector) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("garbage collector is already running")
	}

	defer c.closed.Store(true)
	for {
		select {
		case <-ctx.Done():
			c.log.Info("Shutting down garbage collector")
			return c.collect()

		case <-c.soonerCh:
			if err := c.collect(); err != nil {
				return err
			}

		case <-c.clock.After(c.collectionInterval):
			if err := c.collect(); err != nil {
				return err
			}
		}
	}
}

func (c *collector) Push(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.keys[key] = struct{}{}

	// If we have more than 500k garbage keys, we should trigger a collection
	// sooner.
	if len(c.keys) >= c.garbageLimit {
		if c.closed.Load() {
			return
		}
		c.soonerCh <- struct{}{}
	}
}

func (c *collector) Pop(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.keys, key)
}

// collect deletes all the keys which have been queued up for deletion.
func (c *collector) collect() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.keys) == 0 {
		//nolint:gomnd
		c.log.V(3).Info("No keys to collect, skipping collection")
		return nil
	}

	c.log.Info("Collecting garbage", "keys", len(c.keys))

	keyList := make([]string, 0, len(c.keys))
	for key := range c.keys {
		keyList = append(keyList, key)
	}

	if err := c.client.DeleteMulti(keyList...); err != nil {
		return fmt.Errorf("failed to delete keys: %w", err)
	}

	c.log.Info("Garbage collection complete", "keys", len(c.keys))
	c.keys = make(map[string]struct{})

	return nil
}
