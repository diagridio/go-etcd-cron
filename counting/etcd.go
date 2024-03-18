/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counting

import (
	"context"
	"fmt"
	"strconv"
	"time"

	etcdclientv3 "go.etcd.io/etcd/client/v3"
)

type Counter interface {
	// Applies by the given delta (+ or -) and return the updated value.
	// Count has a ttl calculated using the next tick's time.
	// Returns (updated value, true if value was updated in memory, err if any error happened)
	// It is possible that the value is updated but an error occurred while trying to persist it.
	Add(context.Context, int, time.Time) (int, bool, error)
}

// It keeps a cache of the value and updates async.
// It works assuming there cannot be two concurrent writes to the same key.
// Concurrency is handled at the job level, which makes this work.
type etcdcounter struct {
	etcdclient *etcdclientv3.Client
	key        string

	loaded    bool
	value     int
	ttlOffset time.Duration
}

func NewEtcdCounter(c *etcdclientv3.Client, key string, ttlOffset time.Duration) Counter {
	return &etcdcounter{
		etcdclient: c,
		key:        key,
		ttlOffset:  ttlOffset,
	}
}

func (c *etcdcounter) Add(ctx context.Context, delta int, next time.Time) (int, bool, error) {
	if !c.loaded {
		// First, load the key's value.
		res, err := c.etcdclient.KV.Get(ctx, c.key)
		if err != nil {
			return 0, false, err
		}
		if len(res.Kvs) == 0 {
			c.value = 0
			c.loaded = true
		} else {
			if res.Kvs[0].Value == nil {
				return 0, false, fmt.Errorf("nil value for key %s", c.key)
			}
			if len(res.Kvs[0].Value) == 0 {
				return 0, false, fmt.Errorf("empty value for key %s", c.key)
			}

			c.value, err = strconv.Atoi(string(res.Kvs[0].Value))
			if err != nil {
				return 0, false, err
			}
		}
	}

	c.value += delta
	// Create a lease
	ttl := time.Until(next.Add(c.ttlOffset))
	lease, err := c.etcdclient.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return c.value, true, err
	}
	_, err = c.etcdclient.KV.Put(ctx, c.key, strconv.Itoa(c.value), etcdclientv3.WithLease(lease.ID))
	return c.value, true, err
}
