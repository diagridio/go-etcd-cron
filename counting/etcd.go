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
	// Returns (updated value, true if value was updated in memory, err if any error happened)
	// It is possible that the value is updated but an error occurred while trying to persist it.
	Increment(context.Context, int) (int, bool, error)

	Delete(context.Context) error
}

// It keeps a cache of the value and updates async.
// It works assuming there cannot be two concurrent writes to the same key.
// Concurrency is handled at the job level, which makes this work.
type etcdcounter struct {
	etcdclient   *etcdclientv3.Client
	key          string
	initialValue int

	loaded bool
	value  int
	ttl    time.Duration
}

func NewEtcdCounter(c *etcdclientv3.Client, key string, initialValue int, ttl time.Duration) Counter {
	return &etcdcounter{
		etcdclient:   c,
		initialValue: initialValue,
		key:          key,
		ttl:          ttl,
	}
}

func (c *etcdcounter) Increment(ctx context.Context, delta int) (int, bool, error) {
	firstWrite := false

	if !c.loaded {
		// First, load the key's value.
		res, err := c.etcdclient.KV.Get(ctx, c.key)
		if err != nil {
			return 0, false, err
		}
		if len(res.Kvs) == 0 {
			c.value = c.initialValue
			c.loaded = true
			firstWrite = true // No value for key, this is first write.
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

	if firstWrite && (c.ttl > time.Duration(0)) {
		// Counter will expire after some time, so first write is special in this case.
		lease, err := c.etcdclient.Grant(ctx, int64(c.ttl.Seconds()))
		if err != nil {
			return c.value, true, err
		}
		_, err = c.etcdclient.KV.Put(ctx, c.key, strconv.Itoa(c.value), etcdclientv3.WithLease(lease.ID))
		return c.value, true, err
	}

	_, err := c.etcdclient.KV.Put(ctx, c.key, strconv.Itoa(c.value))
	return c.value, true, err
}

func (c *etcdcounter) Delete(ctx context.Context) error {
	_, err := c.etcdclient.KV.Delete(ctx, c.key)
	return err
}
