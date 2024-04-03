/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counting

import (
	"context"
	"fmt"
	"strconv"

	etcdclientv3 "go.etcd.io/etcd/client/v3"
)

type Counter interface {
	// Applies by the given delta (+ or -) and return the updated value.
	// Returns (updated value, true if value was updated in memory, err if any error happened)
	// It is possible that the value is updated but an error occurred while trying to persist it.
	Increment(context.Context, int) (int, bool, error)
	Refresh(context.Context) (int, error)
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
}

func NewEtcdCounter(c *etcdclientv3.Client, key string, initialValue int) Counter {
	return &etcdcounter{
		etcdclient:   c,
		initialValue: initialValue,
		key:          key,
		value:        initialValue,
	}
}

func (c *etcdcounter) Increment(ctx context.Context, delta int) (int, bool, error) {
	if !c.loaded {
		// First, load the key's value.
		_, err := c.Refresh(ctx)
		if err != nil {
			return c.value, false, err
		}
		c.loaded = true
	}

	if delta == 0 {
		// No need to do a db write for a no-change operation.
		return c.value, true, nil
	}

	c.value += delta

	_, err := c.etcdclient.KV.Put(ctx, c.key, strconv.Itoa(c.value))
	return c.value, true, err
}

func (c *etcdcounter) Delete(ctx context.Context) error {
	_, err := c.etcdclient.KV.Delete(ctx, c.key)
	return err
}

func (c *etcdcounter) Refresh(ctx context.Context) (int, error) {
	c.loaded = false
	res, err := c.etcdclient.KV.Get(ctx, c.key)
	if err != nil {
		return c.value, err
	}
	if len(res.Kvs) == 0 {
		c.value = c.initialValue
		c.loaded = true
		return c.value, nil
	}

	if res.Kvs[0].Value == nil {
		return c.value, fmt.Errorf("nil value for key %s", c.key)
	}
	if len(res.Kvs[0].Value) == 0 {
		return c.value, fmt.Errorf("empty value for key %s", c.key)
	}
	v, err := strconv.Atoi(string(res.Kvs[0].Value))
	if err == nil {
		c.value = v
	}
	return c.value, err
}

func (c *etcdcounter) Delete(ctx context.Context) error {
	_, err := c.etcdclient.KV.Delete(ctx, c.key)
	return err
}
