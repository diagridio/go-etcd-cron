/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counting

import (
	"context"
	"testing"
	"time"

	"github.com/diagridio/go-etcd-cron/partitioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdclientv3 "go.etcd.io/etcd/client/v3"
)

const defaultEtcdEndpoint = "127.0.0.1:2379"

func TestCounterIncrement(t *testing.T) {
	ctx := context.TODO()
	organizer := partitioning.NewOrganizer(randomNamespace(), partitioning.NoPartitioning())
	etcdClient, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: []string{defaultEtcdEndpoint},
	})
	require.NoError(t, err)

	key := organizer.CounterPath(0, "count")
	// This counter will expire keys 1s after their next scheduled trigger.
	counter := NewEtcdCounter(etcdClient, key, 0, time.Duration(0))

	value, updated, err := counter.Increment(ctx, 1)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 1, value)

	value, updated, err = counter.Increment(ctx, 2)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 3, value)

	value, updated, err = counter.Increment(ctx, -4)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, -1, value)

	// Deletes in the database.
	err = counter.Delete(ctx)
	assert.NoError(t, err)

	// Counter deleted but the in-memory value continues.
	// Even if key is deleted in the db, a new operation will set it again.
	value, updated, err = counter.Increment(ctx, 0)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, -1, value)

	// Counter is deleted in db again.
	err = counter.Delete(ctx)
	assert.NoError(t, err)

	// A new instance will start from 0 since the db record does not exist.
	counter = NewEtcdCounter(etcdClient, key, 0, time.Duration(0))
	value, updated, err = counter.Increment(ctx, 0)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 0, value)
}

func TestCounterDecrement(t *testing.T) {
	ctx := context.TODO()
	organizer := partitioning.NewOrganizer(randomNamespace(), partitioning.NoPartitioning())
	etcdClient, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: []string{defaultEtcdEndpoint},
	})
	require.NoError(t, err)

	key := organizer.CounterPath(0, "count")
	// This counter will expire keys 1s after their next scheduled trigger.
	counter := NewEtcdCounter(etcdClient, key, 5, time.Duration(0))

	value, updated, err := counter.Increment(ctx, -1)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 4, value)

	value, updated, err = counter.Increment(ctx, -2)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 2, value)

	time.Sleep(time.Second)

	value, updated, err = counter.Increment(ctx, -3)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, -1, value)

	// Deletes db record.
	err = counter.Delete(ctx)
	assert.NoError(t, err)

	// Counter is deleted in db but the in-memory value continues.
	// Even if key is deleted in the db, a new operation will set it again.
	value, updated, err = counter.Increment(ctx, 0)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, -1, value)

	// Deletes db record again.
	err = counter.Delete(ctx)
	assert.NoError(t, err)

	// A new instance will start from initialValue since the db record is deleted.
	counter = NewEtcdCounter(etcdClient, key, 5, time.Duration(0))
	value, updated, err = counter.Increment(ctx, 0)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 5, value)
}

func TestCounterExpiration(t *testing.T) {
	ctx := context.TODO()
	organizer := partitioning.NewOrganizer(randomNamespace(), partitioning.NoPartitioning())
	etcdClient, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: []string{defaultEtcdEndpoint},
	})
	require.NoError(t, err)

	key := organizer.CounterPath(0, "count")
	// This counter will expire keys 1s after their next scheduled trigger.
	counter := NewEtcdCounter(etcdClient, key, 0, 2*time.Second)

	value, updated, err := counter.Increment(ctx, 1)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 1, value)

	// Enough time to expire in the database.
	time.Sleep(3 * time.Second)

	// New instance to make sure we re-read it from DB.
	counter = NewEtcdCounter(etcdClient, key, 0, time.Duration(0))

	value, updated, err = counter.Increment(ctx, 2)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 2, value)

	// Zero duration means it never expires.
	// Enough time to make sure the previous TTL did not apply anymore.
	time.Sleep(3 * time.Second)

	value, updated, err = counter.Increment(ctx, 3)
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 5, value)
}

func TestCounterDeleteUnknownKey(t *testing.T) {
	ctx := context.TODO()
	organizer := partitioning.NewOrganizer(randomNamespace(), partitioning.NoPartitioning())
	etcdClient, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: []string{defaultEtcdEndpoint},
	})
	require.NoError(t, err)

	key := organizer.CounterPath(0, "unknown")
	// This counter will expire keys 1s after their next scheduled trigger.
	counter := NewEtcdCounter(etcdClient, key, 0, 2*time.Second)

	err = counter.Delete(ctx)
	assert.NoError(t, err)
}

func randomNamespace() string {
	return uuid.New().String()
}
