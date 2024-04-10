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
	counter := NewEtcdCounter(etcdClient, key, 0)

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

	counterToDelete := NewEtcdCounter(etcdClient, key, 0)
	// Deletes in the database using a different instance.
	// It means the first instance will have a cache still.
	err = counterToDelete.Delete(ctx)
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
	counter = NewEtcdCounter(etcdClient, key, 0)
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
	counter := NewEtcdCounter(etcdClient, key, 5)

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

	// Deletes db record with a different instance, to keep cache of previous instance.
	counterToDelete := NewEtcdCounter(etcdClient, key, 5)
	err = counterToDelete.Delete(ctx)
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
	counter = NewEtcdCounter(etcdClient, key, 5)
	value, updated, err = counter.Increment(ctx, 0)

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
	counter := NewEtcdCounter(etcdClient, key, 0)

	err = counter.Delete(ctx)
	assert.NoError(t, err)
}

func randomNamespace() string {
	return uuid.New().String()
}
