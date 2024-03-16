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

func TestCounterTTL(t *testing.T) {
	ctx := context.TODO()
	organizer := partitioning.NewOrganizer(randomNamespace(), partitioning.NoPartitioning())
	etcdClient, err := etcdclientv3.New(etcdclientv3.Config{
		Endpoints: []string{defaultEtcdEndpoint},
	})
	require.NoError(t, err)

	key := organizer.CounterPath(0, "count")
	// This counter will expire keys 1s after their next scheduled trigger.
	counter := NewEtcdCounter(etcdClient, key, time.Second)

	value, updated, err := counter.Add(ctx, 1, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 1, value)

	value, updated, err = counter.Add(ctx, 2, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 3, value)

	time.Sleep(time.Second)

	value, updated, err = counter.Add(ctx, -4, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, -1, value)

	// Counter expires 1 second after the next scheduled trigger (in this test's config)
	time.Sleep(3 * time.Second)

	// Counter should have expired but the in-memory value continues.
	// Even if key is expired in the db, a new operation will set it again, with a new TTL.
	value, updated, err = counter.Add(ctx, 0, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, -1, value)

	// Counter expires 1 second after the next scheduled trigger.
	time.Sleep(3 * time.Second)

	// A new instance will start from 0 since the db record is expired.
	counter = NewEtcdCounter(etcdClient, key, time.Second)
	value, updated, err = counter.Add(ctx, 0, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, 0, value)
}

func randomNamespace() string {
	return uuid.New().String()
}
