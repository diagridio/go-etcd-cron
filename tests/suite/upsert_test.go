/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/api/errors"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_upsert(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))
	job = &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))

	assert.Eventually(t, func() bool {
		return cron.Triggered() == 1
	}, 5*time.Second, 1*time.Second)

	resp, err := cron.Client().Get(t.Context(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_upsert_duetime(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 3)

	job := &api.Job{
		DueTime:  ptr.Of("1s"),
		Schedule: ptr.Of("@every 5s"),
	}
	now := time.Now().Format(time.RFC3339)
	require.NoError(t, cron.API().Add(cron.Context(), now, job))

	time.Sleep(time.Second * 2)
	assert.Equal(t, 1, cron.Triggered())

	job = &api.Job{
		DueTime:  ptr.Of("20s"),
		Schedule: ptr.Of("@every 5s"),
	}
	require.NoError(t, cron.API().Add(cron.Context(), now, job))

	time.Sleep(time.Second * 5)
	assert.Equal(t, 1, cron.Triggered())
}

func Test_upsert_ifnotexists(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
	}

	require.NoError(t, cron.API().AddIfNotExists(cron.Context(), "def", job))
	err := cron.API().AddIfNotExists(cron.Context(), "def", job)
	require.Error(t, err)
	assert.True(t, errors.IsJobAlreadyExists(err))
	assert.Equal(t, "job already exists: 'def'", err.Error())
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))

	job = &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))

	assert.Eventually(t, func() bool {
		return cron.Triggered() == 1
	}, 5*time.Second, 1*time.Second)

	resp, err := cron.Client().Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_upsert_ifnotexists_delete(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 1)

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
	}

	require.NoError(t, cron.API().AddIfNotExists(cron.Context(), "def", job))
	err := cron.API().AddIfNotExists(cron.Context(), "def", job)
	require.Error(t, err)
	assert.True(t, errors.IsJobAlreadyExists(err))
	assert.Equal(t, "job already exists: 'def'", err.Error())

	require.NoError(t, cron.API().Delete(cron.Context(), "def"))
	require.NoError(t, cron.API().AddIfNotExists(cron.Context(), "def", job))

	job = &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
	}
	require.NoError(t, cron.API().Add(cron.Context(), "def", job))

	assert.Eventually(t, func() bool {
		return cron.Triggered() == 1
	}, 5*time.Second, 1*time.Second)

	resp, err := cron.Client().Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_upsert_duetime_ifnotexists(t *testing.T) {
	t.Parallel()

	cron := integration.NewBase(t, 3)

	job := &api.Job{
		DueTime:  ptr.Of("1s"),
		Schedule: ptr.Of("@every 5s"),
	}
	now := time.Now().Format(time.RFC3339)

	require.NoError(t, cron.API().AddIfNotExists(cron.Context(), now, job))
	err := cron.API().AddIfNotExists(cron.Context(), now, job)
	require.Error(t, err)
	assert.True(t, errors.IsJobAlreadyExists(err))
	require.NoError(t, cron.API().Add(cron.Context(), now, job))

	time.Sleep(time.Second * 2)
	assert.Equal(t, 1, cron.Triggered())

	job = &api.Job{
		DueTime:  ptr.Of("20s"),
		Schedule: ptr.Of("@every 5s"),
	}
	require.NoError(t, cron.API().Add(cron.Context(), now, job))

	time.Sleep(time.Second * 5)
	assert.Equal(t, 1, cron.Triggered())
}
