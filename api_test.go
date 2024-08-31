/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	etcdcron "github.com/diagridio/go-etcd-cron"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests"
	"github.com/diagridio/go-etcd-cron/tests/cron"
)

func Test_CRUD(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	cron, err := etcdcron.New(etcdcron.Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
	})
	require.NoError(t, err)

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for cron to stop")
		}
	})
	go func() {
		errCh <- cron.Run(ctx)
	}()

	now := time.Now()

	resp, err := cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, cron.Add(context.Background(), "def", &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}))

	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}, resp)

	newNow := time.Now()
	require.NoError(t, cron.Add(context.Background(), "def", &api.Job{
		DueTime: ptr.Of(newNow.Add(time.Hour).Format(time.RFC3339)),
	}))
	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &api.Job{
		DueTime: ptr.Of(newNow.Add(time.Hour).Format(time.RFC3339)),
	}, resp)

	require.NoError(t, cron.Delete(context.Background(), "def"))

	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Nil(t, resp)

	require.NoError(t, cron.Add(context.Background(), "def", &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}))

	resp, err = cron.Get(context.Background(), "def")
	require.NoError(t, err)
	assert.Equal(t, &api.Job{
		DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339)),
	}, resp)
}

func Test_Add(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, cron.Add(ctx, "def", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, cron.Run(ctx))

		require.Error(t, cron.Add(context.Background(), "def", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		require.Error(t, cron.Add(context.Background(), "./.", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))
	})

	t.Run("empty job should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		require.Error(t, cron.Add(context.Background(), "def", nil))
	})
}

func Test_Get(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp, err := cron.Get(ctx, "def")
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, cron.Run(ctx))

		resp, err := cron.Get(context.Background(), "def")
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		cron, err := etcdcron.New(etcdcron.Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn:      func(context.Context, *api.TriggerRequest) bool { return true },
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		})
		go func() {
			errCh <- cron.Run(ctx)
		}()

		resp, err := cron.Get(context.Background(), "./.")
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func Test_Delete(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartition(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, c.Delete(ctx, "def"))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartition(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, c.Run(ctx))

		require.Error(t, c.Delete(context.Background(), "def"))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartitionRun(t)

		require.Error(t, c.Delete(context.Background(), "./."))
	})

	t.Run("deleting a job should dequeue it and delete counter", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartitionRun(t)

		require.NoError(t, c.Add(context.Background(), "abc", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.Eventually(t, func() bool {
			return c.Calls.Load() > 0
		}, time.Second*3, time.Millisecond*10)

		assert.EventuallyWithT(t, func(co *assert.CollectT) {
			assert.Len(co, c.Counters(t).Kvs, 1)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, c.Delete(context.Background(), "abc"))

		assert.EventuallyWithT(t, func(co *assert.CollectT) {
			assert.Len(co, c.Counters(t).Kvs, 0)
		}, time.Second*3, time.Millisecond*10)

		current := c.Calls.Load()

		time.Sleep(time.Second * 2)
		assert.Equal(t, current, c.Calls.Load())
	})

	t.Run("deleting jobs in cluster should delete jobs", func(t *testing.T) {
		t.Parallel()

		cr := cron.TripplePartitionRun(t)

		for i := 0; i < 1000; i++ {
			require.NoError(t, cr.Add(context.Background(), "a"+strconv.Itoa(i), &api.Job{Schedule: ptr.Of("@every 1s")}))
		}

		assert.Len(t, cr.Jobs(t).Kvs, 1000)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 1000)
		}, time.Second*10, time.Millisecond*10)

		for i := 0; i < 1000; i++ {
			require.NoError(t, cr.Delete(context.Background(), "a"+strconv.Itoa(i)))
		}

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*10, time.Millisecond*10)

		calls := cr.Calls.Load()
		time.Sleep(time.Second * 2)
		assert.Equal(t, calls, cr.Calls.Load())
	})
}

func Test_DeletePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("returns context error if cron not ready in time", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartition(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, c.DeletePrefixes(ctx, "foobar"))
	})

	t.Run("returns closed error if cron is closed", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartition(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, c.Run(ctx))

		require.Error(t, c.DeletePrefixes(context.Background(), "foobar"))
	})

	t.Run("invalid name should error", func(t *testing.T) {
		t.Parallel()

		c := cron.SinglePartitionRun(t)
		require.Error(t, c.DeletePrefixes(context.Background(), "./."))
	})

	t.Run("deleting a job should dequeue it and delete counter", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(context.Background(), "a1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))
		require.NoError(t, cr.Add(context.Background(), "b2", &api.Job{
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.Len(t, cr.Jobs(t).Kvs, 2)

		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			assert.Greater(t, cr.Calls.Load(), int64(0))
		}, time.Second*3, time.Millisecond*10)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			counters := cr.Counters(t)
			if assert.Len(c, counters.Kvs, 2) {
				assert.ElementsMatch(t,
					[]string{"abc/counters/a1", "abc/counters/b2"},
					[]string{string(counters.Kvs[0].Key), string(counters.Kvs[1].Key)},
				)
			}
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background(), "a"))

		assert.Len(t, cr.Jobs(t).Kvs, 1)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 1)
		}, time.Second*3, time.Millisecond*10)
	})

	t.Run("deleting with empty string should delete all jobs", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(context.Background(), "a1", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "b2", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "c3", &api.Job{Schedule: ptr.Of("@every 1s")}))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 3)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background(), ""))

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*3, time.Millisecond*10)
	})

	t.Run("deleting all with prefix in cluster should delete all jobs with prefix", func(t *testing.T) {
		t.Parallel()

		cr := cron.TripplePartitionRun(t)

		for i := 0; i < 1000; i++ {
			require.NoError(t, cr.Add(context.Background(), "a"+strconv.Itoa(i), &api.Job{Schedule: ptr.Of("@every 1s")}))
		}

		assert.Len(t, cr.Jobs(t).Kvs, 1000)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 1000)
		}, time.Second*10, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background(), "a"))

		assert.Empty(t, cr.Jobs(t).Kvs)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Empty(c, cr.Counters(t).Kvs)
		}, time.Second*10, time.Millisecond*10)

		calls := cr.Calls.Load()
		time.Sleep(time.Second * 2)
		assert.Equal(t, calls, cr.Calls.Load())
	})

	t.Run("deleting no prefixes should delete nothing", func(t *testing.T) {
		t.Parallel()

		cr := cron.SinglePartitionRun(t)

		require.NoError(t, cr.Add(context.Background(), "a1", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "b2", &api.Job{Schedule: ptr.Of("@every 1s")}))
		require.NoError(t, cr.Add(context.Background(), "c3", &api.Job{Schedule: ptr.Of("@every 1s")}))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Len(c, cr.Counters(t).Kvs, 3)
		}, time.Second*3, time.Millisecond*10)

		require.NoError(t, cr.DeletePrefixes(context.Background()))

		assert.Len(t, cr.Jobs(t).Kvs, 3)
		assert.Len(t, cr.Counters(t).Kvs, 3)
	})
}
