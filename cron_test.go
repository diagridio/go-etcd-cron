/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/tests"
)

func Test_retry(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggerd atomic.Int64
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) bool {
			triggerd.Add(1)
			return triggerd.Load() != 1
		},
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

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
	}
	require.NoError(t, cron.Add(ctx, "yoyo", job))

	<-time.After(3 * time.Second)
	assert.Equal(t, int64(2), triggerd.Load())
}

func Test_payload(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	gotCh := make(chan *api.TriggerRequest, 1)
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(_ context.Context, api *api.TriggerRequest) bool {
			gotCh <- api
			return true
		},
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

	payload, err := anypb.New(wrapperspb.String("hello"))
	require.NoError(t, err)
	meta, err := anypb.New(wrapperspb.String("world"))
	require.NoError(t, err)
	job := &api.Job{
		DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		Payload:  payload,
		Metadata: meta,
	}
	require.NoError(t, cron.Add(ctx, "yoyo", job))

	select {
	case got := <-gotCh:
		assert.Equal(t, "yoyo", got.GetName())
		var gotPayload wrapperspb.StringValue
		require.NoError(t, got.GetPayload().UnmarshalTo(&gotPayload))
		assert.Equal(t, "hello", gotPayload.GetValue())
		var gotMeta wrapperspb.StringValue
		require.NoError(t, got.GetMetadata().UnmarshalTo(&gotMeta))
		assert.Equal(t, "world", gotMeta.GetValue())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for trigger")
	}
}

func Test_remove(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggered atomic.Int64
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) bool {
			triggered.Add(1)
			return true
		},
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

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second * 2).Format(time.RFC3339)),
	}
	require.NoError(t, cron.Add(ctx, "def", job))
	require.NoError(t, cron.Delete(ctx, "def"))

	<-time.After(3 * time.Second)

	assert.Equal(t, int64(0), triggered.Load())
}

func Test_upsert(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggered atomic.Int64
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) bool {
			triggered.Add(1)

			return true
		},
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

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Hour).Format(time.RFC3339)),
	}
	require.NoError(t, cron.Add(ctx, "def", job))
	job = &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
	}
	require.NoError(t, cron.Add(ctx, "def", job))

	assert.Eventually(t, func() bool {
		return triggered.Load() == 1
	}, 5*time.Second, 1*time.Second)

	resp, err := client.Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_patition(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggered atomic.Int64

	crons := make([]Interface, 100)
	for i := 0; i < 100; i++ {
		cron, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    uint32(i),
			PartitionTotal: 100,
			TriggerFn: func(context.Context, *api.TriggerRequest) bool {
				triggered.Add(1)
				return true
			},
		})
		require.NoError(t, err)
		crons[i] = cron
	}

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		for i := 0; i < 100; i++ {
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		}
	})
	for i := 0; i < 100; i++ {
		go func(i int) {
			errCh <- crons[i].Run(ctx)
		}(i)
	}

	for i := 0; i < 100; i++ {
		job := &api.Job{
			DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
		}
		require.NoError(t, crons[0].Add(ctx, "test-"+strconv.Itoa(i), job))
	}

	assert.Eventually(t, func() bool {
		return triggered.Load() == 100
	}, 5*time.Second, 1*time.Second)

	resp, err := client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_oneshot(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggered atomic.Int64
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) bool {
			triggered.Add(1)
			return true
		},
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

	job := &api.Job{
		DueTime: ptr.Of(time.Now().Add(time.Second).Format(time.RFC3339)),
	}

	require.NoError(t, cron.Add(ctx, "def", job))

	assert.Eventually(t, func() bool {
		return triggered.Load() == 1
	}, 5*time.Second, 1*time.Second)

	resp, err := client.Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_repeat(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggered atomic.Int64
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) bool {
			triggered.Add(1)
			return true
		},
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

	job := &api.Job{
		Schedule: ptr.Of("@every 1s"),
		Repeats:  ptr.Of(uint32(3)),
	}

	require.NoError(t, cron.Add(ctx, "def", job))

	assert.Eventually(t, func() bool {
		return triggered.Load() == 3
	}, 5*time.Second, 1*time.Second)

	resp, err := client.Get(context.Background(), "abc/jobs/def")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs)
}

func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("Running multiple times should error", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)
		var triggered atomic.Int64
		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn: func(context.Context, *api.TriggerRequest) bool {
				triggered.Add(1)
				return true
			},
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(context.Background())
		errCh1 := make(chan error)
		errCh2 := make(chan error)

		go func() {
			errCh1 <- cronI.Run(ctx)
		}()

		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		go func() {
			errCh2 <- cronI.Run(ctx)
		}()

		select {
		case err := <-errCh2:
			require.Error(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}

		cancel()
		select {
		case err := <-errCh1:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting Run response")
		}
	})
}

func Test_schedule(t *testing.T) {
	t.Parallel()

	t.Run("if no counter, job should not be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)

		now := time.Now().UTC().Add(time.Hour)
		job := &api.JobStored{
			Begin: &api.JobStored_DueTime{
				DueTime: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)

		var triggered atomic.Int64
		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn: func(context.Context, *api.TriggerRequest) bool {
				triggered.Add(1)
				return true
			},
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- cron.Run(ctx)
		}()

		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		require.NoError(t, cron.schedule(context.Background(), "1", job))

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to return")
		}

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(jobBytes), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		assert.Equal(t, int64(0), triggered.Load())
	})

	t.Run("if schedule is not done, job and counter should not be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)

		now := time.Now().UTC().Add(time.Hour)
		job := &api.JobStored{
			Begin: &api.JobStored_DueTime{
				DueTime: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger:    nil,
			Count:          0,
			JobPartitionId: 123,
		}

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		var triggered atomic.Int64
		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn: func(context.Context, *api.TriggerRequest) bool {
				triggered.Add(1)
				return true
			},
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- cron.Run(ctx)
		}()

		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		require.NoError(t, cron.schedule(context.Background(), "1", job))

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to return")
		}

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(jobBytes), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(counterBytes), string(resp.Kvs[0].Value))

		assert.Equal(t, int64(0), triggered.Load())
	})

	t.Run("if schedule is done, expect job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)

		now := time.Now().UTC()
		job := &api.JobStored{
			Begin: &api.JobStored_DueTime{
				DueTime: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger:    timestamppb.New(now),
			Count:          1,
			JobPartitionId: 123,
		}

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		var triggered atomic.Int64
		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn: func(context.Context, *api.TriggerRequest) bool {
				triggered.Add(1)
				return true
			},
		})
		require.NoError(t, err)
		cron := cronI.(*cron)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- cron.Run(ctx)
		}()

		select {
		case <-cron.readyCh:
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to be ready")
		}

		require.NoError(t, cron.schedule(context.Background(), "1", job))

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for cron to return")
		}

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		assert.Equal(t, int64(0), triggered.Load())
	})
}

func Test_zeroDueTime(t *testing.T) {
	t.Parallel()

	client := tests.EmbeddedETCDBareClient(t)
	var triggerd atomic.Int64
	cron, err := New(Options{
		Log:            logr.Discard(),
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) bool {
			triggerd.Add(1)
			return true
		},
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

	require.NoError(t, cron.Add(ctx, "yoyo", &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of("0s"),
	}))
	assert.Eventually(t, func() bool {
		return triggerd.Load() == 1
	}, 3*time.Second, time.Millisecond*10)

	require.NoError(t, cron.Add(ctx, "yoyo2", &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of("1s"),
	}))
	assert.Eventually(t, func() bool {
		return triggerd.Load() == 2
	}, 3*time.Second, time.Millisecond*10)

	require.NoError(t, cron.Add(ctx, "yoyo3", &api.Job{
		Schedule: ptr.Of("@every 1h"),
	}))
	<-time.After(2 * time.Second)
	assert.Equal(t, int64(2), triggerd.Load())
}
