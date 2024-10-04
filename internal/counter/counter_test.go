/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client/fake"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
	"github.com/diagridio/go-etcd-cron/tests"
)

func Test_New(t *testing.T) {
	t.Parallel()

	t.Run("New pops the job key on the collector", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{
			LastTrigger:    nil,
			Count:          0,
			JobPartitionId: 123,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)
		collector.Push("abc/counters/1")

		yard := grave.New()
		yard.Deleted("abc/jobs/1")
		c, ok, err := New(context.Background(), Options{
			Name: "1",
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			Client:    client,
			Schedule:  sched,
			Job:       job,
			Yard:      yard,
			Collector: collector,
		})
		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, c)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, collector.Run(ctx))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the counter already exists and partition ID matches, expect counter be kept the same", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{
			LastTrigger:    nil,
			Count:          0,
			JobPartitionId: 123,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		yard := grave.New()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		c, ok, err := New(context.Background(), Options{
			Name: "1",
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			Client:    client,
			Schedule:  sched,
			Job:       job,
			Yard:      yard,
			Collector: collector,
		})

		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, c)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.False(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the counter already exists but partition ID doesn't match, expect counter to be written with new value", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{
			LastTrigger:    timestamppb.New(now),
			Count:          1,
			JobPartitionId: 456,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		yard := grave.New()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		c, ok, err := New(context.Background(), Options{
			Name: "1",
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			Client:    client,
			Schedule:  sched,
			Job:       job,
			Yard:      yard,
			Collector: collector,
		})

		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, c)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.False(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		counter = &stored.Counter{
			LastTrigger:    nil,
			Count:          0,
			JobPartitionId: 123,
		}
		counterBytes, err = proto.Marshal(counter)
		require.NoError(t, err)
		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the counter already exists and partition ID matches but is expired, expect both job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{
			LastTrigger:    timestamppb.New(now),
			Count:          1,
			JobPartitionId: 123,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		yard := grave.New()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		c, ok, err := New(context.Background(), Options{
			Name: "1",
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			Client:    client,
			Schedule:  sched,
			Job:       job,
			Yard:      yard,
			Collector: collector,
		})

		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, c)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.True(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("if the counter doesn't exist, create new counter and update next but don't write, return true", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		yard := grave.New()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		counter, ok, err := New(context.Background(), Options{
			Name: "1",
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
			Client:    client,
			Schedule:  sched,
			Job:       job,
			Yard:      yard,
			Collector: collector,
		})

		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, counter)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.False(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})
}

func Test_TriggerSuccess(t *testing.T) {
	t.Parallel()

	t.Run("if tick next is true, expect job be kept and counter to incremented", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_DueTime{
				DueTime: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
			},
		}
		counter := &stored.Counter{LastTrigger: nil, JobPartitionId: 123}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		yard := grave.New()
		c := &Counter{
			yard:       yard,
			client:     client,
			collector:  collector,
			job:        job,
			count:      counter,
			schedule:   sched,
			jobKey:     "abc/jobs/1",
			counterKey: "abc/counters/1",
			next:       now,
		}

		ok, err := c.TriggerSuccess(ctx)
		require.NoError(t, err)
		assert.True(t, ok)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.False(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		counterBytes, err = proto.Marshal(&stored.Counter{
			LastTrigger:    timestamppb.New(now),
			JobPartitionId: 123,
			Count:          1,
		})
		require.NoError(t, err)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if tick next is false, expect job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{
			LastTrigger:    nil,
			JobPartitionId: 123,
			Count:          0,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		yard := grave.New()
		c := &Counter{
			yard:       yard,
			client:     client,
			collector:  collector,
			job:        job,
			next:       now,
			count:      counter,
			schedule:   sched,
			jobKey:     "abc/jobs/1",
			counterKey: "abc/counters/1",
		}

		ok, err := c.TriggerSuccess(ctx)
		require.NoError(t, err)
		assert.False(t, ok)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.True(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("The number of attempts on the counter should always be reset to 0 when Trigger is called", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_DueTime{
				DueTime: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
			},
		}
		counter := &stored.Counter{LastTrigger: nil, JobPartitionId: 123, Attempts: 456}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		yard := grave.New()
		c := &Counter{
			yard:       yard,
			client:     client,
			collector:  collector,
			job:        job,
			count:      counter,
			schedule:   sched,
			jobKey:     "abc/jobs/1",
			counterKey: "abc/counters/1",
			next:       now,
		}

		ok, err := c.TriggerSuccess(ctx)
		require.NoError(t, err)
		assert.True(t, ok)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.False(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		counterBytes, err = proto.Marshal(&stored.Counter{
			LastTrigger:    timestamppb.New(now),
			JobPartitionId: 123,
			Count:          1,
			Attempts:       0,
		})
		require.NoError(t, err)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})
}

func Test_tickNext(t *testing.T) {
	t.Parallel()

	t.Run("if the updateNext returns true, expect no delete", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{LastTrigger: nil, JobPartitionId: 123}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		yard := grave.New()
		c := &Counter{
			yard:       yard,
			client:     client,
			collector:  collector,
			job:        job,
			count:      counter,
			schedule:   sched,
			jobKey:     "abc/jobs/1",
			counterKey: "abc/counters/1",
		}

		ok, err := c.tickNext()
		require.NoError(t, err)
		assert.True(t, ok)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.False(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the updateNext returns false, expect delete", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &stored.Counter{
			LastTrigger:    timestamppb.New(now),
			JobPartitionId: 123,
			Count:          1,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error)
		go func() {
			errCh <- collector.Run(ctx)
		}()

		yard := grave.New()
		c := &Counter{
			yard:       yard,
			client:     client,
			collector:  collector,
			job:        job,
			count:      counter,
			schedule:   sched,
			jobKey:     "abc/jobs/1",
			counterKey: "abc/counters/1",
		}

		ok, err := c.tickNext()
		require.NoError(t, err)
		assert.False(t, ok)

		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timedout waiting for the collector to finish")
		}

		assert.True(t, yard.HasJustDeleted("abc/jobs/1"))

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})
}

func Test_updateNext(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	builder := scheduler.NewBuilder()

	oneshot, err := builder.Schedule(&stored.Job{
		Begin: &stored.Job_DueTime{
			DueTime: timestamppb.New(now),
		},
		Job: &api.Job{
			DueTime: ptr.Of(now.Format(time.RFC3339)),
		},
	})
	require.NoError(t, err)

	repeats, err := builder.Schedule(&stored.Job{
		Begin: &stored.Job_Start{
			Start: timestamppb.New(now),
		},
		Job: &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(4)),
		},
	})
	require.NoError(t, err)

	expires, err := builder.Schedule(&stored.Job{
		Begin: &stored.Job_Start{
			Start: timestamppb.New(now),
		},
		Expiration: timestamppb.New(now.Add(5 * time.Second)),
		Job: &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(4)),
		},
	})
	require.NoError(t, err)

	tests := map[string]struct {
		counter *Counter
		exp     bool
		expNext time.Time
	}{
		"if the number of counts is the same as repeats return false": {
			counter: &Counter{
				schedule: repeats,
				job: &stored.Job{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &stored.Counter{
					Count: 4,
				},
			},
			exp: false,
		},
		"if the number of counts is more than repeats return false (should never happen)": {
			counter: &Counter{
				schedule: repeats,
				job: &stored.Job{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &stored.Counter{Count: 5},
			},
			exp: false,
		},
		"if the last trigger time if the same as the expiry, expect false": {
			counter: &Counter{
				schedule: expires,
				job: &stored.Job{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &stored.Counter{
					Count:       2,
					LastTrigger: timestamppb.New(now.Add(5 * time.Second)),
				},
			},
			exp: false,
		},
		"if the count is equal to total, return false": {
			counter: &Counter{
				schedule: expires,
				job: &stored.Job{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &stored.Counter{
					Count:       4,
					LastTrigger: timestamppb.New(now),
				},
			},
			exp: false,
		},
		"if under the number of counts, but job is past expiry time, return false": {
			counter: &Counter{
				schedule: expires,
				job: &stored.Job{
					Expiration: timestamppb.New(now.Add(-5 * time.Second)),
					Job:        new(api.Job),
				},
				count: &stored.Counter{
					Count:       0,
					LastTrigger: nil,
				},
			},
			exp: false,
		},
		"if time is past the trigger time but no triggered yet for one shot, return true and set trigger time": {
			counter: &Counter{
				schedule: oneshot,
				job:      &stored.Job{Job: new(api.Job)},
				count: &stored.Counter{
					Count:       0,
					LastTrigger: nil,
				},
			},
			exp:     true,
			expNext: now,
		},
		"if oneshot trigger but has already been triggered, expect false": {
			counter: &Counter{
				schedule: oneshot,
				job:      &stored.Job{Job: new(api.Job)},
				count: &stored.Counter{
					Count:       1,
					LastTrigger: nil,
				},
			},
			exp: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.exp, test.counter.updateNext())
			assert.Equal(t, test.expNext, test.counter.next)
		})
	}
}

func Test_TriggerFailed(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	type putExp struct {
		key     string
		counter *stored.Counter
	}

	tests := map[string]struct {
		job             *api.Job
		count           uint32
		attempts        uint32
		next            *time.Time
		lastTriggerTime *timestamppb.Timestamp
		exp             bool
		expNext         *time.Time
		expPut          *putExp
		expDel          *string
	}{
		"no failure policy defined, just due time, expect job deleted": {
			job: &api.Job{
				DueTime:       ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: nil,
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"no failure policy defined, schedule, expect counter forward": {
			job: &api.Job{
				Schedule:      ptr.Of("@every 1s"),
				DueTime:       ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: nil,
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"no failure policy Policy defined, just due time, expect job delete": {
			job: &api.Job{
				DueTime:       ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: new(api.FailurePolicy),
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"no failure policy Policy defined, schedule, expect counter forward": {
			job: &api.Job{
				Schedule:      ptr.Of("@every 1s"),
				DueTime:       ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: new(api.FailurePolicy),
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"failure policy Drop defined, just due time, expect job delete": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: new(api.FailurePolicy_Drop),
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"failure policy Drop defined, schedule, expect counter forward": {
			job: &api.Job{
				Schedule: ptr.Of("@every 1s"),
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: new(api.FailurePolicy_Drop),
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"failure policy Drop defined, schedule and count is not up, expect count forward": {
			job: &api.Job{
				Schedule: ptr.Of("@every 1s"),
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Repeats:  ptr.Of(uint32(3)),
				FailurePolicy: &api.FailurePolicy{
					Policy: new(api.FailurePolicy_Drop),
				},
			},
			count:           1,
			attempts:        0,
			next:            ptr.Of(now.Add(time.Second)),
			lastTriggerTime: timestamppb.New(now),
			exp:             true,
			expNext:         ptr.Of(now.Add(time.Second * 2)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       2,
					LastTrigger: timestamppb.New(now.Add(time.Second)),
				},
			},
			expDel: nil,
		},
		"failure policy Drop defined, schedule but count is up, expect job delete": {
			job: &api.Job{
				Schedule: ptr.Of("@every 1s"),
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Repeats:  ptr.Of(uint32(3)),
				FailurePolicy: &api.FailurePolicy{
					Policy: new(api.FailurePolicy_Drop),
				},
			},
			count:    2,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expNext:  nil,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"failure policy Constant default, nil delay and nil max retries, one shot, expect true with Put and next now": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: nil, MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 1,
				},
			},
			expDel: nil,
		},
		"failure policy Constant default, nil delay and 0 max retries, one shot, expect false with del": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: nil, MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expNext:  nil,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"failure policy Constant default, nil delay and 0 max retries, schedule, expect true with count forward": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: nil, MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"failure policy Constant default, nil delay and 0 max retries, schedule but at expiration, expect false with job delete": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				Repeats:  ptr.Of(uint32(3)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: nil, MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    2,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expNext:  nil,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"failure policy Constant default, 3s delay and nil max retries, one shot, expect true with Put and next now+3s": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 3)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 1,
				},
			},
			expDel: nil,
		},
		"failure policy Constant default, 3s delay and nil max retries, schedule, expect true with Put and next now+3s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 3)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 1,
				},
			},
			expDel: nil,
		},
		"failure policy Constant default, 3s delay and nil max retries, schedule but at expiration, expect true with Put and next now+9": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				Repeats:  ptr.Of(uint32(3)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    2,
			attempts: 0,
			next:     ptr.Of(now.Add(time.Second * 6)),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 9)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    2,
					Attempts: 1,
				},
			},
			expDel: nil,
		},
		"failure policy Constant default, 3s delay and nil max retries, schedule attempt 5, expect true with Put and next now+3*5": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 4,
			next:     ptr.Of(now.Add(time.Second * 3 * 4)),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 3 * 5)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 5,
				},
			},
			expDel: nil,
		},
		"failure policy Constant default, 0s delay and nil max retries, schedule attempt 5, expect true with Put and next now": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(0), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 4,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 5,
				},
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s delay and 0 max retries with 0 attempts, schedule, expect true with Put and next now+5s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s delay and 0 max retries with 0 attempts, one shot, expect false with del job:": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      false,
			expNext:  nil,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},
		"if failure policy is constant with 5s delay and 1 max retries with 0 attempts, schedule, expect true with Put and next now+5s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 5)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 1,
				},
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s delay and 1 max retries with 0 attempts, oneshot, expect true with Put and next now+5s": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 5)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 1,
				},
			},
			expDel: nil,
		},

		"if failure policy is constant with 5s delay and 1 max retries with 1 attempts, schedule, expect true with count forward and next now+1s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 1,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s delay and 1 max retries with 1 attempts, oneshot, expect false with del job": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 1,
			next:     ptr.Of(now),
			exp:      false,
			expNext:  nil,
			expPut:   nil,
			expDel:   ptr.Of("abc/jobs/1"),
		},

		"if failure policy is constant with 5s delay and 2 max retries with 2 attempts, schedule, expect true with count forward and next now+1s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(2)),
						},
					},
				},
			},
			count:    0,
			attempts: 2,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				},
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s delay and 3 max retries with 2 attempts, schedule, expect true with retry": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(3)),
						},
					},
				},
			},
			count:    0,
			attempts: 2,
			next:     ptr.Of(now.Add(time.Second * 5 * 2)),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 5 * 3)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:    0,
					Attempts: 3,
				},
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s delay and 2 max retries with 2 attempts, count 3, schedule, expect true with counter forward": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(2)),
						},
					},
				},
			},
			count:           3,
			lastTriggerTime: timestamppb.New(now.Add(time.Second * 3)),
			attempts:        2,
			next:            ptr.Of(now.Add((time.Second * 2) + (time.Second * 5 * 2))),
			exp:             true,
			expNext:         ptr.Of(now.Add(time.Second * 5)),
			expPut: &putExp{
				key: "abc/counters/1",
				counter: &stored.Counter{
					Count:       4,
					LastTrigger: timestamppb.New(now.Add((time.Second * 4))),
				},
			},
			expDel: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := fake.New().
				WithPutFn(func(_ context.Context, jobKey string, counter string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
					if test.expPut == nil {
						assert.Fail(t, "unexpected put call")
					} else {
						var counterAPI stored.Counter
						require.NoError(t, proto.Unmarshal([]byte(counter), &counterAPI))
						assert.Equal(t, test.expPut.key, jobKey)
						assert.Truef(t, proto.Equal(test.expPut.counter, &counterAPI), "%v != %v", test.expPut.counter, &counterAPI)
					}
					return nil, nil
				}).
				WithDeleteMultiFn(func(keys ...string) error {
					if test.expDel == nil {
						assert.Fail(t, "unexpected delete call")
					} else {
						assert.Equal(t, []string{*test.expDel}, keys)
					}
					return nil
				})

			collector, err := garbage.New(garbage.Options{Client: client})
			require.NoError(t, err)

			next := now
			if test.next != nil {
				next = *test.next
			}

			job := &stored.Job{
				Begin: &stored.Job_DueTime{DueTime: timestamppb.New(now)},
				Job:   test.job,
			}

			sched, err := scheduler.NewBuilder().Schedule(job)
			require.NoError(t, err)

			counter := &Counter{
				jobKey:     "abc/jobs/1",
				counterKey: "abc/counters/1",
				client:     client,
				count: &stored.Counter{
					Attempts:    test.attempts,
					Count:       test.count,
					LastTrigger: test.lastTriggerTime,
				},
				job:       job,
				next:      next,
				schedule:  sched,
				collector: collector,
				yard:      grave.New(),
			}

			ok, err := counter.TriggerFailed(context.Background())
			require.NoError(t, err)
			assert.Equal(t, test.exp, ok)
			if test.expDel == nil {
				assert.Equal(t, *test.expNext, counter.next, "next")
			}
			assert.NotEqual(t, test.expDel != nil, test.expPut != nil)
		})
	}
}

func Test_TriggerFailureSuccess(t *testing.T) {
	t.Parallel()

	var putCall, delCall atomic.Uint32
	var count, del atomic.Value
	client := fake.New().
		WithPutFn(func(_ context.Context, _ string, counter string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
			putCall.Add(1)
			var counterAPI stored.Counter
			require.NoError(t, proto.Unmarshal([]byte(counter), &counterAPI))
			count.Store(&counterAPI)
			return nil, nil
		}).
		WithDeleteMultiFn(func(keys ...string) error {
			delCall.Add(1)
			del.Store(keys)
			return nil
		})

	collector, err := garbage.New(garbage.Options{Client: client})
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	job := &stored.Job{
		Begin: &stored.Job_DueTime{DueTime: timestamppb.New(now)},
		Job: &api.Job{
			DueTime:  ptr.Of(now.Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(3)),
			FailurePolicy: &api.FailurePolicy{
				Policy: &api.FailurePolicy_Constant{
					Constant: &api.FailurePolicyConstant{
						Delay: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(2)),
					},
				},
			},
		},
	}

	sched, err := scheduler.NewBuilder().Schedule(job)
	require.NoError(t, err)

	counter := &Counter{
		jobKey:     "abc/jobs/1",
		counterKey: "abc/counters/1",
		client:     client,
		count:      new(stored.Counter),
		job:        job,
		next:       now,
		schedule:   sched,
		collector:  collector,
		yard:       grave.New(),
	}

	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 0, Attempts: 0, LastTrigger: nil,
	}))
	assert.Equal(t, uint32(0), putCall.Load())

	ok, err := counter.TriggerSuccess(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 1, Attempts: 0, LastTrigger: timestamppb.New(now),
	}))
	assert.Equal(t, uint32(1), putCall.Load())
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 1, Attempts: 0, LastTrigger: timestamppb.New(now),
	}))

	ok, err = counter.TriggerFailed(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*6), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 1, Attempts: 1, LastTrigger: timestamppb.New(now),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 1, Attempts: 1, LastTrigger: timestamppb.New(now),
	}))
	ok, err = counter.TriggerFailed(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*11), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 1, Attempts: 2, LastTrigger: timestamppb.New(now),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 1, Attempts: 2, LastTrigger: timestamppb.New(now),
	}))
	ok, err = counter.TriggerSuccess(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*2), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 2, Attempts: 0, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 2, Attempts: 0, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))

	ok, err = counter.TriggerFailed(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*7), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 2, Attempts: 1, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 2, Attempts: 1, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	ok, err = counter.TriggerFailed(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*12), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 2, Attempts: 2, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 2, Attempts: 2, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	assert.Equal(t, uint32(0), delCall.Load())
	assert.Nil(t, del.Load())
	ok, err = counter.TriggerFailed(context.Background())
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint32(1), delCall.Load())
	assert.Equal(t, []string{"abc/jobs/1"}, del.Load())
}
