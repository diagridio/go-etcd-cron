/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package counter

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
	"github.com/diagridio/go-etcd-cron/internal/tests"
)

func Test_New(t *testing.T) {
	t.Parallel()

	t.Run("New pops the job key on the collector", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Uuid: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger: nil,
			Count:       0,
			JobUuid:     123,
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})
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

	t.Run("if the counter already exists and UUID matches, expect counter be kept the same", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Uuid: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger: nil,
			Count:       0,
			JobUuid:     123,
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

	t.Run("if the counter already exists but UUID doesn't match, expect counter to be written with new value", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Uuid: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger: timestamppb.New(now),
			Count:       1,
			JobUuid:     456,
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

		counter = &api.Counter{
			LastTrigger: nil,
			Count:       0,
			JobUuid:     123,
		}
		counterBytes, err = proto.Marshal(counter)
		require.NoError(t, err)
		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the counter already exists and UUID matches but is expired, expect both job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Uuid: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger: timestamppb.New(now),
			Count:       1,
			JobUuid:     123,
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Uuid: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

func Test_Trigger(t *testing.T) {
	t.Parallel()

	t.Run("if tick next is true, expect job be kept and counter to incremented", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
			},
		}
		counter := &api.Counter{LastTrigger: nil, JobUuid: 123}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

		ok, err := c.Trigger(ctx)
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

		counterBytes, err = proto.Marshal(&api.Counter{
			LastTrigger: timestamppb.New(now),
			JobUuid:     123,
			Count:       1,
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

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger: nil,
			JobUuid:     123,
			Count:       0,
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

		ok, err := c.Trigger(ctx)
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

func Test_tickNext(t *testing.T) {
	t.Parallel()

	t.Run("if the updateNext returns true, expect no delete", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)

		now := time.Now().UTC()

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{LastTrigger: nil, JobUuid: 123}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

		job := &api.JobStored{
			Begin: &api.JobStored_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		counter := &api.Counter{
			LastTrigger: timestamppb.New(now),
			JobUuid:     123,
			Count:       1,
		}

		sched, err := scheduler.NewBuilder().Scheduler(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(counter)
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		collector := garbage.New(garbage.Options{Client: client})

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

	oneshot, err := builder.Scheduler(&api.JobStored{
		Begin: &api.JobStored_DueTime{
			DueTime: timestamppb.New(now),
		},
		Job: &api.Job{
			DueTime: ptr.Of(now.Format(time.RFC3339)),
		},
	})
	require.NoError(t, err)

	repeats, err := builder.Scheduler(&api.JobStored{
		Begin: &api.JobStored_Start{
			Start: timestamppb.New(now),
		},
		Job: &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(4)),
		},
	})
	require.NoError(t, err)

	expires, err := builder.Scheduler(&api.JobStored{
		Begin: &api.JobStored_Start{
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
				job: &api.JobStored{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &api.Counter{
					Count: 4,
				},
			},
			exp: false,
		},
		"if the number of counts is more than repeats return false (should never happen)": {
			counter: &Counter{
				schedule: repeats,
				job: &api.JobStored{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &api.Counter{Count: 5},
			},
			exp: false,
		},
		"if the last trigger time if the same as the expiry, expect false": {
			counter: &Counter{
				schedule: expires,
				job: &api.JobStored{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &api.Counter{
					Count:       2,
					LastTrigger: timestamppb.New(now.Add(5 * time.Second)),
				},
			},
			exp: false,
		},
		"if the count is equal to total, return false": {
			counter: &Counter{
				schedule: expires,
				job: &api.JobStored{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &api.Counter{
					Count:       4,
					LastTrigger: timestamppb.New(now),
				},
			},
			exp: false,
		},
		"if under the number of counts, but job is past expiry time, return false": {
			counter: &Counter{
				schedule: expires,
				job: &api.JobStored{
					Expiration: timestamppb.New(now.Add(-5 * time.Second)),
					Job:        new(api.Job),
				},
				count: &api.Counter{
					Count:       0,
					LastTrigger: nil,
				},
			},
			exp: false,
		},
		"if time is past the trigger time but no triggered yet for one shot, return true and set trigger time": {
			counter: &Counter{
				schedule: oneshot,
				job:      &api.JobStored{Job: new(api.Job)},
				count: &api.Counter{
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
				job:      &api.JobStored{Job: new(api.Job)},
				count: &api.Counter{
					Count:       1,
					LastTrigger: nil,
				},
			},
			exp: false,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := test.counter
			assert.Equal(t, test.exp, c.updateNext())
			assert.Equal(t, test.expNext, c.next)
		})
	}
}
