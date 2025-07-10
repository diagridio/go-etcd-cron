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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/kit/ptr"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/client/fake"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_New(t *testing.T) {
	t.Parallel()

	t.Run("if the counter already exists and partition ID matches, expect counter be kept the same", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

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

		_, err = client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "0",
		})
		require.NoError(t, err)

		c, ok, err := New(t.Context(), Options{
			Name:     "1",
			Key:      key,
			Client:   client,
			Schedule: sched,
			Job:      job,
		})

		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, c)

		assert.Equal(t, "1", c.JobName())

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the counter already exists but partition ID doesn't match, expect counter to be written with new value", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

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

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "0",
		})
		require.NoError(t, err)
		c, ok, err := New(t.Context(), Options{
			Name:           "1",
			Key:            key,
			Client:         client,
			Schedule:       sched,
			Job:            job,
			JobModRevision: presp.Header.GetRevision(),
		})

		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, c)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
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
		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the counter already exists and partition ID matches but is expired, expect both job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

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

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "0",
		})
		require.NoError(t, err)
		c, ok, err := New(t.Context(), Options{
			Name:           "1",
			Key:            key,
			Client:         client,
			Schedule:       sched,
			Job:            job,
			JobModRevision: presp.Header.GetRevision(),
		})

		require.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, c)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("if the counter doesn't exist, create new counter and update next but don't write, return true", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

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

		_, err = client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)

		key, err := key.New(key.Options{
			Namespace: "abc",
			ID:        "0",
		})
		require.NoError(t, err)
		counter, ok, err := New(t.Context(), Options{
			Name:     "1",
			Key:      key,
			Client:   client,
			Schedule: sched,
			Job:      job,
		})

		require.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, counter)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})
}

func Test_TriggerSuccess(t *testing.T) {
	t.Parallel()

	t.Run("if tick next is true, expect job be kept and counter to incremented", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

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
		scounter := &stored.Counter{LastTrigger: nil, JobPartitionId: 123}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(scounter)
		require.NoError(t, err)

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		c := &counter{
			client:      client,
			job:         job,
			count:       scounter,
			schedule:    sched,
			jobKey:      "abc/jobs/1",
			counterKey:  "abc/counters/1",
			next:        now,
			modRevision: presp.Header.GetRevision(),
		}

		ok, err := c.TriggerSuccess(t.Context())
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		counterBytes, err = proto.Marshal(&stored.Counter{
			LastTrigger:    timestamppb.New(now),
			JobPartitionId: 123,
			Count:          1,
		})
		require.NoError(t, err)

		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if tick next is false, expect job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		scounter := &stored.Counter{
			LastTrigger:    nil,
			JobPartitionId: 123,
			Count:          0,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(scounter)
		require.NoError(t, err)

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		c := &counter{
			client:      client,
			job:         job,
			next:        now,
			count:       scounter,
			schedule:    sched,
			jobKey:      "abc/jobs/1",
			counterKey:  "abc/counters/1",
			modRevision: presp.Header.GetRevision(),
		}

		ok, err := c.TriggerSuccess(t.Context())
		require.NoError(t, err)
		assert.False(t, ok)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)
	})

	t.Run("The number of attempts on the counter should always be reset to 0 when Trigger is called", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

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
		scounter := &stored.Counter{LastTrigger: nil, JobPartitionId: 123, Attempts: 456}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(scounter)
		require.NoError(t, err)

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		c := &counter{
			client:      client,
			job:         job,
			count:       scounter,
			schedule:    sched,
			jobKey:      "abc/jobs/1",
			counterKey:  "abc/counters/1",
			next:        now,
			modRevision: presp.Header.GetRevision(),
		}

		ok, err := c.TriggerSuccess(t.Context())
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
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

		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})
}

func Test_tickNext(t *testing.T) {
	t.Parallel()

	t.Run("if the updateNext returns true, expect no delete", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		scounter := &stored.Counter{LastTrigger: nil, JobPartitionId: 123}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(scounter)
		require.NoError(t, err)

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		c := &counter{
			client:      client,
			job:         job,
			count:       scounter,
			schedule:    sched,
			jobKey:      "abc/jobs/1",
			counterKey:  "abc/counters/1",
			modRevision: presp.Header.GetRevision(),
		}

		ok, err := c.tickNext()
		require.NoError(t, err)
		assert.True(t, ok)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, jobBytes, resp.Kvs[0].Value)

		resp, err = client.Get(t.Context(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, counterBytes, resp.Kvs[0].Value)
	})

	t.Run("if the updateNext returns false, expect delete", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		now := time.Now().UTC()

		job := &stored.Job{
			Begin: &stored.Job_Start{
				Start: timestamppb.New(now),
			},
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		}
		scounter := &stored.Counter{
			LastTrigger:    timestamppb.New(now),
			JobPartitionId: 123,
			Count:          1,
		}

		sched, err := scheduler.NewBuilder().Schedule(job)
		require.NoError(t, err)

		jobBytes, err := proto.Marshal(job)
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(scounter)
		require.NoError(t, err)

		presp, err := client.Put(t.Context(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		c := &counter{
			client:      client,
			job:         job,
			count:       scounter,
			schedule:    sched,
			jobKey:      "abc/jobs/1",
			counterKey:  "abc/counters/1",
			modRevision: presp.Header.GetRevision(),
		}

		ok, err := c.tickNext()
		require.NoError(t, err)
		assert.False(t, ok)

		resp, err := client.Get(t.Context(), "abc/jobs/1")
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		resp, err = client.Get(t.Context(), "abc/counters/1")
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
		counter *counter
		exp     bool
		expNext time.Time
	}{
		"if the number of counts is the same as repeats return false": {
			counter: &counter{
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
			counter: &counter{
				schedule: repeats,
				job: &stored.Job{Job: &api.Job{
					Repeats: ptr.Of(uint32(4)),
				}},
				count: &stored.Counter{Count: 5},
			},
			exp: false,
		},
		"if the last trigger time if the same as the expiry, expect false": {
			counter: &counter{
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
			counter: &counter{
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
			counter: &counter{
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
			counter: &counter{
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
			counter: &counter{
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

	now := time.Now().UTC()

	marshalCounter := func(c *stored.Counter) string {
		t.Helper()
		counterBytes, err := proto.Marshal(c)
		require.NoError(t, err)
		return string(counterBytes)
	}

	tests := map[string]struct {
		job             *api.Job
		count           uint32
		attempts        uint32
		next            *time.Time
		lastTriggerTime *timestamppb.Timestamp
		exp             bool
		expNext         *time.Time
		expPut          *clientapi.PutIfOtherHasRevisionOpts
		expDel          *clientapi.DeleteBothIfOtherHasRevisionOpts
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
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
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					LastTrigger: timestamppb.New(now),
				}),
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
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
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					LastTrigger: timestamppb.New(now),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
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
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					LastTrigger: timestamppb.New(now),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
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
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       2,
					LastTrigger: timestamppb.New(now.Add(time.Second)),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
		},
		"failure policy Constant default, nil interval and nil max retries, one shot, expect true with Put and next now": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: nil, MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 1,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"failure policy Constant default, nil interval and 0 max retries, one shot, expect false with del": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: nil, MaxRetries: ptr.Of(uint32(0)),
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
		},
		"failure policy Constant default, nil interval and 0 max retries, schedule, expect true with count forward": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: nil, MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"failure policy Constant default, nil interval and 0 max retries, schedule but at expiration, expect false with job delete": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				Repeats:  ptr.Of(uint32(3)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: nil, MaxRetries: ptr.Of(uint32(0)),
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
		},
		"failure policy Constant default, 3s interval and nil max retries, one shot, expect true with Put and next now+3s": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 3)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 1,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},

			expDel: nil,
		},
		"failure policy Constant default, 3s interval and nil max retries, schedule, expect true with Put and next now+3s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 3)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 1,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"failure policy Constant default, 3s interval and nil max retries, schedule but at expiration, expect true with Put and next now+9": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				Repeats:  ptr.Of(uint32(3)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    2,
			attempts: 0,
			next:     ptr.Of(now.Add(time.Second * 6)),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 9)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    2,
					Attempts: 1,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"failure policy Constant default, 3s interval and nil max retries, schedule attempt 5, expect true with Put and next now+3*5": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 3), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 4,
			next:     ptr.Of(now.Add(time.Second * 3 * 4)),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 3 * 5)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 5,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"failure policy Constant default, 0s interval and nil max retries, schedule attempt 5, expect true with Put and next now": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(0), MaxRetries: nil,
						},
					},
				},
			},
			count:    0,
			attempts: 4,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 5,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 0 max retries with 0 attempts, schedule, expect true with Put and next now+5s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(0)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 0 max retries with 0 attempts, one shot, expect false with del job:": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(0)),
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
		},
		"if failure policy is constant with 5s interval and 1 max retries with 0 attempts, schedule, expect true with Put and next now+5s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 5)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 1,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 1 max retries with 0 attempts, oneshot, expect true with Put and next now+5s": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 0,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 5)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 1,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 1 max retries with 1 attempts, schedule, expect true with count forward and next now+1s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
						},
					},
				},
			},
			count:    0,
			attempts: 1,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 1 max retries with 1 attempts, oneshot, expect false with del job": {
			job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(1)),
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
			expDel: &clientapi.DeleteBothIfOtherHasRevisionOpts{
				Key:           "abc/counters/1",
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
		},
		"if failure policy is constant with 5s interval and 2 max retries with 2 attempts, schedule, expect true with count forward and next now+1s": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(2)),
						},
					},
				},
			},
			count:    0,
			attempts: 2,
			next:     ptr.Of(now),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       1,
					Attempts:    0,
					LastTrigger: timestamppb.New(now),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 3 max retries with 2 attempts, schedule, expect true with retry": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(3)),
						},
					},
				},
			},
			count:    0,
			attempts: 2,
			next:     ptr.Of(now.Add(time.Second * 5 * 2)),
			exp:      true,
			expNext:  ptr.Of(now.Add(time.Second * 5 * 3)),
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:    0,
					Attempts: 3,
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},
			expDel: nil,
		},
		"if failure policy is constant with 5s interval and 2 max retries with 2 attempts, count 3, schedule, expect true with counter forward": {
			job: &api.Job{
				DueTime:  ptr.Of(now.Format(time.RFC3339)),
				Schedule: ptr.Of("@every 1s"),
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{
						Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(2)),
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
			expPut: &clientapi.PutIfOtherHasRevisionOpts{
				Key: "abc/counters/1",
				Val: marshalCounter(&stored.Counter{
					Count:       4,
					LastTrigger: timestamppb.New(now.Add((time.Second * 4))),
				}),
				OtherKey:      "abc/jobs/1",
				OtherRevision: 123,
			},

			expDel: nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var gotDel *clientapi.DeleteBothIfOtherHasRevisionOpts
			var gotPut *clientapi.PutIfOtherHasRevisionOpts
			client := fake.New().
				WithPutIfOtherHasRevisionFn(func(_ context.Context, opts clientapi.PutIfOtherHasRevisionOpts) (bool, error) {
					if test.expPut == nil {
						assert.Fail(t, "unexpected put call")
					}
					gotPut = &opts
					return true, nil
				}).
				WithDeleteBothIfOtherHasRevisionFn(func(_ context.Context, opts clientapi.DeleteBothIfOtherHasRevisionOpts) error {
					if test.expDel == nil {
						assert.Fail(t, "unexpected delete call")
					}

					gotDel = &opts
					return nil
				})

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

			counter := &counter{
				jobKey:     "abc/jobs/1",
				counterKey: "abc/counters/1",
				client:     client,
				count: &stored.Counter{
					Attempts:    test.attempts,
					Count:       test.count,
					LastTrigger: test.lastTriggerTime,
				},
				job:         job,
				next:        next,
				schedule:    sched,
				modRevision: 123,
			}

			ok, err := counter.TriggerFailed(t.Context())
			require.NoError(t, err)
			assert.Equal(t, test.exp, ok)
			if test.expDel == nil {
				assert.Equal(t, *test.expNext, counter.next, "next")
			}
			assert.Equal(t, test.expDel, gotDel)
			assert.Equal(t, test.expPut, gotPut)
		})
	}
}

func Test_TriggerFailureSuccess(t *testing.T) {
	t.Parallel()

	var putCall, delCall atomic.Uint32
	var count, del atomic.Value
	client := fake.New().
		WithPutIfOtherHasRevisionFn(func(_ context.Context, opts clientapi.PutIfOtherHasRevisionOpts) (bool, error) {
			putCall.Add(1)
			var counterAPI stored.Counter
			require.NoError(t, proto.Unmarshal([]byte(opts.Val), &counterAPI))
			count.Store(&counterAPI)
			return true, nil
		}).
		WithDeleteBothIfOtherHasRevisionFn(func(_ context.Context, opts clientapi.DeleteBothIfOtherHasRevisionOpts) error {
			delCall.Add(1)
			del.Store([]string{opts.Key, opts.OtherKey})
			return nil
		})

	now := time.Now().UTC()

	job := &stored.Job{
		Begin: &stored.Job_DueTime{DueTime: timestamppb.New(now)},
		Job: &api.Job{
			DueTime:  ptr.Of(now.Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(3)),
			FailurePolicy: &api.FailurePolicy{
				Policy: &api.FailurePolicy_Constant{
					Constant: &api.FailurePolicyConstant{
						Interval: durationpb.New(time.Second * 5), MaxRetries: ptr.Of(uint32(2)),
					},
				},
			},
		},
	}

	sched, err := scheduler.NewBuilder().Schedule(job)
	require.NoError(t, err)

	counter := &counter{
		jobKey:     "abc/jobs/1",
		counterKey: "abc/counters/1",
		client:     client,
		count:      new(stored.Counter),
		job:        job,
		next:       now,
		schedule:   sched,
	}

	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 0, Attempts: 0, LastTrigger: nil,
	}))
	assert.Equal(t, uint32(0), putCall.Load())

	ok, err := counter.TriggerSuccess(t.Context())
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

	ok, err = counter.TriggerFailed(t.Context())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*6), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 1, Attempts: 1, LastTrigger: timestamppb.New(now),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 1, Attempts: 1, LastTrigger: timestamppb.New(now),
	}))
	ok, err = counter.TriggerFailed(t.Context())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*11), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 1, Attempts: 2, LastTrigger: timestamppb.New(now),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 1, Attempts: 2, LastTrigger: timestamppb.New(now),
	}))
	ok, err = counter.TriggerSuccess(t.Context())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*2), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 2, Attempts: 0, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 2, Attempts: 0, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))

	ok, err = counter.TriggerFailed(t.Context())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, now.Add(time.Second*7), counter.next)
	assert.True(t, proto.Equal(counter.count, &stored.Counter{
		Count: 2, Attempts: 1, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	assert.True(t, proto.Equal(count.Load().(*stored.Counter), &stored.Counter{
		Count: 2, Attempts: 1, LastTrigger: timestamppb.New(now.Add(time.Second)),
	}))
	ok, err = counter.TriggerFailed(t.Context())
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
	ok, err = counter.TriggerFailed(t.Context())
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, uint32(1), delCall.Load())
	assert.Equal(t, []string{"abc/counters/1", "abc/jobs/1"}, del.Load())
}
