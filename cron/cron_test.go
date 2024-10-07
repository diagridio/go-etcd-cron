/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Run(t *testing.T) {
	t.Parallel()

	t.Run("Running multiple times should error", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)
		var triggered atomic.Int64
		cronI, err := New(Options{
			Log:            logr.Discard(),
			Client:         client,
			Namespace:      "abc",
			PartitionID:    0,
			PartitionTotal: 1,
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
				triggered.Add(1)
				return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
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

func Test_zeroDueTime(t *testing.T) {
	t.Parallel()

	helper := testCron(t, 1)

	require.NoError(t, helper.api.Add(helper.ctx, "yoyo", &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of("0s"),
	}))
	assert.Eventually(t, func() bool {
		return helper.triggered.Load() == 1
	}, 3*time.Second, time.Millisecond*10)

	require.NoError(t, helper.api.Add(helper.ctx, "yoyo2", &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of("1s"),
	}))
	assert.Eventually(t, func() bool {
		return helper.triggered.Load() == 2
	}, 3*time.Second, time.Millisecond*10)

	require.NoError(t, helper.api.Add(helper.ctx, "yoyo3", &api.Job{
		Schedule: ptr.Of("@every 1h"),
	}))
	<-time.After(2 * time.Second)
	assert.Equal(t, int64(2), helper.triggered.Load())
}

func Test_parallel(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		total uint32
	}{
		{"1 queue", 1},
		{"multi queue", 50},
	} {
		total := test.total
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			releaseCh := make(chan struct{})
			var waiting atomic.Int32
			var done atomic.Int32
			helper := testCronWithOptions(t, testCronOptions{
				total: total,
				triggerFn: func(*api.TriggerRequest) bool {
					waiting.Add(1)
					<-releaseCh
					done.Add(1)
					return true
				},
			})

			for i := range 100 {
				require.NoError(t, helper.api.Add(helper.ctx, strconv.Itoa(i), &api.Job{
					DueTime: ptr.Of("0s"),
				}))
			}

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, int32(100), waiting.Load())
			}, 5*time.Second, 10*time.Millisecond)
			close(releaseCh)
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, int32(100), done.Load())
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func Test_schedule(t *testing.T) {
	t.Parallel()

	t.Run("if no counter, job should not be deleted and no counter created", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)

		now := time.Now().UTC()
		jobBytes1, err := proto.Marshal(&stored.Job{
			Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(now.Add(time.Hour))},
			PartitionId: 123,
			Job:         &api.Job{DueTime: ptr.Of(now.Add(time.Hour).Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes1))
		require.NoError(t, err)

		jobBytes2, err := proto.Marshal(&stored.Job{
			Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(now)},
			PartitionId: 123,
			Job:         &api.Job{DueTime: ptr.Of(now.Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/jobs/2", string(jobBytes2))
		require.NoError(t, err)

		resp, err := client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(t, resp.Kvs, 2)

		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: client,
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, int64(1), cron.triggered.Load())
		}, 5*time.Second, 10*time.Millisecond)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err = client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
			require.NoError(t, err)
			assert.Len(c, resp.Kvs, 1)
		}, 5*time.Second, 10*time.Millisecond)

		cron.closeCron()

		resp, err = client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(jobBytes1), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/counters", clientv3.WithPrefix())
		require.NoError(t, err)
		require.Empty(t, resp.Kvs)

		assert.Equal(t, int64(1), cron.triggered.Load())
	})

	t.Run("if schedule is not done, job and counter should not be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)

		future := time.Now().UTC().Add(time.Hour)
		jobBytes, err := proto.Marshal(&stored.Job{
			Begin: &stored.Job_DueTime{
				DueTime: timestamppb.New(future),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(future.Format(time.RFC3339)),
			},
		})
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(&stored.Counter{
			LastTrigger:    nil,
			Count:          0,
			JobPartitionId: 123,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		now := time.Now().UTC()
		jobBytes2, err := proto.Marshal(&stored.Job{
			Begin: &stored.Job_DueTime{DueTime: timestamppb.New(now)},
			Job:   &api.Job{DueTime: ptr.Of(now.Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/jobs/2", string(jobBytes2))
		require.NoError(t, err)

		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: client,
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, int64(1), cron.triggered.Load())
		}, 5*time.Second, 10*time.Millisecond)

		resp, err := client.Get(context.Background(), "abc/jobs/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(jobBytes), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/counters/1")
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		assert.Equal(t, string(counterBytes), string(resp.Kvs[0].Value))

		resp, err = client.Get(context.Background(), "abc/jobs", clientv3.WithPrefix())
		require.NoError(t, err)
		assert.Len(t, resp.Kvs, 1)
	})

	t.Run("if schedule is done, expect job and counter to be deleted", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCDBareClient(t)

		now := time.Now().UTC()
		jobBytes, err := proto.Marshal(&stored.Job{
			Begin: &stored.Job_DueTime{
				DueTime: timestamppb.New(now),
			},
			PartitionId: 123,
			Job: &api.Job{
				DueTime: ptr.Of(now.Format(time.RFC3339)),
			},
		})
		require.NoError(t, err)
		counterBytes, err := proto.Marshal(&stored.Counter{
			LastTrigger:    timestamppb.New(now),
			Count:          1,
			JobPartitionId: 123,
		})
		require.NoError(t, err)

		_, err = client.Put(context.Background(), "abc/jobs/1", string(jobBytes))
		require.NoError(t, err)
		_, err = client.Put(context.Background(), "abc/counters/1", string(counterBytes))
		require.NoError(t, err)

		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: client,
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Get(context.Background(), "abc/jobs/1")
			require.NoError(t, err)
			assert.Empty(c, resp.Kvs)
			resp, err = client.Get(context.Background(), "abc/counters/1")
			require.NoError(t, err)
			assert.Empty(c, resp.Kvs)
		}, 5*time.Second, 10*time.Millisecond)

		assert.Equal(t, int64(0), cron.triggered.Load())
	})
}

func Test_jobWithSpace(t *testing.T) {
	t.Parallel()

	cron := testCronWithOptions(t, testCronOptions{
		total:  1,
		client: tests.EmbeddedETCDBareClient(t),
	})

	require.NoError(t, cron.api.Add(context.Background(), "hello world", &api.Job{
		DueTime: ptr.Of(time.Now().Add(2).Format(time.RFC3339)),
	}))
	resp, err := cron.api.Get(context.Background(), "hello world")
	require.NoError(t, err)
	assert.NotNil(t, resp)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(1), cron.triggered.Load())
		resp, err = cron.api.Get(context.Background(), "hello world")
		assert.NoError(c, err)
		assert.Nil(c, resp)
	}, time.Second*10, time.Millisecond*10)

	require.NoError(t, cron.api.Add(context.Background(), "another hello world", &api.Job{
		Schedule: ptr.Of("@every 1s"),
	}))
	resp, err = cron.api.Get(context.Background(), "another hello world")
	require.NoError(t, err)
	assert.NotNil(t, resp)
	listresp, err := cron.api.List(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, listresp.GetJobs(), 1)
	require.NoError(t, cron.api.Delete(context.Background(), "another hello world"))
	resp, err = cron.api.Get(context.Background(), "another hello world")
	require.NoError(t, err)
	assert.Nil(t, resp)
	listresp, err = cron.api.List(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, listresp.GetJobs())
}

func Test_FailurePolicy(t *testing.T) {
	t.Parallel()

	t.Run("default policy should retry 3 times with a 1sec interval", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: tests.EmbeddedETCDBareClient(t),
			triggerFn: func(*api.TriggerRequest) bool {
				assert.GreaterOrEqual(t, uint32(8), got.Add(1))
				return false
			},
			gotCh: gotCh,
		})

		require.NoError(t, cron.api.Add(context.Background(), "test", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(2)),
		}))

		for range 8 {
			resp, err := cron.api.Get(context.Background(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.api.Get(context.Background(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("drop policy should not retry triggering", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: tests.EmbeddedETCDBareClient(t),
			triggerFn: func(*api.TriggerRequest) bool {
				assert.GreaterOrEqual(t, uint32(2), got.Add(1))
				return false
			},
			gotCh: gotCh,
		})

		require.NoError(t, cron.api.Add(context.Background(), "test", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(2)),
			FailurePolicy: &api.FailurePolicy{
				Policy: new(api.FailurePolicy_Drop),
			},
		}))

		for range 2 {
			resp, err := cron.api.Get(context.Background(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.api.Get(context.Background(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("constant policy should only retry when it fails ", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: tests.EmbeddedETCDBareClient(t),
			triggerFn: func(*api.TriggerRequest) bool {
				assert.GreaterOrEqual(t, uint32(5), got.Add(1))
				return got.Load() == 3
			},
			gotCh: gotCh,
		})

		require.NoError(t, cron.api.Add(context.Background(), "test", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(3)),
			FailurePolicy: &api.FailurePolicy{
				Policy: &api.FailurePolicy_Constant{
					Constant: &api.FailurePolicyConstant{
						Interval: durationpb.New(time.Millisecond), MaxRetries: ptr.Of(uint32(1)),
					},
				},
			},
		}))

		for range 5 {
			resp, err := cron.api.Get(context.Background(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.api.Get(context.Background(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})

	t.Run("constant policy can retry forever until it succeeds", func(t *testing.T) {
		t.Parallel()

		gotCh := make(chan *api.TriggerRequest, 1)
		var got atomic.Uint32
		cron := testCronWithOptions(t, testCronOptions{
			total:  1,
			client: tests.EmbeddedETCDBareClient(t),
			triggerFn: func(*api.TriggerRequest) bool {
				assert.GreaterOrEqual(t, uint32(100), got.Add(1))
				return got.Load() == 100
			},
			gotCh: gotCh,
		})

		require.NoError(t, cron.api.Add(context.Background(), "test", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
			FailurePolicy: &api.FailurePolicy{
				Policy: &api.FailurePolicy_Constant{
					Constant: &api.FailurePolicyConstant{
						Interval: durationpb.New(time.Millisecond),
					},
				},
			},
		}))

		for range 100 {
			resp, err := cron.api.Get(context.Background(), "test")
			require.NoError(t, err)
			assert.NotNil(t, resp)
			select {
			case <-gotCh:
			case <-time.After(time.Second * 3):
				assert.Fail(t, "timeout waiting for trigger")
			}
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.api.Get(context.Background(), "test")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*5, time.Millisecond*10)
	})
}

type testCronOptions struct {
	total     uint32
	gotCh     chan *api.TriggerRequest
	triggerFn func(*api.TriggerRequest) bool
	client    *clientv3.Client
}

type helper struct {
	ctx       context.Context
	closeCron func()
	client    client.Interface
	api       api.Interface
	allCrons  []api.Interface
	triggered *atomic.Int64
}

func testCron(t *testing.T, total uint32) *helper {
	t.Helper()
	return testCronWithOptions(t, testCronOptions{
		total: total,
	})
}

func testCronWithOptions(t *testing.T, opts testCronOptions) *helper {
	t.Helper()

	require.Positive(t, opts.total)
	cl := opts.client
	if cl == nil {
		cl = tests.EmbeddedETCDBareClient(t)
	}

	var triggered atomic.Int64
	var a api.Interface
	allCrns := make([]api.Interface, opts.total)
	for i := range opts.total {
		c, err := New(Options{
			Log:            logr.Discard(),
			Client:         cl,
			Namespace:      "abc",
			PartitionID:    i,
			PartitionTotal: opts.total,
			TriggerFn: func(_ context.Context, req *api.TriggerRequest) bool {
				defer func() { triggered.Add(1) }()
				if opts.gotCh != nil {
					opts.gotCh <- req
				}
				if opts.triggerFn != nil {
					return opts.triggerFn(req)
				}
				return true
			},

			CounterGarbageCollectionInterval: ptr.Of(time.Millisecond * 300),
		})
		require.NoError(t, err)
		allCrns[i] = c
		if i == 0 {
			a = c
		}
	}

	errCh := make(chan error, opts.total)
	ctx, cancel := context.WithCancel(context.Background())

	closeOnce := sync.OnceFunc(func() {
		cancel()
		for range opts.total {
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for cron to stop")
			}
		}
	})
	t.Cleanup(closeOnce)
	for i := range opts.total {
		go func(i uint32) {
			errCh <- allCrns[i].Run(ctx)
		}(i)
	}

	return &helper{
		ctx:       ctx,
		client:    client.New(client.Options{Client: cl, Log: logr.Discard()}),
		api:       a,
		allCrons:  allCrns,
		triggered: &triggered,
		closeCron: closeOnce,
	}
}
