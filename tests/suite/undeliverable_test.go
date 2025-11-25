/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/concurrency/slice"
	"github.com/dapr/kit/ptr"
	"github.com/dapr/kit/slices"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_undeliverable(t *testing.T) {
	t.Parallel()

	t.Run("single: jobs which are marked as undeliverable, should be triggered when their prefix is registered", func(t *testing.T) {
		t.Parallel()

		var got []string
		var lock sync.Mutex
		ret := api.TriggerResponseResult_UNDELIVERABLE
		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				lock.Lock()
				defer lock.Unlock()
				got = append(got, req.GetName())
				fn(&api.TriggerResponse{Result: ret})
			},
		})

		job := &api.Job{
			Schedule: ptr.Of("@every 1h"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}
		names := make([]string, 100)
		for i := range 100 {
			names[i] = "abc" + strconv.Itoa(i)
			require.NoError(t, cron.API().Add(cron.Context(), names[i], job))
		}
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 100, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		assert.ElementsMatch(t, names, got)

		lock.Lock()
		ret = api.TriggerResponseResult_SUCCESS
		lock.Unlock()

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 200, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		//nolint:makezero
		assert.ElementsMatch(t, append(names, names...), got)
	})

	t.Run("multiple: jobs which are marked as undeliverable, should be triggered when their prefix is registered", func(t *testing.T) {
		t.Parallel()

		var got []string
		var lock sync.Mutex
		ret := api.TriggerResponseResult_UNDELIVERABLE
		cron := integration.New(t, integration.Options{
			Instances: 4,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				lock.Lock()
				defer lock.Unlock()
				got = append(got, req.GetName())
				fn(&api.TriggerResponse{Result: ret})
			},
		})

		job := &api.Job{
			Schedule: ptr.Of("@every 1h"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}
		names := make([]string, 100)
		for i := range 100 {
			names[i] = "abc" + strconv.Itoa(i)
			require.NoError(t, cron.API().Add(cron.Context(), names[i], job))
		}
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 100, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		assert.ElementsMatch(t, names, got)

		lock.Lock()
		ret = api.TriggerResponseResult_SUCCESS
		lock.Unlock()

		for _, api := range cron.AllCrons() {
			cancel, err := api.DeliverablePrefixes(cron.Context(), "abc")
			require.NoError(t, err)
			t.Cleanup(func() { cancel(assert.AnError) })
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 200, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		//nolint:makezero
		assert.ElementsMatch(t, append(names, names...), got)
	})

	t.Run("single: some jobs should be re-enqueued based on the prefix", func(t *testing.T) {
		t.Parallel()

		var got []string
		var lock sync.Mutex
		ret := api.TriggerResponseResult_UNDELIVERABLE
		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				lock.Lock()
				defer lock.Unlock()
				got = append(got, req.GetName())
				fn(&api.TriggerResponse{Result: ret})
			},
		})

		job := &api.Job{
			Schedule: ptr.Of("@every 1h"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", job))
		require.NoError(t, cron.API().Add(cron.Context(), "abc2", job))
		require.NoError(t, cron.API().Add(cron.Context(), "def3", job))
		require.NoError(t, cron.API().Add(cron.Context(), "def4", job))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 4, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		assert.ElementsMatch(t, []string{"abc1", "abc2", "def3", "def4"}, got)

		lock.Lock()
		ret = api.TriggerResponseResult_SUCCESS
		lock.Unlock()

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 6, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		assert.ElementsMatch(t, []string{
			"abc1", "abc2", "def3", "def4",
			"abc1", "abc2",
		}, got)
	})

	t.Run("multiple: some jobs should be re-enqueued based on the prefix", func(t *testing.T) {
		t.Parallel()

		var got []string
		var lock sync.Mutex
		ret := api.TriggerResponseResult_UNDELIVERABLE
		cron := integration.New(t, integration.Options{
			Instances: 4,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				lock.Lock()
				defer lock.Unlock()
				got = append(got, req.GetName())
				fn(&api.TriggerResponse{Result: ret})
			},
		})

		job := &api.Job{
			Schedule: ptr.Of("@every 1h"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", job))
		require.NoError(t, cron.API().Add(cron.Context(), "abc2", job))
		require.NoError(t, cron.API().Add(cron.Context(), "def3", job))
		require.NoError(t, cron.API().Add(cron.Context(), "def4", job))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 4, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		assert.ElementsMatch(t, []string{"abc1", "abc2", "def3", "def4"}, got)

		lock.Lock()
		ret = api.TriggerResponseResult_SUCCESS
		lock.Unlock()

		for _, api := range cron.AllCrons() {
			cancel, err := api.DeliverablePrefixes(cron.Context(), "abc")
			require.NoError(t, err)
			t.Cleanup(func() { cancel(assert.AnError) })
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 6, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
		assert.ElementsMatch(t, []string{
			"abc1", "abc2", "def3", "def4",
			"abc1", "abc2",
		}, got)
	})

	t.Run("should redeliver immediately if prefix added during trigger", func(t *testing.T) {
		t.Parallel()

		var inTrigger atomic.Uint32
		var gotFn func(*api.TriggerResponse)
		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				gotFn = fn
				inTrigger.Add(1)
			},
		})

		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{
			DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
		}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(1), inTrigger.Load())
		}, time.Second*10, time.Millisecond*10)

		resp, err := cron.API().Get(cron.Context(), "abc1")
		require.NoError(t, err)
		assert.NotNil(t, resp)

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })
		gotFn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, inTrigger.Load(), uint32(2))
		}, time.Second*10, time.Millisecond*10)

		gotFn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(cron.Context(), "abc1")
			require.NoError(t, err)
			assert.Nil(c, resp)
		}, time.Second*10, time.Millisecond*10)
	})

	t.Run("ignore prefix if return SUCCESS", func(t *testing.T) {
		t.Parallel()

		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			},
		})

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })

		require.NoError(t, cron.API().Add(cron.Context(), "def1", &api.Job{
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			Repeats:  ptr.Of(uint32(2)),
			Schedule: ptr.Of("@every 1s"),
		}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(cron.Context(), "def1")
			require.NoError(t, err)
			assert.Nil(c, resp)
		}, time.Second*10, time.Millisecond*10)
	})

	t.Run("ignore prefix if return FAILURE", func(t *testing.T) {
		t.Parallel()

		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_FAILED})
			},
		})

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })

		require.NoError(t, cron.API().Add(cron.Context(), "def1", &api.Job{
			DueTime:       ptr.Of(time.Now().Format(time.RFC3339)),
			Repeats:       ptr.Of(uint32(2)),
			Schedule:      ptr.Of("@every 1s"),
			FailurePolicy: &api.FailurePolicy{Policy: new(api.FailurePolicy_Drop)},
		}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, cron.Triggered())
			resp, err := cron.API().Get(cron.Context(), "def1")
			require.NoError(t, err)
			assert.Nil(c, resp)
		}, time.Second*10, time.Millisecond*10)
	})

	t.Run("single: load jobs from db which are undeliverable should be re-tried when deliverable", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		jobBytes, err := proto.Marshal(&stored.Job{
			Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(time.Now())},
			PartitionId: 123,
			Job:         &api.Job{DueTime: ptr.Of(time.Now().Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/jobs/helloworld", string(jobBytes))
		require.NoError(t, err)

		var inTrigger atomic.Uint32
		var gotFn func(*api.TriggerResponse)
		var ret atomic.Value
		ret.Store(api.TriggerResponseResult_UNDELIVERABLE)
		cron := integration.New(t, integration.Options{
			Instances: 1,
			Client:    client,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				gotFn = fn
				inTrigger.Add(1)
			},
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(1), inTrigger.Load())
		}, time.Second*10, time.Millisecond*10)
		gotFn(&api.TriggerResponse{Result: ret.Load().(api.TriggerResponseResult)})
		<-time.After(time.Second)
		assert.Equal(t, uint32(1), inTrigger.Load())

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "hello")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(2), inTrigger.Load())
		}, time.Second*10, time.Millisecond*10)
		ret.Store(api.TriggerResponseResult_SUCCESS)
		gotFn(&api.TriggerResponse{Result: ret.Load().(api.TriggerResponseResult)})
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
	})

	t.Run("multiple: load jobs from db which are undeliverable should be re-tried when deliverable", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		jobBytes, err := proto.Marshal(&stored.Job{
			Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(time.Now())},
			PartitionId: 123,
			Job:         &api.Job{DueTime: ptr.Of(time.Now().Format(time.RFC3339))},
		})
		require.NoError(t, err)
		_, err = client.Put(t.Context(), "abc/jobs/helloworld", string(jobBytes))
		require.NoError(t, err)

		var inTrigger atomic.Uint32
		var gotFn func(*api.TriggerResponse)
		cron := integration.New(t, integration.Options{
			Instances: 5,
			Client:    client,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				gotFn = fn
				inTrigger.Add(1)
			},
		})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(1), inTrigger.Load())
		}, time.Second*10, time.Millisecond*10)
		gotFn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
		<-time.After(time.Second)
		assert.Equal(t, uint32(1), inTrigger.Load())

		for _, api := range cron.AllCrons() {
			cancel, err := api.DeliverablePrefixes(cron.Context(), "hello")
			require.NoError(t, err)
			t.Cleanup(func() { cancel(assert.AnError) })
		}

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(2), inTrigger.Load())
		}, time.Second*10, time.Millisecond*10)
		gotFn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, cron.Triggered())
		}, time.Second*10, time.Millisecond*10)
	})

	t.Run("keep delivering undeliverable until cancel called", func(t *testing.T) {
		t.Parallel()

		var inTrigger atomic.Uint32
		var gotFn func(*api.TriggerResponse)

		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				gotFn = fn
				inTrigger.Add(1)
			},
		})

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)

		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{
			Schedule: ptr.Of("@every 1h"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}))

		for i := range uint32(10) {
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, i+1, inTrigger.Load())
			}, time.Second*10, time.Millisecond*10)
			gotFn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
		}

		<-time.After(time.Second)
		trigger := inTrigger.Load()
		cancel(assert.AnError)
		<-time.After(time.Second)
		assert.Equal(t, trigger, inTrigger.Load())
	})

	t.Run("Deleting a staged job should not be triggered once it has been marked as deliverable", func(t *testing.T) {
		t.Parallel()

		triggered := slice.String()
		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				if triggered.Append(req.GetName()) <= 2 {
					fn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
					return
				}
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			},
		})

		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}))
		require.NoError(t, cron.API().Add(cron.Context(), "xyz1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}))
		require.NoError(t, cron.API().Delete(cron.Context(), "abc1"))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c, []string{"abc1", "xyz1"}, triggered.Slice())
		}, time.Second*10, time.Millisecond*10)

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })
		time.Sleep(time.Second * 2)
		assert.ElementsMatch(t, []string{"abc1", "xyz1"}, triggered.Slice())
	})

	t.Run("Deleting prefixed staged jobs should not be triggered once it has been marked as deliverable", func(t *testing.T) {
		t.Parallel()

		triggered := slice.String()
		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				triggered.Append(req.GetName())
				if req.GetName() == "abc1" || req.GetName() == "def1" {
					fn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
					return
				}
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			},
		})

		jobTmpl := func() *api.Job {
			return &api.Job{
				Schedule: ptr.Of("@every 2s"),
				DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
			}
		}
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", jobTmpl()))
		require.NoError(t, cron.API().Add(cron.Context(), "def1", jobTmpl()))
		require.NoError(t, cron.API().Add(cron.Context(), "xyz1", jobTmpl()))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			triggered.Store(slices.Deduplicate(triggered.Slice())...)
			assert.ElementsMatch(c, []string{"abc1", "def1", "xyz1"}, triggered.Slice())
		}, time.Second*10, time.Millisecond*10)

		require.NoError(t, cron.API().DeletePrefixes(cron.Context(), "abc", "def"))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c, []string{"abc1", "def1", "xyz1", "xyz1"}, triggered.Slice())
		}, time.Second*10, time.Millisecond*10)

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc", "def")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c, []string{"abc1", "def1", "xyz1", "xyz1", "xyz1"}, triggered.Slice())
		}, time.Second*10, time.Millisecond*10)
	})

	t.Run("Re-scheduling the job should not trigger the old staged job when prefix is added", func(t *testing.T) {
		t.Parallel()

		var ret atomic.Value
		var triggered atomic.Uint32
		ret.Store(api.TriggerResponseResult_UNDELIVERABLE)

		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				triggered.Add(1)
				fn(&api.TriggerResponse{Result: ret.Load().(api.TriggerResponseResult)})
			},
		})

		dueTime := ptr.Of(time.Now().Format(time.RFC3339))
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{DueTime: dueTime}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(1), triggered.Load())
		}, time.Second*10, time.Millisecond*10)

		ret.Store(api.TriggerResponseResult_SUCCESS)
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{DueTime: dueTime}))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(2), triggered.Load())
		}, time.Second*10, time.Millisecond*10)

		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })
		time.Sleep(time.Second * 2)
		assert.Equal(t, uint32(2), triggered.Load())
	})

	t.Run("Re-scheduling the job after multiple puts should not trigger the old staged job when prefix is added", func(t *testing.T) {
		t.Parallel()

		var ret atomic.Value
		var triggered atomic.Uint32
		ret.Store(api.TriggerResponseResult_UNDELIVERABLE)

		cron := integration.New(t, integration.Options{
			Instances: 1,
			TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				triggered.Add(1)
				fn(&api.TriggerResponse{Result: ret.Load().(api.TriggerResponseResult)})
			},
		})

		dueTime := ptr.Of(time.Now().Format(time.RFC3339))
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{DueTime: dueTime}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(1), triggered.Load())
		}, time.Second*10, time.Millisecond*10)

		ret.Store(api.TriggerResponseResult_SUCCESS)
		cancel, err := cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(2), triggered.Load())
		}, time.Second*10, time.Millisecond*10)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := cron.API().Get(cron.Context(), "abc1")
			assert.NoError(c, err)
			assert.Nil(c, resp)
		}, time.Second*10, time.Millisecond*10)

		cancel(assert.AnError)

		ret.Store(api.TriggerResponseResult_UNDELIVERABLE)
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{DueTime: dueTime}))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(3), triggered.Load())
		}, time.Second*10, time.Millisecond*10)

		ret.Store(api.TriggerResponseResult_SUCCESS)
		require.NoError(t, cron.API().Add(cron.Context(), "abc1", &api.Job{DueTime: dueTime}))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, uint32(4), triggered.Load())
		}, time.Second*10, time.Millisecond*10)
		resp, err := cron.API().Get(cron.Context(), "abc1")
		require.NoError(t, err)
		assert.Nil(t, resp)

		cancel, err = cron.API().DeliverablePrefixes(cron.Context(), "abc")
		require.NoError(t, err)
		t.Cleanup(func() { cancel(assert.AnError) })
		time.Sleep(time.Second * 2)
		assert.Equal(t, uint32(4), triggered.Load())
	})
}
