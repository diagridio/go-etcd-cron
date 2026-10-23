/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

// queueStateKey counts jobs named "<namespace>||<app ID>||<id>" under their
// namespace and app ID, and does not count any other job.
func queueStateKey(jobName string) (string, string, bool) {
	parts := strings.Split(jobName, "||")
	if len(parts) != 3 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// sumQueueState adds up the queue state of every replica.
func sumQueueState(crons []api.Interface) (inFlight, staged map[string]map[string]uint64) {
	inFlight = make(map[string]map[string]uint64)
	staged = make(map[string]map[string]uint64)
	add := func(dst, src map[string]map[string]uint64) {
		for ns, apps := range src {
			if dst[ns] == nil {
				dst[ns] = make(map[string]uint64)
			}
			for app, n := range apps {
				dst[ns][app] += n
			}
		}
	}
	for _, c := range crons {
		state := c.GetQueueState()
		add(inFlight, state.InFlight)
		add(staged, state.Staged)
	}
	return inFlight, staged
}

func Test_queueState(t *testing.T) {
	t.Parallel()

	job := func() *api.Job {
		return &api.Job{
			Schedule: ptr.Of("@every 1h"),
			DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		}
	}

	for _, instances := range []uint64{1, 3} {
		t.Run(strconv.FormatUint(instances, 10)+" instances", func(t *testing.T) {
			t.Parallel()

			t.Run("undeliverable jobs are counted as staged until delivered or deleted", func(t *testing.T) {
				t.Parallel()

				var lock sync.Mutex
				ret := api.TriggerResponseResult_UNDELIVERABLE
				cron := integration.New(t, integration.Options{
					Instances:     instances,
					QueueStateKey: queueStateKey,
					TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
						lock.Lock()
						defer lock.Unlock()
						fn(&api.TriggerResponse{Result: ret})
					},
				})

				for i := range 10 {
					require.NoError(t, cron.API().Add(cron.Context(), "ns1||app1||"+strconv.Itoa(i), job()))
				}
				for i := range 5 {
					require.NoError(t, cron.API().Add(cron.Context(), "ns1||app2||"+strconv.Itoa(i), job()))
				}
				require.NoError(t, cron.API().Add(cron.Context(), "not-counted", job()))

				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					inFlight, staged := sumQueueState(cron.AllCrons())
					assert.Empty(c, inFlight)
					assert.Equal(c, map[string]map[string]uint64{
						"ns1": {"app1": 10, "app2": 5},
					}, staged)
				}, time.Second*10, time.Millisecond*10)

				// A job deleted while staged leaves the count.
				require.NoError(t, cron.API().Delete(cron.Context(), "ns1||app2||0"))
				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					_, staged := sumQueueState(cron.AllCrons())
					assert.Equal(c, map[string]map[string]uint64{
						"ns1": {"app1": 10, "app2": 4},
					}, staged)
				}, time.Second*10, time.Millisecond*10)

				// A job updated while staged is counted once, under its new version.
				require.NoError(t, cron.API().Add(cron.Context(), "ns1||app2||1", job()))
				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					_, staged := sumQueueState(cron.AllCrons())
					assert.Equal(c, map[string]map[string]uint64{
						"ns1": {"app1": 10, "app2": 4},
					}, staged)
				}, time.Second*10, time.Millisecond*10)

				lock.Lock()
				ret = api.TriggerResponseResult_SUCCESS
				lock.Unlock()

				for _, c := range cron.AllCrons() {
					cancel, err := c.DeliverablePrefixes(cron.Context(), "ns1||app1||")
					require.NoError(t, err)
					t.Cleanup(func() { cancel(assert.AnError) })
				}

				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					inFlight, staged := sumQueueState(cron.AllCrons())
					assert.Empty(c, inFlight)
					assert.Equal(c, map[string]map[string]uint64{
						"ns1": {"app2": 4},
					}, staged)
				}, time.Second*10, time.Millisecond*10)
			})

			t.Run("triggered jobs are counted in flight until answered, even if deleted meanwhile", func(t *testing.T) {
				t.Parallel()

				var lock sync.Mutex
				var answers []func(*api.TriggerResponse)
				cron := integration.New(t, integration.Options{
					Instances:     instances,
					QueueStateKey: queueStateKey,
					TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
						lock.Lock()
						defer lock.Unlock()
						answers = append(answers, fn)
					},
				})

				for i := range 10 {
					require.NoError(t, cron.API().Add(cron.Context(), "ns1||app1||"+strconv.Itoa(i), job()))
				}
				require.NoError(t, cron.API().Add(cron.Context(), "ns2||app1||0", job()))
				require.NoError(t, cron.API().Add(cron.Context(), "not-counted", job()))

				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					inFlight, staged := sumQueueState(cron.AllCrons())
					assert.Empty(c, staged)
					assert.Equal(c, map[string]map[string]uint64{
						"ns1": {"app1": 10},
						"ns2": {"app1": 1},
					}, inFlight)
				}, time.Second*10, time.Millisecond*10)

				// Deleting a job does not answer its trigger.
				require.NoError(t, cron.API().Delete(cron.Context(), "ns1||app1||0"))
				require.NoError(t, cron.API().Delete(cron.Context(), "ns2||app1||0"))
				time.Sleep(time.Millisecond * 500)
				inFlight, _ := sumQueueState(cron.AllCrons())
				assert.Equal(t, map[string]map[string]uint64{
					"ns1": {"app1": 10},
					"ns2": {"app1": 1},
				}, inFlight)

				lock.Lock()
				for _, answer := range answers {
					answer(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
				}
				lock.Unlock()

				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					inFlight, staged := sumQueueState(cron.AllCrons())
					assert.Empty(c, inFlight)
					assert.Empty(c, staged)
				}, time.Second*10, time.Millisecond*10)
			})
		})
	}
}

func Test_queueState_noKeyFunc(t *testing.T) {
	t.Parallel()

	cron := integration.New(t, integration.Options{
		Instances: 1,
		TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
			fn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
		},
	})

	require.NoError(t, cron.API().Add(cron.Context(), "ns1||app1||0", &api.Job{
		DueTime: ptr.Of(time.Now().Format(time.RFC3339)),
	}))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, cron.Triggered())
	}, time.Second*10, time.Millisecond*10)

	state := cron.API().GetQueueState()
	assert.Empty(t, state.InFlight)
	assert.Empty(t, state.Staged)
}
