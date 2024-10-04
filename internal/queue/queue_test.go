/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package queue

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/informer"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/scheduler"
	"github.com/diagridio/go-etcd-cron/tests"
)

func Test_delete_race(t *testing.T) {
	t.Parallel()

	triggered := make([]atomic.Int64, 20)
	queue := newQueue(t, func(_ context.Context, req *api.TriggerRequest) bool {
		i, err := strconv.Atoi(req.GetName())
		require.NoError(t, err)
		triggered[i].Add(1)
		return true
	})

	jobKeys := make([]string, 20)
	for i := range jobKeys {
		jobKeys[i] = fmt.Sprintf("abc/jobs/%d", i)
		require.NoError(t, queue.HandleInformerEvent(context.Background(), &informer.Event{
			IsPut: true,
			Key:   []byte(jobKeys[i]),
			Job: &stored.Job{
				Begin:       &stored.Job_DueTime{DueTime: timestamppb.New(time.Now())},
				PartitionId: 1,
				Job:         &api.Job{Schedule: ptr.Of("@every 1s")},
			},
		}))
	}

	for i, key := range jobKeys {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			_, ok := queue.cache.Load(key)
			assert.True(t, ok)

			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.GreaterOrEqual(c, triggered[i].Load(), int64(1))
			}, 5*time.Second, time.Millisecond)

			_, ok = queue.cache.Load(key)
			assert.True(t, ok)

			require.NoError(t, queue.HandleInformerEvent(context.Background(), &informer.Event{
				IsPut: false,
				Key:   []byte(key),
			}))

			_, ok = queue.cache.Load(key)
			assert.False(t, ok)

			currentTriggered := triggered[i].Load()
			time.Sleep(time.Second * 2)
			assert.Equal(t, currentTriggered, triggered[i].Load())
			_, ok = queue.cache.Load(key)
			assert.False(t, ok)
		})
	}
}

func newQueue(t *testing.T, triggerFn api.TriggerFunction) *Queue {
	t.Helper()

	client := tests.EmbeddedETCD(t)

	collector, err := garbage.New(garbage.Options{Client: client})
	require.NoError(t, err)

	queue := New(Options{
		Log:              logr.Discard(),
		Client:           client,
		Key:              key.New(key.Options{Namespace: "abc"}),
		SchedulerBuilder: scheduler.NewBuilder(),
		Collector:        collector,
		TriggerFn:        triggerFn,
		Yard:             grave.New(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error)
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for queue to return")
		}
	})
	go func() {
		errCh <- queue.Run(ctx)
	}()

	return queue
}
