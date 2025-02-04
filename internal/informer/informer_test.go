/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package informer

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/garbage/fake"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/partitioner"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_Run(t *testing.T) {
	t.Parallel()

	key, err := key.New(key.Options{
		Namespace: "abc",
		ID:        "0",
	})
	require.NoError(t, err)

	part, err := partitioner.New(partitioner.Options{
		Key: key,
		Leaders: []*mvccpb.KeyValue{
			{Key: []byte("abc/leader/0")},
			{Key: []byte("abc/leader/1")},
		},
	})
	require.NoError(t, err)

	t.Run("No keys in the db should return no events after ready", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)
		i := New(Options{
			Partitioner: part,
			Client:      client,
			Collector:   collector,
			Yard:        grave.New(),
			Key:         key,
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		t.Cleanup(func() {
			cancel()
			select {
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for Run to exit")
			case err := <-errCh:
				require.NoError(t, err)
			}
		})
		go func() {
			errCh <- i.Run(ctx)
		}()

		require.NoError(t, i.Ready(ctx))
		ch, err := i.Events()
		require.NoError(t, err)

		select {
		case ev := <-ch:
			t.Fatalf("unexpected event: %v", ev)
		default:
		}
	})

	t.Run("keys in the db should be returned after ready, filtered by partition", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		jobUID1, err := proto.Marshal(&stored.Job{PartitionId: 1})
		require.NoError(t, err)
		jobUID2, err := proto.Marshal(&stored.Job{PartitionId: 2})
		require.NoError(t, err)
		jobUID3, err := proto.Marshal(&stored.Job{PartitionId: 3})
		require.NoError(t, err)
		jobUID4, err := proto.Marshal(&stored.Job{PartitionId: 4})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		for i, jobUID := range [][]byte{jobUID1, jobUID2, jobUID3, jobUID4} {
			_, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i), string(jobUID))
			require.NoError(t, err)
		}

		jobs := make([]stored.Job, 2)
		require.NoError(t, proto.Unmarshal(jobUID2, &jobs[0]))
		require.NoError(t, proto.Unmarshal(jobUID4, &jobs[1]))

		i := New(Options{
			Partitioner: part,
			Client:      client,
			Collector:   collector,
			Yard:        grave.New(),
			Key:         key,
		})

		errCh := make(chan error, 1)
		t.Cleanup(func() {
			cancel()
			select {
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for Run to exit")
			case err := <-errCh:
				require.NoError(t, err)
			}
		})
		go func() {
			errCh <- i.Run(ctx)
		}()

		require.NoError(t, i.Ready(ctx))
		ch, err := i.Events()
		require.NoError(t, err)

		for i := range 2 {
			select {
			case ev := <-ch:
				assert.False(t, ev.IsPut)
				assert.Nil(t, ev.Job)
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for event %d", i)
			}

			select {
			case ev := <-ch:
				assert.True(t, ev.IsPut)
				//nolint:govet
				assert.Equal(t, jobs[i], *ev.Job)
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for event %d", i)
			}
		}

		select {
		case ev := <-ch:
			t.Fatalf("unexpected event: %v", ev)
		default:
		}
	})

	t.Run("keys added to the db after Ready should be synced, filtering by partition", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)
		collector, err := garbage.New(garbage.Options{Client: client})
		require.NoError(t, err)

		jobUID1, err := proto.Marshal(&stored.Job{PartitionId: 1})
		require.NoError(t, err)
		jobUID2, err := proto.Marshal(&stored.Job{PartitionId: 2})
		require.NoError(t, err)
		jobUID3, err := proto.Marshal(&stored.Job{PartitionId: 3})
		require.NoError(t, err)
		jobUID4, err := proto.Marshal(&stored.Job{PartitionId: 4})
		require.NoError(t, err)
		jobUID5, err := proto.Marshal(&stored.Job{PartitionId: 5})
		require.NoError(t, err)
		jobUID6, err := proto.Marshal(&stored.Job{PartitionId: 6})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		for i, jobUID := range [][]byte{jobUID1, jobUID2, jobUID3, jobUID4} {
			_, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i+1), string(jobUID))
			require.NoError(t, err)
		}

		jobs := make([]stored.Job, 3)
		require.NoError(t, proto.Unmarshal(jobUID2, &jobs[0]))
		require.NoError(t, proto.Unmarshal(jobUID4, &jobs[1]))
		require.NoError(t, proto.Unmarshal(jobUID6, &jobs[2]))

		i := New(Options{
			Partitioner: part,
			Client:      client,
			Collector:   collector,
			Yard:        grave.New(),
			Key:         key,
		})

		errCh := make(chan error, 1)
		t.Cleanup(func() {
			cancel()
			select {
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for Run to exit")
			case err := <-errCh:
				require.NoError(t, err)
			}
		})
		go func() {
			errCh <- i.Run(ctx)
		}()

		require.NoError(t, i.Ready(ctx))
		ch, err := i.Events()
		require.NoError(t, err)

		_, err = client.Delete(ctx, "abc/jobs/1")
		require.NoError(t, err)
		_, err = client.Delete(ctx, "abc/jobs/2")
		require.NoError(t, err)
		for i, jobUID := range [][]byte{jobUID5, jobUID6} {
			_, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i+5), string(jobUID))
			require.NoError(t, err)
		}
		_, err = client.Delete(ctx, "abc/jobs/4")
		require.NoError(t, err)

		expEvents := []*Event{
			{IsPut: false, Job: nil, Key: []byte("abc/jobs/1")},
			{IsPut: true, Job: &jobs[0], Key: []byte("abc/jobs/2")},
			{IsPut: false, Job: nil, Key: []byte("abc/jobs/3")},
			{IsPut: true, Job: &jobs[1], Key: []byte("abc/jobs/4")},
			{IsPut: false, Job: nil, Key: []byte("abc/jobs/1")},
			{IsPut: false, Job: &jobs[0], Key: []byte("abc/jobs/2")},
			{IsPut: false, Job: nil, Key: []byte("abc/jobs/5")},
			{IsPut: true, Job: &jobs[2], Key: []byte("abc/jobs/6")},
			{IsPut: false, Job: &jobs[1], Key: []byte("abc/jobs/4")},
		}

		for _, expEvent := range expEvents {
			select {
			case ev := <-ch:
				assert.Equal(t, expEvent, ev)
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for event %v", expEvent)
			}
		}

		select {
		case ev := <-ch:
			t.Fatalf("unexpected event: %v", ev)
		case <-time.After(time.Second):
		}
	})
}

func Test_handleEvent(t *testing.T) {
	t.Parallel()

	jobUID1, err := proto.Marshal(&stored.Job{PartitionId: 1})
	require.NoError(t, err)
	jobUID2, err := proto.Marshal(&stored.Job{PartitionId: 2})
	require.NoError(t, err)

	var job2 stored.Job
	require.NoError(t, proto.Unmarshal(jobUID2, &job2))

	tests := map[string]struct {
		ev               *clientv3.Event
		expEvent         *Event
		yardDelete       *string
		expCollectorPops []string
		expErr           bool
	}{
		"if event is not recognized, it should return an error": {
			ev: &clientv3.Event{
				Type: mvccpb.Event_EventType(50),
			},
			expEvent: nil,
			expErr:   true,
		},
		"if value has bad data then error": {
			ev: &clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("abc/jobs/1"),
					Value: []byte("bad data"),
				},
			},
			expCollectorPops: []string{"abc/counters/1"},
			expEvent:         nil,
			expErr:           true,
		},
		"if job is for different partition, return delete event": {
			ev: &clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("abc/jobs/2"),
					Value: jobUID1,
				},
			},
			expCollectorPops: []string{"abc/counters/2"},
			expEvent: &Event{
				IsPut: false,
				Key:   []byte("abc/jobs/2"),
			},
			expErr: false,
		},
		"if job is for partition, return job on PUT": {
			ev: &clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Value: jobUID2,
					Key:   []byte("abc/jobs/3"),
				},
			},
			expCollectorPops: []string{"abc/counters/3"},
			expEvent: &Event{
				IsPut: true,
				Job:   &job2,
				Key:   []byte("abc/jobs/3"),
			},
			expErr: false,
		},
		"if job is for partition, return job on DELETE": {
			ev: &clientv3.Event{
				Type:   clientv3.EventTypeDelete,
				Kv:     &mvccpb.KeyValue{Value: jobUID2, Key: []byte("abc/jobs/3")},
				PrevKv: &mvccpb.KeyValue{Value: jobUID2},
			},
			expEvent: &Event{
				IsPut: false,
				Job:   &job2,
			},
			expErr: false,
		},
		"if job is for partition, don't return job on DELETE if just deleted": {
			ev: &clientv3.Event{
				Type: clientv3.EventTypeDelete,
				PrevKv: &mvccpb.KeyValue{
					Key:   []byte("jobs/1"),
					Value: jobUID2,
				},
			},
			yardDelete: ptr.Of("jobs/1"),
			expEvent:   nil,
			expErr:     false,
		},
	}

	for name, test := range tests {
		testInLoop := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			key, err := key.New(key.Options{
				Namespace: "abc",
				ID:        "0",
			})
			require.NoError(t, err)

			part, err := partitioner.New(partitioner.Options{
				Key: key,
				Leaders: []*mvccpb.KeyValue{
					{Key: []byte("abc/leader/0")},
					{Key: []byte("abc/leader/1")},
				},
			})
			require.NoError(t, err)

			collector := fake.New()

			yard := grave.New()
			if testInLoop.yardDelete != nil {
				yard.Deleted(*testInLoop.yardDelete)
			}

			i := New(Options{
				Partitioner: part,
				Collector:   collector,
				Yard:        yard,
				Key:         key,
			})
			gotEvent, err := i.handleEvent(testInLoop.ev)
			assert.Equal(t, testInLoop.expEvent, gotEvent)
			assert.Equal(t, testInLoop.expErr, err != nil, "%v", err)
			assert.Equal(t, testInLoop.expCollectorPops, collector.HasPoped())
		})
	}
}

func Test_Ready(t *testing.T) {
	t.Parallel()

	t.Run("Ready returns when the given context is cancelled with the context error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		i := New(Options{})
		assert.Equal(t, context.Canceled, i.Ready(ctx))
	})

	t.Run("Ready returns nil when ready", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		i := New(Options{})
		close(i.readyCh)
		assert.NoError(t, i.Ready(ctx))
	})
}

func Test_Events(t *testing.T) {
	t.Parallel()

	t.Run("calling Events() multiple times should error", func(t *testing.T) {
		t.Parallel()

		i := New(Options{})
		var expCh <-chan *Event = i.ch

		gotCh, err := i.Events()
		require.NoError(t, err)
		assert.Equal(t, expCh, gotCh)

		gotCh, err = i.Events()
		require.Error(t, err)
		assert.Nil(t, gotCh)

		gotCh, err = i.Events()
		require.Error(t, err)
		assert.Nil(t, gotCh)
	})
}
