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

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/garbage/fake"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
	"github.com/diagridio/go-etcd-cron/internal/tests"
)

func Test_Run(t *testing.T) {
	t.Parallel()

	part, err := partitioner.New(partitioner.Options{
		ID:    0,
		Total: 2,
	})
	require.NoError(t, err)

	t.Run("No keys in the db should return no events after ready", func(t *testing.T) {
		t.Parallel()

		client := tests.EmbeddedETCD(t)
		i := New(Options{
			Partitioner: part,
			Client:      client,
			Collector:   garbage.New(garbage.Options{Client: client}),
			Yard:        grave.New(),
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
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

		client := tests.EmbeddedETCD(t)

		jobUID1, err := proto.Marshal(&api.JobStored{Uuid: 1})
		require.NoError(t, err)
		jobUID2, err := proto.Marshal(&api.JobStored{Uuid: 2})
		require.NoError(t, err)
		jobUID3, err := proto.Marshal(&api.JobStored{Uuid: 3})
		require.NoError(t, err)
		jobUID4, err := proto.Marshal(&api.JobStored{Uuid: 4})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		for i, jobUID := range [][]byte{jobUID1, jobUID2, jobUID3, jobUID4} {
			_, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i), string(jobUID))
			require.NoError(t, err)
		}

		jobs := make([]api.JobStored, 2)
		require.NoError(t, proto.Unmarshal(jobUID2, &jobs[0]))
		require.NoError(t, proto.Unmarshal(jobUID4, &jobs[1]))

		i := New(Options{
			Partitioner: part,
			Client:      client,
			Collector:   garbage.New(garbage.Options{Client: client}),
			Yard:        grave.New(),
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
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

		for i := 0; i < 2; i++ {
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

		client := tests.EmbeddedETCD(t)

		jobUID1, err := proto.Marshal(&api.JobStored{Uuid: 1})
		require.NoError(t, err)
		jobUID2, err := proto.Marshal(&api.JobStored{Uuid: 2})
		require.NoError(t, err)
		jobUID3, err := proto.Marshal(&api.JobStored{Uuid: 3})
		require.NoError(t, err)
		jobUID4, err := proto.Marshal(&api.JobStored{Uuid: 4})
		require.NoError(t, err)
		jobUID5, err := proto.Marshal(&api.JobStored{Uuid: 5})
		require.NoError(t, err)
		jobUID6, err := proto.Marshal(&api.JobStored{Uuid: 6})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		for i, jobUID := range [][]byte{jobUID1, jobUID2, jobUID3, jobUID4} {
			_, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i+1), string(jobUID))
			require.NoError(t, err)
		}

		jobs := make([]api.JobStored, 3)
		require.NoError(t, proto.Unmarshal(jobUID2, &jobs[0]))
		require.NoError(t, proto.Unmarshal(jobUID4, &jobs[1]))
		require.NoError(t, proto.Unmarshal(jobUID6, &jobs[2]))

		i := New(Options{
			Partitioner: part,
			Client:      client,
			Collector:   garbage.New(garbage.Options{Client: client}),
			Yard:        grave.New(),
			Key: key.New(key.Options{
				Namespace:   "abc",
				PartitionID: 0,
			}),
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
			{IsPut: true, Job: &jobs[0], Key: []byte("abc/jobs/2")},
			{IsPut: true, Job: &jobs[1], Key: []byte("abc/jobs/4")},
			{IsPut: false, Job: &jobs[0], Key: []byte("abc/jobs/2")},
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
		default:
		}
	})
}

func Test_handleEvent(t *testing.T) {
	t.Parallel()

	jobUID1, err := proto.Marshal(&api.JobStored{Uuid: 1})
	require.NoError(t, err)
	jobUID2, err := proto.Marshal(&api.JobStored{Uuid: 2})
	require.NoError(t, err)

	var job2 api.JobStored
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
		"if job is for different partition, return nil": {
			ev: &clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("abc/jobs/2"),
					Value: jobUID1,
				},
			},
			expCollectorPops: []string{"abc/counters/2"},
			expEvent:         nil,
			expErr:           false,
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
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			part, err := partitioner.New(partitioner.Options{
				ID:    0,
				Total: 2,
			})
			require.NoError(t, err)

			collector := fake.New()

			yard := grave.New()
			if test.yardDelete != nil {
				yard.Deleted(*test.yardDelete)
			}

			i := New(Options{
				Partitioner: part,
				Collector:   collector,
				Yard:        yard,
				Key: key.New(key.Options{
					Namespace:   "abc",
					PartitionID: 0,
				}),
			})
			gotEvent, err := i.handleEvent(test.ev)
			assert.Equal(t, test.expEvent, gotEvent)
			assert.Equal(t, test.expErr, err != nil, "%v", err)
			assert.Equal(t, test.expCollectorPops, collector.HasPoped())
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
