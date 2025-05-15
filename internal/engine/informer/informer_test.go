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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
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
		i, ch := New(Options{
			Partitioner: part,
			Client:      client,
			Key:         key,
		})

		ctx, cancel := context.WithCancel(t.Context())
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

		select {
		case ev := <-ch:
			t.Fatalf("unexpected event: %v", ev)
		default:
		}
	})

	t.Run("keys in the db should be returned after ready, filtered by partition", func(t *testing.T) {
		t.Parallel()

		client := etcd.Embedded(t)

		jobUID1, err := proto.Marshal(&stored.Job{PartitionId: 1})
		require.NoError(t, err)
		jobUID2, err := proto.Marshal(&stored.Job{PartitionId: 2})
		require.NoError(t, err)
		jobUID3, err := proto.Marshal(&stored.Job{PartitionId: 3})
		require.NoError(t, err)
		jobUID4, err := proto.Marshal(&stored.Job{PartitionId: 4})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		for i, jobUID := range [][]byte{jobUID1, jobUID2, jobUID3, jobUID4} {
			_, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i), string(jobUID))
			require.NoError(t, err)
		}

		jobs := make([]stored.Job, 2)
		require.NoError(t, proto.Unmarshal(jobUID2, &jobs[0]))
		require.NoError(t, proto.Unmarshal(jobUID4, &jobs[1]))

		i, ch := New(Options{
			Partitioner: part,
			Client:      client,
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

		ctx, cancel := context.WithCancel(t.Context())
		modRevision := make([]int64, 6)
		for i, jobUID := range [][]byte{jobUID1, jobUID2, jobUID3, jobUID4} {
			resp, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i+1), string(jobUID))
			require.NoError(t, err)
			modRevision[i] = resp.Header.GetRevision()
		}

		jobs := make([]stored.Job, 3)
		require.NoError(t, proto.Unmarshal(jobUID2, &jobs[0]))
		require.NoError(t, proto.Unmarshal(jobUID4, &jobs[1]))
		require.NoError(t, proto.Unmarshal(jobUID6, &jobs[2]))

		i, ch := New(Options{
			Partitioner: part,
			Client:      client,
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

		expEvents := []*queue.Informed{
			{IsPut: false, Job: nil, Name: "1"},
			{IsPut: true, Job: &jobs[0], Name: "2", JobModRevision: modRevision[1]},
			{IsPut: false, Job: nil, Name: "3"},
			{IsPut: true, Job: &jobs[1], Name: "4", JobModRevision: modRevision[3]},
		}

		for _, expEvent := range expEvents {
			select {
			case ev := <-ch:
				assert.True(t, proto.Equal(expEvent, ev), "%v != %v", expEvent, ev)
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for event %v", expEvent)
			}
		}

		require.NoError(t, i.Ready(ctx))

		_, err = client.Delete(ctx, "abc/jobs/1")
		require.NoError(t, err)
		_, err = client.Delete(ctx, "abc/jobs/2")
		require.NoError(t, err)
		for i, jobUID := range [][]byte{jobUID5, jobUID6} {
			resp, err := client.Put(ctx, "abc/jobs/"+strconv.Itoa(i+5), string(jobUID))
			require.NoError(t, err)
			modRevision[i+4] = resp.Header.GetRevision()
		}
		_, err = client.Delete(ctx, "abc/jobs/4")
		require.NoError(t, err)

		expEvents = []*queue.Informed{
			{IsPut: false, Job: nil, Name: "1"},
			{IsPut: false, Job: &jobs[0], Name: "2", JobModRevision: modRevision[1]},
			{IsPut: false, Job: nil, Name: "5"},
			{IsPut: true, Job: &jobs[2], Name: "6", JobModRevision: modRevision[5]},
			{IsPut: false, Job: &jobs[1], Name: "4", JobModRevision: modRevision[3]},
		}
		for _, expEvent := range expEvents {
			select {
			case ev := <-ch:
				assert.True(t, proto.Equal(expEvent, ev), "%v != %v", expEvent, ev)
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
		ev       *clientv3.Event
		expEvent *queue.Informed
		expErr   bool
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
			expEvent: nil,
			expErr:   true,
		},
		"if job is for different partition, return delete event": {
			ev: &clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("abc/jobs/2"),
					Value: jobUID1,
				},
			},
			expEvent: &queue.Informed{
				IsPut: false,
				Name:  "2",
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
			expEvent: &queue.Informed{
				IsPut: true,
				Job:   &job2,
				Name:  "3",
			},
			expErr: false,
		},
		"if job is for partition, return job on DELETE": {
			ev: &clientv3.Event{
				Type:   clientv3.EventTypeDelete,
				Kv:     &mvccpb.KeyValue{Value: jobUID2, Key: []byte("abc/jobs/3")},
				PrevKv: &mvccpb.KeyValue{Value: jobUID2},
			},
			expEvent: &queue.Informed{
				IsPut: false,
				Job:   &job2,
				Name:  "3",
			},
			expErr: false,
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

			i, _ := New(Options{
				Partitioner: part,
				Key:         key,
			})
			gotEvent, err := i.handleEvent(testInLoop.ev)
			assert.Equal(t, testInLoop.expEvent, gotEvent)
			assert.Equal(t, testInLoop.expErr, err != nil, "%v", err)
		})
	}
}

func Test_Ready(t *testing.T) {
	t.Parallel()

	t.Run("Ready returns when the given context is cancelled with the context error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		i, _ := New(Options{})
		assert.Equal(t, context.Canceled, i.Ready(ctx))
	})

	t.Run("Ready returns nil when ready", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		i, _ := New(Options{})
		close(i.readyCh)
		assert.NoError(t, i.Ready(ctx))
	})
}
