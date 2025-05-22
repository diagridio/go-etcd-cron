/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func Test_consumersink(t *testing.T) {
	t.Parallel()

	t.Run("crud", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		ch := make(chan *api.InformerEvent, 10)
		opts := cron.Options{
			Client:    client,
			Log:       logr.Discard(),
			Namespace: "test",
			ID:        "123",
			TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
				return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
			},
			ConsumerSink: ch,
		}

		c, err := cron.New(opts)
		require.NoError(t, err)

		errCh := make(chan error)
		t.Cleanup(func() { require.NoError(t, <-errCh) })
		go func() { errCh <- c.Run(t.Context()) }()

		meta1, err := anypb.New(wrapperspb.String("meta1"))
		require.NoError(t, err)
		meta2, err := anypb.New(wrapperspb.String("meta2"))
		require.NoError(t, err)

		pay1, err := anypb.New(wrapperspb.String("pay1"))
		require.NoError(t, err)
		pay2, err := anypb.New(wrapperspb.String("pay2"))
		require.NoError(t, err)

		require.NoError(t, c.Add(t.Context(), "name1", &api.Job{
			DueTime:  ptr.Of("0s"),
			Metadata: meta1,
			Payload:  pay1,
		}))

		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Put{
				Put: &api.InformerEventJob{
					Name:     "name1",
					Metadata: meta1,
					Payload:  pay1,
				},
			},
		}, <-ch))
		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Delete{
				Delete: &api.InformerEventJob{
					Name:     "name1",
					Metadata: meta1,
					Payload:  pay1,
				},
			},
		}, <-ch))

		require.NoError(t, c.Add(t.Context(), "name2", &api.Job{
			DueTime:  ptr.Of("100s"),
			Metadata: meta2,
			Payload:  pay2,
		}))
		require.NoError(t, c.Delete(t.Context(), "name2"))

		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Put{
				Put: &api.InformerEventJob{
					Name:     "name2",
					Metadata: meta2,
					Payload:  pay2,
				},
			},
		}, <-ch))
		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Delete{
				Delete: &api.InformerEventJob{
					Name:     "name2",
					Metadata: meta2,
					Payload:  pay2,
				},
			},
		}, <-ch))
	})

	t.Run("leader election", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)

		optsFn := func(id string) (cron.Options, chan *api.InformerEvent) {
			ch := make(chan *api.InformerEvent, 10)
			return cron.Options{
				Client:    client,
				Log:       logr.Discard(),
				Namespace: "test",
				ID:        id,
				TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
					return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
				},
				ConsumerSink: ch,
			}, ch
		}

		errCh := make(chan error)
		opts1, ch1 := optsFn("123")
		c1, err := cron.New(opts1)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, <-errCh) })
		go func() { errCh <- c1.Run(t.Context()) }()

		assert.Eventually(t, c1.IsElected, time.Second*10, time.Millisecond*10)

		require.NoError(t, c1.Add(t.Context(), "name1", &api.Job{DueTime: ptr.Of("120s")}))
		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Put{
				Put: &api.InformerEventJob{Name: "name1"},
			},
		}, <-ch1))

		c2ctx, cancel := context.WithCancel(t.Context())
		opts2, ch2 := optsFn("456")
		c2, err := cron.New(opts2)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, <-errCh) })
		go func() { errCh <- c2.Run(c2ctx) }()

		assert.Eventually(t, c2.IsElected, time.Second*10, time.Millisecond*10)

		select {
		case e := <-ch1:
			assert.True(t, proto.Equal(&api.InformerEvent{
				Event: &api.InformerEvent_DropAll{
					DropAll: new(api.InformerEventDropAll),
				},
			}, e))
		case <-time.After(time.Second):
			assert.Fail(t, "did not receive event")
		}

		var e *api.InformerEvent
		select {
		case e = <-ch1:
		case e = <-ch2:
		case <-time.After(time.Second):
			assert.Fail(t, "did not receive event")
		}
		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Put{Put: &api.InformerEventJob{Name: "name1"}},
		}, e))

		opts3, ch3 := optsFn("789")
		c3, err := cron.New(opts3)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, <-errCh) })
		go func() { errCh <- c3.Run(t.Context()) }()

		assert.Eventually(t, c3.IsElected, time.Second*10, time.Millisecond*10)

		for _, ch := range []chan *api.InformerEvent{ch1, ch2} {
			select {
			case e = <-ch:
				assert.True(t, proto.Equal(&api.InformerEvent{
					Event: &api.InformerEvent_DropAll{DropAll: new(api.InformerEventDropAll)},
				}, e))
			case <-time.After(time.Second):
				assert.Fail(t, "did not receive event")
			}
		}

		select {
		case e = <-ch1:
		case e = <-ch2:
		case e = <-ch3:
		case <-time.After(time.Second):
			assert.Fail(t, "did not receive event")
		}
		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Put{Put: &api.InformerEventJob{Name: "name1"}},
		}, e))

		cancel()

		for _, ch := range []chan *api.InformerEvent{ch1, ch2, ch3} {
			select {
			case e = <-ch:
				assert.True(t, proto.Equal(&api.InformerEvent{
					Event: &api.InformerEvent_DropAll{DropAll: new(api.InformerEventDropAll)},
				}, e))
			case <-time.After(time.Second):
				assert.Fail(t, "did not receive event")
			}
		}

		select {
		case e = <-ch1:
		case e = <-ch3:
		case <-time.After(time.Second):
			assert.Fail(t, "did not receive event")
		}
		assert.True(t, proto.Equal(&api.InformerEvent{
			Event: &api.InformerEvent_Put{Put: &api.InformerEventJob{Name: "name1"}},
		}, e))
	})
}
