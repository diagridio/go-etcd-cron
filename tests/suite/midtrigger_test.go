/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func Test_midtrigger(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	p1, err := anypb.New(wrapperspb.String("hello"))
	require.NoError(t, err)
	p2, err := anypb.New(wrapperspb.String("world"))
	require.NoError(t, err)

	gotCh := make(chan *anypb.Any)
	releaseCh := make(chan struct{})
	cron := integration.New(t, integration.Options{
		Instances: 1,
		Client:    client,
		TriggerFn: func(job *api.TriggerRequest) *api.TriggerResponse {
			gotCh <- job.Payload
			<-releaseCh
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		},
	})

	require.NoError(t, cron.API().Add(cron.Context(), "123", &api.Job{
		DueTime: ptr.Of("0s"),
		Payload: p1,
	}))

	select {
	case <-time.After(time.Second * 5):
		require.Fail(t, "timed out waiting for trigger")
	case got := <-gotCh:
		assert.True(t, proto.Equal(p1, got))
	}

	require.NoError(t, cron.API().Add(cron.Context(), "123", &api.Job{
		DueTime: ptr.Of("0s"),
		Payload: p2,
	}))
	releaseCh <- struct{}{}

	select {
	case <-time.After(time.Second * 5):
		require.Fail(t, "timed out waiting for trigger")
	case got := <-gotCh:
		assert.True(t, proto.Equal(p2, got))
	}
	releaseCh <- struct{}{}
}
