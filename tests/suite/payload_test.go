/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
)

func Test_payload(t *testing.T) {
	t.Parallel()

	gotCh := make(chan *api.TriggerRequest, 1)
	cron := integration.New(t, integration.Options{
		Instances: 1,
		GotCh:     gotCh,
	})

	payload, err := anypb.New(wrapperspb.String("hello"))
	require.NoError(t, err)
	meta, err := anypb.New(wrapperspb.String("world"))
	require.NoError(t, err)
	job := &api.Job{
		DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
		Payload:  payload,
		Metadata: meta,
	}
	require.NoError(t, cron.API().Add(cron.Context(), "yoyo", job))

	select {
	case got := <-gotCh:
		assert.Equal(t, "yoyo", got.GetName())
		var gotPayload wrapperspb.StringValue
		require.NoError(t, got.GetPayload().UnmarshalTo(&gotPayload))
		assert.Equal(t, "hello", gotPayload.GetValue())
		var gotMeta wrapperspb.StringValue
		require.NoError(t, got.GetMetadata().UnmarshalTo(&gotMeta))
		assert.Equal(t, "world", gotMeta.GetValue())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for trigger")
	}
}
