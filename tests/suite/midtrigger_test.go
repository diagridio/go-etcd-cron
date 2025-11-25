/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/kit/concurrency/slice"
	"github.com/dapr/kit/ptr"
	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron/integration"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_midtrigger(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	p1, err := anypb.New(wrapperspb.String("hello"))
	require.NoError(t, err)
	p2, err := anypb.New(wrapperspb.String("world"))
	require.NoError(t, err)

	fns := slice.New[func(*api.TriggerResponse)]()
	cron := integration.New(t, integration.Options{
		Instances: 1,
		Client:    client,
		TriggerFn: func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
			fns.Append(fn)
		},
	})

	require.NoError(t, cron.API().Add(cron.Context(), "123", &api.Job{
		DueTime: ptr.Of("0s"),
		Payload: p1,
	}))

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, fns.Slice(), 1)
	}, time.Second*10, time.Millisecond*10)

	require.NoError(t, cron.API().Add(cron.Context(), "123", &api.Job{
		DueTime: ptr.Of("0s"),
		Payload: p2,
	}))

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, fns.Slice(), 2)
	}, time.Second*10, time.Millisecond*10)

	for _, fn := range fns.Slice() {
		fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := cron.API().List(t.Context(), "")
		require.NoError(c, err)
		assert.Empty(c, resp.Jobs)
	}, time.Second*10, time.Millisecond*10)
}
