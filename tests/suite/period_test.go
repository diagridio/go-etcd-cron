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

func Test_period(t *testing.T) {
	t.Parallel()

	t.Run("single", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 1)

		data, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)

		require.NoError(t, cron.API().Add(cron.Context(), "test1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Ttl:      ptr.Of("3s"),
			Payload:  data,
		}))
		require.NoError(t, cron.API().Add(cron.Context(), "test2", &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(2)),
			Payload:  data,
		}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c,
				[]string{"test1", "test1", "test1", "test2", "test2"},
				cron.TriggeredNames(),
			)
		}, time.Second*10, time.Millisecond*10)

		time.Sleep(time.Second * 2)
		assert.ElementsMatch(t,
			[]string{"test1", "test1", "test1", "test2", "test2"},
			cron.TriggeredNames(),
		)
	})

	t.Run("multiple", func(t *testing.T) {
		t.Parallel()

		cron := integration.NewBase(t, 3)

		data, err := anypb.New(wrapperspb.Bytes([]byte("data")))
		require.NoError(t, err)

		require.NoError(t, cron.API().Add(cron.Context(), "test1", &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Ttl:      ptr.Of("3s"),
			Payload:  data,
		}))
		require.NoError(t, cron.API().Add(cron.Context(), "test2", &api.Job{
			Schedule: ptr.Of("@every 1s"),
			Repeats:  ptr.Of(uint32(2)),
			Payload:  data,
		}))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ElementsMatch(c,
				[]string{"test1", "test1", "test1", "test2", "test2"},
				cron.TriggeredNames(),
			)
		}, time.Second*10, time.Millisecond*10)

		time.Sleep(time.Second * 2)
		assert.ElementsMatch(t,
			[]string{"test1", "test1", "test1", "test2", "test2"},
			cron.TriggeredNames(),
		)
	})
}
