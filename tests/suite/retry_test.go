/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_leadership_retry(t *testing.T) {
	t.Parallel()

	t.Run("a API call should never fail during leadership changes", func(t *testing.T) {
		t.Parallel()

		client := etcd.EmbeddedBareClient(t)
		cron1, err := cron.New(cron.Options{
			Client: client,
			ID:     "123",
			TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
				fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			},
		})
		require.NoError(t, err)

		errCh := make(chan error)
		ctx1, cancel1 := context.WithCancel(t.Context())
		go func() { errCh <- cron1.Run(ctx1) }()

		t.Cleanup(func() {
			cancel1()
			require.NoError(t, <-errCh)
		})

		ierrCh := make(chan error, 10*2)
		go func() {
			for range 10 {
				ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*500)
				c, err := cron.New(cron.Options{
					Client: client,
					ID:     "456",
					TriggerFn: func(_ *api.TriggerRequest, fn func(*api.TriggerResponse)) {
						fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
					},
				})
				ierrCh <- err
				if err := c.Run(ctx); errors.Is(err, context.DeadlineExceeded) {
					ierrCh <- nil
				} else {
					ierrCh <- err
				}
				cancel()
			}
		}()

		for i := range 5000 {
			require.NoError(t, cron1.Add(t.Context(), strconv.Itoa(i), &api.Job{
				DueTime: ptr.Of("100000s"),
			}))
		}

		for range 10 * 2 {
			require.NoError(t, <-ierrCh)
		}
	})
}
