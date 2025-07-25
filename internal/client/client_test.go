/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/client/fake"
)

func Test_CRUD(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*client) error{
		"Put": func(c *client) error {
			_, err := c.Put(t.Context(), "123", "abc")
			return err
		},
		"Get": func(c *client) error {
			_, err := c.Get(t.Context(), "123")
			return err
		},
		"Delete": func(c *client) error {
			_, err := c.Delete(t.Context(), "123")
			return err
		},
		"PutIfNotExists": func(c *client) error {
			_, err := c.PutIfNotExists(t.Context(), "123", "abc")
			return err
		},
		"DeletePair": func(c *client) error {
			return c.DeletePair(t.Context(), "123", "abc")
		},
		"DeleteBothIfOtherHasRevision": func(c *client) error {
			return c.DeleteBothIfOtherHasRevision(t.Context(), api.DeleteBothIfOtherHasRevisionOpts{})
		},
		"PutIfOtherHasRevision": func(c *client) error {
			_, err := c.PutIfOtherHasRevision(t.Context(), api.PutIfOtherHasRevisionOpts{})
			return err
		},
		"DeletePrefixes": func(c *client) error {
			return c.DeletePrefixes(t.Context())
		},
	}

	for name, test := range tests {
		testInLoop := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Run("Successful should return nil", func(t *testing.T) {
				t.Parallel()

				kv := fake.New()
				require.NoError(t, testInLoop(&client{kv: kv}))
				assert.Equal(t, uint32(1), kv.Calls())
			})

			t.Run("With error should return error", func(t *testing.T) {
				t.Parallel()

				kv := fake.New().WithError(errors.New("this is an error"))
				require.Error(t, testInLoop(&client{kv: kv}))
				assert.Equal(t, uint32(1), kv.Calls())
			})

			t.Run("Too many request errors should be retried until successful", func(t *testing.T) {
				t.Parallel()

				clock := clocktesting.NewFakeClock(time.Now())
				kv := fake.New().WithError(rpctypes.ErrTooManyRequests)
				errCh := make(chan error)
				t.Cleanup(func() {
					select {
					case err := <-errCh:
						require.NoError(t, err)
					case <-time.After(time.Second):
						assert.Fail(t, "timeout waiting for err")
					}
					assert.Equal(t, uint32(4), kv.Calls())
				})

				go func() {
					errCh <- testInLoop(&client{kv: kv, clock: clock})
				}()

				sleepTime := time.Second + time.Millisecond*1000
				assert.Eventually(t, clock.HasWaiters, sleepTime, time.Millisecond*10)
				clock.Sleep(sleepTime)
				assert.Eventually(t, clock.HasWaiters, sleepTime, time.Millisecond*10)
				clock.Sleep(sleepTime)
				assert.Eventually(t, clock.HasWaiters, sleepTime, time.Millisecond*10)
				kv.WithError(nil)
				clock.Sleep(sleepTime)
			})

			t.Run("Too many request errors should be retried until another error", func(t *testing.T) {
				t.Parallel()

				clock := clocktesting.NewFakeClock(time.Now())
				kv := fake.New().WithError(rpctypes.ErrTooManyRequests)
				errCh := make(chan error)
				t.Cleanup(func() {
					select {
					case err := <-errCh:
						require.Error(t, err)
					case <-time.After(time.Second):
						assert.Fail(t, "timeout waiting for err")
					}
					assert.Equal(t, uint32(4), kv.Calls())
				})

				go func() {
					errCh <- testInLoop(&client{kv: kv, clock: clock})
				}()

				sleepTime := time.Second + time.Millisecond*1000
				assert.Eventually(t, clock.HasWaiters, sleepTime, time.Millisecond*10)
				clock.Sleep(sleepTime)
				assert.Eventually(t, clock.HasWaiters, sleepTime, time.Millisecond*10)
				clock.Sleep(sleepTime)
				assert.Eventually(t, clock.HasWaiters, sleepTime, time.Millisecond*10)
				kv.WithError(errors.New("this is an error"))
				clock.Sleep(sleepTime)
			})
		})
	}
}
