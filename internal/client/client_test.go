/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/diagridio/go-etcd-cron/internal/client/fake"
)

func Test_CRUD(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*client) error{
		"Put": func(c *client) error {
			_, err := c.Put(context.Background(), "123", "abc")
			return err
		},
		"Get": func(c *client) error {
			_, err := c.Get(context.Background(), "123")
			return err
		},
		"Delete": func(c *client) error {
			_, err := c.Delete(context.Background(), "123")
			return err
		},
		"DeleteMulti": func(c *client) error {
			return c.DeleteMulti("123")
		},
		"PutIfNotExists": func(c *client) error {
			_, err := c.PutIfNotExists(context.Background(), "123", "abc")
			return err
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

				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				kv.WithError(nil)
				clock.Sleep(time.Second)
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

				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				kv.WithError(errors.New("this is an error"))
				clock.Sleep(time.Second)
			})
		})
	}
}
