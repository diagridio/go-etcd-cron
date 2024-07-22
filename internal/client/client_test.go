/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	clocktesting "k8s.io/utils/clock/testing"
)

type mock struct {
	clientv3.KV
	calls atomic.Uint32
	err   error
}

func (m *mock) If(...clientv3.Cmp) clientv3.Txn {
	return m
}

func (m *mock) Else(...clientv3.Op) clientv3.Txn {
	return m
}

func (m *mock) Then(...clientv3.Op) clientv3.Txn {
	return m
}

func (m *mock) Txn(context.Context) clientv3.Txn {
	return m
}

func (m *mock) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	m.calls.Add(1)
	return nil, m.err
}

func (m *mock) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	m.calls.Add(1)
	return nil, m.err
}

func (m *mock) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	m.calls.Add(1)
	return nil, m.err
}

func (m *mock) Commit() (*clientv3.TxnResponse, error) {
	m.calls.Add(1)
	return nil, m.err
}

func Test_Delete(t *testing.T) {
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
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Run("Successful should return nil", func(t *testing.T) {
				t.Parallel()

				kv := new(mock)
				require.NoError(t, test(&client{kv: kv}))
				assert.Equal(t, uint32(1), kv.calls.Load())
			})

			t.Run("With error should return error", func(t *testing.T) {
				t.Parallel()

				kv := new(mock)
				kv.err = errors.New("this is an error")
				require.Error(t, test(&client{kv: kv}))
				assert.Equal(t, uint32(1), kv.calls.Load())
			})

			t.Run("Too many request errors should be retried until successful", func(t *testing.T) {
				t.Parallel()

				clock := clocktesting.NewFakeClock(time.Now())
				kv := &mock{err: rpctypes.ErrTooManyRequests}
				errCh := make(chan error)
				t.Cleanup(func() {
					select {
					case err := <-errCh:
						require.NoError(t, err)
					case <-time.After(time.Second):
						assert.Fail(t, "timeout waiting for err")
					}
					assert.Equal(t, uint32(4), kv.calls.Load())
				})

				go func() {
					errCh <- test(&client{kv: kv, clock: clock})
				}()

				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				kv.err = nil
				clock.Sleep(time.Second)
			})

			t.Run("Too many request errors should be retried until another error", func(t *testing.T) {
				t.Parallel()

				clock := clocktesting.NewFakeClock(time.Now())
				kv := &mock{err: rpctypes.ErrTooManyRequests}
				errCh := make(chan error)
				t.Cleanup(func() {
					select {
					case err := <-errCh:
						require.Error(t, err)
					case <-time.After(time.Second):
						assert.Fail(t, "timeout waiting for err")
					}
					assert.Equal(t, uint32(4), kv.calls.Load())
				})

				go func() {
					errCh <- test(&client{kv: kv, clock: clock})
				}()

				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				clock.Sleep(time.Second)
				assert.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond*10)
				kv.err = errors.New("this is an error")
				clock.Sleep(time.Second)
			})
		})
	}
}
