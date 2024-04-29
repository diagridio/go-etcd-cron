/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

type mock struct {
	clientv3.KV
	calls atomic.Int32
	resp  *clientv3.DeleteResponse
	err   error
	lock  sync.Mutex
}

func (m *mock) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	m.calls.Add(1)
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.resp, m.err
}

func Test_Delete(t *testing.T) {
	t.Parallel()

	t.Run("Successful delete should return nil", func(t *testing.T) {
		t.Parallel()

		kv := new(mock)
		require.NoError(t, Delete(kv, "123"))
		assert.Equal(t, int32(1), kv.calls.Load())
	})

	t.Run("Delete with error should return error", func(t *testing.T) {
		t.Parallel()

		kv := new(mock)
		kv.err = errors.New("this is an error")
		require.Error(t, Delete(kv, "123"))
		assert.Equal(t, int32(1), kv.calls.Load())
	})

	t.Run("Too many request errors should be retired until successful", func(t *testing.T) {
		t.Parallel()

		kv := new(mock)

		go func() {
			for {
				if kv.calls.Load() == 3 {
					kv.lock.Lock()
					kv.err = nil
					kv.lock.Unlock()

					break
				}
			}
		}()

		kv.err = etcdserver.ErrTooManyRequests
		require.NoError(t, Delete(kv, "123"))
		assert.GreaterOrEqual(t, kv.calls.Load(), int32(3))
	})

	t.Run("Too many request errors should be retired until another error", func(t *testing.T) {
		t.Parallel()

		kv := new(mock)

		go func() {
			for {
				if kv.calls.Load() == 3 {
					kv.lock.Lock()
					kv.err = errors.New("this is an error")
					kv.lock.Unlock()

					break
				}
			}
		}()

		kv.err = etcdserver.ErrTooManyRequests
		require.Error(t, Delete(kv, "123"))
		assert.GreaterOrEqual(t, kv.calls.Load(), int32(3))
	})
}
