/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Fake struct {
	clientv3.KV
	clientv3.Watcher
	clientv3.Lease

	calls atomic.Uint32
	putFn func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
	getFn func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	delFn func(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	delMu func(...string) error

	putIfNotExistsFn func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)

	err error
}

func New() *Fake {
	return new(Fake)
}

func (f *Fake) WithPutFn(fn func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)) *Fake {
	f.putFn = fn
	return f
}

func (f *Fake) WithGetFn(fn func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)) *Fake {
	f.getFn = fn
	return f
}

func (f *Fake) WithDeleteFn(fn func(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)) *Fake {
	f.delFn = fn
	return f
}

func (f *Fake) WithDeleteMultiFn(fn func(...string) error) *Fake {
	f.delMu = fn
	return f
}

func (f *Fake) WithPutIfNotExistsFn(fn func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)) *Fake {
	f.putIfNotExistsFn = fn
	return f
}

func (f *Fake) WithError(err error) *Fake {
	f.err = err
	return f
}

func (f *Fake) If(...clientv3.Cmp) clientv3.Txn {
	return f
}

func (f *Fake) Else(...clientv3.Op) clientv3.Txn {
	return f
}

func (f *Fake) Then(...clientv3.Op) clientv3.Txn {
	return f
}

func (f *Fake) Txn(context.Context) clientv3.Txn {
	return f
}

func (f *Fake) Put(_ context.Context, k string, b string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.calls.Add(1)
	if f.putFn != nil {
		return f.putFn(context.Background(), k, b)
	}
	return nil, f.err
}

func (f *Fake) Get(_ context.Context, k string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.calls.Add(1)
	if f.getFn != nil {
		return f.getFn(context.Background(), k)
	}
	return nil, f.err
}

func (f *Fake) Delete(_ context.Context, k string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	f.calls.Add(1)
	if f.delFn != nil {
		return f.delFn(context.Background(), k)
	}
	return nil, f.err
}

func (f *Fake) Commit() (*clientv3.TxnResponse, error) {
	f.calls.Add(1)
	return &clientv3.TxnResponse{
		Succeeded: true,
	}, f.err
}

func (f *Fake) DeleteMulti(keys ...string) error {
	f.calls.Add(1)
	if f.delMu != nil {
		return f.delMu(keys...)
	}
	return f.err
}

func (f *Fake) PutIfNotExists(_ context.Context, k string, b string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.calls.Add(1)
	if f.putIfNotExistsFn != nil {
		return f.putIfNotExistsFn(context.Background(), k, b)
	}
	return nil, f.err
}

func (f *Fake) Close() error {
	return f.err
}

func (f *Fake) Calls() uint32 {
	return f.calls.Load()
}
