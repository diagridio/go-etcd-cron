/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/internal/client/api"
)

type Fake struct {
	clientv3.KV
	clientv3.Watcher
	clientv3.Lease

	calls atomic.Uint32
	putFn func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
	getFn func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	delFn func(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)

	putIfNotExistsFn             func(context.Context, string, string, ...clientv3.OpOption) (bool, error)
	delPairFn                    func(context.Context, string, string) error
	putIfOtherHasRevisionFn      func(context.Context, api.PutIfOtherHasRevisionOpts) (bool, error)
	deleteBothIfOtherHasRevionFn func(context.Context, api.DeleteBothIfOtherHasRevisionOpts) error
	deletePrefixesFn             func(context.Context, ...string) error

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

func (f *Fake) WithDeletePairFn(fn func(context.Context, string, string) error) *Fake {
	f.delPairFn = fn
	return f
}

func (f *Fake) WithPutIfNotExistsFn(fn func(context.Context, string, string, ...clientv3.OpOption) (bool, error)) *Fake {
	f.putIfNotExistsFn = fn
	return f
}

func (f *Fake) WithError(err error) *Fake {
	f.err = err
	return f
}

func (f *Fake) WithPutIfOtherHasRevisionFn(fn func(context.Context, api.PutIfOtherHasRevisionOpts) (bool, error)) *Fake {
	f.putIfOtherHasRevisionFn = fn
	return f
}

func (f *Fake) WithDeleteBothIfOtherHasRevisionFn(fn func(context.Context, api.DeleteBothIfOtherHasRevisionOpts) error) *Fake {
	f.deleteBothIfOtherHasRevionFn = fn
	return f
}

func (f *Fake) WithDeletePrefixesFn(fn func(context.Context, ...string) error) *Fake {
	f.deletePrefixesFn = fn
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

func (f *Fake) Commit() (*clientv3.TxnResponse, error) {
	f.calls.Add(1)
	return &clientv3.TxnResponse{
		Succeeded: true,
	}, f.err
}

func (f *Fake) DeletePair(_ context.Context, k string, v string) error {
	f.calls.Add(1)
	if f.delPairFn != nil {
		return f.delPairFn(context.Background(), k, v)
	}
	return f.err
}

func (f *Fake) PutIfNotExists(_ context.Context, k string, b string, _ ...clientv3.OpOption) (bool, error) {
	f.calls.Add(1)
	if f.putIfNotExistsFn != nil {
		return f.putIfNotExistsFn(context.Background(), k, b)
	}
	return true, f.err
}

func (f *Fake) Close() error {
	return f.err
}

func (f *Fake) Calls() uint32 {
	return f.calls.Load()
}

func (f *Fake) PutIfOtherHasRevision(_ context.Context, opts api.PutIfOtherHasRevisionOpts) (bool, error) {
	f.calls.Add(1)
	if f.putIfOtherHasRevisionFn != nil {
		return f.putIfOtherHasRevisionFn(context.Background(), opts)
	}
	return true, f.err
}

func (f *Fake) DeleteBothIfOtherHasRevision(_ context.Context, opts api.DeleteBothIfOtherHasRevisionOpts) error {
	f.calls.Add(1)
	if f.deleteBothIfOtherHasRevionFn != nil {
		return f.deleteBothIfOtherHasRevionFn(context.Background(), opts)
	}

	return f.err
}

func (f *Fake) DeletePrefixes(_ context.Context, prefixes ...string) error {
	f.calls.Add(1)
	if f.deletePrefixesFn != nil {
		return f.deletePrefixesFn(context.Background(), prefixes...)
	}
	return f.err
}
