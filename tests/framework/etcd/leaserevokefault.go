/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package etcd

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

// LeaseRevokeDropper drops LeaseRevoke requests the way an etcd leader change
// does. The request is never sent and the call blocks until its context ends.
type LeaseRevokeDropper struct {
	drops atomic.Int64
	hits  atomic.Int64

	lock  sync.Mutex
	fails []error
}

func NewLeaseRevokeDropper() *LeaseRevokeDropper {
	return new(LeaseRevokeDropper)
}

func (d *LeaseRevokeDropper) DialOption() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(d.intercept)
}

// DropNext drops the next n LeaseRevoke requests.
func (d *LeaseRevokeDropper) DropNext(n int64) {
	d.drops.Store(n)
}

// FailNext fails the next LeaseRevoke request with err without sending it.
func (d *LeaseRevokeDropper) FailNext(err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.fails = append(d.fails, err)
}

func (d *LeaseRevokeDropper) Hits() int64 {
	return d.hits.Load()
}

func (d *LeaseRevokeDropper) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if method != "/etcdserverpb.Lease/LeaseRevoke" {
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	d.hits.Add(1)

	d.lock.Lock()
	if len(d.fails) > 0 {
		err := d.fails[0]
		d.fails = d.fails[1:]
		d.lock.Unlock()
		return err
	}
	d.lock.Unlock()

	if d.drops.Load() > 0 {
		d.drops.Add(-1)
		<-ctx.Done()
		return ctx.Err()
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}
