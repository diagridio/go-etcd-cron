/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcd

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

// TxnFaultInjector fails etcd Txn requests which put a key with the given
// prefix.
type TxnFaultInjector struct {
	prefix []byte

	lock   sync.Mutex
	faults []txnFault
	hits   atomic.Int64
}

type txnFault struct {
	err   error
	apply bool
}

func NewTxnFaultInjector(keyPrefix string) *TxnFaultInjector {
	return &TxnFaultInjector{prefix: []byte(keyPrefix)}
}

func (f *TxnFaultInjector) DialOption() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(f.intercept)
}

// FailNext fails the next n matching Txns without sending them.
func (f *TxnFaultInjector) FailNext(n int, err error) {
	f.add(n, txnFault{err: err})
}

// FailNextAfterApply sends the next n matching Txns, then returns err.
func (f *TxnFaultInjector) FailNextAfterApply(n int, err error) {
	f.add(n, txnFault{err: err, apply: true})
}

func (f *TxnFaultInjector) Pending() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return len(f.faults)
}

func (f *TxnFaultInjector) Hits() int64 {
	return f.hits.Load()
}

func (f *TxnFaultInjector) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if method != "/etcdserverpb.KV/Txn" || !f.matches(req) {
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	f.hits.Add(1)

	f.lock.Lock()
	if len(f.faults) == 0 {
		f.lock.Unlock()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	fault := f.faults[0]
	f.faults = f.faults[1:]
	f.lock.Unlock()

	if fault.apply {
		if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
			return err
		}
	}

	return fault.err
}

func (f *TxnFaultInjector) matches(req any) bool {
	txn, ok := req.(*etcdserverpb.TxnRequest)
	if !ok {
		return false
	}
	for _, ops := range [][]*etcdserverpb.RequestOp{txn.GetSuccess(), txn.GetFailure()} {
		for _, op := range ops {
			if put := op.GetRequestPut(); put != nil && bytes.HasPrefix(put.GetKey(), f.prefix) {
				return true
			}
		}
	}
	return false
}

func (f *TxnFaultInjector) add(n int, fault txnFault) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for range n {
		f.faults = append(f.faults, fault)
	}
}
