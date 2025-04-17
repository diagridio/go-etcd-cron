/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package api

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type PutIfOtherHasRevisionOpts struct {
	Key           string
	Val           string
	OtherKey      string
	OtherRevision int64
}

type DeleteBothIfOtherHasRevisionOpts struct {
	Key           string
	OtherKey      string
	OtherRevision int64
}

type Interface interface {
	clientv3.Lease
	clientv3.KV
	clientv3.Watcher

	DeletePair(context.Context, string, string) error
	PutIfNotExists(context.Context, string, string, ...clientv3.OpOption) (bool, error)

	PutIfOtherHasRevision(context.Context, PutIfOtherHasRevisionOpts) (bool, error)
	DeleteBothIfOtherHasRevision(context.Context, DeleteBothIfOtherHasRevisionOpts) error

	DeletePrefixes(context.Context, ...string) error
}
