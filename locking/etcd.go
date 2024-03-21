/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package locking

import (
	"context"

	etcdclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type DistributedMutex interface {
	Key() string
	Lock(ctx context.Context) error
	TryLock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

func NewDistributedMutex(etcdclient *etcdclient.Client, pfx string) (DistributedMutex, error) {
	// We keep the lock per run, reusing the lock over multiple iterations.
	// If we lose the lock, another instance will take it.
	session, err := concurrency.NewSession(etcdclient)
	if err != nil {
		return nil, err
	}
	return concurrency.NewMutex(session, pfx), nil
}

func NewDistributedMutexBuilderFunc(etcdclient *etcdclient.Client) func(string) (DistributedMutex, error) {
	return func(pfx string) (DistributedMutex, error) {
		return NewDistributedMutex(etcdclient, pfx)
	}
}
