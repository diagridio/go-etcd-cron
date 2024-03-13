/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package locking

import (
	"context"

	etcdclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/diagridio/go-etcd-cron/partitioning"
	"github.com/diagridio/go-etcd-cron/storage"
)

type DistributedMutex interface {
	Key() string
	Lock(ctx context.Context) error
	TryLock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type EtcdMutexBuilder interface {
	NewMutex(pfx string) (DistributedMutex, error)
	NewJobStore(
		organizer partitioning.Organizer,
		partitioning partitioning.Partitioner,
		putCallback func(context.Context, *storage.JobRecord) error,
		deleteCallback func(context.Context, string) error) storage.JobStore
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
