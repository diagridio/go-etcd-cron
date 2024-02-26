/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"

	etcdclient "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type DistributedMutex interface {
	IsOwner() etcdclient.Cmp
	Key() string
	Lock(ctx context.Context) error
	TryLock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type EtcdMutexBuilder interface {
	NewMutex(pfx string) (DistributedMutex, error)
	NewJobStore(
		ctx context.Context,
		organizer Organizer,
		partitioning Partitioning,
		putCallback func(context.Context, Job) error,
		deleteCallback func(context.Context, string) error) (JobStore, error)
}

type etcdMutexBuilder struct {
	*etcdclient.Client
}

func NewEtcdMutexBuilder(config etcdclient.Config) (EtcdMutexBuilder, error) {
	c, err := etcdclient.New(config)
	if err != nil {
		return nil, err
	}
	return etcdMutexBuilder{Client: c}, nil
}

func (c etcdMutexBuilder) NewMutex(pfx string) (DistributedMutex, error) {
	// We keep the lock per run, reusing the lock over multiple iterations.
	// If we lose the lock, another instance will take it.
	session, err := concurrency.NewSession(c.Client)
	if err != nil {
		return nil, err
	}
	return concurrency.NewMutex(session, pfx), nil
}

func (c etcdMutexBuilder) NewJobStore(
	ctx context.Context,
	organizer Organizer,
	p Partitioning,
	putCallback func(context.Context, Job) error,
	deleteCallback func(context.Context, string) error) (JobStore, error) {
	return NewEtcdJobStore(ctx, c.Client, organizer, p, putCallback, deleteCallback)
}
