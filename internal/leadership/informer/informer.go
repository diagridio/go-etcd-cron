/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package informer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/key"
)

type Options struct {
	// Client is the etcd client.
	Client client.Interface

	// Key is the ETCD key generator.
	Key *key.Key
}

// Informer watches the leadership namespace for changes, returning the latest
// key-value pairs when a change is observed.
type Informer struct {
	nsKey  string
	ch     clientv3.WatchChan
	client client.Interface
	inNext atomic.Bool
}

// New creates a new Informer. Returns the Informer and the initial key-value
// pairs in the leadership namespace.
func New(ctx context.Context, opts Options) (*Informer, error) {
	nsKey := opts.Key.LeadershipNamespace()

	resp, err := opts.Client.Get(ctx, nsKey, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("failed to get leadership namespace: %w", err)
	}

	ch := opts.Client.Watch(ctx,
		nsKey,
		clientv3.WithRev(resp.Header.Revision),
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
	)

	return &Informer{
		nsKey:  nsKey,
		ch:     ch,
		client: opts.Client,
	}, nil
}

// Next returns the next key-value pairs in the leadership namespace when a
// change is observed. The context is used to cancel the operation.
func (i *Informer) Next(ctx context.Context) (*clientv3.GetResponse, error) {
	if !i.inNext.CompareAndSwap(false, true) {
		return nil, errors.New("leadership informer next already in use")
	}
	defer i.inNext.Store(false)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e, ok := <-i.ch:
		if !ok {
			return nil, errors.New("leadership informer watch channel closed")
		}

		if err := e.Err(); err != nil {
			return nil, fmt.Errorf("leadership informer watch error: %w", err)
		}
		resp, err := i.client.Get(ctx, i.nsKey, clientv3.WithPrefix())
		if err != nil {
			return nil, fmt.Errorf("failed to get key: %w", err)
		}

		return resp, nil
	}
}
