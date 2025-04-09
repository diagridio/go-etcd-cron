/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/utils/clock"

	clienterrors "github.com/diagridio/go-etcd-cron/internal/client/errors"
)

type Interface interface {
	clientv3.Lease
	clientv3.KV
	clientv3.Watcher

	DeleteMulti(keys ...string) error
	PutIfNotExists(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
}

type Options struct {
	Client *clientv3.Client
	Log    logr.Logger
}

type client struct {
	*clientv3.Client
	kv    clientv3.KV
	log   logr.Logger
	clock clock.Clock
}

func New(opts Options) Interface {
	var kv clientv3.KV
	if opts.Client != nil {
		kv = opts.Client.KV
	}
	return &client{
		Client: opts.Client,
		kv:     kv,
		log:    opts.Log,
		clock:  clock.RealClock{},
	}
}

func (c *client) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return genericPP[string, string, clientv3.OpOption, clientv3.PutResponse](ctx, c.log, c, c.kv.Put, key, val, opts...)
}

func (c *client) PutIfNotExists(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	fn := func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
		tif := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		tthen := clientv3.OpPut(key, val, opts...)
		gresp, err := c.kv.Txn(ctx).If(tif).Then(tthen).Commit()
		if err != nil {
			return nil, err
		}

		if gresp == nil || !gresp.Succeeded {
			return nil, clienterrors.NewKeyAlreadyExists(key)
		}

		return gresp.OpResponse().Put(), nil
	}

	return genericPP[string, string, clientv3.OpOption, clientv3.PutResponse](ctx, c.log, c, fn, key, val, opts...)
}

func (c *client) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return genericP[string, clientv3.OpOption, clientv3.GetResponse](ctx, c.log, c, c.kv.Get, key, opts...)
}

func (c *client) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return genericP[string, clientv3.OpOption, clientv3.DeleteResponse](ctx, c.log, c, c.kv.Delete, key, opts...)
}

func (c *client) DeleteMulti(keys ...string) error {
	for i := 0; i < len(keys); i += 128 {
		batchI := min(128, len(keys)-i)
		ops := make([]clientv3.Op, batchI)
		for j := range batchI {
			ops[j] = clientv3.OpDelete(keys[i+j])
		}

		err := generic(context.Background(), c.log, c, func(ctx context.Context) error {
			_, terr := c.kv.Txn(ctx).Then(ops...).Commit()
			return terr
		})
		if err != nil {
			return fmt.Errorf("failed to delete keys (%d): %w", len(ops), err)
		}
	}

	return nil
}

type genericPPFunc[T any, K any, O any, R any] func(context.Context, T, K, ...O) (*R, error)

func genericPP[T any, K any, O any, R any](ctx context.Context, log logr.Logger, c *client, op genericPPFunc[T, K, O, R], t T, k K, o ...O) (*R, error) {
	var r *R
	var err error
	return r, generic(ctx, log, c, func(ctx context.Context) error {
		r, err = op(ctx, t, k, o...)
		return err
	})
}

type genericPFunc[T any, O any, R any] func(context.Context, T, ...O) (*R, error)

func genericP[T any, O any, R any](ctx context.Context, log logr.Logger, c *client, op genericPFunc[T, O, R], t T, o ...O) (*R, error) {
	var r *R
	var err error
	return r, generic(ctx, log, c, func(ctx context.Context) error {
		r, err = op(ctx, t, o...)
		return err
	})
}

func generic(ctx context.Context, log logr.Logger, c *client, op func(context.Context) error) error {
	for {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		err := op(ctx)
		if err == nil {
			return nil
		}

		if !errors.Is(err, rpctypes.ErrTooManyRequests) {
			return err
		}

		log.Error(err, "etcd client request rate limited, waiting before retrying")

		select {
		case <-c.clock.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
