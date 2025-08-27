/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/utils/clock"

	"github.com/diagridio/go-etcd-cron/internal/client/api"
	clienterrors "github.com/diagridio/go-etcd-cron/internal/client/errors"
)

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

func New(opts Options) api.Interface {
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

func (c *client) PutIfNotExists(ctx context.Context, key, val string, opts ...clientv3.OpOption) (bool, error) {
	var put bool
	err := generic(ctx, c.log, c, func(ctx context.Context) error {
		tif := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		tthen := clientv3.OpPut(key, val, opts...)
		gresp, err := c.kv.Txn(ctx).If(tif).Then(tthen).
			Commit()
		if err != nil {
			return err
		}

		put = gresp != nil && gresp.Succeeded

		return nil
	})

	return put, err
}

func (c *client) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return genericP[string, clientv3.OpOption, clientv3.GetResponse](ctx, c.log, c, c.kv.Get, key, opts...)
}

func (c *client) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return genericP[string, clientv3.OpOption, clientv3.DeleteResponse](ctx, c.log, c, c.kv.Delete, key, opts...)
}

func (c *client) DeletePair(ctx context.Context, key1, key2 string) error {
	err := generic(ctx, c.log, c, func(ctx context.Context) error {
		_, terr := c.kv.Txn(ctx).Then(
			clientv3.OpDelete(key1),
			clientv3.OpDelete(key2),
		).Commit()
		return terr
	})
	if err != nil {
		return fmt.Errorf("failed to delete keys (%s %s): %w", key1, key2, err)
	}

	return nil
}

// PutIfOtherHasRevision puts the value at the key if the other key has the given revision.
func (c *client) PutIfOtherHasRevision(ctx context.Context, opts api.PutIfOtherHasRevisionOpts) (bool, error) {
	var didput bool
	err := generic(ctx, c.log, c, func(ctx context.Context) error {
		resp, err := c.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(opts.OtherKey), "=", opts.OtherRevision)).
			Then(clientv3.OpPut(opts.Key, opts.Val)).
			Commit()
		if err != nil {
			return err
		}

		didput = resp.Succeeded
		return nil
	})

	return didput, err
}

// DeleteBothIfOtherHasRevision deletes both keys if the other key has the given revision.
func (c *client) DeleteBothIfOtherHasRevision(ctx context.Context, opts api.DeleteBothIfOtherHasRevisionOpts) error {
	return generic(ctx, c.log, c, func(ctx context.Context) error {
		_, err := c.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(opts.OtherKey), "=", opts.OtherRevision)).
			Then(clientv3.OpDelete(opts.Key), clientv3.OpDelete(opts.OtherKey)).
			Else(clientv3.OpDelete(opts.Key)).
			Commit()

		return err
	})
}

func (c *client) DeleteIfHasRevision(ctx context.Context, key string, revision int64) error {
	return generic(ctx, c.log, c, func(ctx context.Context) error {
		_, err := c.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
			Then(clientv3.OpDelete(key)).
			Commit()
		if err != nil {
			return err
		}

		return nil
	})
}

// DeletePrefixes deletes all keys with the given prefixes.
func (c *client) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	return generic(ctx, c.log, c, func(ctx context.Context) error {
		ops := make([]clientv3.Op, 0, len(prefixes))
		for _, prefix := range prefixes {
			ops = append(ops, clientv3.OpDelete(prefix, clientv3.WithPrefix()))
		}

		_, err := c.kv.Txn(ctx).Then(ops...).Commit()

		return err
	})
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
		err := op(ctx)
		if err == nil {
			return nil
		}

		if !clienterrors.ShouldRetry(err) {
			return err
		}

		log.Error(err, "etcd error, waiting before retrying")

		jitter := time.Duration(rand.IntN(1000)) * time.Millisecond
		select {
		case <-c.clock.After(time.Second + jitter):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
