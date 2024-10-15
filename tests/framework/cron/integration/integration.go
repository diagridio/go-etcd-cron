/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dapr/kit/concurrency/slice"
	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

type Options struct {
	PartitionTotal uint32
	GotCh          chan *api.TriggerRequest
	TriggerFn      func(*api.TriggerRequest) *api.TriggerResponse
	Client         *clientv3.Client
}

type Integration struct {
	ctx       context.Context
	closeCron func()
	client    client.Interface
	api       api.Interface
	allCrons  []api.Interface
	triggered slice.Slice[string]
}

func NewBase(t *testing.T, partitionTotal uint32) *Integration {
	t.Helper()
	return New(t, Options{
		PartitionTotal: partitionTotal,
	})
}

func New(t *testing.T, opts Options) *Integration {
	t.Helper()

	require.Positive(t, opts.PartitionTotal)
	cl := opts.Client
	if cl == nil {
		cl = etcd.EmbeddedBareClient(t)
	}

	triggered := slice.String()
	var a api.Interface
	allCrns := make([]api.Interface, opts.PartitionTotal)
	for i := range opts.PartitionTotal {
		c, err := cron.New(cron.Options{
			Log:            logr.Discard(),
			Client:         cl,
			Namespace:      "abc",
			PartitionID:    i,
			PartitionTotal: opts.PartitionTotal,
			TriggerFn: func(_ context.Context, req *api.TriggerRequest) *api.TriggerResponse {
				defer triggered.Append(req.GetName())

				if opts.GotCh != nil {
					opts.GotCh <- req
				}
				if opts.TriggerFn != nil {
					return opts.TriggerFn(req)
				}
				return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
			},

			CounterGarbageCollectionInterval: ptr.Of(time.Millisecond * 300),
		})
		require.NoError(t, err)
		allCrns[i] = c
		if i == 0 {
			a = c
		}
	}

	errCh := make(chan error, opts.PartitionTotal)
	ctx, cancel := context.WithCancel(context.Background())

	closeOnce := sync.OnceFunc(func() {
		cancel()
		for range opts.PartitionTotal {
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("timeout waiting for cron to stop")
			}
		}
	})
	t.Cleanup(closeOnce)
	for i := range opts.PartitionTotal {
		go func(i uint32) {
			errCh <- allCrns[i].Run(ctx)
		}(i)
	}

	return &Integration{
		ctx:       ctx,
		client:    client.New(client.Options{Client: cl, Log: logr.Discard()}),
		api:       a,
		allCrons:  allCrns,
		triggered: triggered,
		closeCron: closeOnce,
	}
}

func (i *Integration) Context() context.Context {
	return i.ctx
}

func (i *Integration) Client() client.Interface {
	return i.client
}

func (i *Integration) API() api.Interface {
	return i.api
}

func (i *Integration) AllCrons() []api.Interface {
	return i.allCrons
}

func (i *Integration) Triggered() int {
	return i.triggered.Len()
}

func (i *Integration) TriggeredNames() []string {
	return i.triggered.Slice()
}

func (i *Integration) Close() {
	i.closeCron()
}
