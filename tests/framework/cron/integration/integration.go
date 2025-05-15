/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package integration

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dapr/kit/concurrency/slice"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/internal/client"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

type Options struct {
	Instances uint64
	GotCh     chan *api.TriggerRequest
	TriggerFn func(*api.TriggerRequest) *api.TriggerResponse
	Client    *clientv3.Client
}

type Integration struct {
	ctx       context.Context
	closeCron func()
	client    clientapi.Interface
	api       api.Interface
	allCrons  []api.Interface
	triggered slice.Slice[string]
}

func NewBase(t *testing.T, instances uint64) *Integration {
	t.Helper()
	return New(t, Options{
		Instances: instances,
	})
}

func New(t *testing.T, opts Options) *Integration {
	t.Helper()

	cl := opts.Client
	if cl == nil {
		cl = etcd.EmbeddedBareClient(t)
	}

	wleader := make(chan []*anypb.Any)
	triggered := slice.String()
	allCrns := make([]api.Interface, opts.Instances)

	var err error
	for i := range opts.Instances {
		opts := cron.Options{
			Log:       logr.Discard(),
			Client:    cl,
			Namespace: "abc",
			ID:        strconv.FormatUint(i, 10),
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
		}

		if i == 0 {
			opts.WatchLeadership = wleader
		}

		allCrns[i], err = cron.New(opts)
		require.NoError(t, err)
	}

	a := allCrns[0]

	errCh := make(chan error, opts.Instances)
	ctx, cancel := context.WithCancel(context.Background())

	closeOnce := sync.OnceFunc(func() {
		cancel()
		for range opts.Instances {
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("timeout waiting for cron to stop")
			}
		}
	})
	t.Cleanup(closeOnce)
	for i := range opts.Instances {
		go func(i uint64) {
			errCh <- allCrns[i].Run(ctx)
		}(i)
	}

	waitForQuorum(t, wleader, opts.Instances)

	return &Integration{
		ctx:       ctx,
		client:    client.New(client.Options{Client: cl, Log: logr.Discard()}),
		api:       a,
		allCrons:  allCrns,
		triggered: triggered,
		closeCron: closeOnce,
	}
}

func waitForQuorum(t *testing.T, ch <-chan []*anypb.Any, exp uint64) {
	t.Helper()

	for {
		select {
		case <-time.After(time.Second * 30):
			require.Fail(t, "failed to get leadership quorum in time")
		case d := <-ch:
			if uint64(len(d)) == exp {
				return
			}
		}
	}
}

func (i *Integration) Context() context.Context {
	return i.ctx
}

func (i *Integration) Client() clientapi.Interface {
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
