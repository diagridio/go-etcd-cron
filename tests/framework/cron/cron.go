/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
)

type Cron struct {
	api.Interface
	KV     clientv3.KV
	Calls  *atomic.Int64
	cancel context.CancelFunc
}

func newCron(t *testing.T, client *clientv3.Client, id string) *Cron {
	t.Helper()

	replicaData, err := anypb.New(wrapperspb.Bytes([]byte("data")))
	require.NoError(t, err)
	var calls atomic.Int64
	cron, err := cron.New(cron.Options{
		Log:       logr.Discard(),
		Client:    client,
		Namespace: "abc",
		ID:        id,
		TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
			calls.Add(1)
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		},
		ReplicaData: replicaData,
	})
	require.NoError(t, err)

	return &Cron{
		Interface: cron,
		KV:        client.KV,
		Calls:     &calls,
	}
}

func (c *Cron) run(t *testing.T) *Cron {
	t.Helper()

	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	t.Cleanup(func() {
		c.Stop(t)
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for cron to stop")
		}
	})

	go func() {
		errCh <- c.Run(ctx)
	}()

	return c
}

func (c *Cron) Jobs(t *testing.T) *clientv3.GetResponse {
	t.Helper()
	return c.get(t, "abc/jobs")
}

func (c *Cron) Counters(t *testing.T) *clientv3.GetResponse {
	t.Helper()
	return c.get(t, "abc/counters")
}

func (c *Cron) get(t *testing.T, prefix string) *clientv3.GetResponse {
	t.Helper()

	resp, err := c.KV.Get(t.Context(), prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	return resp
}

func (c *Cron) Stop(t *testing.T) {
	t.Helper()

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}
