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

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
)

type Cron struct {
	api.Interface
	KV    clientv3.KV
	Calls *atomic.Int64
}

func newCron(t *testing.T, client *clientv3.Client, total, id uint32) *Cron {
	var calls atomic.Int64
	cron, err := cron.New(cron.Options{
		Log:                              logr.Discard(),
		Client:                           client,
		Namespace:                        "abc",
		PartitionID:                      id,
		PartitionTotal:                   total,
		TriggerFn:                        func(context.Context, *api.TriggerRequest) bool { calls.Add(1); return true },
		CounterGarbageCollectionInterval: ptr.Of(time.Millisecond * 300),
	})
	require.NoError(t, err)

	return &Cron{
		Interface: cron,
		KV:        client.KV,
		Calls:     &calls,
	}
}

func (c *Cron) run(t *testing.T) *Cron {
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
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

	resp, err := c.KV.Get(context.Background(), prefix, clientv3.WithPrefix())
	require.NoError(t, err)

	return resp
}
