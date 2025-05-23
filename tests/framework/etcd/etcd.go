/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcd

import (
	"net/url"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/client/api"
)

func Embedded(t *testing.T) api.Interface {
	t.Helper()
	return client.New(client.Options{
		Log:    logr.Discard(),
		Client: EmbeddedBareClient(t),
	})
}

func EmbeddedBareClient(t *testing.T) *clientv3.Client {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.LogLevel = "error"
	cfg.Dir = t.TempDir()
	lurl, err := url.Parse("http://127.0.0.1:0")
	require.NoError(t, err)
	cfg.ListenPeerUrls = []url.URL{*lurl}
	cfg.ListenClientUrls = []url.URL{*lurl}

	etcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(etcd.Close)

	cl, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcd.Clients[0].Addr().String()},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cl.Close()) })

	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(2 * time.Second):
		assert.Fail(t, "server took too long to start")
	}

	return cl
}
