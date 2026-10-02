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
	"google.golang.org/grpc"

	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/client/api"
)

func Embedded(t *testing.T) api.Interface {
	t.Helper()
	return Client(t, EmbeddedServer(t))
}

func EmbeddedBareClient(t *testing.T) *clientv3.Client {
	t.Helper()
	return BareClient(t, EmbeddedServer(t))
}

func Client(t *testing.T, endpoint string, dialOpts ...grpc.DialOption) api.Interface {
	t.Helper()
	return client.New(client.Options{
		Log:    logr.Discard(),
		Client: BareClient(t, endpoint, dialOpts...),
	})
}

func EmbeddedServer(t *testing.T) string {
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

	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(2 * time.Second):
		assert.Fail(t, "server took too long to start")
	}

	return etcd.Clients[0].Addr().String()
}

func BareClient(t *testing.T, endpoint string, dialOpts ...grpc.DialOption) *clientv3.Client {
	t.Helper()

	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialOptions: dialOpts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cl.Close()) })

	return cl
}
