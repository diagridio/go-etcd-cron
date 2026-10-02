/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcd

import (
	"net"
	"net/url"
	"strconv"
	"strings"
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

// Cluster is a multi member embedded etcd cluster.
type Cluster struct {
	Members []*embed.Etcd
}

func EmbeddedCluster(t *testing.T, size int) *Cluster {
	t.Helper()

	peers := make([]string, size)
	for i := range peers {
		peers[i] = "http://127.0.0.1:" + strconv.Itoa(freePort(t))
	}

	initial := make([]string, size)
	for i, p := range peers {
		initial[i] = "m" + strconv.Itoa(i) + "=" + p
	}

	cl := &Cluster{Members: make([]*embed.Etcd, size)}
	for i := range cl.Members {
		cfg := embed.NewConfig()
		cfg.LogLevel = "error"
		cfg.Name = "m" + strconv.Itoa(i)
		cfg.Dir = t.TempDir()
		cfg.InitialCluster = strings.Join(initial, ",")
		cfg.ClusterState = embed.ClusterStateFlagNew

		purl, err := url.Parse(peers[i])
		require.NoError(t, err)
		curl, err := url.Parse("http://127.0.0.1:" + strconv.Itoa(freePort(t)))
		require.NoError(t, err)
		cfg.ListenPeerUrls = []url.URL{*purl}
		cfg.AdvertisePeerUrls = []url.URL{*purl}
		cfg.ListenClientUrls = []url.URL{*curl}
		cfg.AdvertiseClientUrls = []url.URL{*curl}

		etcd, err := embed.StartEtcd(cfg)
		require.NoError(t, err)
		t.Cleanup(etcd.Close)
		cl.Members[i] = etcd
	}

	for _, m := range cl.Members {
		select {
		case <-m.Server.ReadyNotify():
		case <-time.After(10 * time.Second):
			require.Fail(t, "cluster took too long to start")
		}
	}

	return cl
}

func (c *Cluster) Endpoint(i int) string {
	return c.Members[i].Clients[0].Addr().String()
}

func (c *Cluster) Leader() *embed.Etcd {
	for _, m := range c.Members {
		if m.Server.Leader() == m.Server.ID() {
			return m
		}
	}
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
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
