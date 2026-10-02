/*
Copyright (c) 2026 Diagrid Inc.
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

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

// Cluster is a multi member embedded etcd cluster.
type Cluster struct {
	Members []*embed.Etcd
}

// EmbeddedCluster starts size members that know each other from the start.
// Peer ports are held by a listener until the member binds them, client
// ports are chosen by etcd.
func EmbeddedCluster(t *testing.T, size int) *Cluster {
	t.Helper()

	listeners := make([]net.Listener, size)
	initial := make([]string, size)
	for i := range size {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		initial[i] = "m" + strconv.Itoa(i) + "=http://" + l.Addr().String()
	}

	cl := &Cluster{Members: make([]*embed.Etcd, size)}
	for i := range size {
		cfg := embed.NewConfig()
		cfg.LogLevel = "error"
		cfg.Name = "m" + strconv.Itoa(i)
		cfg.Dir = t.TempDir()
		cfg.InitialCluster = strings.Join(initial, ",")
		cfg.ClusterState = embed.ClusterStateFlagNew

		purl, err := url.Parse("http://" + listeners[i].Addr().String())
		require.NoError(t, err)
		curl, err := url.Parse("http://127.0.0.1:0")
		require.NoError(t, err)
		cfg.ListenPeerUrls = []url.URL{*purl}
		cfg.AdvertisePeerUrls = []url.URL{*purl}
		cfg.ListenClientUrls = []url.URL{*curl}

		require.NoError(t, listeners[i].Close())
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
