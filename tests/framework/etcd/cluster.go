/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcd

import (
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Cluster struct {
	Clients [3]*clientv3.Client
}

// TrippleEmbeddedBareClient creates three embedded etcd clients, each using a unique port.
func TrippleEmbeddedBareClient(t *testing.T) [3]*clientv3.Client {
	t.Helper()

	var clients [3]*clientv3.Client
	for i := 0; i < 3; i++ {
		clients[i] = EmbeddedBareClient(t)
	}

	return clients
}
