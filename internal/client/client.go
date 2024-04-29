/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

func Delete(client clientv3.KV, key string) error {
	for {
		ok, err := deleteOp(client, key)
		if err != nil || ok {
			return err
		}
		time.Sleep(time.Second)
	}
}

func deleteOp(client clientv3.KV, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := client.Delete(ctx, key)
	if err == nil {
		return true, nil
	}

	if strings.HasSuffix(err.Error(), etcdserver.ErrTooManyRequests.Error()) {
		return false, nil
	}

	return false, err
}
