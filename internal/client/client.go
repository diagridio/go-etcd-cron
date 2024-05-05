/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

func Delete(client clientv3.KV, keys ...string) error {
	for i := 0; i < len(keys); i += 128 {
		for {
			ikeys := keys[i:min(len(keys), i+128)]
			ok, err := deleteOp(client, ikeys...)
			if err != nil {
				return fmt.Errorf("failed to delete keys: %w", err)
			}

			if ok {
				break
			}

			time.Sleep(time.Second)
		}
	}

	return nil
}

func deleteOp(client clientv3.KV, keys ...string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ops := make([]clientv3.Op, len(keys))
	for i, key := range keys {
		ops[i] = clientv3.OpDelete(key)
	}

	_, err := client.Txn(ctx).Then(ops...).Commit()
	if err == nil {
		return true, nil
	}

	if strings.HasSuffix(err.Error(), etcdserver.ErrTooManyRequests.Error()) {
		return false, nil
	}

	return false, err
}
