/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"testing"
	"time"

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/tests/framework/cron"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

func Test_leadership_change(t *testing.T) {
	t.Parallel()
	cron := cron.TripplePartitionRun(t)
	time.Sleep(time.Second)
	_, err := cron.Cron.List(context.Background(), "")
	require.NoError(t, err)
	leaderBytes, err := proto.Marshal(&stored.Leadership{
		Total: 0,
	})
	require.NoError(t, err)
	_, err = cron.Cron.KV.Do(context.Background(), clientv3.OpPut("abc/leadership/0", string(leaderBytes)))
	require.NoError(t, err)
	time.Sleep(time.Second * 3)
	_, err = cron.Cron.KV.Do(context.Background(), clientv3.OpDelete("abc/leadership/0"))
	require.NoError(t, err)
	time.Sleep(time.Second * 3)
}
