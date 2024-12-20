/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

func Test_leaderdata(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	crs := make([]api.Interface, 3)
	ch := make(chan []*anypb.Any)
	opts := cron.Options{Client: client}

	var err error
	for i := range 3 {
		if i == 0 {
			opts.WatchLeadership = ch
		}

		opts.TriggerFn = func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
			return &api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS}
		}
		opts.ID = strconv.Itoa(i)
		opts.ReplicaData = &anypb.Any{Value: []byte(opts.ID)}
		crs[i], err = cron.New(opts)
		require.NoError(t, err)
		opts.WatchLeadership = nil
	}

	errCh := make(chan error)

	ctx1, cancel1 := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel1(); require.NoError(t, <-errCh) })
	go func() { errCh <- crs[0].Run(ctx1) }()
	select {
	case <-time.After(time.Second * 3):
	case ds := <-ch:
		require.Len(t, ds, 1)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("0")}, ds[0]), ds)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel2(); require.NoError(t, <-errCh) })
	go func() { errCh <- crs[1].Run(ctx2) }()
	select {
	case <-time.After(time.Second * 3):
	case ds := <-ch:
		require.Len(t, ds, 2)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("0")}, ds[0]), ds)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("1")}, ds[1]), ds)
	}

	ctx3, cancel3 := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel3(); require.NoError(t, <-errCh) })
	go func() { errCh <- crs[2].Run(ctx3) }()
	select {
	case <-time.After(time.Second * 3):
	case ds := <-ch:
		require.Len(t, ds, 3)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("0")}, ds[0]), ds)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("1")}, ds[1]), ds)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("2")}, ds[2]), ds)
	}

	cancel2()
	select {
	case <-time.After(time.Second * 3):
	case ds := <-ch:
		require.Len(t, ds, 2)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("0")}, ds[0]), ds)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("2")}, ds[1]), ds)
	}

	cancel3()
	select {
	case <-time.After(time.Second * 3):
	case ds := <-ch:
		require.Len(t, ds, 1)
		assert.True(t, proto.Equal(&anypb.Any{Value: []byte("0")}, ds[0]), ds)
	}
}
