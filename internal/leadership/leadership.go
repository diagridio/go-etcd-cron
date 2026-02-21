/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package leadership

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/elector"
	"github.com/diagridio/go-etcd-cron/internal/leadership/informer"
)

// Options are the options for the Leadership.
type Options struct {
	// Log is the logger for the leadership.
	Log logr.Logger

	// Client is the etcd client.
	Client api.Interface

	// Key is the ETCD key generator.
	Key *key.Key

	// ReplicaData is the replicaData for the instance using the cron library.
	// This will contain data like host + port for keeping track of active replicas.
	ReplicaData *anypb.Any

	// LeaseTTL is the TTL for the leadership lease. Used for testing. Default
	// 20s.
	leaseTTL *time.Duration
}

// Leadership manages the leadership for this replica. It will elect and
// re-elect leadership, returning contexts which cancel on lost quorum.
type Leadership struct {
	log         logr.Logger
	client      api.Interface
	key         *key.Key
	replicaData *anypb.Any

	leaseTTL int64
	elector  *elector.Elector

	running  atomic.Bool
	readyCh  chan struct{}
	changeCh chan struct{}
	closeCh  chan struct{}
}

func New(opts Options) *Leadership {
	leaseTTL := int64(20)
	if opts.leaseTTL != nil {
		leaseTTL = int64(opts.leaseTTL.Seconds())
	}

	return &Leadership{
		log:         opts.Log.WithName("leadership"),
		client:      opts.Client,
		key:         opts.Key,
		leaseTTL:    leaseTTL,
		replicaData: opts.ReplicaData,

		readyCh:  make(chan struct{}),
		changeCh: make(chan struct{}),
		closeCh:  make(chan struct{}),
	}
}

func (l *Leadership) Run(ctx context.Context) error {
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("leadership already running")
	}

	defer close(l.closeCh)

	informer, err := informer.New(ctx, informer.Options{
		Client: l.client,
		Key:    l.key,
	})
	if err != nil {
		return err
	}

	lease, err := l.client.Grant(ctx, l.leaseTTL)
	if err != nil {
		return err
	}

	l.elector = elector.New(elector.Options{
		Log:         l.log,
		Client:      l.client,
		Key:         l.key,
		ReplicaData: l.replicaData,
		LeaseID:     lease.ID,
		Informer:    informer,
	})

	close(l.readyCh)

	ch, err := l.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}

	for {
		_, ok := <-ch
		if !ok {
			break
		}
	}

	rctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err = l.client.Revoke(rctx, lease.ID)
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, rpctypes.ErrLeaseNotFound) ||
		errors.Is(err, rpctypes.ErrGRPCLeaseNotFound) {
		return nil
	}

	return err
}

// Elect will elect this replica as the leader. It will return a context which
// will cancel when the leadership quorum is lost.
func (l *Leadership) Elect(ctx context.Context) (context.Context, *elector.Elected, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-l.closeCh:
		return nil, nil, nil
	case <-l.readyCh:
		return l.elector.Elect(ctx)
	}
}

// Reelect will re-elect this replica as the leader. It will return a context
// which will cancel when the leadership quorum is lost. Should be called after
// initial election quorum is lost.
func (l *Leadership) Reelect(ctx context.Context) (context.Context, *elector.Elected, error) {
	select {
	case <-l.closeCh:
		return nil, nil, errors.New("cannot re-elect: leadership closed")
	case <-l.readyCh:
	default:
		return nil, nil, errors.New("leadership not ready")
	}

	return l.elector.Reelect(ctx)
}
