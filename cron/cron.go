/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/concurrency"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	internalapi "github.com/diagridio/go-etcd-cron/internal/api"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/engine"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
)

// Options are the options for creating a new cron instance.
type Options struct {
	// Log is the logger to use for logging.
	Log logr.Logger

	// Client is the etcd client to use for storing cron entries.
	Client *clientv3.Client

	// Namespace is the etcd namespace to use for storing cron entries.
	Namespace string

	// PartitionID is the partition ID to use for storing cron entries.
	PartitionID uint32

	// PartitionTotal is the total number of partitions to use for storing cron
	// entries.
	PartitionTotal uint32

	// TriggerFn is the function to call when a cron job is triggered.
	TriggerFn api.TriggerFunction

	// CounterGarbageCollectionInterval is the interval at which to run the
	// garbage collection for counters is run. Counters are also garbage
	// collected on shutdown. Counters are batch deleted, so a larger value
	// increases the counter bucket and reduces the number of database
	// operations.
	// This value rarely needs to be set and is mostly used for testing. A small
	// interval value will increase database operations and thus degrade cron
	// performance.
	// Defaults to 180 seconds.
	CounterGarbageCollectionInterval *time.Duration

	// ReplicaData is custom data associated with the replica, for example,
	// host + port for the active replica. This data will be written to the leadership keyspace, with the latest cluster values being returned from `WatchLeadership`. Useful for consumer coordination.
	ReplicaData *anypb.Any
}

// cron is the implementation of the cron interface.
type cron struct {
	log logr.Logger
	wg  sync.WaitGroup

	key         *key.Key
	leadership  *leadership.Leadership
	client      client.Interface
	part        partitioner.Interface
	triggerFn   api.TriggerFunction
	gcgInterval *time.Duration

	lock    sync.RWMutex
	engine  atomic.Pointer[engine.Engine]
	running atomic.Bool
	readyCh chan struct{}
	closeCh chan struct{}
}

// New creates a new cron instance.
func New(opts Options) (api.Interface, error) {
	if opts.TriggerFn == nil {
		return nil, errors.New("trigger function is required")
	}

	if opts.Client == nil {
		return nil, errors.New("client is required")
	}

	log := opts.Log
	if log.GetSink() == nil {
		sink, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
		if err != nil {
			return nil, err
		}
		log = zapr.NewLogger(sink)
		log = log.WithName("diagrid-cron")
	}

	part, err := partitioner.New(partitioner.Options{
		ID:    opts.PartitionID,
		Total: opts.PartitionTotal,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create partitioner: %w", err)
	}

	client := client.New(client.Options{
		Log:    opts.Log,
		Client: opts.Client,
	})

	key := key.New(key.Options{
		Namespace:   opts.Namespace,
		PartitionID: opts.PartitionID,
	})

	leadership := leadership.New(leadership.Options{
		Log:            log,
		Client:         client,
		PartitionTotal: opts.PartitionTotal,
		Key:            key,
	})

	return &cron{
		log:         log,
		wg:          sync.WaitGroup{},
		key:         key,
		client:      client,
		part:        part,
		triggerFn:   opts.TriggerFn,
		gcgInterval: opts.CounterGarbageCollectionInterval,
		leadership:  leadership,
		readyCh:     make(chan struct{}),
		closeCh:     make(chan struct{}),
	}, nil
}

// Run is a blocking function that runs the cron instance.
func (c *cron) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("cron already running")
	}

	defer close(c.closeCh)

	return concurrency.NewRunnerManager(
		c.leadership.Run,
		func(ctx context.Context) error {
			for {
				engine, err := engine.New(engine.Options{
					Log:                              c.log,
					Key:                              c.key,
					Partitioner:                      c.part,
					Client:                           c.client,
					TriggerFn:                        c.triggerFn,
					Leadership:                       c.leadership,
					CounterGarbageCollectionInterval: c.gcgInterval,
				})
				if err != nil {
					return err
				}

				c.engine.Store(engine)

				leadershipCtx, err := c.leadership.WaitForLeadership(ctx)
				if err != nil {
					return err
				}

				c.lock.Lock()
				close(c.readyCh)
				c.lock.Unlock()
				err = engine.Run(leadershipCtx)

				c.lock.Lock()
				c.readyCh = make(chan struct{})
				c.lock.Unlock()

				if ctx.Err() != nil {
					c.log.Info("cron shutdown gracefully")
					return err
				}

				c.log.Info("Restarting engine due to leadership change")
			}
		},
	).Run(ctx)
}

// Add forwards the call to the embedded API.
func (c *cron) Add(ctx context.Context, name string, job *api.Job) error {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return err
	}

	return api.Add(ctx, name, job)
}

// Get forwards the call to the embedded API.
func (c *cron) Get(ctx context.Context, name string) (*api.Job, error) {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return nil, err
	}

	return api.Get(ctx, name)
}

// Delete forwards the call to the embedded API.
func (c *cron) Delete(ctx context.Context, name string) error {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return err
	}

	return api.Delete(ctx, name)
}

// DeletePrefixes forwards the call to the embedded API.
func (c *cron) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return err
	}

	return api.DeletePrefixes(ctx, prefixes...)
}

// List forwards the call to the embedded API.
func (c *cron) List(ctx context.Context, prefix string) (*api.ListResponse, error) {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return nil, err
	}

	return api.List(ctx, prefix)
}

// DeliverablePrefixes forwards the call to the embedded API.
func (c *cron) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return nil, err
	}

	return api.DeliverablePrefixes(ctx, prefixes...)
}

func (c *cron) WatchLeadership(ctx context.Context) (chan []*anypb.Any, error) {
	api, err := c.waitAPIReady(ctx)
	if err != nil {
		return nil, err
	}

	return api.WatchLeadership(ctx)
}

func (c *cron) waitAPIReady(ctx context.Context) (internalapi.Interface, error) {
	c.lock.Lock()
	readyCh := c.readyCh
	c.lock.Unlock()

	select {
	case <-readyCh:
		return c.engine.Load().API(), nil
	case <-c.closeCh:
		return nil, errors.New("cron is closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
