/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dapr/kit/concurrency"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/client"
	clientapi "github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/engine"
	"github.com/diagridio/go-etcd-cron/internal/engine/handler/retry"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership"
	"github.com/diagridio/go-etcd-cron/internal/leadership/elector"
)

// Options are the options for creating a new cron instance.
type Options struct {
	// Log is the logger to use for logging.
	Log logr.Logger

	// Client is the etcd client to use for storing cron entries.
	Client *clientv3.Client

	// Namespace is the etcd namespace to use for storing cron entries.
	Namespace string

	// ID is the unique ID which is associated with this replica. Duplicate IDs
	// will cause only one random replica to be active, with the rest being
	// dormant.
	ID string

	// TriggerFn is the function to call when a cron job is triggered.
	TriggerFn api.TriggerFunction

	// ReplicaData is custom data associated with the replica, for example,
	// host + port for the active replica. This data will be written to the
	// leadership keyspace, with the latest cluster values being returned from
	// `WatchLeadership`. Useful for consumer coordination.
	ReplicaData *anypb.Any

	// WatchLeadership is an optional channel that will be written with all
	// leader replica data every time there is leadership quorum. Useful for
	// consumer coordination. Failing to read from this channel will cause the
	// replica to fail to start the cron engine.
	WatchLeadership chan<- []*anypb.Any

	// ConsumerSink is an optional channel that will be written with all informer
	// events for this partition. During shutdown and leadership events, the sink
	// will receive a `DropAll` event. If defined, this channel will block the
	// queue unless read from.
	// Channel will be closed on final shutdown.
	ConsumerSink chan<- *api.InformerEvent
}

// cron is the implementation of the cron interface.
type cron struct {
	log logr.Logger

	key          *key.Key
	client       clientapi.Interface
	triggerFn    api.TriggerFunction
	replicaData  *anypb.Any
	consumerSink chan<- *api.InformerEvent

	api       *retry.Retry
	elected   atomic.Bool
	wleaderCh chan<- []*anypb.Any

	running atomic.Bool
}

// New creates a new cron instance.
func New(opts Options) (api.Interface, error) {
	if opts.TriggerFn == nil {
		return nil, errors.New("trigger function is required")
	}

	if opts.Client == nil {
		return nil, errors.New("client is required")
	}

	key, err := key.New(key.Options{
		Namespace: opts.Namespace,
		ID:        opts.ID,
	})
	if err != nil {
		return nil, err
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
	log = log.WithValues("id", opts.ID)

	client := client.New(client.Options{
		Log:    opts.Log,
		Client: opts.Client,
	})

	return &cron{
		log:          log,
		key:          key,
		replicaData:  opts.ReplicaData,
		client:       client,
		triggerFn:    opts.TriggerFn,
		wleaderCh:    opts.WatchLeadership,
		api:          retry.New(retry.Options{Log: log}),
		consumerSink: opts.ConsumerSink,
	}, nil
}

// Run is a blocking function that runs the cron instance.
func (c *cron) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("cron already running")
	}

	defer func() {
		c.log.Info("cron instance shutdown")
		c.api.Close()

		// If defined, close consumer sink to single end.
		if c.consumerSink != nil {
			close(c.consumerSink)
		}
	}()

	leadership := leadership.New(leadership.Options{
		Log:         c.log,
		Client:      c.client,
		Key:         c.key,
		ReplicaData: c.replicaData,
	})

	return concurrency.NewRunnerManager(
		leadership.Run,
		func(ctx context.Context) error {
			ectx, elected, err := leadership.Elect(ctx)
			if err != nil || elected == nil {
				return err
			}

			if err := c.runEngine(ectx, elected); err != nil {
				return err
			}

			for {
				c.log.Info("engine restarting due to leadership rebalance")

				ectx, elected, err := leadership.Reelect(ctx)
				if ctx.Err() != nil {
					c.log.Error(err, "cron instance shutting down during leadership re-election")
					return ctx.Err()
				}

				if err != nil {
					return err
				}

				c.log.Info("starting engine after re-election")

				if err := c.runEngine(ectx, elected); err != nil {
					return err
				}
			}
		},
	).Run(ctx)
}

// Add forwards the call to the embedded API.
func (c *cron) Add(ctx context.Context, name string, job *api.Job) error {
	return c.api.Add(ctx, name, job)
}

// AddIfNotExists forwards the call to the embedded API. If the Job already
// exists, then an error is returned.
// Error can be checked against `JobAlreadyExists` in `/api/errors/exists`.
func (c *cron) AddIfNotExists(ctx context.Context, name string, job *api.Job) error {
	return c.api.AddIfNotExists(ctx, name, job)
}

// Get forwards the call to the embedded API.
func (c *cron) Get(ctx context.Context, name string) (*api.Job, error) {
	return c.api.Get(ctx, name)
}

// Delete forwards the call to the embedded API.
func (c *cron) Delete(ctx context.Context, name string) error {
	return c.api.Delete(ctx, name)
}

// DeletePrefixes forwards the call to the embedded API.
func (c *cron) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	return c.api.DeletePrefixes(ctx, prefixes...)
}

// List forwards the call to the embedded API.
func (c *cron) List(ctx context.Context, prefix string) (*api.ListResponse, error) {
	return c.api.List(ctx, prefix)
}

// DeliverablePrefixes forwards the call to the embedded API.
func (c *cron) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelCauseFunc, error) {
	return c.api.DeliverablePrefixes(ctx, prefixes...)
}

// IsElected returns true if cron is currently elected for leadership of its
// partition.
func (c *cron) IsElected() bool {
	return c.elected.Load()
}

// runEngine runs the cron engine with the given elected leadership.
func (c *cron) runEngine(ctx context.Context, elected *elector.Elected) error {
	c.elected.Store(true)

	engine, err := engine.New(engine.Options{
		Log:          c.log,
		Key:          c.key,
		Partitioner:  elected.Partitioner,
		Client:       c.client,
		TriggerFn:    c.triggerFn,
		ConsumerSink: c.consumerSink,
	})
	if err != nil {
		return err
	}

	c.api.Ready(engine)

	if c.wleaderCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.wleaderCh <- elected.LeadershipData:
		}
	}

	err = engine.Run(ctx)
	c.elected.Store(false)

	c.api.NotReady()

	if err != nil || ctx.Err() != nil {
		return err
	}

	c.log.Info("engine restarting due to leadership rebalance")

	return nil
}
