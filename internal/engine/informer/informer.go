/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package informer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mirror"
	"google.golang.org/protobuf/proto"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/partitioner"
	"github.com/go-logr/logr"
)

// Options are the options for the Informer.
type Options struct {
	// Key provides the job key in the etcd data.
	Key *key.Key

	// Client is the etcd client to use for storing cron entries.
	Client api.Interface

	// Partitioner determines if a job belongs to this partition.
	Partitioner partitioner.Interface

	// Log is the logger to use for logging.
	Log logr.Logger
}

// Informer informs when a job is either created or deleted in the etcd data
// for this partition.
type Informer struct {
	key    *key.Key
	client api.Interface
	part   partitioner.Interface
	log    logr.Logger

	ch      chan *queue.Informed
	readyCh chan struct{}
	running atomic.Bool
}

// New creates a new Informer.
func New(opts Options) (*Informer, chan *queue.Informed) {
	ch := make(chan *queue.Informed)
	return &Informer{
		key:     opts.Key,
		part:    opts.Partitioner,
		client:  opts.Client,
		ch:      ch,
		log:     opts.Log.WithName("informer"),
		readyCh: make(chan struct{}),
	}, ch
}

// Run runs the Informer.
func (i *Informer) Run(ctx context.Context) error {
	if !i.running.CompareAndSwap(false, true) {
		return errors.New("informer already running")
	}

	var syncer mirror.Syncer
	var events []*queue.Informed
	var err error
	for {
		syncer, events, err = i.getSyncBase(ctx)
		if err == nil {
			break
		}

		if ctx.Err() != nil {
			return err
		}

		i.log.Error(err, "Failed to get base sync from informer, retrying.")
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for _, ev := range events {
		select {
		case i.ch <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	close(i.readyCh)

	ch := syncer.SyncUpdates(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case evs := <-ch:
			for _, ev := range evs.Events {
				events, err := i.handleEvent(ev, false)
				if err != nil {
					i.log.Error(err, "Failed to handle informer event, backing out to rebuild queue.")
					return nil
				}

				if len(events) == 0 {
					continue
				}

				for _, event := range events {
					select {
					case i.ch <- event:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}
}

func (i *Informer) getSyncBase(ctx context.Context) (mirror.Syncer, []*queue.Informed, error) {
	var events []*queue.Informed

	syncer := newSyncer(i.client, i.key.JobNamespace(), 0)
	respCh, errCh := syncer.SyncBase(ctx)
	for resp := range respCh {
		for _, ev := range resp.Kvs {
			event, err := i.handleEvent(&clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv:   ev,
			}, true)
			if err != nil {
				return nil, nil, err
			}

			events = append(events, event...)
		}
	}

	return syncer, events, <-errCh
}

func (i *Informer) handleEvent(ev *clientv3.Event, base bool) ([]*queue.Informed, error) {
	switch ev.Type {
	case clientv3.EventTypePut:
		var job stored.Job
		if err := proto.Unmarshal(ev.Kv.Value, &job); err != nil {
			return nil, fmt.Errorf("failed to unmarshal job: %w", err)
		}

		if i.part.IsJobManaged(job.GetPartitionId()) {
			if ev.IsCreate() {
				return []*queue.Informed{{
					IsPut: true,
					Name:  i.key.JobName(ev.Kv.Key),
					QueuedJob: &queue.QueuedJob{
						Stored:      &job,
						ModRevision: ev.Kv.ModRevision,
					},
				}}, nil
			}

			return []*queue.Informed{{
				IsPut: false,
				QueuedJob: &queue.QueuedJob{
					ModRevision: ev.PrevKv.ModRevision,
				},
			}, {
				IsPut: true,
				Name:  i.key.JobName(ev.Kv.Key),
				QueuedJob: &queue.QueuedJob{
					Stored:      &job,
					ModRevision: ev.Kv.ModRevision,
				},
			}}, nil
		}

		// If the event is a creation or we are doing the base sync and the job is
		// not managed by this partition, we can ignore it.
		if base || ev.IsCreate() {
			return nil, nil
		}

		if ev.PrevKv == nil {
			return nil, errors.New("previous key is nil for non-create event, likely due to loss of leadership or compaction, returning to rebuild queue")
		}

		return []*queue.Informed{{
			IsPut: false,
			QueuedJob: &queue.QueuedJob{
				ModRevision: ev.PrevKv.ModRevision,
			},
		}}, nil

	case clientv3.EventTypeDelete:
		return []*queue.Informed{{
			IsPut: false,
			QueuedJob: &queue.QueuedJob{
				ModRevision: ev.PrevKv.ModRevision,
			},
		}}, nil

	default:
		return nil, errors.New("unexpected event type")
	}
}

func (i *Informer) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.readyCh:
		return nil
	}
}
