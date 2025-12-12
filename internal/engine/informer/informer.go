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

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/leadership/partitioner"
)

// Options are the options for the Informer.
type Options struct {
	// Key provides the job key in the etcd data.
	Key *key.Key

	// Client is the etcd client to use for storing cron entries.
	Client api.Interface

	// Partitioner determines if a job belongs to this partition.
	Partitioner partitioner.Interface
}

// Informer informs when a job is either created or deleted in the etcd data
// for this partition.
type Informer struct {
	key    *key.Key
	client api.Interface
	part   partitioner.Interface

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
		readyCh: make(chan struct{}),
	}, ch
}

// Run runs the Informer.
func (i *Informer) Run(ctx context.Context) error {
	if !i.running.CompareAndSwap(false, true) {
		return errors.New("informer already running")
	}

	syncer := newSyncer(i.client, i.key.JobNamespace(), 0)
	respCh, errCh := syncer.SyncBase(ctx)
	for resp := range respCh {
		for _, ev := range resp.Kvs {
			event, err := i.handleEvent(&clientv3.Event{
				Type: clientv3.EventTypePut,
				Kv:   ev,
			})
			if err != nil {
				return fmt.Errorf("failed to handle base sync informer event: %w", err)
			}

			if event == nil {
				continue
			}

			for _, event := range event {
				select {
				case i.ch <- event:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}

	err := <-errCh
	if err != nil {
		return err
	}

	close(i.readyCh)

	ch := syncer.SyncUpdates(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case evs := <-ch:
			for _, ev := range evs.Events {
				events, err := i.handleEvent(ev)
				if err != nil {
					return fmt.Errorf("failed to handle informer event: %w", err)
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

func (i *Informer) handleEvent(ev *clientv3.Event) ([]*queue.Informed, error) {
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

		// If the event is a creation and the job is not managed by this partition,
		// we can ignore it.
		if ev.IsCreate() {
			return nil, nil
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
