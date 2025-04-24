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

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
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

	eventsCalled atomic.Bool
	ch           chan *queue.Informed
	readyCh      chan struct{}
	running      atomic.Bool
}

// New creates a new Informer.
func New(opts Options) *Informer {
	return &Informer{
		key:     opts.Key,
		part:    opts.Partitioner,
		client:  opts.Client,
		ch:      make(chan *queue.Informed),
		readyCh: make(chan struct{}),
	}
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

			select {
			case i.ch <- event:
			case <-ctx.Done():
				return ctx.Err()
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
				event, err := i.handleEvent(ev)
				if err != nil {
					return fmt.Errorf("failed to handle informer event: %w", err)
				}

				if event == nil {
					continue
				}

				select {
				case i.ch <- event:
				case <-ctx.Done():
				}
			}
		}
	}
}

func (i *Informer) handleEvent(ev *clientv3.Event) (*queue.Informed, error) {
	var isPut bool
	var kv *mvccpb.KeyValue
	switch ev.Type {
	case clientv3.EventTypePut:
		isPut = true
		kv = ev.Kv
	case clientv3.EventTypeDelete:

		kv = ev.PrevKv
	default:
		return nil, errors.New("unexpected event type")
	}

	var job stored.Job
	if err := proto.Unmarshal(kv.Value, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	jobName := i.key.JobName(ev.Kv.Key)
	if !i.part.IsJobManaged(job.GetPartitionId()) {
		return &queue.Informed{
			IsPut: false,
			Name:  jobName,
		}, nil
	}

	return &queue.Informed{
		IsPut: isPut,
		Name:  jobName,
		Job:   &job,

		JobModRevision: kv.ModRevision,
	}, nil
}

func (i *Informer) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.readyCh:
		return nil
	}
}

func (i *Informer) Events() (<-chan *queue.Informed, error) {
	if !i.eventsCalled.CompareAndSwap(false, true) {
		return nil, errors.New("events already being consumed")
	}
	return i.ch, nil
}
