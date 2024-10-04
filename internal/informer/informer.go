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

	"github.com/diagridio/go-etcd-cron/internal/api/stored"
	"github.com/diagridio/go-etcd-cron/internal/client"
	"github.com/diagridio/go-etcd-cron/internal/garbage"
	"github.com/diagridio/go-etcd-cron/internal/grave"
	"github.com/diagridio/go-etcd-cron/internal/key"
	"github.com/diagridio/go-etcd-cron/internal/partitioner"
)

// Options are the options for the Informer.
type Options struct {
	// Key provides the job key in the etcd data.
	Key *key.Key

	// Client is the etcd client to use for storing cron entries.
	Client client.Interface

	// Partitioner determines if a job belongs to this partition.
	Partitioner partitioner.Interface

	// Yard is a graveyard to check whether a job has just been deleted, and
	// therefore a delete informer event should be ignored.
	Yard *grave.Yard

	// Collector is the garbage collector to use for collecting counters which
	// need to be deleted at some point. Used to prevent a counter from being
	// deleted after a job gets re-created with the same name.
	Collector garbage.Interface
}

// Informer informs when a job is either created or deleted in the etcd data
// for this partition.
type Informer struct {
	key       *key.Key
	client    client.Interface
	part      partitioner.Interface
	yard      *grave.Yard
	collector garbage.Interface

	eventsCalled atomic.Bool
	ch           chan *Event
	readyCh      chan struct{}
	running      atomic.Bool
}

// Event is the event that is sent.
type Event struct {
	// IsPut is true if the event is a put event, else it is a delete event.
	IsPut bool

	// Key is the ETCD key that was created or deleted.
	Key []byte

	// Job is the job that was created or deleted.
	Job *stored.Job
}

// New creates a new Informer.
func New(opts Options) *Informer {
	return &Informer{
		key:       opts.Key,
		part:      opts.Partitioner,
		client:    opts.Client,
		yard:      opts.Yard,
		collector: opts.Collector,
		ch:        make(chan *Event, 1000),
		readyCh:   make(chan struct{}),
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

func (i *Informer) handleEvent(ev *clientv3.Event) (*Event, error) {
	var isPut bool
	var kv *mvccpb.KeyValue
	switch ev.Type {
	case clientv3.EventTypePut:
		isPut = true
		kv = ev.Kv
		i.collector.Pop(i.key.CounterKey(i.key.JobName(ev.Kv.Key)))
	case clientv3.EventTypeDelete:
		// If this job key has just been deleted (by us as the collector ticked
		// nil), return early and ignore the deletion.
		if i.yard.HasJustDeleted(string(ev.PrevKv.Key)) {
			return nil, nil
		}
		if ev.Kv != nil {
			i.collector.Push(i.key.CounterKey(i.key.JobName(ev.Kv.Key)))
		}

		kv = ev.PrevKv
	default:
		return nil, errors.New("unexpected event type")
	}

	var job stored.Job
	if err := proto.Unmarshal(kv.Value, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	if !i.part.IsJobManaged(job.GetPartitionId()) {
		return nil, nil
	}

	return &Event{
		IsPut: isPut,
		Key:   kv.Key,
		Job:   &job,
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

func (i *Informer) Events() (<-chan *Event, error) {
	if !i.eventsCalled.CompareAndSwap(false, true) {
		return nil, errors.New("events already being consumed")
	}
	return i.ch, nil
}
