/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
	etcdclient "go.etcd.io/etcd/client/v3"
)

// The JobStore persists and reads jobs from Etcd.
type JobStore interface {
	Put(ctx context.Context, job Job) error
	Delete(ctx context.Context, jobName string) error
}

type etcdStore struct {
	kvStore        etcdclient.KV
	watcher        etcdclient.Watcher
	partitioning   Partitioning
	organizer      Organizer
	putCallback    func(context.Context, Job) error
	deleteCallback func(context.Context, string) error
}

type noStore struct {
	putCallback    func(context.Context, Job) error
	deleteCallback func(context.Context, string) error
}

func NoStore(
	putCallback func(context.Context, Job) error,
	deleteCallback func(context.Context, string) error) JobStore {
	return &noStore{
		putCallback:    putCallback,
		deleteCallback: deleteCallback,
	}
}

func (s *noStore) Put(ctx context.Context, job Job) error {
	s.putCallback(ctx, job)
	return nil
}

func (s *noStore) Delete(ctx context.Context, jobName string) error {
	s.deleteCallback(ctx, jobName)
	return nil
}

func NewEtcdJobStore(
	ctx context.Context,
	client *etcdclient.Client,
	organizer Organizer,
	partitioning Partitioning,
	putCallback func(context.Context, Job) error,
	deleteCallback func(context.Context, string) error) (JobStore, error) {
	s := &etcdStore{
		kvStore:        etcdclient.NewKV(client),
		watcher:        etcdclient.NewWatcher(client),
		partitioning:   partitioning,
		organizer:      organizer,
		putCallback:    putCallback,
		deleteCallback: deleteCallback,
	}
	err := s.init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *etcdStore) init(ctx context.Context) error {
	for _, partitionId := range s.partitioning.ListPartitions() {
		// TODO(artursouza): parallelize this per partition.
		partitionPrefix := s.organizer.JobsPath(partitionId) + "/"
		s.watch(ctx, partitionPrefix)
		// TODO(artursouza): updates from watch and the loading below are competing,
		// needs to make sure the most recent version of each record wins.
		res, err := s.kvStore.Get(ctx, partitionPrefix, etcdclient.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range res.Kvs {
			err = s.notifyPut(ctx, kv, s.putCallback)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *etcdStore) Put(ctx context.Context, job Job) error {
	payloadBase64 := ""
	if job.TriggerPayload != nil {
		payloadBase64 = base64.StdEncoding.EncodeToString(job.TriggerPayload)
	}
	// TODO(artursouza): Use protobuf to serialize.
	_, err := s.kvStore.Put(
		ctx,
		s.organizer.JobPath(job.Name),
		fmt.Sprintf("%s||%s||%s||%s", job.Name, job.Rhythm, job.TriggerType, payloadBase64))
	return err
}

func (s *etcdStore) Delete(ctx context.Context, jobName string) error {
	_, err := s.kvStore.Delete(
		ctx,
		s.organizer.JobPath(jobName))
	return err
}

func (s *etcdStore) notifyPut(ctx context.Context, kv *mvccpb.KeyValue, callback func(context.Context, Job) error) error {
	value := string(kv.Value)
	split := strings.Split(value, "||")

	triggerType := ""
	if len(split) >= 3 {
		triggerType = split[2]
	}

	var payload []byte
	if len(split) >= 4 {
		payload, _ = base64.StdEncoding.DecodeString(split[3])
	}

	if len(split) < 2 {
		// Malformed entry, ignored.
		return nil
	}

	return callback(ctx, Job{
		Name:           split[0],
		Rhythm:         split[1],
		TriggerType:    triggerType,
		TriggerPayload: payload,
	})
}

func (s *etcdStore) notifyDelete(ctx context.Context, name string, callback func(context.Context, string) error) error {
	return callback(ctx, name)
}

func (s *etcdStore) watch(ctx context.Context, prefix string) {
	watchChan := s.watcher.Watch(ctx, prefix, etcdclient.WithPrefix())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notif := <-watchChan:
				if notif.Canceled {
					s.watch(ctx, prefix)
					return
				}
				for _, event := range notif.Events {
					switch event.Type {
					case mvccpb.PUT:
						s.notifyPut(ctx, event.Kv, s.putCallback)
					case mvccpb.DELETE:
						_, name := filepath.Split(string(event.Kv.Key))
						s.notifyDelete(ctx, name, s.deleteCallback)
					}
				}
			}
		}
	}()
}
