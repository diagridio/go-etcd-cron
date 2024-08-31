/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/diagridio/go-etcd-cron/api"
)

// Add adds a new cron job to the cron instance.
func (c *cron) Add(ctx context.Context, name string, job *api.Job) error {
	select {
	case <-c.readyCh:
	case <-c.closeCh:
		return errors.New("cron is closed")
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.validateName(name); err != nil {
		return err
	}

	if job == nil {
		return errors.New("job cannot be nil")
	}

	storedJob, err := c.schedBuilder.Parse(job)
	if err != nil {
		return fmt.Errorf("job failed validation: %w", err)
	}

	b, err := proto.Marshal(storedJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	_, err = c.client.Put(ctx, c.key.JobKey(name), string(b))
	return err
}

// Get gets a cron job from the cron instance.
func (c *cron) Get(ctx context.Context, name string) (*api.Job, error) {
	select {
	case <-c.readyCh:
	case <-c.closeCh:
		return nil, errors.New("cron is closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if err := c.validateName(name); err != nil {
		return nil, err
	}

	resp, err := c.client.Get(ctx, c.key.JobKey(name))
	if err != nil {
		return nil, err
	}

	// No entry is not an error, but a nil object.
	if resp.Count == 0 {
		return nil, nil
	}

	var stored api.JobStored
	if err := proto.Unmarshal(resp.Kvs[0].Value, &stored); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return stored.GetJob(), nil
}

// Delete deletes a cron job from the cron instance.
func (c *cron) Delete(ctx context.Context, name string) error {
	select {
	case <-c.readyCh:
	case <-c.closeCh:
		return errors.New("cron is closed")
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.validateName(name); err != nil {
		return err
	}

	jobKey := c.key.JobKey(name)

	c.queueLock.Lock(jobKey)
	defer c.queueLock.DeleteUnlock(jobKey)

	if _, err := c.client.Delete(ctx, jobKey); err != nil {
		return err
	}

	if _, ok := c.queueCache.Load(jobKey); !ok {
		return nil
	}

	c.queueCache.Delete(jobKey)
	return c.queue.Dequeue(jobKey)
}

// DeletePrefixes deletes cron jobs with the given prefixes from the cron
// instance.
func (c *cron) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	select {
	case <-c.readyCh:
	case <-c.closeCh:
		return errors.New("cron is closed")
	case <-ctx.Done():
		return ctx.Err()
	}

	for _, prefix := range prefixes {
		if len(prefix) == 0 {
			continue
		}

		if err := c.validateName(prefix); err != nil {
			return err
		}
	}

	var errs []error
	removeFromCache := func(jobKey string) {
		c.queueLock.Lock(jobKey)
		defer c.queueLock.DeleteUnlock(jobKey)

		if _, ok := c.queueCache.Load(jobKey); ok {
			return
		}

		c.queueCache.Delete(jobKey)
		errs = append(errs, c.queue.Dequeue(jobKey))
	}

	for _, prefix := range prefixes {
		keyPrefix := c.key.JobKey(prefix)
		c.log.V(3).Info("deleting jobs with prefix", "prefix", keyPrefix)

		resp, err := c.client.Delete(ctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete jobs with prefix %q: %w", prefix, err))
			continue
		}

		for _, kv := range resp.PrevKvs {
			removeFromCache(string(kv.Key))
		}
	}

	return errors.Join(errs...)
}
