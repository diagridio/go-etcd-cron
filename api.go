/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

	if err := validateName(name); err != nil {
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

	if err := validateName(name); err != nil {
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

	if err := validateName(name); err != nil {
		return err
	}

	if _, err := c.client.Delete(ctx, c.key.JobKey(name)); err != nil {
		return err
	}

	return c.queue.Dequeue(name)
}

// validateName validates the name of a job.
func validateName(name string) error {
	if len(name) == 0 {
		return errors.New("job name cannot be empty")
	}

	if strings.Contains(name, "/") || strings.Contains(name, ".") {
		return fmt.Errorf("job name cannot contain '/' or '.': %s", name)
	}

	return nil
}
