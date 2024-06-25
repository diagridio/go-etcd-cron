/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

import (
	"errors"
	"fmt"
)

// Interface returns whether a key is managed by the partition.
type Interface interface {
	IsJobManaged(jobPartitionID uint32) bool
}

// Options are the options for the partitioner.
type Options struct {
	// ID is the partition ID.
	ID uint32

	// Total is the total number of partitions.
	Total uint32
}

// New returns a new partitioner given the partition id and total partitions in
// the replica set.
func New(opts Options) (Interface, error) {
	if opts.Total == 0 {
		return nil, errors.New("total partitions must be greater than 0")
	}

	if opts.ID >= opts.Total {
		return nil, fmt.Errorf("partition id %d is greater/equal to total %d", opts.ID, opts.Total)
	}

	if opts.Total == 1 {
		return new(zero), nil
	}

	return &modulo{id: opts.ID, total: opts.Total}, nil
}
