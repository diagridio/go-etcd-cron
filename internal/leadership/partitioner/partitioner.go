/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

import (
	"errors"
	"fmt"
	"path"
	"sort"

	"github.com/dapr/kit/ptr"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/diagridio/go-etcd-cron/internal/key"
)

// Interface returns whether a key is managed by the partition.
type Interface interface {
	IsJobManaged(jobPartitionID uint64) bool
	Total() uint64
}

// Options are the options for the partitioner.
type Options struct {
	// Key is the ETCD key generator.
	Key *key.Key

	// Leaders is the list of currently active leaders.
	Leaders []*mvccpb.KeyValue
}

// New returns a new Partitioner which determines whether we are responsible
// for a particular job.
// When there are more than 1 leader in a cluster, the module id is determined
// by our index in the sorted list of all leader key strings.
func New(opts Options) (Interface, error) {
	if len(opts.Leaders) == 0 {
		return nil, errors.New("total partitions must be greater than 0")
	}

	// Get all leader keys and sort them.
	leaders := make([]string, len(opts.Leaders))
	for i := range opts.Leaders {
		leaders[i] = string(opts.Leaders[i].Key)
	}
	sort.Strings(leaders)

	// Determine our index in the sorted list to use as modulo.
	var self *uint64
	for i := range leaders {
		if path.Base(leaders[i]) == opts.Key.ID() {
			//nolint:gosec
			self = ptr.Of(uint64(i))
			break
		}
	}

	if self == nil {
		return nil, fmt.Errorf("self leader %s not found in %v", opts.Key.ID(), leaders)
	}

	// We use zero here as a sanity check that we exist in the given leadership
	// list.
	if len(opts.Leaders) == 1 {
		return new(zero), nil
	}

	return &modulo{id: *self, total: uint64(len(opts.Leaders))}, nil
}
