/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package key

import (
	"errors"
	"path"
)

type Options struct {
	// Namespace is the key namespace of all objects. Should be the same for the
	// cron partition replica cluster.
	Namespace string

	// ID is the unique identifier for this replica.
	ID string
}

// Key returns the correct namespaced key for the given object name.
type Key struct {
	// namespace is the key namespace of all objects.
	namespace string

	jobNamespace        string
	counterNamespace    string
	leadershipNamespace string
	leadershipKey       string

	// id is the unique identifier for this replica.
	id string
}

// New returns a new Key with the given namespace.
func New(opts Options) (*Key, error) {
	if len(opts.ID) == 0 {
		return nil, errors.New("replica id cannot be empty")
	}

	return &Key{
		namespace:           opts.Namespace,
		jobNamespace:        path.Join(opts.Namespace, "jobs"),
		counterNamespace:    path.Join(opts.Namespace, "counters"),
		leadershipNamespace: path.Join(opts.Namespace, "leadership"),
		leadershipKey:       path.Join(opts.Namespace, "leadership", opts.ID),
		id:                  opts.ID,
	}, nil
}

// JobKey returns the job key for the given job name.
func (k *Key) JobKey(name string) string {
	return k.jobNamespace + "/" + name
}

// CounterKey returns the counter key for the given job name.
func (k *Key) CounterKey(name string) string {
	return k.counterNamespace + "/" + name
}

// LeadershipNamespace returns the namespace key for the leadership keys.
func (k *Key) LeadershipNamespace() string {
	return k.leadershipNamespace
}

// LeadershipKey returns the leadership key for this replica ID.
func (k *Key) LeadershipKey() string {
	return k.leadershipKey
}

// JobNamespace returns the job namespace key.
func (k *Key) JobNamespace() string {
	return k.jobNamespace
}

// JobName returns the job name from the given key.
func (k *Key) JobName(key []byte) string {
	return string(key)[len(k.jobNamespace)+1:]
}

// ID returns the replica ID.
func (k *Key) ID() string {
	return k.id
}
