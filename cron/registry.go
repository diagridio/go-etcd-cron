/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"errors"
	"fmt"
	"sync"

	"github.com/diagridio/go-etcd-cron/api"
)

// BackendEtcd is the name of the built-in etcd backend, registered by default.
const BackendEtcd = "etcd"

// Factory builds a cron api.Interface for a named backend from the given
// Options. Backend-specific configuration is carried opaquely in
// Options.BackendConfig; the built-in etcd backend instead reads the dedicated
// Options fields (Client, Namespace).
type Factory func(opts Options) (api.Interface, error)

var (
	registryMu     sync.RWMutex
	registry       = make(map[string]Factory)
	lastRegistered string
)

// Register registers a cron backend Factory under the given name. It is
// typically called from a backend package's init(). The most recently
// registered backend becomes the default when Options.Backend is empty ("use
// last registered"), so a blank import of an out-of-tree backend overrides the
// built-in etcd default without any other code change. An explicit
// Options.Backend always takes precedence over the last-registered default.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = f
	lastRegistered = name
}

// resolveFactory returns the Factory for the named backend. An empty name
// selects the last-registered backend.
func resolveFactory(name string) (Factory, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	if name == "" {
		name = lastRegistered
	}
	if name == "" {
		return nil, errors.New("no cron backend registered")
	}

	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("cron backend %q not registered", name)
	}

	return f, nil
}
