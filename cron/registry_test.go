/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package cron

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
)

// saveRegistry snapshots the global registry state and restores it on cleanup so
// tests that register backends do not leak into one another.
func saveRegistry(t *testing.T) {
	t.Helper()
	registryMu.Lock()
	savedReg := make(map[string]Factory, len(registry))
	for k, v := range registry {
		savedReg[k] = v
	}
	savedLast := lastRegistered
	registryMu.Unlock()

	t.Cleanup(func() {
		registryMu.Lock()
		registry = savedReg
		lastRegistered = savedLast
		registryMu.Unlock()
	})
}

func Test_resolveFactory(t *testing.T) {
	t.Run("empty backend resolves to the built-in etcd backend by default", func(t *testing.T) {
		saveRegistry(t)
		f, err := resolveFactory("")
		require.NoError(t, err)
		assert.NotNil(t, f)
	})

	t.Run("explicit etcd backend resolves", func(t *testing.T) {
		saveRegistry(t)
		f, err := resolveFactory(BackendEtcd)
		require.NoError(t, err)
		assert.NotNil(t, f)
	})

	t.Run("unknown backend errors", func(t *testing.T) {
		saveRegistry(t)
		_, err := resolveFactory("does-not-exist")
		assert.Error(t, err)
	})

	t.Run("last registered backend wins when backend is empty", func(t *testing.T) {
		saveRegistry(t)

		var called bool
		Register("fake", func(Options) (api.Interface, error) {
			called = true
			return nil, nil
		})

		// Empty backend should now select "fake" (last registered).
		f, err := resolveFactory("")
		require.NoError(t, err)
		_, _ = f(Options{})
		assert.True(t, called, "expected last-registered backend to be selected")
	})

	t.Run("explicit backend overrides the last-registered default", func(t *testing.T) {
		saveRegistry(t)

		Register("fake", func(Options) (api.Interface, error) { return nil, nil })

		// Even though "fake" was registered last, an explicit "etcd" wins.
		f, err := resolveFactory(BackendEtcd)
		require.NoError(t, err)
		assert.NotNil(t, f)
	})
}
