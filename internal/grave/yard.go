/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package grave

import (
	"sync"
)

// Yard is a graveyard for deleted keys. It keeps track of the keys that have
// been deleted so that they do not attempt to get re-de-queued from the queue
// from the informer if they have just been deleted.
// Keys are tracked by their name and the index at which they were deleted.
// If more than 500000 keys are tracked, the graveyard will start to remove
// keys that are older than 10000 indexes to prevent the map from growing
// indefinitely from missed deletes. This _should_ never happen.
type Yard struct {
	idx        uint64
	deletesMap map[string]uint64
	lock       sync.Mutex
}

const (
	maxKeys = 500000
)

func New() *Yard {
	return &Yard{
		idx:        0,
		deletesMap: make(map[string]uint64),
	}
}

// Deleted marks a key as just been deleted.
func (y *Yard) Deleted(key string) {
	y.lock.Lock()
	defer y.lock.Unlock()

	if _, ok := y.deletesMap[key]; ok {
		return
	}

	y.deletesMap[key] = y.idx
	y.idx++

	// If the deletes map is getting too big (over 500k), remove the oldest 10k.
	if len(y.deletesMap) >= maxKeys {
		target := y.idx - (maxKeys - 10000)
		for k := range y.deletesMap {
			if y.deletesMap[k] < target {
				delete(y.deletesMap, k)
			}
		}
	}
}

// HasJustDeleted returns true if the key has just been deleted.
func (y *Yard) HasJustDeleted(key string) bool {
	y.lock.Lock()
	defer y.lock.Unlock()

	if _, ok := y.deletesMap[key]; ok {
		delete(y.deletesMap, key)
		return true
	}

	return false
}
