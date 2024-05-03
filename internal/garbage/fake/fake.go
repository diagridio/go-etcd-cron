/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"
	"sync"
)

// Fake is an implementation of the garbage Interface.
type Fake struct {
	keys     map[string]bool
	hasPoped []string
	lock     sync.Mutex
}

func New() *Fake {
	return &Fake{
		keys: map[string]bool{},
	}
}

func (f *Fake) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (f *Fake) Push(key string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.keys[key] = true
}

func (f *Fake) Pop(key string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.hasPoped = append(f.hasPoped, key)
	delete(f.keys, key)
}

func (f *Fake) Keys() []string {
	f.lock.Lock()
	defer f.lock.Unlock()
	keys := make([]string, 0, len(f.keys))
	for k := range f.keys {
		keys = append(keys, k)
	}
	return keys
}

func (f *Fake) HasPoped() []string {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.hasPoped
}
