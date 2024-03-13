/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package collector

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoEntries(t *testing.T) {
	collector := New(time.Second, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	collector.Start(ctx)

	select {
	case <-time.After(3 * time.Second):
		t.FailNow()
	case <-stop(collector, cancel):
	}
}

func TestAddEntryAfterStart(t *testing.T) {
	collector := New(time.Second, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collector.Start(ctx)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	collector.Add(func(ctx context.Context) {
		wg.Done()
	})

	select {
	case <-time.After(2 * time.Second):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestAddEntryBeforeStart(t *testing.T) {
	collector := New(time.Second, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	collector.Add(func(ctx context.Context) {
		wg.Done()
	})

	collector.Start(ctx)

	select {
	case <-time.After(2 * time.Second):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestExpireOnlyOneOutOfTwo(t *testing.T) {
	collector := New(time.Second, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg1 := &sync.WaitGroup{}
	wg1.Add(1)

	wg2 := &sync.WaitGroup{}
	wg2.Add(1)

	called1 := atomic.Bool{}
	called2 := atomic.Bool{}

	collector.Add(func(ctx context.Context) {
		called1.Store(true)
		wg1.Done()
	})

	time.Sleep(3 * time.Second)

	collector.Add(func(ctx context.Context) {
		called2.Store(true)
		wg2.Done()
	})

	collector.Start(ctx)

	select {
	case <-time.After(2 * time.Second):
		t.FailNow()
	case <-wait(wg1):
		assert.True(t, called1.Load())
		assert.False(t, called2.Load())
	}

	select {
	case <-time.After(2 * time.Second):
		t.FailNow()
	case <-wait(wg2):
		assert.True(t, called1.Load())
		assert.True(t, called2.Load())
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(collector *Collector, cancel context.CancelFunc) chan bool {
	ch := make(chan bool)
	go func() {
		cancel()
		collector.Wait()
		ch <- true
	}()
	return ch
}
