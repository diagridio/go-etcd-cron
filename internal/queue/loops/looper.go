/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package loops

import (
	"context"
	"sync"
)

type Handler[T any] interface {
	Handle(context.Context, T) error
}

type Interface[T any] interface {
	Run(context.Context) error
	Enqueue(T)
	Close(T)
	Reset(h Handler[T], size uint64) Interface[T]
}

type looper[T any] struct {
	queue   chan T
	handler Handler[T]

	closed  bool
	closeCh chan struct{}
	lock    sync.RWMutex
}

func New[T any](h Handler[T], size uint64) Interface[T] {
	return &looper[T]{
		queue:   make(chan T, size),
		handler: h,
		closeCh: make(chan struct{}),
	}
}

func Empty[T any]() Interface[T] {
	return new(looper[T])
}

func (l *looper[T]) Run(ctx context.Context) error {
	defer close(l.closeCh)

	for {
		req, ok := <-l.queue
		if !ok {
			return nil
		}

		if err := l.handler.Handle(ctx, req); err != nil {
			return err
		}
	}
}

func (l *looper[T]) Enqueue(req T) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	if l.closed {
		return
	}

	select {
	case l.queue <- req:
	case <-l.closeCh:
	}
}

func (l *looper[T]) Close(req T) {
	l.lock.Lock()
	l.closed = true
	l.queue <- req
	close(l.queue)
	l.lock.Unlock()
	<-l.closeCh
}

func (l *looper[T]) Reset(h Handler[T], size uint64) Interface[T] {
	if l == nil {
		return New[T](h, size)
	}

	l.closed = false
	l.closeCh = make(chan struct{})
	l.handler = h

	// TODO: @joshvanl: use a ring buffer so that we don't need to reallocate and
	// improve performance.
	l.queue = make(chan T, size)

	return l
}
