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

type Options[T any] struct {
	Handler    Handler[T]
	BufferSize *uint64
}

type Interface[T any] interface {
	Run(ctx context.Context) error
	Enqueue(req T)
	Close(req T)
}

type looper[T any] struct {
	queue   chan T
	handler Handler[T]

	closed  bool
	closeCh chan struct{}
	lock    sync.RWMutex
}

func New[T any](opts Options[T]) Interface[T] {
	size := 1
	if opts.BufferSize != nil {
		size = int(*opts.BufferSize)
	}

	return &looper[T]{
		queue:   make(chan T, size),
		handler: opts.Handler,
		closeCh: make(chan struct{}),
	}
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
	l.queue <- req
	close(l.queue)
	l.closed = true
	l.lock.Unlock()
	<-l.closeCh
}

func (l *looper[T]) Reset(h Handler[T]) {
	l.queue = make(chan T, cap(l.queue))
	l.closed = false
	l.closeCh = make(chan struct{})
	l.handler = h
}
