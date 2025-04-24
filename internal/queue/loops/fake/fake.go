/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"
)

type Fake[T any] struct {
	runFn     func(context.Context) error
	enqueueFn func(T)
	closeFn   func(T)
}

func New[T any]() *Fake[T] {
	return &Fake[T]{
		runFn:     func(context.Context) error { return nil },
		enqueueFn: func(T) {},
		closeFn:   func(T) {},
	}
}

func (f *Fake[T]) WithRun(fn func(context.Context) error) *Fake[T] {
	f.runFn = fn
	return f
}

func (f *Fake[T]) WithEnqueue(fn func(T)) *Fake[T] {
	f.enqueueFn = fn
	return f
}

func (f *Fake[T]) WithClose(fn func(T)) *Fake[T] {
	f.closeFn = fn
	return f
}

func (f *Fake[T]) Run(ctx context.Context) error {
	return f.runFn(ctx)
}

func (f *Fake[T]) Enqueue(t T) {
	f.enqueueFn(t)
}

func (f *Fake[T]) Close(t T) {
	f.closeFn(t)
}
