/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"

	"github.com/diagridio/go-etcd-cron/internal/engine/handler"
)

type Fake struct {
	runFn func(context.Context) error
	apiFn func() handler.Interface
}

func New() *Fake {
	return &Fake{
		runFn: func(context.Context) error {
			return nil
		},
		apiFn: func() handler.Interface {
			return nil
		},
	}
}

func (f *Fake) WithRun(fn func(context.Context) error) *Fake {
	f.runFn = fn
	return f
}

func (f *Fake) WithAPI(a handler.Interface) *Fake {
	f.apiFn = func() handler.Interface { return a }

	return f
}

func (f *Fake) Run(ctx context.Context) error {
	return f.runFn(ctx)
}

func (f *Fake) API() handler.Interface {
	return f.apiFn()
}
