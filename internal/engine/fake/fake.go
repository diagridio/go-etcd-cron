/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"

	"github.com/diagridio/go-etcd-cron/internal/api"
)

type Fake struct {
	runFn func(context.Context) error
	apiFn func() api.Interface
}

func New() *Fake {
	return &Fake{
		runFn: func(context.Context) error {
			return nil
		},
		apiFn: func() api.Interface {
			return nil
		},
	}
}

func (f *Fake) WithRun(fn func(context.Context) error) *Fake {
	f.runFn = fn
	return f
}

func (f *Fake) WithAPI(a api.Interface) *Fake {
	f.apiFn = func() api.Interface { return a }

	return f
}

func (f *Fake) Run(ctx context.Context) error {
	return f.runFn(ctx)
}

func (f *Fake) API() api.Interface {
	return f.apiFn()
}
