/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"context"

	"github.com/diagridio/go-etcd-cron/api"
	"google.golang.org/protobuf/types/known/anypb"
)

// Fake is a fake cron instance used for testing.
type Fake struct {
	runFn  func(ctx context.Context) error
	addFn  func(ctx context.Context, name string, job *api.Job) error
	getFn  func(ctx context.Context, name string) (*api.Job, error)
	delFn  func(ctx context.Context, name string) error
	delPFn func(ctx context.Context, prefixes ...string) error
	listFn func(ctx context.Context, prefix string) (*api.ListResponse, error)

	deliverablePrefixesFn func(ctx context.Context, prefixes ...string) (context.CancelFunc, error)
}

func (f *Fake) WatchLeadership(ctx context.Context) (chan []*anypb.Any, error) {
	panic("implement me")
}

func New() *Fake {
	return &Fake{
		runFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		addFn: func(context.Context, string, *api.Job) error {
			return nil
		},
		getFn: func(context.Context, string) (*api.Job, error) {
			return nil, nil
		},
		delFn: func(context.Context, string) error {
			return nil
		},
		delPFn: func(context.Context, ...string) error {
			return nil
		},
		listFn: func(context.Context, string) (*api.ListResponse, error) {
			return nil, nil
		},
		deliverablePrefixesFn: func(context.Context, ...string) (context.CancelFunc, error) {
			return func() {}, nil
		},
	}
}

func (f *Fake) WithRun(fn func(context.Context) error) *Fake {
	f.runFn = fn
	return f
}

func (f *Fake) WithAdd(fn func(context.Context, string, *api.Job) error) *Fake {
	f.addFn = fn
	return f
}

func (f *Fake) WithGet(fn func(context.Context, string) (*api.Job, error)) *Fake {
	f.getFn = fn
	return f
}

func (f *Fake) WithDelete(fn func(context.Context, string) error) *Fake {
	f.delFn = fn
	return f
}

func (f *Fake) WithDeletePrefixes(fn func(context.Context, ...string) error) *Fake {
	f.delPFn = fn
	return f
}

func (f *Fake) WithList(fn func(context.Context, string) (*api.ListResponse, error)) *Fake {
	f.listFn = fn
	return f
}

func (f *Fake) WithDeliverablePrefixes(fn func(context.Context, ...string) (context.CancelFunc, error)) *Fake {
	f.deliverablePrefixesFn = fn
	return f
}

func (f *Fake) Run(ctx context.Context) error {
	return f.runFn(ctx)
}

func (f *Fake) Add(ctx context.Context, name string, job *api.Job) error {
	return f.addFn(ctx, name, job)
}

func (f *Fake) Get(ctx context.Context, name string) (*api.Job, error) {
	return f.getFn(ctx, name)
}

func (f *Fake) Delete(ctx context.Context, name string) error {
	return f.delFn(ctx, name)
}

func (f *Fake) DeletePrefixes(ctx context.Context, prefixes ...string) error {
	return f.delPFn(ctx, prefixes...)
}

func (f *Fake) List(ctx context.Context, prefix string) (*api.ListResponse, error) {
	return f.listFn(ctx, prefix)
}

func (f *Fake) DeliverablePrefixes(ctx context.Context, prefixes ...string) (context.CancelFunc, error) {
	return f.deliverablePrefixesFn(ctx, prefixes...)
}
