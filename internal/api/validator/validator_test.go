/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_JobName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		expErr bool
	}{
		{
			name:   "",
			expErr: true,
		},
		{
			name:   "/",
			expErr: true,
		},
		{
			name:   "/foo/",
			expErr: true,
		},
		{
			name:   "foo/",
			expErr: true,
		},
		{
			name:   ".",
			expErr: true,
		},
		{
			name:   "..",
			expErr: true,
		},
		{
			name:   "./.",
			expErr: true,
		},
		{
			name:   "fo.o",
			expErr: false,
		},
		{
			name:   "fo...o",
			expErr: true,
		},
		{
			name:   "valid",
			expErr: false,
		},
		{
			name:   "||",
			expErr: true,
		},
		{
			name:   "foo||",
			expErr: true,
		},
		{
			name:   "||foo",
			expErr: true,
		},
		{
			name:   "foo||foo",
			expErr: false,
		},
		{
			name:   "foo.bar||foo",
			expErr: false,
		},
		{
			name:   "foo.BAR||foo",
			expErr: false,
		},
		{
			name:   "foo.BAR_f-oo||foo",
			expErr: false,
		},
		{
			name:   "actorreminder||dapr-tests||dapr.internal.dapr-tests.perf-workflowsapp.workflow||24b3fbad-0db5-4e81-a272-71f6018a66a6||start-4NYDFil-",
			expErr: false,
		},
		{
			name:   "aABVCD||dapr-::123:123||dapr.internal.dapr-tests.perf-workflowsapp.workflow||24b3fbad-0db5-4e81-a272-71f6018a66a6||start-4NYDFil-",
			expErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := New(Options{}).JobName(test.name)
			assert.Equal(t, test.expErr, err != nil, "%v", err)
		})
	}
}
