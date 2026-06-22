/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package validator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_JobName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc   string
		name   string
		expErr bool
	}{
		{desc: "empty", name: "", expErr: true},
		{desc: "slash", name: "/", expErr: true},
		{desc: "wrapped in slashes", name: "/foo/", expErr: true},
		{desc: "trailing slash", name: "foo/", expErr: true},
		{desc: "dot", name: ".", expErr: true},
		{desc: "double dot", name: "..", expErr: true},
		{desc: "dot slash dot", name: "./.", expErr: true},
		{desc: "backslash", name: "foo\\bar", expErr: true},
		{desc: "hash", name: "foo#bar", expErr: true},
		{desc: "question mark", name: "foo?bar", expErr: true},
		{desc: "nul byte", name: "foo\x00bar", expErr: true},
		{desc: "newline control char", name: "foo\nbar", expErr: true},
		{desc: "del control char", name: "foo\x7fbar", expErr: true},
		{desc: "too long", name: strings.Repeat("a", maxJobNameLength+1), expErr: true},

		{desc: "single dot in middle", name: "fo.o", expErr: false},
		{desc: "consecutive dots in middle", name: "fo...o", expErr: false},
		{desc: "max length", name: strings.Repeat("a", maxJobNameLength), expErr: false},
		{desc: "simple", name: "valid", expErr: false},
		{desc: "bare delimiter", name: "||", expErr: false},
		{desc: "trailing delimiter", name: "foo||", expErr: false},
		{desc: "leading delimiter", name: "||foo", expErr: false},
		{desc: "two segments", name: "foo||foo", expErr: false},
		{desc: "dotted segment", name: "foo.bar||foo", expErr: false},
		{desc: "uppercase preserved", name: "foo.BAR||foo", expErr: false},
		{desc: "underscore and hyphen", name: "foo.BAR_f-oo||foo", expErr: false},
		{desc: "single pipe in segment", name: "foo|bar", expErr: false},
		{desc: "at sign", name: "my@reminder", expErr: false},
		{
			desc:   "workflow reminder name",
			name:   "actorreminder||dapr-tests||dapr.internal.dapr-tests.perf-workflowsapp.workflow||24b3fbad-0db5-4e81-a272-71f6018a66a6||start-4NYDFil-",
			expErr: false,
		},
		{
			desc:   "schreder actor id with pipes and at sign",
			name:   "actorreminder||dapr-tests||SmokeDetectorActor||nexus-fire-safety-api||SmokeDetectorGroupMonitorActor||Nexus|6671cfbe7f48af247700a24b||SmokeDetectorGroupInformation||my@reminder|name",
			expErr: false,
		},
		{
			desc:   "colons and spaces in segments",
			name:   "aABVCD||dapr-::123:123||dapr.internal.dapr-tests.perf-  workflowsapp.workflow||24b3fbad-0db5-4e81        -a272-71f6018a66a6||start-4NYDFil-",
			expErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			err := New(Options{}).JobName(test.name)
			assert.Equal(t, test.expErr, err != nil, "%v", err)
		})
	}
}
