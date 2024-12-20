/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/diagridio/go-etcd-cron/internal/key"
)

func Test_New(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		id      string
		ns      string
		leaders []*mvccpb.KeyValue
		exp     Interface
		expErr  bool
	}{
		"if leaders is empty, expect error": {
			id:      "123",
			ns:      "abc",
			leaders: []*mvccpb.KeyValue{},
			exp:     nil,
			expErr:  true,
		},
		"if leaders is 1 expect zero partitioner": {
			id: "0",
			ns: "",
			leaders: []*mvccpb.KeyValue{
				{Key: []byte("0")},
			},
			exp:    new(zero),
			expErr: false,
		},
		"if 2 leaders expect modulo partitioner": {
			id: "1",
			ns: "",
			leaders: []*mvccpb.KeyValue{
				{Key: []byte("0")},
				{Key: []byte("1")},
			},
			exp:    &modulo{id: 1, total: 2},
			expErr: false,
		},
		"if 2 leaders backwards expect modulo partitioner": {
			id: "1",
			ns: "",
			leaders: []*mvccpb.KeyValue{
				{Key: []byte("1")},
				{Key: []byte("0")},
			},
			exp:    &modulo{id: 1, total: 2},
			expErr: false,
		},
		"if id is not in leaders, expect error": {
			id: "3",
			ns: "",
			leaders: []*mvccpb.KeyValue{
				{Key: []byte("1")},
				{Key: []byte("0")},
			},
			exp:    nil,
			expErr: true,
		},
		"if ids are letters not number, should put in correct partition id": {
			id: "def",
			ns: "abc",
			leaders: []*mvccpb.KeyValue{
				{Key: []byte("abc/def")},
				{Key: []byte("abc/abc")},
				{Key: []byte("abc/zyx")},
			},
			exp:    &modulo{id: 1, total: 3},
			expErr: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			key, err := key.New(key.Options{
				ID:        test.id,
				Namespace: test.ns,
			})
			require.NoError(t, err)

			parter, err := New(Options{
				Key:     key,
				Leaders: test.leaders,
			})
			assert.Equal(t, test.exp, parter)
			assert.Equal(t, test.expErr, err != nil, "%v", err)
		})
	}
}
