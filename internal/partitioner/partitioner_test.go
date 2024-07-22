/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_New(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		id     uint32
		total  uint32
		exp    Interface
		expErr bool
	}{
		"if total is zero expect error": {
			id:     1,
			total:  0,
			exp:    nil,
			expErr: true,
		},
		"if id is equal to total expect error": {
			id:     2,
			total:  2,
			exp:    nil,
			expErr: true,
		},
		"if id is greater than total expect error": {
			id:     3,
			total:  2,
			exp:    nil,
			expErr: true,
		},
		"if total is 1 expect zero partitioner": {
			id:     0,
			total:  1,
			exp:    new(zero),
			expErr: false,
		},
		"if total is 2 expect modulo partitioner": {
			id:     1,
			total:  2,
			exp:    &modulo{id: 1, total: 2},
			expErr: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parter, err := New(Options{
				ID:    test.id,
				Total: test.total,
			})
			assert.Equal(t, test.exp, parter)
			assert.Equal(t, test.expErr, err != nil, "%v", err)
		})
	}
}
