/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id    uint32
		total uint32
		uuid  uint32
		exp   bool
	}{
		{
			id:    0,
			total: 2,
			uuid:  1,
			exp:   false,
		},
		{
			id:    1,
			total: 2,
			uuid:  1,
			exp:   true,
		},
		{
			id:    0,
			total: 2,
			uuid:  2,
			exp:   true,
		},
		{
			id:    1,
			total: 2,
			uuid:  2,
			exp:   false,
		},
		{
			id:    1,
			total: 3,
			uuid:  1,
			exp:   true,
		},
		{
			id:    1,
			total: 3,
			uuid:  2,
			exp:   false,
		},
		{
			id:    1,
			total: 3,
			uuid:  3,
			exp:   false,
		},
		{
			id:    1,
			total: 4,
			uuid:  1,
			exp:   true,
		},
		{
			id:    1,
			total: 4,
			uuid:  2,
			exp:   false,
		},
		{
			id:    0,
			total: 4,
			uuid:  3,
			exp:   false,
		},
		{
			id:    2,
			total: 4,
			uuid:  3,
			exp:   false,
		},
		{
			id:    3,
			total: 4,
			uuid:  3,
			exp:   true,
		},
		{
			id:    2,
			total: 4,
			uuid:  4,
			exp:   false,
		},
		{
			id:    1,
			total: 5,
			uuid:  1,
			exp:   true,
		},
	}

	for _, test := range tests {
		parter := &modulo{
			id:    test.id,
			total: test.total,
		}
		assert.Equal(t, test.exp, parter.IsJobManaged(test.uuid), "test: %+v", test)
	}
}
