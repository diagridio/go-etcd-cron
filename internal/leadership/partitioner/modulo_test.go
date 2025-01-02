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
		id          uint64
		total       uint64
		partitionID uint64
		exp         bool
	}{
		{
			id:          0,
			total:       2,
			partitionID: 1,
			exp:         false,
		},
		{
			id:          1,
			total:       2,
			partitionID: 1,
			exp:         true,
		},
		{
			id:          0,
			total:       2,
			partitionID: 2,
			exp:         true,
		},
		{
			id:          1,
			total:       2,
			partitionID: 2,
			exp:         false,
		},
		{
			id:          1,
			total:       3,
			partitionID: 1,
			exp:         true,
		},
		{
			id:          1,
			total:       3,
			partitionID: 2,
			exp:         false,
		},
		{
			id:          1,
			total:       3,
			partitionID: 3,
			exp:         false,
		},
		{
			id:          1,
			total:       4,
			partitionID: 1,
			exp:         true,
		},
		{
			id:          1,
			total:       4,
			partitionID: 2,
			exp:         false,
		},
		{
			id:          0,
			total:       4,
			partitionID: 3,
			exp:         false,
		},
		{
			id:          2,
			total:       4,
			partitionID: 3,
			exp:         false,
		},
		{
			id:          3,
			total:       4,
			partitionID: 3,
			exp:         true,
		},
		{
			id:          2,
			total:       4,
			partitionID: 4,
			exp:         false,
		},
		{
			id:          1,
			total:       5,
			partitionID: 1,
			exp:         true,
		},
	}

	for _, test := range tests {
		parter := &modulo{
			id:    test.id,
			total: test.total,
		}
		assert.Equal(t, test.exp, parter.IsJobManaged(test.partitionID), "test: %+v", test)
	}
}
