/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package grave

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Deleted(t *testing.T) {
	t.Parallel()

	t.Run("Deleting to a key should add to map, subsequent deletes should not increment", func(t *testing.T) {
		t.Parallel()

		yard := New()
		assert.Empty(t, yard.deletesMap)
		yard.Deleted("test")
		assert.Equal(t, map[string]uint64{"test": 0}, yard.deletesMap)
		yard.Deleted("test")
		assert.Equal(t, map[string]uint64{"test": 0}, yard.deletesMap)
		yard.Deleted("test2")
		assert.Equal(t, map[string]uint64{"test": 0, "test2": 1}, yard.deletesMap)
		yard.Deleted("test2")
		assert.Equal(t, map[string]uint64{"test": 0, "test2": 1}, yard.deletesMap)
	})

	t.Run("Adding over 500k deletes should remove the oldest 10k keys", func(t *testing.T) {
		t.Parallel()

		yard := New()
		exp := make(map[string]uint64)

		for i := 0; i < 500000-1; i++ {
			exp[strconv.Itoa(i)] = uint64(i)
			yard.Deleted(strconv.Itoa(i))
		}

		assert.Equal(t, exp, yard.deletesMap)

		yard.Deleted("499999")
		assert.Len(t, yard.deletesMap, (500000 - 10000))
		newExp := make(map[string]uint64)
		for i := 10000; i < 500000; i++ {
			newExp[strconv.Itoa(i)] = uint64(i)
		}
		assert.Equal(t, newExp, yard.deletesMap)

		yard.Deleted("500000")
		newExp["500000"] = 500000
		assert.Equal(t, newExp, yard.deletesMap)
	})

	t.Run("Heat death of the universe", func(t *testing.T) {
		t.Parallel()

		const maxUint64 = ^uint64(0)

		yard := New()
		yard.idx = maxUint64 - 10000

		exp := make(map[string]uint64)
		for i := maxUint64 - 10000; i < maxUint64-1; i++ {
			exp[strconv.FormatUint(i, 10)] = i
			yard.Deleted(strconv.FormatUint(i, 10))
		}

		assert.Equal(t, exp, yard.deletesMap)

		yard.Deleted("helloworld")
		assert.Empty(t, yard.deletesMap)
		assert.Equal(t, uint64(0), yard.idx)
	})
}

func Test_HasJustDeleted(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		key     string
		deletes []string
		expBool bool
		expMap  map[string]uint64
	}{
		"empty map returns false": {
			key:     "test",
			deletes: nil,
			expBool: false,
			expMap:  map[string]uint64{},
		},
		"map with entry returns true": {
			key:     "test",
			deletes: []string{"test"},
			expBool: true,
			expMap:  map[string]uint64{},
		},
		"map without entry returns false": {
			key:     "test",
			deletes: []string{"test2"},
			expBool: false,
			expMap:  map[string]uint64{"test2": 0},
		},
		"map with entry and others returns true": {
			key:     "test2",
			deletes: []string{"test1", "test2", "test3"},
			expBool: true,
			expMap:  map[string]uint64{"test1": 0, "test3": 2},
		},
		"map with no entry and others returns false": {
			key:     "test",
			deletes: []string{"test2", "test3"},
			expBool: false,
			expMap:  map[string]uint64{"test2": 0, "test3": 1},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			yard := New()

			for _, key := range test.deletes {
				yard.Deleted(key)
			}

			assert.Equal(t, test.expBool, yard.HasJustDeleted(test.key))
			assert.Equal(t, test.expMap, yard.deletesMap)
		})
	}
}
