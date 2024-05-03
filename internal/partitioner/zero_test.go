/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_zero(t *testing.T) {
	t.Parallel()

	t.Run("always return true", func(t *testing.T) {
		t.Parallel()
		z := new(zero)
		for i := 0; i < 100; i++ {
			//nolint:gosec
			assert.True(t, z.IsJobManaged(rand.Uint32()))
		}
	})
}
