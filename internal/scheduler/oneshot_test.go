/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_oneshot(t *testing.T) {
	t.Parallel()

	dueTime := time.Now().Add(1 * time.Second)

	tests := []struct {
		count   uint32
		expNext *time.Time
	}{
		{
			count:   0,
			expNext: &dueTime,
		},
		{
			count:   1,
			expNext: nil,
		},
		{
			count:   5,
			expNext: nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(strconv.Itoa(int(test.count)), func(t *testing.T) {
			t.Parallel()
			oneshot := &oneshot{dueTime: dueTime}
			assert.Equal(t, test.expNext, oneshot.Next(test.count, nil))
		})
	}
}
