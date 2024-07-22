/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package scheduler

import (
	"testing"
	"time"

	"github.com/dapr/kit/cron"
	"github.com/dapr/kit/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Test_repeats(t *testing.T) {
	t.Parallel()

	start := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	tests := map[string]struct {
		exp      *timestamppb.Timestamp
		schedule string
		total    *uint32
		count    uint32
		start    *time.Time
		dueTime  *time.Time
		last     *timestamppb.Timestamp
		expNext  *time.Time
	}{
		"no count, no last, no exp, no total, returns next time after start": {
			exp:      nil,
			schedule: "@every 1h",
			total:    nil,
			start:    &start,
			count:    0,
			last:     nil,
			expNext:  ptr.Of(start.Add(time.Hour)),
		},
		"count, no last, no exp, no total, returns next time after start": {
			exp:      nil,
			schedule: "@every 1h",
			total:    nil,
			start:    &start,
			count:    5,
			last:     nil,
			expNext:  ptr.Of(start.Add(time.Hour)),
		},
		"count, no last, no exp, total, returns next time after start if count less than total": {
			exp:      nil,
			schedule: "@every 1h",
			total:    ptr.Of(uint32(10)),
			start:    &start,
			count:    5,
			last:     nil,
			expNext:  ptr.Of(start.Add(time.Hour)),
		},
		"count, no last, no exp, total, returns nil if count is same as total": {
			exp:      nil,
			schedule: "@every 1h",
			total:    ptr.Of(uint32(10)),
			start:    &start,
			count:    10,
			last:     nil,
			expNext:  nil,
		},
		"count, no last, no exp, total, returns nil if count is more than total": {
			exp:      nil,
			schedule: "@every 1h",
			total:    ptr.Of(uint32(10)),
			start:    &start,
			count:    11,
			last:     nil,
			expNext:  nil,
		},
		"no count, no total,last, no exp, returns next time after last": {
			exp:      nil,
			schedule: "@every 1h",
			total:    nil,
			start:    &start,
			count:    0,
			last:     timestamppb.New(start.Add(time.Hour + 50)),
			expNext:  ptr.Of(start.Add(time.Hour * 2)),
		},
		"no count, no total,last, no exp, returns next time after start": {
			exp:      nil,
			schedule: "@every 1h",
			total:    nil,
			count:    0,
			start:    &start,
			last:     timestamppb.New(start),
			expNext:  ptr.Of(start.Add(time.Hour)),
		},
		"no count, no total, last, exp, returns next time if before expiry": {
			exp:      timestamppb.New(start.Add(time.Hour*2 + 1)),
			schedule: "@every 1h",
			total:    nil,
			start:    &start,
			count:    0,
			last:     timestamppb.New(start.Add(time.Hour)),
			expNext:  ptr.Of(start.Add(time.Hour * 2)),
		},
		"no count, no total, last, exp, returns nil if after expiry": {
			exp:      timestamppb.New(start.Add(time.Hour*2 + 1)),
			schedule: "@every 1h",
			total:    nil,
			start:    &start,
			count:    0,
			last:     timestamppb.New(start.Add(time.Hour * 2)),
			expNext:  nil,
		},
		"if dueTime and scheduler in future, expect trigger at dueTime": {
			exp:      timestamppb.New(start.Add(time.Second)),
			schedule: "@every 1h",
			total:    nil,
			dueTime:  ptr.Of(start.Add(time.Second)),
			count:    0,
			last:     nil,
			expNext:  ptr.Of(start.Add(time.Second)),
		},
		"if dueTime and scheduler in future, expect trigger at start + schedule": {
			exp:      timestamppb.New(start.Add(time.Second)),
			schedule: "@every 1h",
			total:    nil,
			start:    ptr.Of(start.Add(time.Second)),
			count:    0,
			last:     nil,
			expNext:  ptr.Of(start.Add(time.Hour + time.Second)),
		},
	}

	for name, test := range tests {
		testInLoop := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cron, err := cron.ParseStandard(testInLoop.schedule)
			require.NoError(t, err)

			repeats := &repeats{
				start:   testInLoop.start,
				dueTime: testInLoop.dueTime,
				exp:     testInLoop.exp,
				cron:    cron,
				total:   testInLoop.total,
			}

			assert.Equal(t, testInLoop.expNext, repeats.Next(testInLoop.count, testInLoop.last))
		})
	}
}
