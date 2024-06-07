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
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/diagridio/go-etcd-cron/api"
)

func Test_Scheduler(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	clock := clocktesting.NewFakeClock(now)

	cronSched, err := cron.ParseStandard("@every 1h")
	require.NoError(t, err)

	tests := map[string]struct {
		job          *api.JobStored
		expScheduler Interface
		expErr       bool
	}{
		"if no schedule, expect oneshot": {
			job: &api.JobStored{
				Begin: &api.JobStored_DueTime{
					DueTime: timestamppb.New(now),
				},
				Job: &api.Job{Schedule: nil},
			},
			expScheduler: &oneshot{dueTime: now},
			expErr:       false,
		},
		"if schedule, expect repeats": {
			job: &api.JobStored{
				Begin: &api.JobStored_Start{
					Start: timestamppb.New(now),
				},
				Expiration: timestamppb.New(now.Add(2 * time.Hour)),
				Job: &api.Job{
					Schedule: ptr.Of("@every 1h"),
				},
			},
			expScheduler: &repeats{
				start: ptr.Of(now),
				exp:   timestamppb.New(now.Add(2 * time.Hour)),
				cron:  cronSched,
				total: nil,
			},
			expErr: false,
		},
		"if schedule, expect repeats with exp nil": {
			job: &api.JobStored{
				Begin: &api.JobStored_Start{
					Start: timestamppb.New(now),
				},
				Expiration: nil,
				Job: &api.Job{
					Schedule: ptr.Of("@every 1h"),
					Repeats:  ptr.Of(uint32(100)),
				},
			},
			expScheduler: &repeats{
				start: ptr.Of(now),
				exp:   nil,
				cron:  cronSched,
				total: ptr.Of(uint32(100)),
			},
			expErr: false,
		},
		"if bad schedule string, expect error": {
			job: &api.JobStored{
				Begin: &api.JobStored_Start{
					Start: timestamppb.New(now),
				},
				Expiration: timestamppb.New(now.Add(2 * time.Hour)),
				Job: &api.Job{
					Schedule: ptr.Of("bad string"),
					Repeats:  ptr.Of(uint32(100)),
				},
			},
			expScheduler: nil,
			expErr:       true,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			builder := &Builder{clock: clock}
			gotScheduler, gotErr := builder.Scheduler(test.job)
			assert.Equal(t, test.expScheduler, gotScheduler)
			assert.Equal(t, test.expErr, gotErr != nil, "%v", gotErr)
		})
	}
}

func Test_Parse(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	clock := clocktesting.NewFakeClock(now)

	tests := map[string]struct {
		job       *api.Job
		expStored *api.JobStored
		expErr    bool
	}{
		"if due time and schedule nil, expect error": {
			job: &api.Job{
				DueTime:  nil,
				Schedule: nil,
				Ttl:      ptr.Of("1h"),
			},
			expStored: nil,
			expErr:    true,
		},
		"if schedule is nil with repeats, expect error": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: nil,
				Ttl:      nil,
				Repeats:  ptr.Of(uint32(3)),
			},
			expStored: nil,
			expErr:    true,
		},
		"if schedule is nil with ttl, expect error": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: nil,
				Ttl:      ptr.Of("1h"),
				Repeats:  nil,
			},
			expStored: nil,
			expErr:    true,
		},
		"if repeats is 0, expect error": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      nil,
				Repeats:  ptr.Of(uint32(0)),
			},
			expStored: nil,
			expErr:    true,
		},
		"only due time": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: nil,
				Ttl:      nil,
				Repeats:  nil,
			},
			expStored: &api.JobStored{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: nil,
					Ttl:      nil,
					Repeats:  nil,
				},
				Begin: &api.JobStored_DueTime{
					DueTime: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
				},
				Expiration: nil,
			},
			expErr: false,
		},
		"due time & schedule": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      nil,
				Repeats:  nil,
			},
			expStored: &api.JobStored{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: ptr.Of("@every 1h"),
					Ttl:      nil,
					Repeats:  nil,
				},
				Begin: &api.JobStored_DueTime{
					DueTime: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
				},
				Expiration: nil,
			},
			expErr: false,
		},
		"schedule & ttl": {
			job: &api.Job{
				DueTime:  nil,
				Schedule: ptr.Of("@every 1h"),
				Ttl:      ptr.Of("2h"),
				Repeats:  nil,
			},
			expStored: &api.JobStored{
				Job: &api.Job{
					DueTime:  nil,
					Schedule: ptr.Of("@every 1h"),
					Ttl:      ptr.Of("2h"),
					Repeats:  nil,
				},
				Begin: &api.JobStored_Start{
					Start: timestamppb.New(now),
				},
				Expiration: timestamppb.New(now.Add(2 * time.Hour)),
			},
			expErr: false,
		},
		"due time & schedule & ttl": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      ptr.Of("2h"),
				Repeats:  ptr.Of(uint32(100)),
			},
			expStored: &api.JobStored{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: ptr.Of("@every 1h"),
					Ttl:      ptr.Of("2h"),
					Repeats:  ptr.Of(uint32(100)),
				},
				Begin: &api.JobStored_DueTime{
					DueTime: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
				},
				Expiration: timestamppb.New(time.Date(2024, 4, 24, 13, 42, 22, 0, time.UTC)),
			},
			expErr: false,
		},
		"ttl after due time": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      ptr.Of("2024-04-24T11:42:21Z"),
				Repeats:  nil,
			},
			expStored: nil,
			expErr:    true,
		},
		"ttl same as due time": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      ptr.Of("2024-04-24T11:42:22Z"),
				Repeats:  nil,
			},
			expStored: &api.JobStored{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: ptr.Of("@every 1h"),
					Ttl:      ptr.Of("2024-04-24T11:42:22Z"),
					Repeats:  nil,
				},
				Begin: &api.JobStored_DueTime{
					DueTime: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
				},
				Expiration: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
			},
			expErr: false,
		},
		"bad due time": {
			job: &api.Job{
				DueTime:  ptr.Of("bad"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      ptr.Of("2024-04-24T11:42:22Z"),
				Repeats:  nil,
			},
			expErr: true,
		},
		"bad ttl": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: ptr.Of("@every 1h"),
				Ttl:      ptr.Of("bad"),
				Repeats:  nil,
			},
			expErr: true,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			builder := &Builder{clock: clock}
			gotStored, gotErr := builder.Parse(test.job)
			if gotStored != nil {
				assert.NotEqual(t, uint32(0), gotStored.GetUuid())
				gotStored.Uuid = 0
			}
			assert.Equal(t, test.expStored, gotStored)
			assert.Equal(t, test.expErr, gotErr != nil, "%v", gotErr)
		})
	}
}

func Test_parsePointInTime(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	tests := map[string]struct {
		str     string
		expTime time.Time
		expErr  bool
	}{
		"RFC3339": {
			str:     "2024-04-24T11:42:22Z",
			expTime: time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC),
			expErr:  false,
		},
		"duration": {
			str:     "10s",
			expTime: now.Add(10 * time.Second),
			expErr:  false,
		},
		"duration-ms": {
			str:     "10ms",
			expTime: now.Add(10 * time.Millisecond),
			expErr:  false,
		},
		"ISO8601": {
			str:     "P1Y2M3DT4H5M6S",
			expTime: now.AddDate(1, 2, 3).Add(4*time.Hour + 5*time.Minute + 6*time.Second),
			expErr:  false,
		},
		"ISO8601-with repeats": {
			str:     "R3/P1Y2M3DT4H5M6S",
			expTime: time.Time{},
			expErr:  true,
		},
		"unsupported format": {
			str:     "unsupported",
			expTime: time.Time{},
			expErr:  true,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gotTime, gotErr := parsePointInTime(test.str, now)
			assert.Equal(t, test.expTime, gotTime)
			assert.Equal(t, test.expErr, gotErr != nil, "%v", gotErr)
		})
	}
}
