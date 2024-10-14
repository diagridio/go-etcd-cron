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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/stored"
)

func Test_Schedule(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	clock := clocktesting.NewFakeClock(now)

	cronSched, err := cron.ParseStandard("@every 1h")
	require.NoError(t, err)

	tests := map[string]struct {
		job          *stored.Job
		expScheduler Interface
		expErr       bool
	}{
		"if no schedule, expect oneshot": {
			job: &stored.Job{
				Begin: &stored.Job_DueTime{
					DueTime: timestamppb.New(now),
				},
				Job: &api.Job{Schedule: nil},
			},
			expScheduler: &oneshot{dueTime: now},
			expErr:       false,
		},
		"if schedule, expect repeats": {
			job: &stored.Job{
				Begin: &stored.Job_Start{
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
			job: &stored.Job{
				Begin: &stored.Job_Start{
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
			job: &stored.Job{
				Begin: &stored.Job_Start{
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
		testInLoop := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			builder := &Builder{clock: clock}
			gotScheduler, gotErr := builder.Schedule(testInLoop.job)
			assert.Equal(t, testInLoop.expScheduler, gotScheduler)
			assert.Equal(t, testInLoop.expErr, gotErr != nil, "%v", gotErr)
		})
	}
}

func Test_Parse(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	clock := clocktesting.NewFakeClock(now)

	tests := map[string]struct {
		job       *api.Job
		expStored *stored.Job
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
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: nil,
					Ttl:      nil,
					Repeats:  nil,
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
						}},
					},
				},
				Begin: &stored.Job_DueTime{
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
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: ptr.Of("@every 1h"),
					Ttl:      nil,
					Repeats:  nil,
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
						}},
					},
				},
				Begin: &stored.Job_DueTime{
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
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  nil,
					Schedule: ptr.Of("@every 1h"),
					Ttl:      ptr.Of("2h"),
					Repeats:  nil,
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
						}},
					},
				},
				Begin: &stored.Job_Start{
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
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: ptr.Of("@every 1h"),
					Ttl:      ptr.Of("2h"),
					Repeats:  ptr.Of(uint32(100)),
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
						}},
					},
				},
				Begin: &stored.Job_DueTime{
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
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: ptr.Of("@every 1h"),
					Ttl:      ptr.Of("2024-04-24T11:42:22Z"),
					Repeats:  nil,
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second), MaxRetries: ptr.Of(uint32(3)),
						}},
					},
				},
				Begin: &stored.Job_DueTime{
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
		"don't overwrite failure policy constant": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: nil,
				Ttl:      nil,
				Repeats:  nil,
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
						Interval: durationpb.New(time.Second * 3), MaxRetries: ptr.Of(uint32(5)),
					}},
				},
			},
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: nil,
					Ttl:      nil,
					Repeats:  nil,
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Constant{Constant: &api.FailurePolicyConstant{
							Interval: durationpb.New(time.Second * 3), MaxRetries: ptr.Of(uint32(5)),
						}},
					},
				},
				Begin: &stored.Job_DueTime{
					DueTime: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
				},
				Expiration: nil,
			},
			expErr: false,
		},
		"don't overwrite failure policy drop": {
			job: &api.Job{
				DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
				Schedule: nil,
				Ttl:      nil,
				Repeats:  nil,
				FailurePolicy: &api.FailurePolicy{
					Policy: &api.FailurePolicy_Drop{Drop: new(api.FailurePolicyDrop)},
				},
			},
			expStored: &stored.Job{
				Job: &api.Job{
					DueTime:  ptr.Of("2024-04-24T11:42:22Z"),
					Schedule: nil,
					Ttl:      nil,
					Repeats:  nil,
					FailurePolicy: &api.FailurePolicy{
						Policy: &api.FailurePolicy_Drop{Drop: new(api.FailurePolicyDrop)},
					},
				},
				Begin: &stored.Job_DueTime{
					DueTime: timestamppb.New(time.Date(2024, 4, 24, 11, 42, 22, 0, time.UTC)),
				},
				Expiration: nil,
			},
			expErr: false,
		},
	}

	for name, test := range tests {
		testInLoop := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			builder := &Builder{clock: clock}
			gotStored, gotErr := builder.Parse(testInLoop.job)
			if gotStored != nil {
				assert.NotEqual(t, uint32(0), gotStored.GetPartitionId())
				gotStored.PartitionId = 0
			}
			assert.Equal(t, testInLoop.expStored, gotStored)
			assert.Equal(t, testInLoop.expErr, gotErr != nil, "%v", gotErr)
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
		testInLoop := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gotTime, gotErr := parsePointInTime(testInLoop.str, now)
			assert.Equal(t, testInLoop.expTime, gotTime)
			assert.Equal(t, testInLoop.expErr, gotErr != nil, "%v", gotErr)
		})
	}
}
