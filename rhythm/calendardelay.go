/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package rhythm

import "time"

// CalendarDelaySchedule represents a recurring duty cycle that includes days, months or years too, e.g. "Every 3 months".
// It does not support jobs more frequent than once a second.
type CalendarDelaySchedule struct {
	calendarStep CalendarStep

	Delay time.Duration
}

type CalendarStep struct {
	years  int
	months int
	days   int
}

func EveryCalendar(c CalendarStep, duration time.Duration) CalendarDelaySchedule {
	return CalendarDelaySchedule{
		calendarStep: c,
		Delay:        duration,
	}
}

// Next returns the next time this should be run.
// This rounds so that the next activation time will be on the second.
func (schedule CalendarDelaySchedule) Next(start, t time.Time) time.Time {
	if start.IsZero() {
		// schedule is not bound to a starting point
		return t.
			Add(schedule.Delay).
			AddDate(schedule.calendarStep.years, schedule.calendarStep.months, schedule.calendarStep.days)
	}

	s := start

	if t.Before(s) {
		return s
	}

	// We cannot count steps since those are calendar days increments.
	next := s.Add(schedule.Delay).
		AddDate(schedule.calendarStep.years, schedule.calendarStep.months, schedule.calendarStep.days)
	for !next.After(t) {
		// This is not highly efficient but it is the only way to make sure the next trigger
		// is relative to the start time in calendar increments.
		// Remember that adding 1 month is not the same as adding 30 days.
		next = next.Add(schedule.Delay).
			AddDate(schedule.calendarStep.years, schedule.calendarStep.months, schedule.calendarStep.days)
	}
	return next
}
