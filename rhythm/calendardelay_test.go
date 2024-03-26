/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package rhythm

import (
	"testing"
	"time"
)

func TestCalendarDelayNext(t *testing.T) {
	tests := []struct {
		time     string
		years    int
		months   int
		days     int
		delay    time.Duration
		expected string
	}{
		// Simple cases
		{"Mon Jul 9 14:45 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:59 2012", 0, 0, 0, 15 * time.Minute, "Mon Jul 9 15:14 2012"},
		{"Mon Jul 9 14:59:59 2012", 0, 0, 0, 15 * time.Minute, "Mon Jul 9 15:14:59 2012"},
		{"Mon Jul 9 14:59:59 2012", 1, 0, 0, 15 * time.Minute, "Mon Jul 9 15:14:59 2013"},
		{"Mon Jul 9 14:59:59 2012", 0, 1, 0, 15 * time.Minute, "Mon Aug 9 15:14:59 2012"},
		{"Mon Jul 31 14:59:59 2012", 0, 0, 1, 15 * time.Minute, "Mon Aug 1 15:14:59 2012"},

		// Wrap around hours
		{"Mon Jul 9 15:45 2012", 0, 0, 0, 35 * time.Minute, "Mon Jul 9 16:20 2012"},
		{"Mon Jul 9 15:45 2012", 0, 0, 1, 35 * time.Minute, "Mon Jul 10 16:20 2012"},

		// Wrap around days
		{"Mon Jul 9 23:46 2012", 0, 0, 0, 14 * time.Minute, "Tue Jul 10 00:00 2012"},
		{"Mon Jul 9 23:45 2012", 0, 0, 0, 35 * time.Minute, "Tue Jul 10 00:20 2012"},
		{"Mon Jul 9 23:35:51 2012", 0, 0, 0, 44*time.Minute + 24*time.Second, "Tue Jul 10 00:20:15 2012"},
		{"Mon Jul 9 23:35:51 2012", 0, 0, 0, 25*time.Hour + 44*time.Minute + 24*time.Second, "Wed Jul 11 01:20:15 2012"},
		{"Mon Jul 9 23:35:51 2012", 0, 0, 1, 25*time.Hour + 44*time.Minute + 24*time.Second, "Thu Jul 12 01:20:15 2012"},

		// Wrap around months
		{"Mon Jul 9 23:35 2012", 0, 0, 0, 91*24*time.Hour + 25*time.Minute, "Thu Oct 9 00:00 2012"},
		{"Mon Jul 9 23:35 2013", 0, 1, 0, 91*24*time.Hour + 25*time.Minute, "Thu Nov 9 00:00 2013"},

		// Wrap around minute, hour, day, month, and year
		{"Mon Dec 31 23:59:45 2012", 0, 0, 0, 15 * time.Second, "Tue Jan 1 00:00:00 2013"},
		{"Mon Dec 31 23:59:45 2013", 1, 0, 0, 15 * time.Second, "Tue Jan 1 00:00:00 2015"},

		// Truncate to second on the delay
		{"Mon Jul 9 14:45 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:45 2013", 0, 0, 1, 15*time.Minute + 50*time.Nanosecond, "Tue Jul 10 15:00 2013"},
		{"Mon Jul 9 14:45 2012", 0, 0, 0, 15*time.Minute + 999*time.Millisecond, "Mon Jul 9 15:00 2012"},

		// Round up to 1 second if the duration is less.
		{"Mon Jul 9 14:45:00 2012", 0, 0, 0, 15 * time.Millisecond, "Mon Jul 9 14:45:01 2012"},
		// Don't round up if we have to increment days, months or years
		{"Mon Jul 9 14:45:00 2013", 0, 0, 1, 15 * time.Millisecond, "Mon Jul 10 14:45:00 2013"},

		// Truncate to second when calculating the next time.
		{"Mon Jul 9 14:45:00.005 2012", 0, 0, 0, 15 * time.Minute, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:45:00.005 2013", 0, 0, 1, 0, "Mon Jul 10 14:45 2013"},

		// Truncate to second for both input and delay.
		{"Mon Jul 9 14:45:00.005 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:45:00.005 2013", 1, 0, 0, 15*time.Minute + 999*time.Millisecond, "Mon Jul 9 15:00 2014"},
		{"Mon Jul 9 14:45:00.999 2014", 1, 0, 0, 15*time.Minute + 1*time.Millisecond, "Mon Jul 9 15:00 2015"},
		{"Mon Jul 9 14:45:00.999 2015", 1, 0, 0, 15*time.Minute + 999*time.Millisecond, "Mon Jul 9 15:00 2016"},
	}

	for _, c := range tests {
		actual := EveryCalendar(CalendarStep{c.years, c.months, c.days}, c.delay).Next(time.Time{}, getTime(c.time))
		expected := getTime(c.expected)
		if actual != expected {
			t.Errorf("%s, \"%s\": (expected) %v != %v (actual)", c.time, c.delay, expected, actual)
		}
	}
}

func TestCalendarDelayFromStartNext(t *testing.T) {
	tests := []struct {
		time     string
		start    string
		years    int
		months   int
		days     int
		delay    time.Duration
		expected string
	}{
		// Simple cases
		{"Mon Jul 9 14:45 2012", "Mon Jul 9 14:45 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:59 2012", "Mon Jul 9 14:59 2012", 0, 0, 0, 15 * time.Minute, "Mon Jul 9 15:14 2012"},
		{"Mon Jul 9 14:59:59 2012", "Mon Jul 9 14:59:59 2012", 0, 0, 0, 15 * time.Minute, "Mon Jul 9 15:14:59 2012"},
		{"Mon Jul 9 14:59:59 2012", "Mon Jul 9 14:55:01 2012", 0, 0, 0, 10 * time.Minute, "Mon Jul 9 15:05:01 2012"},
		{"Mon Jul 9 15:04:59 2012", "Mon Jul 9 14:55:01 2012", 0, 0, 0, 10 * time.Minute, "Mon Jul 9 15:05:01 2012"},

		// Time < start with calendar skip
		{"Mon Jul 9 15:04:59 2012", "Mon Jul 9 14:55:01 2013", 1, 0, 0, 0 * time.Minute, "Mon Jul 9 14:55:01 2013"},
		{"Mon Jul 9 15:04:59 2012", "Mon Jul 9 14:55:01 2013", 99, 2, 365, 1 * time.Minute, "Mon Jul 9 14:55:01 2013"},

		// Time > start with calendar skip
		{"Mon Jul 9 15:05:59 2012", "Mon Jul 9 14:55:02 2012", 2, 0, 0, 0 * time.Minute, "Mon Jul 9 14:55:02 2014"},
		{"Mon Jul 9 15:05:59 2099", "Mon Jul 9 14:55:02 2012", 2, 0, 0, 0 * time.Minute, "Mon Jul 9 14:55:02 2100"},

		// Simple case for running every second
		{"Mon Jul 9 14:45 2015", "Mon Jul 9 14:45 2015", 0, 0, 0, 1 * time.Second, "Mon Jul 9 14:45:01 2015"},
		{"Mon Jul 9 14:45:01 2015", "Mon Jul 9 14:45 2015", 0, 0, 0, 1 * time.Second, "Mon Jul 9 14:45:02 2015"},

		// Starts only in a distant future
		{"Mon Jul 9 15:04:59 2012", "Mon Jul 9 12:05:01 2050", 0, 0, 0, 10 * time.Minute, "Mon Jul 9 12:05:01 2050"},

		// Wrap around hours
		{"Mon Jul 9 15:45 2012", "Mon Jul 9 15:45 2012", 0, 0, 0, 35 * time.Minute, "Mon Jul 9 16:20 2012"},
		{"Mon Jul 9 15:45 2012", "Mon Jul 9 15:44 2012", 0, 0, 0, 35 * time.Minute, "Mon Jul 9 16:19 2012"},
		{"Mon Jul 9 15:45 2012", "Mon Jul 9 15:46 2012", 0, 0, 0, 35 * time.Minute, "Mon Jul 9 15:46 2012"},

		// Wrap around days
		{"Mon Jul 9 23:46 2012", "Mon Jul 9 23:46 2012", 0, 0, 0, 14 * time.Minute, "Tue Jul 10 00:00 2012"},
		{"Mon Jul 9 23:45 2012", "Mon Jul 9 23:45 2012", 0, 0, 0, 35 * time.Minute, "Tue Jul 10 00:20 2012"},
		{"Mon Jul 9 23:35:51 2012", "Mon Jul 9 23:35:51 2012", 0, 0, 0, 44*time.Minute + 24*time.Second, "Tue Jul 10 00:20:15 2012"},
		{"Mon Jul 9 23:35:51 2012", "Mon Jul 9 23:35:51 2012", 0, 0, 0, 25*time.Hour + 44*time.Minute + 24*time.Second, "Thu Jul 11 01:20:15 2012"},

		// Wrap around months
		{"Mon Jul 9 23:35 2012", "Mon Jul 9 23:35 2012", 0, 0, 0, 91*24*time.Hour + 25*time.Minute, "Thu Oct 9 00:00 2012"},
		{"Mon Jul 9 23:35 2012", "Mon Jul 9 23:35 2012", 0, 1, 0, 91*24*time.Hour + 25*time.Minute, "Thu Nov 9 00:00 2012"},

		// Wrap around minute, hour, day, month, and year
		{"Mon Dec 31 23:59:45 2012", "Mon Dec 31 23:59:45 2012", 0, 0, 0, 15 * time.Second, "Tue Jan 1 00:00:00 2013"},
		{"Mon Dec 31 23:59:45 2012", "Mon Dec 31 23:59:45 2012", 1, 0, 0, 15 * time.Second, "Tue Jan 1 00:00:00 2014"},

		// Truncate to second on the delay
		{"Mon Jul 9 14:45 2012", "Mon Jul 9 14:45 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:45 2012", "Mon Jul 9 14:45 2012", 1, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2013"},

		// Round up to 1 second if the duration is less.
		{"Mon Jul 9 14:45:00 2012", "Mon Jul 9 14:45:00 2012", 0, 0, 0, 15 * time.Millisecond, "Mon Jul 9 14:45:01 2012"},

		// Don't round up if we have calendar skips.
		{"Mon Jul 9 14:45:00 2012", "Mon Jul 9 14:45:00 2012", 0, 0, 1, 15 * time.Millisecond, "Mon Jul 10 14:45:00 2012"},

		// Truncate to second when calculating the next time.
		{"Mon Jul 9 14:45:00.006 2012", "Mon Jul 9 14:45:00.006 2012", 0, 0, 0, 15 * time.Minute, "Mon Jul 9 15:00 2012"},

		//Truncate to second for both.
		{"Mon Jul 9 14:45:00.005 2012", "Mon Jul 9 14:45:00.007 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2012"},
		{"Mon Jul 9 14:45:00.999 2013", "Mon Jul 9 14:45:00.007 2012", 0, 0, 0, 15*time.Minute + 50*time.Nanosecond, "Mon Jul 9 15:00 2013"},
		{"Mon Jul 9 14:45:00.005 2014", "Mon Jul 9 14:45:00.007 2012", 0, 0, 0, 15*time.Minute + 999*time.Millisecond, "Mon Jul 9 15:00 2014"},
		{"Mon Jul 9 14:45:00.999 2015", "Mon Jul 9 14:45:00.007 2012", 0, 0, 0, 15*time.Minute + 999*time.Millisecond, "Mon Jul 9 15:00 2015"},
	}

	for _, c := range tests {
		actual := EveryCalendar(CalendarStep{c.years, c.months, c.days}, c.delay).Next(getTime(c.start), getTime(c.time))
		expected := getTime(c.expected)
		if actual != expected {
			t.Errorf("%s, \"%s\": (expected) %v != %v (actual)", c.time, c.delay, expected, actual)
		}
	}
}
