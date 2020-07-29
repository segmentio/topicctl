package util

import (
	"fmt"
	"time"
)

// PrettyDuration returns a human-formatted duration string given an golang
// duration value.
func PrettyDuration(duration time.Duration) string {
	seconds := duration.Seconds()

	if seconds < 1.0 {
		return fmt.Sprintf("%dms", duration.Milliseconds())
	} else if seconds < 240.0 {
		return fmt.Sprintf("%ds", int(seconds))
	} else if seconds < (2.0 * 60.0 * 60.0) {
		return fmt.Sprintf("%dm", int(duration.Minutes()))
	} else {
		return fmt.Sprintf("%dh", int(duration.Hours()))
	}
}

// PrettyRate returns a human-formatted rate from a count and a duration.
func PrettyRate(count int64, duration time.Duration) string {
	if duration == 0 {
		return ""
	} else if count == 0 {
		return "0"
	}

	ratePerSec := float64(count) / duration.Seconds()
	ratePerMin := float64(count) / duration.Minutes()
	ratePerHour := float64(count) / duration.Hours()

	if ratePerSec >= 10.0 {
		return fmt.Sprintf("%d/sec", int(ratePerSec))
	} else if ratePerSec >= 1.0 {
		return fmt.Sprintf("%0.1f/sec", ratePerSec)
	} else if ratePerMin >= 10.0 {
		return fmt.Sprintf("%d/min", int(ratePerMin))
	} else if ratePerMin >= 1.0 {
		return fmt.Sprintf("%0.1f/min", ratePerMin)
	} else if ratePerHour >= 0.1 {
		return fmt.Sprintf("%0.1f/hour", ratePerHour)
	} else {
		return fmt.Sprintf("~0")
	}
}
