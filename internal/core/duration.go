package core

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var isoDurationPattern = regexp.MustCompile(`^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$`)

// ParseISO8601Duration parses an ISO 8601 duration string (e.g., PT1S, PT5M, PT1H30M).
func ParseISO8601Duration(s string) (time.Duration, error) {
	matches := isoDurationPattern.FindStringSubmatch(s)
	if matches == nil {
		return 0, fmt.Errorf("invalid ISO 8601 duration: %q", s)
	}

	var d time.Duration

	if matches[1] != "" {
		h, _ := strconv.Atoi(matches[1])
		d += time.Duration(h) * time.Hour
	}
	if matches[2] != "" {
		m, _ := strconv.Atoi(matches[2])
		d += time.Duration(m) * time.Minute
	}
	if matches[3] != "" {
		secs, _ := strconv.ParseFloat(matches[3], 64)
		d += time.Duration(secs * float64(time.Second))
	}

	if d == 0 {
		return 0, fmt.Errorf("invalid ISO 8601 duration: %q (zero duration)", s)
	}

	return d, nil
}

// FormatISO8601Duration formats a time.Duration as an ISO 8601 duration string.
func FormatISO8601Duration(d time.Duration) string {
	if d == 0 {
		return "PT0S"
	}

	totalSeconds := d.Seconds()
	hours := int(totalSeconds / 3600)
	remaining := totalSeconds - float64(hours*3600)
	minutes := int(remaining / 60)
	seconds := remaining - float64(minutes*60)

	result := "PT"
	if hours > 0 {
		result += fmt.Sprintf("%dH", hours)
	}
	if minutes > 0 {
		result += fmt.Sprintf("%dM", minutes)
	}
	if seconds > 0 {
		if seconds == float64(int(seconds)) {
			result += fmt.Sprintf("%dS", int(seconds))
		} else {
			result += fmt.Sprintf("%.3fS", seconds)
		}
	}
	return result
}
