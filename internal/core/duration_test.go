package core

import (
	"testing"
	"time"
)

func TestParseISO8601Duration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"PT1S", 1 * time.Second, false},
		{"PT5M", 5 * time.Minute, false},
		{"PT1H", 1 * time.Hour, false},
		{"PT1H30M", 1*time.Hour + 30*time.Minute, false},
		{"PT1H30M45S", 1*time.Hour + 30*time.Minute + 45*time.Second, false},
		{"PT0.5S", 500 * time.Millisecond, false},
		{"PT2.5S", 2500 * time.Millisecond, false},
		{"PT30S", 30 * time.Second, false},
		// Invalid inputs
		{"", 0, true},
		{"P1D", 0, true},      // Days not supported
		{"1S", 0, true},       // Missing PT prefix
		{"PT", 0, true},       // Zero duration (no components)
		{"PT0S", 0, true},     // Explicit zero
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseISO8601Duration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseISO8601Duration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseISO8601Duration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatISO8601Duration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "PT0S"},
		{1 * time.Second, "PT1S"},
		{5 * time.Minute, "PT5M"},
		{1 * time.Hour, "PT1H"},
		{1*time.Hour + 30*time.Minute, "PT1H30M"},
		{1*time.Hour + 30*time.Minute + 45*time.Second, "PT1H30M45S"},
		{90 * time.Second, "PT1M30S"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatISO8601Duration(tt.input)
			if got != tt.want {
				t.Errorf("FormatISO8601Duration(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseFormat_Roundtrip(t *testing.T) {
	durations := []string{"PT1S", "PT5M", "PT1H", "PT1H30M"}
	for _, s := range durations {
		d, err := ParseISO8601Duration(s)
		if err != nil {
			t.Fatalf("ParseISO8601Duration(%q) unexpected error: %v", s, err)
		}
		got := FormatISO8601Duration(d)
		if got != s {
			t.Errorf("roundtrip %q -> %v -> %q", s, d, got)
		}
	}
}
