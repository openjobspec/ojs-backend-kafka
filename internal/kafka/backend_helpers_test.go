package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

func TestComputeScore(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Default priority (nil) = 0 → score = (100-0)*1e15 + unixMilli
	score0 := computeScore(nil, now)
	if score0 <= 0 {
		t.Errorf("expected positive score, got %f", score0)
	}

	// Higher priority (lower number) should have lower score (pop first)
	high := 10
	low := -10
	scoreHigh := computeScore(&high, now)
	scoreLow := computeScore(&low, now)

	if scoreHigh >= scoreLow {
		t.Errorf("high priority score (%f) should be less than low priority (%f)", scoreHigh, scoreLow)
	}
}

func TestComputeScore_TimeOrdering(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(1 * time.Second)

	// Same priority, later time = higher score (FIFO within priority)
	s1 := computeScore(nil, t1)
	s2 := computeScore(nil, t2)
	if s1 >= s2 {
		t.Errorf("earlier time score (%f) should be less than later time (%f)", s1, s2)
	}
}

func TestComputeFingerprint(t *testing.T) {
	job1 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["hello"]`),
		Queue:  "default",
		Unique: &core.UniquePolicy{Keys: []string{"type", "args"}},
	}

	fp1 := computeFingerprint(job1)
	if fp1 == "" {
		t.Fatal("expected non-empty fingerprint")
	}

	// Same job same fingerprint
	job2 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["hello"]`),
		Queue:  "other",
		Unique: &core.UniquePolicy{Keys: []string{"type", "args"}},
	}
	fp2 := computeFingerprint(job2)
	if fp1 != fp2 {
		t.Errorf("same type+args should produce same fingerprint: %q != %q", fp1, fp2)
	}

	// Different args different fingerprint
	job3 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["world"]`),
		Queue:  "default",
		Unique: &core.UniquePolicy{Keys: []string{"type", "args"}},
	}
	fp3 := computeFingerprint(job3)
	if fp1 == fp3 {
		t.Error("different args should produce different fingerprint")
	}
}

func TestComputeFingerprint_WithQueueKey(t *testing.T) {
	job1 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["hello"]`),
		Queue:  "default",
		Unique: &core.UniquePolicy{Keys: []string{"type", "args", "queue"}},
	}

	job2 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["hello"]`),
		Queue:  "other-queue",
		Unique: &core.UniquePolicy{Keys: []string{"type", "args", "queue"}},
	}

	fp1 := computeFingerprint(job1)
	fp2 := computeFingerprint(job2)
	if fp1 == fp2 {
		t.Error("different queues should produce different fingerprints when queue is in keys")
	}
}

func TestComputeFingerprint_DefaultKeys(t *testing.T) {
	job := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`[1]`),
		Unique: &core.UniquePolicy{}, // Empty keys = default ["type", "args"]
	}

	fp := computeFingerprint(job)
	if fp == "" {
		t.Fatal("expected non-empty fingerprint with default keys")
	}
}

func TestMatchesPattern(t *testing.T) {
	tests := []struct {
		s       string
		pattern string
		want    bool
	}{
		// Exact match
		{"validation_error", "validation_error", true},
		{"auth_error", "validation_error", false},
		// Wildcard
		{"auth_expired", "auth*", true},
		{"auth_invalid", "auth*", true},
		{"validation_error", "auth*", false},
		// No false regex injection: dots are literal
		{"auth.error", "auth.error", true},
		{"authXerror", "auth.error", false}, // dot is literal, not regex "any char"
		// Embedded regex chars are safe
		{"test[0]", "test[0]", true},
		{"test0", "test[0]", false}, // brackets are literal, not regex char class
	}

	for _, tt := range tests {
		t.Run(tt.s+"~"+tt.pattern, func(t *testing.T) {
			got := matchesPattern(tt.s, tt.pattern)
			if got != tt.want {
				t.Errorf("matchesPattern(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
			}
		})
	}
}
