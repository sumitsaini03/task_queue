package retry

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestRetry_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxRetries != 5 {
		t.Errorf("expected 5 max retries, got %d", cfg.MaxRetries)
	}
	if cfg.BaseDelay != 1*time.Second {
		t.Errorf("expected 1s base delay, got %v", cfg.BaseDelay)
	}
	if cfg.MaxDelay != 60*time.Second {
		t.Errorf("expected 60s max delay, got %v", cfg.MaxDelay)
	}
	if cfg.Multiplier != 2.0 {
		t.Errorf("expected 2.0 multiplier, got %f", cfg.Multiplier)
	}
}

func TestRetry_NextDelay(t *testing.T) {
	cfg := Config{
		MaxRetries:  5,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    1 * time.Second,
		Multiplier:  2.0,
		JitterRatio: 0.0, // test deterministic backoff first
	}

	expectedDelays := []time.Duration{
		100 * time.Millisecond,  // attempt 0: 100 * 2^0 = 100ms
		200 * time.Millisecond,  // attempt 1: 100 * 2^1 = 200ms
		400 * time.Millisecond,  // attempt 2: 100 * 2^2 = 400ms
		800 * time.Millisecond,  // attempt 3: 100 * 2^3 = 800ms
		1000 * time.Millisecond, // attempt 4: capped at 1s
		1000 * time.Millisecond, // attempt 5: capped at 1s
	}

	for attempt, want := range expectedDelays {
		got := NextDelay(cfg, attempt)
		if got != want {
			t.Errorf("attempt %d: expected %v, got %v", attempt, want, got)
		}
	}
}

func TestRetry_NextDelayWithJitter(t *testing.T) {
	cfg := Config{
		MaxRetries:  5,
		BaseDelay:   1 * time.Second,
		MaxDelay:    10 * time.Second,
		Multiplier:  2.0,
		JitterRatio: 0.2, // +/- 20%
	}

	// 100 samples should all fall within [base * 0.8, base * 1.2]
	base := float64(1 * time.Second)
	minBound := time.Duration(base * 0.8)
	maxBound := time.Duration(base * 1.2)

	for i := 0; i < 100; i++ {
		d := NextDelay(cfg, 0)
		if d < minBound || d > maxBound {
			t.Errorf("delay %v outside bounds [%v, %v]", d, minBound, maxBound)
		}
	}
}

func TestRetry_NonRetryable(t *testing.T) {
	regularErr := errors.New("temporary network error")
	if IsNonRetryable(regularErr) {
		t.Error("regularErr should not be non-retryable")
	}

	nonRetryable := MarkNonRetryable(errors.New("invalid payload syntax"))
	if !IsNonRetryable(nonRetryable) {
		t.Error("expected nonRetryable to be recognized")
	}

	// Test wrapping
	wrapped := fmt.Errorf("outer: %w", nonRetryable)
	if !IsNonRetryable(wrapped) {
		t.Error("expected wrapped nonRetryable to be recognized via Unwrap")
	}

	// Test nil
	if MarkNonRetryable(nil) != nil {
		t.Error("expected nil when marking nil error")
	}
	if IsNonRetryable(nil) {
		t.Error("expected false for nil error")
	}
}
