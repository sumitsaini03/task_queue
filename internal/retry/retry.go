// Package retry implements exponential backoff with jitter and
// dead-letter queue management for tasks that exhaust their retries.
package retry

import (
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

func randomFloat64() float64 {
	var b [8]byte
	_, _ = crand.Read(b[:])
	return float64(binary.LittleEndian.Uint64(b[:])&(1<<53-1)) / (1 << 53)
}

// Config holds retry policy settings.
type Config struct {
	MaxRetries  int           `json:"max_retries"`
	BaseDelay   time.Duration `json:"base_delay"`
	MaxDelay    time.Duration `json:"max_delay"`
	Multiplier  float64       `json:"multiplier"`
	JitterRatio float64       `json:"jitter_ratio"` // 0.0–1.0
}

// DefaultConfig returns sensible defaults for retry behavior.
func DefaultConfig() Config {
	return Config{
		MaxRetries:  5,
		BaseDelay:   1 * time.Second,
		MaxDelay:    60 * time.Second,
		Multiplier:  2.0,
		JitterRatio: 0.3,
	}
}

// NextDelay calculates the delay before the next retry attempt.
// Uses exponential backoff with jitter: delay = min(base * mult^attempt, max) ± jitter.
func NextDelay(cfg Config, attempt int) time.Duration {
	delay := float64(cfg.BaseDelay) * math.Pow(cfg.Multiplier, float64(attempt))
	if delay > float64(cfg.MaxDelay) {
		delay = float64(cfg.MaxDelay)
	}

	// Add jitter: ±(jitterRatio * delay)
	jitter := delay * cfg.JitterRatio
	delay = delay + (randomFloat64()*2-1)*jitter

	if delay < 0 {
		delay = float64(cfg.BaseDelay)
	}

	return time.Duration(delay)
}

// NonRetryableError marks an error as terminal, bypassing retry attempts.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// MarkNonRetryable wraps an error to indicate it should not be retried.
func MarkNonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{Err: err}
}

// IsNonRetryable reports whether err contains a NonRetryableError.
func IsNonRetryable(err error) bool {
	var nre *NonRetryableError
	return errors.As(err, &nre)
}
