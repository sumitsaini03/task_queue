// Package config centralizes all configuration for the task queue server.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the complete server configuration.
type Config struct {
	Queue      QueueConfig      `json:"queue"`
	Worker     WorkerConfig     `json:"worker"`
	Retry      RetryConfig      `json:"retry"`
	WAL        WALConfig        `json:"wal"`
	Checkpoint CheckpointConfig `json:"checkpoint"`
	HTTP       HTTPConfig       `json:"http"`
}

// QueueConfig configures the ring buffer and scheduler.
type QueueConfig struct {
	Capacity       int    `json:"capacity"`
	OverflowPolicy string `json:"overflow_policy"` // "reject" or "block"
}

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	Count          int           `json:"count"`
	DefaultTimeout time.Duration `json:"default_timeout"`
}

// RetryConfig configures retry behavior.
type RetryConfig struct {
	MaxRetries  int           `json:"max_retries"`
	BaseDelay   time.Duration `json:"base_delay"`
	MaxDelay    time.Duration `json:"max_delay"`
	Multiplier  float64       `json:"multiplier"`
	JitterRatio float64       `json:"jitter_ratio"`
}

// WALConfig configures the write-ahead log.
type WALConfig struct {
	Dir        string `json:"dir"`
	SyncPolicy string `json:"sync_policy"` // "every" (fsync per write), "batch" (periodic), "none" (OS)
	MaxSize    int64  `json:"max_size"`    // max WAL file size before rotation
}

// CheckpointConfig configures snapshotting.
type CheckpointConfig struct {
	Interval  time.Duration `json:"interval"`
	Threshold int           `json:"threshold"` // min WAL entries before checkpoint
}

// HTTPConfig configures the HTTP server.
type HTTPConfig struct {
	Addr            string        `json:"addr"`
	ReadTimeout     time.Duration `json:"read_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout"`
	IdleTimeout     time.Duration `json:"idle_timeout"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	RateLimitBurst  float64       `json:"rate_limit_burst"`
	RateLimitPerSec float64       `json:"rate_limit_per_sec"`
}

// Default returns a Config with sensible production defaults.
func Default() Config {
	return Config{
		Queue: QueueConfig{
			Capacity:       4096,
			OverflowPolicy: "reject",
		},
		Worker: WorkerConfig{
			Count:          8,
			DefaultTimeout: 30 * time.Second,
		},
		Retry: RetryConfig{
			MaxRetries:  5,
			BaseDelay:   1 * time.Second,
			MaxDelay:    60 * time.Second,
			Multiplier:  2.0,
			JitterRatio: 0.3,
		},
		WAL: WALConfig{
			Dir:        "data/wal",
			SyncPolicy: "every",
			MaxSize:    64 * 1024 * 1024, // 64 MB
		},
		Checkpoint: CheckpointConfig{
			Interval:  5 * time.Minute,
			Threshold: 10000,
		},
		HTTP: HTTPConfig{
			Addr:            ":8080",
			ReadTimeout:     5 * time.Second,
			WriteTimeout:    10 * time.Second,
			IdleTimeout:     60 * time.Second,
			ShutdownTimeout: 30 * time.Second,
			RateLimitBurst:  100,
			RateLimitPerSec: 50,
		},
	}
}

// Load returns Default() with optional environment variable overrides.
func Load() Config {
	cfg := Default()
	if v := os.Getenv("WAL_DIR"); v != "" {
		cfg.WAL.Dir = v
	}
	if v := os.Getenv("HTTP_ADDR"); v != "" {
		cfg.HTTP.Addr = v
	} else if p := os.Getenv("PORT"); p != "" {
		cfg.HTTP.Addr = ":" + p
	}
	if v := os.Getenv("QUEUE_OVERFLOW_POLICY"); v != "" {
		cfg.Queue.OverflowPolicy = v
	}
	if v := os.Getenv("WAL_SYNC_POLICY"); v != "" {
		cfg.WAL.SyncPolicy = v
	}
	if v := os.Getenv("RATE_LIMIT_BURST"); v != "" {
		if val, err := strconv.ParseFloat(v, 64); err == nil && val > 0 {
			cfg.HTTP.RateLimitBurst = val
		}
	}
	if v := os.Getenv("RATE_LIMIT_PER_SEC"); v != "" {
		if val, err := strconv.ParseFloat(v, 64); err == nil && val > 0 {
			cfg.HTTP.RateLimitPerSec = val
		}
	}
	return cfg
}

// Validate checks configuration for invalid values.
func (c *Config) Validate() error {
	var errs []error

	if c.Queue.Capacity <= 0 {
		errs = append(errs, fmt.Errorf("queue.capacity must be > 0, got %d", c.Queue.Capacity))
	}
	if c.Queue.OverflowPolicy != "reject" && c.Queue.OverflowPolicy != "block" {
		errs = append(errs, fmt.Errorf("queue.overflow_policy must be 'reject' or 'block', got %q", c.Queue.OverflowPolicy))
	}
	if c.Worker.Count <= 0 {
		errs = append(errs, fmt.Errorf("worker.count must be > 0, got %d", c.Worker.Count))
	}
	if c.Retry.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("retry.max_retries must be >= 0, got %d", c.Retry.MaxRetries))
	}
	if c.Retry.BaseDelay <= 0 {
		errs = append(errs, fmt.Errorf("retry.base_delay must be > 0"))
	}
	if c.Retry.Multiplier < 1 {
		errs = append(errs, fmt.Errorf("retry.multiplier must be >= 1, got %f", c.Retry.Multiplier))
	}
	if c.WAL.Dir == "" {
		errs = append(errs, fmt.Errorf("wal.dir must not be empty"))
	}
	switch c.WAL.SyncPolicy {
	case "every", "batch", "none":
		// valid
	default:
		errs = append(errs, fmt.Errorf("wal.sync_policy must be 'every', 'batch', or 'none', got %q", c.WAL.SyncPolicy))
	}
	if c.HTTP.Addr == "" {
		errs = append(errs, fmt.Errorf("http.addr must not be empty"))
	}

	return errors.Join(errs...)
}
