package config

import (
	"testing"
)

func TestConfig_Default(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid, got: %v", err)
	}

	if cfg.Queue.Capacity <= 0 {
		t.Errorf("expected positive capacity, got %d", cfg.Queue.Capacity)
	}
	if cfg.Worker.Count <= 0 {
		t.Errorf("expected positive worker count, got %d", cfg.Worker.Count)
	}
	if cfg.Retry.MaxRetries < 0 {
		t.Errorf("expected non-negative retries, got %d", cfg.Retry.MaxRetries)
	}
}

func TestConfig_ValidateErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{
			name:    "zero queue capacity",
			mutate:  func(c *Config) { c.Queue.Capacity = 0 },
			wantErr: true,
		},
		{
			name:    "negative queue capacity",
			mutate:  func(c *Config) { c.Queue.Capacity = -1 },
			wantErr: true,
		},
		{
			name:    "invalid overflow policy",
			mutate:  func(c *Config) { c.Queue.OverflowPolicy = "drop_silently" },
			wantErr: true,
		},
		{
			name:    "zero worker count",
			mutate:  func(c *Config) { c.Worker.Count = 0 },
			wantErr: true,
		},
		{
			name:    "negative retries",
			mutate:  func(c *Config) { c.Retry.MaxRetries = -1 },
			wantErr: true,
		},
		{
			name:    "zero base delay",
			mutate:  func(c *Config) { c.Retry.BaseDelay = 0 },
			wantErr: true,
		},
		{
			name:    "multiplier less than 1",
			mutate:  func(c *Config) { c.Retry.Multiplier = 0.5 },
			wantErr: true,
		},
		{
			name:    "empty wal dir",
			mutate:  func(c *Config) { c.WAL.Dir = "" },
			wantErr: true,
		},
		{
			name:    "invalid sync policy",
			mutate:  func(c *Config) { c.WAL.SyncPolicy = "unknown" },
			wantErr: true,
		},
		{
			name:    "empty http addr",
			mutate:  func(c *Config) { c.HTTP.Addr = "" },
			wantErr: true,
		},
		{
			name: "valid block policy",
			mutate: func(c *Config) {
				c.Queue.OverflowPolicy = "block"
				c.WAL.SyncPolicy = "batch"
			},
			wantErr: false,
		},
		{
			name: "valid none sync policy",
			mutate: func(c *Config) {
				c.WAL.SyncPolicy = "none"
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
