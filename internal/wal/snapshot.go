package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Snapshot represents a point-in-time capture of the queue state.
// It is written atomically (write-to-temp + rename) so a crash
// during snapshot creation never corrupts the previous snapshot.
type Snapshot struct {
	LastSeq uint64          `json:"last_seq"` // WAL seq at snapshot time
	Tasks   []*SnapshotTask `json:"tasks"`
}

// SnapshotTask is the serialized form of a task in a snapshot.
type SnapshotTask struct {
	ID             string `json:"id"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Payload        []byte `json:"payload"`
	State          uint8  `json:"state"`
	Priority       int    `json:"priority"`
	CreatedAt      int64  `json:"created_at"`
	ScheduledAt    int64  `json:"scheduled_at,omitempty"`
	StartedAt      int64  `json:"started_at,omitempty"`
	CompletedAt    int64  `json:"completed_at,omitempty"`
	RetryCount     int    `json:"retry_count"`
	MaxRetries     int    `json:"max_retries"`
	LastError      string `json:"last_error,omitempty"`
	TimeoutNs      int64  `json:"timeout_ns,omitempty"`
}

// WriteSnapshot atomically writes a snapshot to disk.
// It writes to a temporary file and renames, so a crash during
// snapshot creation never corrupts the previous snapshot.
func WriteSnapshot(dir string, snap *Snapshot) error {
	cleanDir := filepath.Clean(dir)
	if err := os.MkdirAll(cleanDir, 0750); err != nil {
		return fmt.Errorf("wal: create snapshot dir: %w", err)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("wal: marshal snapshot: %w", err)
	}

	tmpPath := filepath.Join(cleanDir, "snapshot.tmp")
	finalPath := filepath.Join(cleanDir, "snapshot.json")

	// Write to temp file with restricted 0600 permissions
	// #nosec G306 -- snapshot file restricted to owner access
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("wal: write snapshot tmp: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("wal: rename snapshot: %w", err)
	}

	return nil
}

// ReadSnapshot reads a snapshot from disk. Returns nil if no snapshot exists.
func ReadSnapshot(dir string) (*Snapshot, error) {
	cleanPath := filepath.Clean(filepath.Join(dir, "snapshot.json"))
	// #nosec G304 -- snapshot path is constructed from validated config directory
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: read snapshot: %w", err)
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("wal: unmarshal snapshot: %w", err)
	}
	return &snap, nil
}

// SnapshotPath returns the path to the snapshot file.
func SnapshotPath(dir string) string {
	return filepath.Join(dir, "snapshot.json")
}
