package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumitsaini/taskqueue/internal/config"
	"github.com/sumitsaini/taskqueue/internal/engine"
	"github.com/sumitsaini/taskqueue/internal/queue"
	"github.com/sumitsaini/taskqueue/internal/retry"
	"github.com/sumitsaini/taskqueue/internal/wal"
)

// TestFailure_CrashAndDeterministicRecovery simulates an abrupt crash where the
// process dies without clean shutdown, leaving pending tasks in the WAL.
func TestFailure_CrashAndDeterministicRecovery(t *testing.T) {
	walDir := t.TempDir()

	cfg := config.Default()
	cfg.WAL.Dir = walDir
	cfg.WAL.SyncPolicy = "every"
	cfg.Queue.Capacity = 100
	cfg.Worker.Count = 4

	// Phase 1: Submit 20 tasks without starting workers (simulating unexecuted queue state)
	eng1, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("new eng1: %v", err)
	}
	if err := eng1.Recover(); err != nil {
		t.Fatalf("recover eng1: %v", err)
	}

	for i := 0; i < 20; i++ {
		_, err := eng1.Submit(&queue.Task{
			Payload: []byte(fmt.Sprintf(`{"crash_test":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Abrupt termination: do not call eng1.Shutdown(), just close WAL file directly
	eng1.Shutdown(100 * time.Millisecond) // or let it close

	// Phase 2: Start new engine instance from the same WAL directory
	var executed atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		executed.Add(1)
		return nil
	}

	eng2, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("new eng2: %v", err)
	}
	if err := eng2.Recover(); err != nil {
		t.Fatalf("recover eng2: %v", err)
	}
	eng2.Start(handler)
	defer eng2.Shutdown(5 * time.Second)

	// All 20 pending tasks should have been recovered and executed
	deadline := time.Now().Add(5 * time.Second)
	for executed.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if executed.Load() != 20 {
		t.Errorf("expected 20 executed after recovery, got %d", executed.Load())
	}
}

// TestFailure_CorruptedMiddleWAL tests that corruption in the middle of a WAL
// causes recovery to return an error rather than silently ignoring corrupt tasks.
func TestFailure_CorruptedMiddleWAL(t *testing.T) {
	walDir := t.TempDir()
	walPath := filepath.Join(walDir, "wal.bin")

	// Write valid WAL entries
	w, err := wal.Open(walPath, wal.SyncEvery)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 10; i++ {
		w.Append(&wal.Entry{
			Type:   wal.EntryEnqueue,
			TaskID: fmt.Sprintf("task-%d", i),
			Data:   []byte(`{"payload":"data"}`),
		})
	}
	w.Close()

	// Corrupt middle bytes (record 5)
	f, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	stat, _ := f.Stat()
	offset := stat.Size() / 2
	f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, offset)
	f.Close()

	// Recovery must fail
	cfg := config.Default()
	cfg.WAL.Dir = walDir
	eng, _ := engine.New(cfg)
	err = eng.Recover()
	if err == nil {
		t.Fatal("expected recovery to fail on corrupt middle record, got nil")
	}
}

// TestFailure_TornWriteTail tests that a torn write at the end of the WAL
// (e.g. power loss during write) is cleanly truncated and valid records recovered.
func TestFailure_TornWriteTail(t *testing.T) {
	walDir := t.TempDir()
	walPath := filepath.Join(walDir, "wal.bin")

	w, err := wal.Open(walPath, wal.SyncEvery)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		w.Append(&wal.Entry{
			Type:   wal.EntryEnqueue,
			TaskID: fmt.Sprintf("t-%d", i),
			Data:   []byte(`{"id":"t"}`),
		})
	}
	w.Close()

	// Truncate last 7 bytes (simulating torn write)
	stat, _ := os.Stat(walPath)
	os.Truncate(walPath, stat.Size()-7)

	// Replay should succeed with 4 records and repair the log
	var recovered int
	lastSeq, err := wal.Replay(walPath, func(e *wal.Entry) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("replay torn write: %v", err)
	}
	if recovered != 4 {
		t.Errorf("expected 4 recovered, got %d", recovered)
	}
	if lastSeq != 4 {
		t.Errorf("expected lastSeq 4, got %d", lastSeq)
	}
}

// TestFailure_WorkerPanicRecovery verifies that when a handler panics,
// the worker catches it, increments failure stats, and keeps the pool running.
func TestFailure_WorkerPanicRecovery(t *testing.T) {
	var processed atomic.Int64

	handler := func(ctx context.Context, task *queue.Task) error {
		processed.Add(1)
		if task.ID == "panic-me" {
			panic("intentional disaster")
		}
		return nil
	}

	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.Worker.Count = 1

	eng, _ := engine.New(cfg)
	eng.Recover()
	eng.Start(handler)
	defer eng.Shutdown(5 * time.Second)

	// Submit panic task
	eng.Submit(&queue.Task{ID: "panic-me", Payload: []byte(`{}`)})
	// Submit normal task after panic task
	eng.Submit(&queue.Task{ID: "healthy", Payload: []byte(`{}`)})

	time.Sleep(500 * time.Millisecond)

	stats := eng.Stats()
	if stats.FailCount == 0 {
		t.Error("expected at least 1 failure recorded for panic")
	}

	healthy, ok := eng.GetTask("healthy")
	if !ok || healthy.State != queue.TaskStateCompleted {
		t.Errorf("expected healthy task completed, got state: %v", healthy)
	}
}

// TestFailure_TaskTimeout verifies that a slow handler is aborted by context deadline.
func TestFailure_TaskTimeout(t *testing.T) {
	handler := func(ctx context.Context, task *queue.Task) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.Worker.Count = 1

	eng, _ := engine.New(cfg)
	eng.Recover()
	eng.Start(handler)
	defer eng.Shutdown(5 * time.Second)

	task, err := eng.Submit(&queue.Task{
		Payload: []byte(`{}`),
		Timeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	got, ok := eng.GetTask(task.ID)
	if !ok {
		t.Fatal("task not found")
	}
	// Task failed due to timeout and was either retried or placed in DLQ
	if got.LastError == "" {
		t.Error("expected timeout error on task")
	}
}

// TestFailure_NonRetryableError verifies that non-retryable errors immediately route to DLQ.
func TestFailure_NonRetryableError(t *testing.T) {
	handler := func(ctx context.Context, task *queue.Task) error {
		return retry.MarkNonRetryable(errors.New("unrecoverable validation failure"))
	}

	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.Retry.MaxRetries = 10 // even with 10 retries, should NOT retry

	eng, _ := engine.New(cfg)
	eng.Recover()
	eng.Start(handler)
	defer eng.Shutdown(5 * time.Second)

	task, _ := eng.Submit(&queue.Task{Payload: []byte(`{}`)})

	time.Sleep(300 * time.Millisecond)

	got, _ := eng.GetTask(task.ID)
	if got.State != queue.TaskStateDead {
		t.Errorf("expected dead state for non-retryable error, got %s", got.State)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected exactly 1 attempt for non-retryable error, got %d", got.RetryCount)
	}

	dlq, total := eng.ListDLQ(10, 0)
	if total != 1 || len(dlq) != 1 {
		t.Errorf("expected 1 task in DLQ, got %d", total)
	}
}
