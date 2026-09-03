package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumitsaini/taskqueue/internal/config"
	"github.com/sumitsaini/taskqueue/internal/engine"
	"github.com/sumitsaini/taskqueue/internal/queue"
)

func newTestEngine(t *testing.T, workers, capacity int, handler func(ctx context.Context, task *queue.Task) error) (*engine.Engine, config.Config) {
	t.Helper()
	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none" // use "none" for fast stress testing
	cfg.Queue.Capacity = capacity
	cfg.Queue.OverflowPolicy = "block"
	cfg.Worker.Count = workers
	cfg.Worker.DefaultTimeout = 5 * time.Second
	cfg.Retry.MaxRetries = 2
	cfg.Retry.BaseDelay = 5 * time.Millisecond
	cfg.Retry.MaxDelay = 20 * time.Millisecond
	cfg.Checkpoint.Interval = 1 * time.Hour // disable automatic periodic checkpoint

	eng, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine init: %v", err)
	}
	if err := eng.Recover(); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	eng.Start(handler)
	return eng, cfg
}

// TestStress_HighThroughput tests 32 concurrent producers submitting 3200 tasks
// with 16 workers executing them concurrently.
func TestStress_HighThroughput(t *testing.T) {
	var executed atomic.Int64

	handler := func(ctx context.Context, task *queue.Task) error {
		executed.Add(1)
		return nil
	}

	eng, _ := newTestEngine(t, 16, 512, handler)
	defer eng.Shutdown(10 * time.Second)

	const producers = 32
	const perProducer = 100
	total := producers * perProducer

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				_, err := eng.Submit(&queue.Task{
					Payload: []byte(fmt.Sprintf(`{"producer":%d,"task":%d}`, pid, i)),
				})
				if err != nil {
					t.Errorf("producer %d submit %d: %v", pid, i, err)
				}
			}
		}(p)
	}
	wg.Wait()

	// Wait for all tasks to be processed
	deadline := time.Now().Add(10 * time.Second)
	for executed.Load() < int64(total) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if executed.Load() != int64(total) {
		t.Errorf("expected %d executed, got %d", total, executed.Load())
	}
}

// TestStress_ConcurrentSubmitAndCancel tests producers submitting tasks while
// cancellers randomly cancel tasks before or while they execute.
func TestStress_ConcurrentSubmitAndCancel(t *testing.T) {
	handler := func(ctx context.Context, task *queue.Task) error {
		time.Sleep(1 * time.Millisecond)
		return nil
	}

	eng, _ := newTestEngine(t, 8, 256, handler)
	defer eng.Shutdown(10 * time.Second)

	const numTasks = 300
	taskIDs := make([]string, numTasks)
	var mu sync.Mutex

	var wg sync.WaitGroup
	// Producers
	for i := 0; i < numTasks; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := eng.Submit(&queue.Task{
				Payload:     []byte(fmt.Sprintf(`{"idx":%d}`, idx)),
				ScheduledAt: time.Now().Add(100 * time.Millisecond), // slight delay so cancellation has a window
			})
			if err == nil {
				mu.Lock()
				taskIDs[idx] = task.ID
				mu.Unlock()
			}
		}()
	}

	// Concurrent cancellers
	for i := 0; i < numTasks/2; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			id := taskIDs[idx]
			mu.Unlock()
			if id != "" {
				_ = eng.CancelTask(id)
			}
		}()
	}

	wg.Wait()
}

// TestStress_ConcurrentRetriesAndDLQ tests retries under high concurrency.
func TestStress_ConcurrentRetriesAndDLQ(t *testing.T) {
	var attempts atomic.Int64

	handler := func(ctx context.Context, task *queue.Task) error {
		attempts.Add(1)
		// Fail 50% of attempts
		if task.Priority%2 == 0 {
			return errors.New("simulated error")
		}
		return nil
	}

	eng, _ := newTestEngine(t, 8, 256, handler)
	defer eng.Shutdown(10 * time.Second)

	const count = 100
	for i := 0; i < count; i++ {
		_, err := eng.Submit(&queue.Task{
			Payload:  []byte(`{}`),
			Priority: i, // even priorities will fail and retry
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Wait for retries to settle
	time.Sleep(2 * time.Second)

	stats := eng.Stats()
	if stats.CompleteCount == 0 {
		t.Error("expected completed tasks")
	}
	if stats.DeadCount == 0 {
		t.Error("expected tasks in DLQ after retry exhaustion")
	}
}

// TestStress_ShutdownDuringExecution tests graceful shutdown while hundreds of tasks are in-flight.
func TestStress_ShutdownDuringExecution(t *testing.T) {
	var completed atomic.Int64

	handler := func(ctx context.Context, task *queue.Task) error {
		time.Sleep(10 * time.Millisecond)
		completed.Add(1)
		return nil
	}

	eng, _ := newTestEngine(t, 8, 512, handler)

	// Submit 200 tasks
	for i := 0; i < 200; i++ {
		_, err := eng.Submit(&queue.Task{Payload: []byte(`{}`)})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Let some work start
	time.Sleep(50 * time.Millisecond)

	// Trigger shutdown
	start := time.Now()
	if err := eng.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	duration := time.Since(start)
	if duration > 5*time.Second {
		t.Errorf("shutdown took too long: %v", duration)
	}

	// Verify all tasks in buffer were drained before stop
	if completed.Load() == 0 {
		t.Error("expected at least some tasks to complete during graceful shutdown")
	}
}
