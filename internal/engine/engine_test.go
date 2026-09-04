package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumitsaini03/task_queue/internal/config"
	"github.com/sumitsaini03/task_queue/internal/queue"
	"github.com/sumitsaini03/task_queue/internal/worker"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "every"
	cfg.Queue.Capacity = 128
	cfg.Queue.OverflowPolicy = "reject"
	cfg.Worker.Count = 4
	cfg.Worker.DefaultTimeout = 5 * time.Second
	cfg.Retry.MaxRetries = 3
	cfg.Retry.BaseDelay = 10 * time.Millisecond
	cfg.Retry.MaxDelay = 100 * time.Millisecond
	cfg.Checkpoint.Interval = 1 * time.Hour // don't auto-checkpoint in tests
	return cfg
}

func startEngine(t *testing.T, handler worker.HandlerFunc) *Engine {
	t.Helper()
	cfg := testConfig(t)
	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := eng.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	eng.Start(handler)
	return eng
}

// --- Basic submit and execute ---

func TestEngine_SubmitAndExecute(t *testing.T) {
	var executed atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		executed.Add(1)
		return nil
	}

	eng := startEngine(t, handler)
	defer eng.Shutdown(5 * time.Second)

	for i := 0; i < 50; i++ {
		task, err := eng.Submit(&queue.Task{
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		if task.ID == "" {
			t.Fatalf("submit %d: empty ID", i)
		}
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	if executed.Load() != 50 {
		t.Errorf("expected 50 executed, got %d", executed.Load())
	}
}

// --- Idempotency ---

func TestEngine_Idempotency(t *testing.T) {
	handler := func(ctx context.Context, task *queue.Task) error { return nil }
	eng := startEngine(t, handler)
	defer eng.Shutdown(5 * time.Second)

	t1, err := eng.Submit(&queue.Task{
		Payload:        []byte(`{}`),
		IdempotencyKey: "unique-key-1",
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Same key — should return existing task and ErrDuplicateIdempotencyKey
	t2, err := eng.Submit(&queue.Task{
		Payload:        []byte(`{"different":"data"}`),
		IdempotencyKey: "unique-key-1",
	})
	if !errors.Is(err, ErrDuplicateIdempotencyKey) {
		t.Fatalf("expected ErrDuplicateIdempotencyKey, got: %v", err)
	}
	if t1.ID != t2.ID {
		t.Errorf("idempotency failed: %s != %s", t1.ID, t2.ID)
	}
}

// --- Cancel ---

func TestEngine_Cancel(t *testing.T) {
	cfg := testConfig(t)
	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := eng.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer eng.Shutdown(5 * time.Second)
	// Don't start workers — task stays pending in queue

	task, err := eng.Submit(&queue.Task{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if err := eng.CancelTask(task.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	got, _ := eng.GetTask(task.ID)
	if got.State != queue.TaskStateCancelled {
		t.Errorf("expected cancelled, got %s", got.State)
	}

	// Cancel already cancelled — should fail
	if err := eng.CancelTask(task.ID); err == nil {
		t.Error("expected error cancelling already cancelled task")
	}
}

// --- Retry and DLQ ---

func TestEngine_RetryAndDLQ(t *testing.T) {
	var attempts atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		attempts.Add(1)
		return errors.New("always fails")
	}

	cfg := testConfig(t)
	cfg.Retry.MaxRetries = 2
	cfg.Retry.BaseDelay = 10 * time.Millisecond
	cfg.Retry.MaxDelay = 50 * time.Millisecond

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := eng.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	eng.Start(handler)
	defer eng.Shutdown(5 * time.Second)

	_, err = eng.Submit(&queue.Task{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Wait for retries + DLQ
	time.Sleep(2 * time.Second)

	// Should have been attempted: 1 initial + 2 retries = 3
	got := attempts.Load()
	if got < 3 {
		t.Errorf("expected at least 3 attempts, got %d", got)
	}

	// Should be in DLQ
	dlq, total := eng.ListDLQ(10, 0)
	if total == 0 {
		t.Error("expected task in DLQ")
	}
	if len(dlq) > 0 && dlq[0].State != queue.TaskStateDead {
		t.Errorf("DLQ task state: expected dead, got %s", dlq[0].State)
	}
}

// --- Recovery: tasks survive restart ---

func TestEngine_Recovery(t *testing.T) {
	cfg := testConfig(t)

	// Phase 1: submit tasks, then shut down
	eng1, err := New(cfg)
	if err != nil {
		t.Fatalf("new eng1: %v", err)
	}
	if err := eng1.Recover(); err != nil {
		t.Fatalf("recover eng1: %v", err)
	}
	// Don't start workers — tasks stay pending

	for i := 0; i < 10; i++ {
		_, err := eng1.Submit(&queue.Task{
			Payload: []byte(fmt.Sprintf(`{"id":%d}`, i)),
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Close WAL
	if err := eng1.walLog.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	// Phase 2: recover from WAL
	eng2, err := New(cfg)
	if err != nil {
		t.Fatalf("new eng2: %v", err)
	}
	if err := eng2.Recover(); err != nil {
		t.Fatalf("recover eng2: %v", err)
	}
	defer eng2.Shutdown(5 * time.Second)

	// Should have 10 tasks
	eng2.mu.RLock()
	taskCount := len(eng2.tasks)
	eng2.mu.RUnlock()

	if taskCount != 10 {
		t.Errorf("expected 10 recovered tasks, got %d", taskCount)
	}

	// Queue should have pending tasks
	queueLen := eng2.buffer.Len()
	if queueLen != 10 {
		t.Errorf("expected 10 tasks in queue, got %d", queueLen)
	}
}

// --- Recovery: completed tasks not re-queued ---

func TestEngine_RecoveryCompletedNotRequeued(t *testing.T) {
	cfg := testConfig(t)

	var executed atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		executed.Add(1)
		return nil
	}

	// Phase 1: submit and process
	eng1, _ := New(cfg)
	eng1.Recover()
	eng1.Start(handler)

	for i := 0; i < 5; i++ {
		eng1.Submit(&queue.Task{Payload: []byte(`{}`)})
	}
	time.Sleep(500 * time.Millisecond)
	eng1.Shutdown(5 * time.Second)

	// Phase 2: recover — completed tasks should not be in queue
	eng2, _ := New(cfg)
	eng2.Recover()

	queueLen := eng2.buffer.Len()
	if queueLen != 0 {
		t.Errorf("expected 0 pending tasks after recovery, got %d", queueLen)
	}
}

// --- Delayed task submission ---

func TestEngine_DelayedTask(t *testing.T) {
	var executed atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		executed.Add(1)
		return nil
	}

	eng := startEngine(t, handler)
	defer eng.Shutdown(5 * time.Second)

	eng.Submit(&queue.Task{
		Payload:     []byte(`{}`),
		ScheduledAt: time.Now().Add(200 * time.Millisecond),
	})

	// Should not execute immediately
	time.Sleep(50 * time.Millisecond)
	if executed.Load() != 0 {
		t.Error("task executed before scheduled time")
	}

	// Should execute after delay
	time.Sleep(500 * time.Millisecond)
	if executed.Load() != 1 {
		t.Errorf("expected 1 executed after delay, got %d", executed.Load())
	}
}

// --- Concurrent submit ---

func TestEngine_ConcurrentSubmit(t *testing.T) {
	var executed atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		executed.Add(1)
		return nil
	}

	cfg := testConfig(t)
	cfg.Queue.Capacity = 256
	cfg.Queue.OverflowPolicy = "block" // block producers when full so no tasks are rejected
	cfg.Worker.Count = 8

	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := eng.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	eng.Start(handler)
	defer eng.Shutdown(5 * time.Second)

	const producers = 8
	const perProducer = 50
	total := producers * perProducer

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				_, err := eng.Submit(&queue.Task{
					Payload: []byte(fmt.Sprintf(`{"p":%d,"i":%d}`, pid, i)),
				})
				if err != nil {
					t.Errorf("producer %d submit %d: %v", pid, i, err)
				}
			}
		}(p)
	}
	wg.Wait()

	// Wait for execution with timeout
	deadline := time.Now().Add(5 * time.Second)
	for executed.Load() < int64(total) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if executed.Load() != int64(total) {
		t.Errorf("expected %d executed, got %d", total, executed.Load())
	}
}

// --- Stats ---

func TestEngine_Stats(t *testing.T) {
	handler := func(ctx context.Context, task *queue.Task) error { return nil }
	eng := startEngine(t, handler)
	defer eng.Shutdown(5 * time.Second)

	for i := 0; i < 10; i++ {
		eng.Submit(&queue.Task{Payload: []byte(`{}`)})
	}
	time.Sleep(500 * time.Millisecond)

	stats := eng.Stats()
	if stats.EnqueueCount != 10 {
		t.Errorf("expected 10 enqueues, got %d", stats.EnqueueCount)
	}
}

// --- Task not found ---

func TestEngine_GetTaskNotFound(t *testing.T) {
	handler := func(ctx context.Context, task *queue.Task) error { return nil }
	eng := startEngine(t, handler)
	defer eng.Shutdown(5 * time.Second)

	_, ok := eng.GetTask("nonexistent")
	if ok {
		t.Error("expected not found")
	}
}
