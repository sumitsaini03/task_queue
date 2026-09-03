package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumitsaini/taskqueue/internal/queue"
)

func makeTask(id string) *queue.Task {
	return &queue.Task{
		ID:        id,
		Payload:   []byte(fmt.Sprintf("payload-%s", id)),
		State:     queue.TaskStatePending,
		CreatedAt: time.Now(),
	}
}

func TestPool_BasicExecution(t *testing.T) {
	rb := queue.NewRingBuffer(16, queue.OverflowBlock)

	var count atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		count.Add(1)
		return nil
	}

	pool := NewPool(4, rb, handler)
	ctx := context.Background()
	pool.Start(ctx)

	// Enqueue tasks in a goroutine (OverflowBlock may block if buffer full)
	go func() {
		for i := 0; i < 100; i++ {
			rb.Enqueue(makeTask(fmt.Sprintf("task-%d", i)))
		}
		rb.Close()
	}()

	// Wait for workers to drain all tasks
	pool.Stop()

	if count.Load() != 100 {
		t.Errorf("expected 100 tasks processed, got %d", count.Load())
	}
}

func TestPool_Stats(t *testing.T) {
	rb := queue.NewRingBuffer(16, queue.OverflowReject)

	handler := func(ctx context.Context, task *queue.Task) error {
		if task.ID == "fail" {
			return errors.New("injected failure")
		}
		return nil
	}

	pool := NewPool(2, rb, handler)
	ctx := context.Background()
	pool.Start(ctx)

	rb.Enqueue(makeTask("ok-1"))
	rb.Enqueue(makeTask("fail"))
	rb.Enqueue(makeTask("ok-2"))

	rb.Close()
	pool.Stop()

	_, processed, failed := pool.Stats()
	if processed != 3 {
		t.Errorf("expected 3 processed, got %d", processed)
	}
	if failed != 1 {
		t.Errorf("expected 1 failed, got %d", failed)
	}
}

func TestPool_PanicRecovery(t *testing.T) {
	rb := queue.NewRingBuffer(16, queue.OverflowReject)

	handler := func(ctx context.Context, task *queue.Task) error {
		if task.ID == "panic" {
			panic("boom")
		}
		return nil
	}

	pool := NewPool(2, rb, handler)
	ctx := context.Background()
	pool.Start(ctx)

	rb.Enqueue(makeTask("panic"))
	rb.Enqueue(makeTask("after-panic"))

	rb.Close()
	pool.Stop()

	_, processed, failed := pool.Stats()
	if processed != 2 {
		t.Errorf("expected 2 processed (panic + after), got %d", processed)
	}
	if failed != 1 {
		t.Errorf("expected 1 failed (from panic), got %d", failed)
	}
}

func TestPool_TaskTimeout(t *testing.T) {
	rb := queue.NewRingBuffer(16, queue.OverflowReject)

	handler := func(ctx context.Context, task *queue.Task) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	pool := NewPool(1, rb, handler)
	ctx := context.Background()
	pool.Start(ctx)

	task := makeTask("slow")
	task.Timeout = 100 * time.Millisecond
	rb.Enqueue(task)

	rb.Close()
	pool.Stop()

	if task.State != queue.TaskStateFailed {
		t.Errorf("expected failed state, got %s", task.State)
	}
}

func TestPool_ContextCancellation(t *testing.T) {
	rb := queue.NewRingBuffer(16, queue.OverflowReject)

	started := make(chan struct{})
	handler := func(ctx context.Context, task *queue.Task) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool := NewPool(1, rb, handler)
	pool.Start(ctx)

	rb.Enqueue(makeTask("blocking"))

	// Wait for worker to pick up the task
	<-started

	// Cancel context — worker should stop
	cancel()
	rb.Close()

	done := make(chan struct{})
	go func() {
		pool.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("pool.Stop() did not return after context cancellation")
	}
}

func TestPool_ConcurrentExecution(t *testing.T) {
	rb := queue.NewRingBuffer(256, queue.OverflowBlock)

	var maxConcurrent atomic.Int64
	var current atomic.Int64

	handler := func(ctx context.Context, task *queue.Task) error {
		c := current.Add(1)
		// Track max concurrency
		for {
			old := maxConcurrent.Load()
			if c <= old || maxConcurrent.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(time.Millisecond) // simulate work
		current.Add(-1)
		return nil
	}

	numWorkers := 8
	pool := NewPool(numWorkers, rb, handler)
	ctx := context.Background()
	pool.Start(ctx)

	for i := 0; i < 200; i++ {
		rb.Enqueue(makeTask(fmt.Sprintf("t-%d", i)))
	}

	rb.Close()
	pool.Stop()

	mc := maxConcurrent.Load()
	if mc < 2 {
		t.Errorf("expected concurrent execution (max concurrent: %d)", mc)
	}
	if mc > int64(numWorkers) {
		t.Errorf("concurrency exceeded pool size: %d > %d", mc, numWorkers)
	}
}
