package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func makeTask(id string) *Task {
	return &Task{
		ID:        id,
		Payload:   []byte(fmt.Sprintf("payload-%s", id)),
		State:     TaskStatePending,
		CreatedAt: time.Now(),
	}
}

// --- Basic FIFO behavior ---

func TestRingBuffer_EnqueueDequeue_FIFO(t *testing.T) {
	rb := NewRingBuffer(4, OverflowReject)

	tasks := make([]*Task, 4)
	for i := 0; i < 4; i++ {
		tasks[i] = makeTask(fmt.Sprintf("task-%d", i))
		if err := rb.Enqueue(tasks[i]); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	if rb.Len() != 4 {
		t.Fatalf("expected len 4, got %d", rb.Len())
	}

	for i := 0; i < 4; i++ {
		got, err := rb.Dequeue()
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		if got.ID != tasks[i].ID {
			t.Errorf("dequeue %d: expected %s, got %s", i, tasks[i].ID, got.ID)
		}
	}

	if rb.Len() != 0 {
		t.Fatalf("expected len 0, got %d", rb.Len())
	}
}

// --- Wraparound ---

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := NewRingBuffer(3, OverflowReject)

	// Fill and drain twice to force head/tail wraparound
	for round := 0; round < 3; round++ {
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("r%d-t%d", round, i)
			if err := rb.Enqueue(makeTask(id)); err != nil {
				t.Fatalf("round %d enqueue %d: %v", round, i, err)
			}
		}
		for i := 0; i < 3; i++ {
			got, err := rb.Dequeue()
			if err != nil {
				t.Fatalf("round %d dequeue %d: %v", round, i, err)
			}
			expected := fmt.Sprintf("r%d-t%d", round, i)
			if got.ID != expected {
				t.Errorf("round %d dequeue %d: expected %s, got %s", round, i, expected, got.ID)
			}
		}
	}
}

// --- Reject policy ---

func TestRingBuffer_RejectWhenFull(t *testing.T) {
	rb := NewRingBuffer(2, OverflowReject)

	rb.Enqueue(makeTask("1"))
	rb.Enqueue(makeTask("2"))

	err := rb.Enqueue(makeTask("3"))
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

// --- Block policy ---

func TestRingBuffer_BlockWhenFull(t *testing.T) {
	rb := NewRingBuffer(1, OverflowBlock)

	rb.Enqueue(makeTask("1"))

	// Enqueue should block until we dequeue
	done := make(chan struct{})
	go func() {
		rb.Enqueue(makeTask("2"))
		close(done)
	}()

	// Give the goroutine time to block
	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("enqueue should have blocked")
	default:
	}

	// Dequeue to unblock
	rb.Dequeue()

	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("enqueue did not unblock after dequeue")
	}
}

// --- Blocking dequeue ---

func TestRingBuffer_DequeueBlocks(t *testing.T) {
	rb := NewRingBuffer(4, OverflowReject)

	result := make(chan *Task, 1)
	go func() {
		task, _ := rb.Dequeue()
		result <- task
	}()

	// Give the goroutine time to block
	time.Sleep(50 * time.Millisecond)

	select {
	case <-result:
		t.Fatal("dequeue should have blocked on empty buffer")
	default:
	}

	// Enqueue to unblock
	expected := makeTask("unblock")
	rb.Enqueue(expected)

	select {
	case got := <-result:
		if got.ID != expected.ID {
			t.Errorf("expected %s, got %s", expected.ID, got.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("dequeue did not unblock after enqueue")
	}
}

// --- Close behavior ---

func TestRingBuffer_CloseUnblocksConsumers(t *testing.T) {
	rb := NewRingBuffer(4, OverflowReject)

	errCh := make(chan error, 1)
	go func() {
		_, err := rb.Dequeue()
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	rb.Close()

	select {
	case err := <-errCh:
		if err != ErrQueueClosed {
			t.Fatalf("expected ErrQueueClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock consumer")
	}
}

func TestRingBuffer_CloseDrainsRemaining(t *testing.T) {
	rb := NewRingBuffer(4, OverflowReject)

	rb.Enqueue(makeTask("1"))
	rb.Enqueue(makeTask("2"))
	rb.Close()

	// Should still be able to dequeue existing tasks
	t1, err := rb.Dequeue()
	if err != nil || t1.ID != "1" {
		t.Fatalf("expected task 1, got %v (err: %v)", t1, err)
	}

	t2, err := rb.Dequeue()
	if err != nil || t2.ID != "2" {
		t.Fatalf("expected task 2, got %v (err: %v)", t2, err)
	}

	// Now should get closed error
	_, err = rb.Dequeue()
	if err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed after drain, got %v", err)
	}
}

func TestRingBuffer_CloseRejectsEnqueue(t *testing.T) {
	rb := NewRingBuffer(4, OverflowReject)
	rb.Close()

	err := rb.Enqueue(makeTask("1"))
	if err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}

// --- TryDequeue ---

func TestRingBuffer_TryDequeue(t *testing.T) {
	rb := NewRingBuffer(4, OverflowReject)

	// Empty — should return false
	_, ok := rb.TryDequeue()
	if ok {
		t.Fatal("expected false on empty buffer")
	}

	rb.Enqueue(makeTask("1"))
	task, ok := rb.TryDequeue()
	if !ok || task.ID != "1" {
		t.Fatalf("expected task 1, got %v (ok: %v)", task, ok)
	}
}

// --- Concurrent stress test ---

func TestRingBuffer_ConcurrentProducersConsumers(t *testing.T) {
	const (
		numProducers = 8
		numConsumers = 8
		tasksPerProd = 1000
		totalTasks   = numProducers * tasksPerProd
	)

	rb := NewRingBuffer(64, OverflowBlock)

	var wg sync.WaitGroup

	// Track received task IDs
	received := make(chan string, totalTasks)

	// Start consumers
	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := rb.Dequeue()
				if err == ErrQueueClosed {
					return
				}
				received <- task.ID
			}
		}()
	}

	// Start producers
	var prodWg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		prodWg.Add(1)
		go func(pid int) {
			defer prodWg.Done()
			for i := 0; i < tasksPerProd; i++ {
				id := fmt.Sprintf("p%d-t%d", pid, i)
				rb.Enqueue(makeTask(id))
			}
		}(p)
	}

	// Wait for all producers, then close
	prodWg.Wait()
	rb.Close()

	// Wait for all consumers
	wg.Wait()
	close(received)

	// Verify we got all tasks
	seen := make(map[string]bool)
	for id := range received {
		if seen[id] {
			t.Errorf("duplicate task: %s", id)
		}
		seen[id] = true
	}

	if len(seen) != totalTasks {
		t.Errorf("expected %d tasks, got %d", totalTasks, len(seen))
	}
}
