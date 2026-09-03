package queue

import (
	"fmt"
	"testing"
	"time"
)

func TestDelayedHeap_Ordering(t *testing.T) {
	dh := NewDelayedHeap()

	now := time.Now()
	// Push in reverse order — heap should reorder
	for i := 4; i >= 0; i-- {
		dh.Push(&Task{
			ID:          fmt.Sprintf("task-%d", i),
			State:       TaskStateScheduled,
			ScheduledAt: now.Add(time.Duration(i) * time.Second),
		})
	}

	if dh.Len() != 5 {
		t.Fatalf("expected 5, got %d", dh.Len())
	}

	// Pop should return in chronological order
	for i := 0; i < 5; i++ {
		task := dh.Pop()
		expected := fmt.Sprintf("task-%d", i)
		if task.ID != expected {
			t.Errorf("pop %d: expected %s, got %s", i, expected, task.ID)
		}
	}
}

func TestDelayedHeap_Peek(t *testing.T) {
	dh := NewDelayedHeap()

	if dh.Peek() != nil {
		t.Fatal("peek on empty heap should return nil")
	}

	now := time.Now()
	dh.Push(&Task{ID: "later", ScheduledAt: now.Add(10 * time.Second)})
	dh.Push(&Task{ID: "sooner", ScheduledAt: now.Add(1 * time.Second)})

	peeked := dh.Peek()
	if peeked.ID != "sooner" {
		t.Errorf("expected sooner, got %s", peeked.ID)
	}

	// Peek should not remove
	if dh.Len() != 2 {
		t.Fatalf("peek should not remove, len: %d", dh.Len())
	}
}

func TestDelayedHeap_PromoteReady(t *testing.T) {
	dh := NewDelayedHeap()

	now := time.Now()
	dh.Push(&Task{ID: "past-1", ScheduledAt: now.Add(-2 * time.Second)})
	dh.Push(&Task{ID: "past-2", ScheduledAt: now.Add(-1 * time.Second)})
	dh.Push(&Task{ID: "future", ScheduledAt: now.Add(10 * time.Second)})

	ready := dh.PromoteReady(now)

	if len(ready) != 2 {
		t.Fatalf("expected 2 ready tasks, got %d", len(ready))
	}

	// Should be in order (earliest first)
	if ready[0].ID != "past-1" {
		t.Errorf("expected past-1 first, got %s", ready[0].ID)
	}
	if ready[1].ID != "past-2" {
		t.Errorf("expected past-2 second, got %s", ready[1].ID)
	}

	// All promoted tasks should be pending
	for _, task := range ready {
		if task.State != TaskStatePending {
			t.Errorf("task %s: expected pending, got %s", task.ID, task.State)
		}
	}

	// Only the future task should remain
	if dh.Len() != 1 {
		t.Fatalf("expected 1 remaining, got %d", dh.Len())
	}
}

func TestDelayedHeap_NextDeadline(t *testing.T) {
	dh := NewDelayedHeap()

	_, ok := dh.NextDeadline()
	if ok {
		t.Fatal("empty heap should return !ok")
	}

	now := time.Now()
	target := now.Add(5 * time.Second)
	dh.Push(&Task{ID: "t1", ScheduledAt: target})

	deadline, ok := dh.NextDeadline()
	if !ok {
		t.Fatal("expected ok")
	}
	if !deadline.Equal(target) {
		t.Errorf("expected %v, got %v", target, deadline)
	}
}

func TestDelayedHeap_Remove(t *testing.T) {
	dh := NewDelayedHeap()

	now := time.Now()
	t1 := &Task{ID: "t1", ScheduledAt: now.Add(1 * time.Second)}
	t2 := &Task{ID: "t2", ScheduledAt: now.Add(2 * time.Second)}
	t3 := &Task{ID: "t3", ScheduledAt: now.Add(3 * time.Second)}

	dh.Push(t1)
	dh.Push(t2)
	dh.Push(t3)

	// Remove the middle one
	if !dh.Remove(t2) {
		t.Fatal("remove should succeed")
	}

	if dh.Len() != 2 {
		t.Fatalf("expected 2, got %d", dh.Len())
	}

	// Pop should give t1, t3
	got := dh.Pop()
	if got.ID != "t1" {
		t.Errorf("expected t1, got %s", got.ID)
	}
	got = dh.Pop()
	if got.ID != "t3" {
		t.Errorf("expected t3, got %s", got.ID)
	}
}

func TestDelayedHeap_PopEmpty(t *testing.T) {
	dh := NewDelayedHeap()
	if dh.Pop() != nil {
		t.Fatal("pop on empty heap should return nil")
	}
}
