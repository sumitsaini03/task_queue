package queue

import (
	"container/heap"
	"sync"
	"time"
)

// delayedHeapInner implements heap.Interface for []*Task,
// ordered by ScheduledAt (earliest first).
type delayedHeapInner []*Task

func (h delayedHeapInner) Len() int { return len(h) }

func (h delayedHeapInner) Less(i, j int) bool {
	return h[i].ScheduledAt.Before(h[j].ScheduledAt)
}

func (h delayedHeapInner) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *delayedHeapInner) Push(x any) {
	t := x.(*Task)
	t.heapIndex = len(*h)
	*h = append(*h, t)
}

func (h *delayedHeapInner) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil // avoid memory leak
	t.heapIndex = -1
	*h = old[:n-1]
	return t
}

// DelayedHeap is a thread-safe min-heap of tasks ordered by ScheduledAt.
// A background scheduler goroutine calls PromoteReady to move due tasks
// into the ring buffer.
type DelayedHeap struct {
	mu    sync.Mutex
	inner delayedHeapInner
}

// NewDelayedHeap creates an empty delayed task heap.
func NewDelayedHeap() *DelayedHeap {
	return &DelayedHeap{}
}

// Push adds a delayed task to the heap.
func (dh *DelayedHeap) Push(t *Task) {
	dh.mu.Lock()
	defer dh.mu.Unlock()
	heap.Push(&dh.inner, t)
}

// Peek returns the earliest-scheduled task without removing it.
// Returns nil if the heap is empty.
func (dh *DelayedHeap) Peek() *Task {
	dh.mu.Lock()
	defer dh.mu.Unlock()
	if len(dh.inner) == 0 {
		return nil
	}
	return dh.inner[0]
}

// Pop removes and returns the earliest-scheduled task.
// Returns nil if the heap is empty.
func (dh *DelayedHeap) Pop() *Task {
	dh.mu.Lock()
	defer dh.mu.Unlock()
	if len(dh.inner) == 0 {
		return nil
	}
	return heap.Pop(&dh.inner).(*Task)
}

// Remove removes a specific task from the heap by its heap index.
// Returns true if the task was found and removed.
func (dh *DelayedHeap) Remove(t *Task) bool {
	dh.mu.Lock()
	defer dh.mu.Unlock()
	if t.heapIndex < 0 || t.heapIndex >= len(dh.inner) {
		return false
	}
	heap.Remove(&dh.inner, t.heapIndex)
	return true
}

// Len returns the number of tasks in the heap.
func (dh *DelayedHeap) Len() int {
	dh.mu.Lock()
	defer dh.mu.Unlock()
	return len(dh.inner)
}

// PromoteReady removes all tasks whose ScheduledAt <= now from the heap
// and returns them. The caller (scheduler goroutine) then enqueues them
// into the ring buffer.
func (dh *DelayedHeap) PromoteReady(now time.Time) []*Task {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	var ready []*Task
	for len(dh.inner) > 0 && !dh.inner[0].ScheduledAt.After(now) {
		t := heap.Pop(&dh.inner).(*Task)
		t.ForceSetState(TaskStatePending)
		ready = append(ready, t)
	}
	return ready
}

// NextDeadline returns the ScheduledAt time of the earliest task,
// or zero time if the heap is empty. Used by the scheduler to set
// its sleep timer.
func (dh *DelayedHeap) NextDeadline() (time.Time, bool) {
	dh.mu.Lock()
	defer dh.mu.Unlock()
	if len(dh.inner) == 0 {
		return time.Time{}, false
	}
	return dh.inner[0].ScheduledAt, true
}
