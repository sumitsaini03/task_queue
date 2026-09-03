package queue

import (
	"context"
	"log"
	"time"
)

const (
	// defaultPollInterval is how often the scheduler checks the heap
	// when there are no tasks with known deadlines.
	defaultPollInterval = 500 * time.Millisecond
)

// Scheduler moves delayed tasks from the DelayedHeap into the RingBuffer
// when their ScheduledAt time arrives. It runs as a single background goroutine.
type Scheduler struct {
	heap   *DelayedHeap
	buffer *RingBuffer
	wake   chan struct{} // signal that a new task was added to the heap
}

// NewScheduler creates a scheduler that promotes tasks from heap to buffer.
func NewScheduler(heap *DelayedHeap, buffer *RingBuffer) *Scheduler {
	return &Scheduler{
		heap:   heap,
		buffer: buffer,
		wake:   make(chan struct{}, 1), // buffered to avoid blocking
	}
}

// NotifyNewTask signals the scheduler that a new delayed task was added,
// so it can recalculate its sleep timer. Non-blocking.
func (s *Scheduler) NotifyNewTask() {
	select {
	case s.wake <- struct{}{}:
	default:
		// Already has a pending wake signal
	}
}

// Run starts the scheduler loop. It blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		// Promote any tasks that are now ready
		ready := s.heap.PromoteReady(time.Now())
		for _, t := range ready {
			if t.GetState() == TaskStateCancelled {
				continue
			}
			if !t.ExpiresAt.IsZero() && time.Now().After(t.ExpiresAt) {
				t.ForceSetState(TaskStateExpired)
				continue
			}
			if err := s.buffer.Enqueue(t); err != nil {
				log.Printf("scheduler: failed to enqueue task %s: %v", t.ID, err)
				// If the buffer is closed, we should stop
				if err == ErrQueueClosed {
					return
				}
				// If the buffer is full (reject policy), push back to heap
				// with a small delay to avoid spin-looping
				t.ForceSetState(TaskStateScheduled)
				t.ScheduledAt = time.Now().Add(100 * time.Millisecond)
				s.heap.Push(t)
			}
		}

		// Calculate how long to sleep until the next task is due
		var timer *time.Timer
		if deadline, ok := s.heap.NextDeadline(); ok {
			delay := time.Until(deadline)
			if delay <= 0 {
				// Tasks are already due, loop immediately
				continue
			}
			timer = time.NewTimer(delay)
		} else {
			// No tasks in heap — use a long poll interval
			timer = time.NewTimer(defaultPollInterval)
		}

		// Wait for: timer expiry, new task signal, or shutdown
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.wake:
			timer.Stop()
			// Recalculate — a new task might be earlier than current timer
			continue
		case <-timer.C:
			// Timer expired, loop to promote
			continue
		}
	}
}
