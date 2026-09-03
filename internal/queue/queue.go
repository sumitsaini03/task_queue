// Package queue implements the core task queue data structures:
// a bounded ring buffer for immediate FIFO tasks and a min-heap
// for scheduled/delayed tasks.
package queue

import (
	"errors"
	"sync"
	"time"
)

// Errors returned by queue operations.
var (
	ErrQueueFull         = errors.New("queue: buffer is full")
	ErrQueueClosed       = errors.New("queue: buffer is closed")
	ErrInvalidTransition = errors.New("queue: invalid task state transition")
)

// TaskState represents the lifecycle state of a task.
type TaskState int

const (
	TaskStatePending   TaskState = iota // Waiting in queue
	TaskStateScheduled                  // Delayed, sitting in the heap
	TaskStateRunning                    // Picked up by a worker
	TaskStateCompleted                  // Finished successfully
	TaskStateFailed                     // Non-retryable or final failure
	TaskStateRetrying                   // Failed, retry in progress
	TaskStateDead                       // Exhausted retries, in DLQ
	TaskStateCancelled                  // Cancelled by user
	TaskStateExpired                    // Expired before execution
)

func (s TaskState) String() string {
	switch s {
	case TaskStatePending:
		return "pending"
	case TaskStateScheduled:
		return "scheduled"
	case TaskStateRunning:
		return "running"
	case TaskStateCompleted:
		return "completed"
	case TaskStateFailed:
		return "failed"
	case TaskStateRetrying:
		return "retrying"
	case TaskStateDead:
		return "dead"
	case TaskStateCancelled:
		return "cancelled"
	case TaskStateExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// CanTransitionTo reports whether transitioning from s to next is valid.
func (s TaskState) CanTransitionTo(next TaskState) bool {
	switch s {
	case TaskStatePending:
		return next == TaskStateRunning || next == TaskStateCancelled || next == TaskStateExpired
	case TaskStateScheduled:
		return next == TaskStatePending || next == TaskStateCancelled || next == TaskStateExpired
	case TaskStateRunning:
		return next == TaskStateCompleted || next == TaskStateFailed || next == TaskStateRetrying || next == TaskStateDead
	case TaskStateRetrying:
		return next == TaskStateScheduled || next == TaskStatePending || next == TaskStateDead || next == TaskStateFailed
	default:
		// Completed, Failed, Dead, Cancelled, Expired are terminal
		return false
	}
}

// Task represents a unit of work in the queue.
type Task struct {
	mu             sync.RWMutex  `json:"-"`
	ID             string        `json:"id"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	Payload        []byte        `json:"payload"`
	State          TaskState     `json:"state"`
	Priority       int           `json:"priority"`
	CreatedAt      time.Time     `json:"created_at"`
	ScheduledAt    time.Time     `json:"scheduled_at,omitempty"` // Zero means immediate
	StartedAt      time.Time     `json:"started_at,omitempty"`
	CompletedAt    time.Time     `json:"completed_at,omitempty"`
	ExpiresAt      time.Time     `json:"expires_at,omitempty"`
	RetryCount     int           `json:"retry_count"`
	MaxRetries     int           `json:"max_retries"`
	LastError      string        `json:"last_error,omitempty"`
	Timeout        time.Duration `json:"timeout,omitempty"`

	// heapIndex is maintained by the heap for efficient removal.
	heapIndex int
}

// GetState returns the task state safely under lock.
func (t *Task) GetState() TaskState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.State
}

// SetState validates the transition and updates state under lock.
func (t *Task) SetState(next TaskState) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.State.CanTransitionTo(next) {
		return errors.New("cannot transition from " + t.State.String() + " to " + next.String())
	}
	t.State = next
	return nil
}

// ForceSetState sets state without transition validation (used during recovery/replay).
func (t *Task) ForceSetState(next TaskState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.State = next
}

// OverflowPolicy determines behavior when the ring buffer is full.
type OverflowPolicy int

const (
	OverflowReject OverflowPolicy = iota // Return ErrQueueFull immediately
	OverflowBlock                        // Block until space is available
)

// RingBuffer is a bounded, thread-safe FIFO queue backed by a fixed-size array.
// Workers block on Dequeue when empty via sync.Cond (see ADR 003).
// Producers either block or get rejected when full, depending on OverflowPolicy.
type RingBuffer struct {
	mu   sync.Mutex
	cond *sync.Cond // wakes blocked consumers AND blocked producers

	buf    []*Task
	head   int // index of the next element to dequeue
	tail   int // index of the next empty slot
	size   int // current number of elements
	cap    int // maximum capacity
	closed bool

	policy OverflowPolicy
}

// NewRingBuffer creates a ring buffer with the given capacity and overflow policy.
func NewRingBuffer(capacity int, policy OverflowPolicy) *RingBuffer {
	rb := &RingBuffer{
		buf:    make([]*Task, capacity),
		cap:    capacity,
		policy: policy,
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// Enqueue adds a task to the back of the ring buffer.
// If the buffer is full:
//   - OverflowReject: returns ErrQueueFull immediately
//   - OverflowBlock: blocks until space is available or the buffer is closed
//
// Returns ErrQueueClosed if the buffer has been closed.
func (rb *RingBuffer) Enqueue(t *Task) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Handle full buffer according to policy
	for rb.size == rb.cap && !rb.closed {
		if rb.policy == OverflowReject {
			return ErrQueueFull
		}
		// OverflowBlock: wait until space opens up
		rb.cond.Wait()
	}

	if rb.closed {
		return ErrQueueClosed
	}

	rb.buf[rb.tail] = t
	rb.tail = (rb.tail + 1) % rb.cap
	rb.size++

	// Wake one blocked consumer (or producer checking space)
	rb.cond.Signal()
	return nil
}

// Dequeue removes and returns the task at the front of the ring buffer.
// Blocks until a task is available or the buffer is closed and drained.
// Returns (nil, ErrQueueClosed) when the buffer is closed and empty.
func (rb *RingBuffer) Dequeue() (*Task, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Block until there's something to dequeue or we're closed
	for rb.size == 0 && !rb.closed {
		rb.cond.Wait()
	}

	// Closed and empty — clean shutdown
	if rb.size == 0 && rb.closed {
		return nil, ErrQueueClosed
	}

	t := rb.buf[rb.head]
	rb.buf[rb.head] = nil // avoid memory leak
	rb.head = (rb.head + 1) % rb.cap
	rb.size--

	// Wake a blocked producer (or another consumer)
	rb.cond.Signal()
	return t, nil
}

// TryDequeue attempts a non-blocking dequeue. Returns (nil, false) if empty.
func (rb *RingBuffer) TryDequeue() (*Task, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.size == 0 {
		return nil, false
	}

	t := rb.buf[rb.head]
	rb.buf[rb.head] = nil
	rb.head = (rb.head + 1) % rb.cap
	rb.size--

	rb.cond.Signal()
	return t, true
}

// Len returns the current number of tasks in the buffer.
func (rb *RingBuffer) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.size
}

// Cap returns the buffer's maximum capacity.
func (rb *RingBuffer) Cap() int {
	return rb.cap // immutable, no lock needed
}

// IsFull returns true if the buffer is at capacity.
func (rb *RingBuffer) IsFull() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.size == rb.cap
}

// Close signals that no more tasks will be enqueued.
// Blocked consumers will drain remaining tasks and then receive ErrQueueClosed.
// Blocked producers will receive ErrQueueClosed.
func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.closed = true
	rb.cond.Broadcast() // wake ALL blocked goroutines
}
