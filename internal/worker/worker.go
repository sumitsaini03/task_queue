// Package worker implements a fixed-size pool of goroutines that
// pull tasks from the queue and execute them.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sumitsaini03/task_queue/internal/logger"
	"github.com/sumitsaini03/task_queue/internal/queue"
)

// HandlerFunc is the function signature for task execution.
// Implementations receive a context (for timeout/cancellation)
// and the task, returning an error on failure.
type HandlerFunc func(ctx context.Context, task *queue.Task) error

// Pool manages a set of worker goroutines that pull tasks from
// a RingBuffer and execute them via a HandlerFunc.
type Pool struct {
	numWorkers int
	handler    HandlerFunc
	buffer     *queue.RingBuffer
	wg         sync.WaitGroup

	// mu protects stats
	mu          sync.Mutex
	busyWorkers int
	processed   int64
	failed      int64
}

// NewPool creates a new worker pool.
func NewPool(numWorkers int, buffer *queue.RingBuffer, handler HandlerFunc) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		handler:    handler,
		buffer:     buffer,
	}
}

// Start launches numWorkers goroutines that pull and execute tasks.
// Each worker runs until the ring buffer is closed and drained, or
// the provided context is cancelled.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

// Stop waits for all workers to finish processing their current tasks.
// The caller should close the ring buffer first (or cancel the context)
// to signal workers to stop pulling new tasks.
func (p *Pool) Stop() {
	p.wg.Wait()
}

// Stats returns current pool statistics.
func (p *Pool) Stats() (busy int, processed, failed int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.busyWorkers, p.processed, p.failed
}

// worker is the main loop for a single worker goroutine.
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		// Check context before blocking on dequeue
		select {
		case <-ctx.Done():
			logger.Debug("worker: context cancelled, stopping", "worker_id", id)
			return
		default:
		}

		// Block until a task is available or buffer is closed
		task, err := p.buffer.Dequeue()
		if err != nil {
			// Buffer closed and drained — clean exit
			logger.Debug("worker: buffer closed, stopping", "worker_id", id)
			return
		}

		// Mark task as running
		p.mu.Lock()
		p.busyWorkers++
		p.mu.Unlock()

		task.ForceSetState(queue.TaskStateRunning)
		task.StartedAt = time.Now()

		// Execute with timeout if configured
		execCtx := ctx
		var cancel context.CancelFunc
		if task.Timeout > 0 {
			execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		} else {
			execCtx, cancel = context.WithCancel(ctx)
		}

		execErr := p.executeTask(execCtx, task)
		cancel()

		// Update stats
		p.mu.Lock()
		p.busyWorkers--
		p.processed++
		if execErr != nil {
			p.failed++
		}
		p.mu.Unlock()

		// Update task state based on result only if handler hasn't already transitioned it
		if task.GetState() == queue.TaskStateRunning {
			if execErr != nil {
				task.ForceSetState(queue.TaskStateFailed)
				task.LastError = execErr.Error()
				logger.Warn("worker: task execution failed", "worker_id", id, "task_id", task.ID, "error", execErr)
			} else {
				task.ForceSetState(queue.TaskStateCompleted)
				task.CompletedAt = time.Now()
			}
		}
	}
}

// executeTask runs the handler with panic recovery.
func (p *Pool) executeTask(ctx context.Context, task *queue.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return p.handler(ctx, task)
}
