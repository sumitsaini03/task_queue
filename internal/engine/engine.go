// Package engine is the central coordinator that integrates the WAL,
// queue, workers, retry manager, and scheduler into a single cohesive
// task processing system.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sumitsaini03/task_queue/internal/config"
	"github.com/sumitsaini03/task_queue/internal/logger"
	"github.com/sumitsaini03/task_queue/internal/metrics"
	"github.com/sumitsaini03/task_queue/internal/queue"
	"github.com/sumitsaini03/task_queue/internal/retry"
	"github.com/sumitsaini03/task_queue/internal/wal"
	"github.com/sumitsaini03/task_queue/internal/worker"
)

// Engine errors
var (
	ErrDuplicateIdempotencyKey = errors.New("engine: duplicate idempotency key")
)

// Engine is the central task queue coordinator.
type Engine struct {
	cfg             config.Config
	walLog          *wal.WAL
	buffer          *queue.RingBuffer
	heap            *queue.DelayedHeap
	scheduler       *queue.Scheduler
	pool            *worker.Pool
	retryCfg        retry.Config
	metricCollector *metrics.Metrics

	// Task registry: all known tasks indexed by ID
	mu    sync.RWMutex
	tasks map[string]*queue.Task

	// Idempotency key dedup
	idempKeys map[string]string // idempotency_key -> task_id

	// DLQ
	dlqMu sync.RWMutex
	dlq   []*queue.Task

	// Metrics
	metricsMu     sync.Mutex
	enqueueCount  int64
	completeCount int64
	failCount     int64
	retryCount    int64
	deadCount     int64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	walDir string
}

// New creates a new Engine from config. Does NOT start processing;
// call Recover() then Start().
func New(cfg config.Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("engine: invalid config: %w", err)
	}

	var overflowPolicy queue.OverflowPolicy
	if cfg.Queue.OverflowPolicy == "block" {
		overflowPolicy = queue.OverflowBlock
	}

	buffer := queue.NewRingBuffer(cfg.Queue.Capacity, overflowPolicy)
	delayedHeap := queue.NewDelayedHeap()
	scheduler := queue.NewScheduler(delayedHeap, buffer)

	retryCfg := retry.Config{
		MaxRetries:  cfg.Retry.MaxRetries,
		BaseDelay:   cfg.Retry.BaseDelay,
		MaxDelay:    cfg.Retry.MaxDelay,
		Multiplier:  cfg.Retry.Multiplier,
		JitterRatio: cfg.Retry.JitterRatio,
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		cfg:             cfg,
		buffer:          buffer,
		heap:            delayedHeap,
		scheduler:       scheduler,
		retryCfg:        retryCfg,
		metricCollector: metrics.New(),
		tasks:           make(map[string]*queue.Task),
		idempKeys:       make(map[string]string),
		ctx:             ctx,
		cancel:          cancel,
		walDir:          cfg.WAL.Dir,
	}

	return e, nil
}

// Recover opens the WAL, replays snapshot + WAL entries, and rebuilds
// in-memory state. Must be called before Start().
func (e *Engine) Recover() error {
	walPath := e.walDir + "/wal.bin"

	// 1. Load snapshot if it exists
	snap, err := wal.ReadSnapshot(e.walDir)
	if err != nil {
		return fmt.Errorf("engine: read snapshot: %w", err)
	}

	var snapSeq uint64
	if snap != nil {
		snapSeq = snap.LastSeq
		for _, st := range snap.Tasks {
			task := snapshotTaskToTask(st)
			e.tasks[task.ID] = task
			if task.IdempotencyKey != "" {
				e.idempKeys[task.IdempotencyKey] = task.ID
			}
		}
		logger.Info("engine: loaded snapshot", "task_count", len(snap.Tasks), "last_seq", snapSeq)
	}

	// 2. Replay WAL entries after snapshot
	lastSeq, err := wal.Replay(walPath, func(entry *wal.Entry) error {
		if entry.Seq <= snapSeq {
			return nil // already covered by snapshot
		}
		return e.applyEntry(entry)
	})
	if err != nil {
		return fmt.Errorf("engine: replay WAL: %w", err)
	}

	// 3. Open WAL for appending
	syncPolicy := wal.ParseSyncPolicy(e.cfg.WAL.SyncPolicy)
	w, err := wal.Open(walPath, syncPolicy)
	if err != nil {
		return fmt.Errorf("engine: open WAL: %w", err)
	}
	if lastSeq > 0 {
		w.SetSeq(lastSeq)
	}
	e.walLog = w

	// 4. Rebuild queue state from recovered tasks
	e.rebuildQueues()

	logger.Info("engine: recovery complete", "recovered_tasks", len(e.tasks), "last_wal_seq", lastSeq)
	return nil
}

// Start begins processing: launches workers, scheduler, and checkpoint goroutine.
func (e *Engine) Start(handler worker.HandlerFunc) {
	// Wrap handler to integrate with WAL and retry
	wrappedHandler := e.wrapHandler(handler)
	e.pool = worker.NewPool(e.cfg.Worker.Count, e.buffer, wrappedHandler)

	e.pool.Start(e.ctx)

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.scheduler.Run(e.ctx)
	}()

	// Start checkpoint goroutine
	if e.cfg.Checkpoint.Interval > 0 {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.checkpointLoop()
		}()
	}
}

// Shutdown performs graceful shutdown in the correct order:
// 1. Stop accepting new work (caller should stop HTTP first)
// 2. Close the ring buffer (stops scheduler from enqueueing, workers drain)
// 3. Wait for workers to finish in-flight tasks
// 4. Final checkpoint
// 5. Close WAL
func (e *Engine) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Signal scheduler to stop
	e.cancel()

	// Close the buffer — workers will drain remaining tasks then stop
	e.buffer.Close()

	// Wait for workers
	if e.pool != nil {
		e.pool.Stop()
	}

	// Wait for scheduler and checkpoint goroutines
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("engine: shutdown timeout")
	}

	// Final checkpoint
	if e.walLog != nil {
		if err := e.checkpoint(); err != nil {
			logger.Error("engine: final checkpoint failed", "error", err)
		}
	}

	// Close WAL
	if e.walLog != nil {
		if err := e.walLog.Close(); err != nil {
			return fmt.Errorf("engine: close WAL: %w", err)
		}
	}

	return nil
}

// Submit enqueues a new task. The task is durably written to the WAL
// before being placed in the queue. Returns the task with its assigned ID.
func (e *Engine) Submit(task *queue.Task) (*queue.Task, error) {
	enqueueStart := time.Now()
	e.mu.Lock()

	// Idempotency check
	if task.IdempotencyKey != "" {
		if existingID, ok := e.idempKeys[task.IdempotencyKey]; ok {
			existing := e.tasks[existingID]
			e.mu.Unlock()
			return existing, ErrDuplicateIdempotencyKey
		}
	}

	// Generate ID if not set
	if task.ID == "" {
		task.ID = generateID()
	}
	task.CreatedAt = time.Now()

	// Determine initial state
	if !task.ScheduledAt.IsZero() && task.ScheduledAt.After(time.Now()) {
		task.State = queue.TaskStateScheduled
	} else {
		task.State = queue.TaskStatePending
		task.ScheduledAt = time.Time{} // clear if in past
	}

	if task.MaxRetries == 0 {
		task.MaxRetries = e.retryCfg.MaxRetries
	}
	if task.Timeout == 0 {
		task.Timeout = e.cfg.Worker.DefaultTimeout
	}

	// Register task
	e.tasks[task.ID] = task
	if task.IdempotencyKey != "" {
		e.idempKeys[task.IdempotencyKey] = task.ID
	}
	e.mu.Unlock()

	// WAL: durable write BEFORE queue placement
	taskData, err := json.Marshal(task)
	if err != nil {
		e.mu.Lock()
		delete(e.tasks, task.ID)
		if task.IdempotencyKey != "" {
			delete(e.idempKeys, task.IdempotencyKey)
		}
		e.mu.Unlock()
		return nil, fmt.Errorf("engine: marshal task: %w", err)
	}

	_, err = e.walLog.Append(&wal.Entry{
		Type:   wal.EntryEnqueue,
		TaskID: task.ID,
		Data:   taskData,
	})
	if err != nil {
		e.mu.Lock()
		delete(e.tasks, task.ID)
		if task.IdempotencyKey != "" {
			delete(e.idempKeys, task.IdempotencyKey)
		}
		e.mu.Unlock()
		return nil, fmt.Errorf("engine: WAL append: %w", err)
	}

	// Place in queue or heap
	if task.State == queue.TaskStateScheduled {
		e.heap.Push(task)
		e.scheduler.NotifyNewTask()
	} else {
		if err := e.buffer.Enqueue(task); err != nil {
			return nil, fmt.Errorf("engine: enqueue: %w", err)
		}
	}

	e.metricsMu.Lock()
	e.enqueueCount++
	e.metricsMu.Unlock()
	e.metricCollector.RecordEnqueue(time.Since(enqueueStart))

	return task, nil
}

// GetTask returns a task by ID.
func (e *Engine) GetTask(id string) (*queue.Task, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	t, ok := e.tasks[id]
	return t, ok
}

// CancelTask cancels a pending or scheduled task.
func (e *Engine) CancelTask(id string) error {
	e.mu.Lock()
	task, ok := e.tasks[id]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("engine: task %s not found", id)
	}

	// Only pending or scheduled tasks can be cancelled
	switch task.State {
	case queue.TaskStatePending, queue.TaskStateScheduled:
		task.State = queue.TaskStateCancelled
		task.CompletedAt = time.Now()
	default:
		e.mu.Unlock()
		return fmt.Errorf("engine: task %s in state %s cannot be cancelled", id, task.State)
	}
	e.mu.Unlock()

	// WAL
	if _, err := e.walLog.Append(&wal.Entry{
		Type:   wal.EntryCancel,
		TaskID: id,
	}); err != nil {
		return fmt.Errorf("engine: WAL cancel: %w", err)
	}

	// Remove from heap if scheduled
	e.heap.Remove(task)

	return nil
}

// ListDLQ returns tasks in the dead-letter queue.
func (e *Engine) ListDLQ(limit, offset int) ([]*queue.Task, int) {
	e.dlqMu.RLock()
	defer e.dlqMu.RUnlock()

	total := len(e.dlq)
	if offset >= total {
		return nil, total
	}

	end := offset + limit
	if end > total {
		end = total
	}
	return e.dlq[offset:end], total
}

// Stats returns engine metrics.
func (e *Engine) Stats() EngineStats {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	var busy int
	var processed, failed int64
	if e.pool != nil {
		busy, processed, failed = e.pool.Stats()
	}

	var walSize int64
	var walSeq uint64
	if e.walLog != nil {
		walSize = e.walLog.Size()
		walSeq = e.walLog.LastSeq()
	}

	e.dlqMu.RLock()
	dlqSize := len(e.dlq)
	e.dlqMu.RUnlock()

	return EngineStats{
		QueueDepth:    e.buffer.Len(),
		HeapSize:      e.heap.Len(),
		TotalTasks:    len(e.tasks),
		EnqueueCount:  e.enqueueCount,
		CompleteCount: e.completeCount,
		FailCount:     e.failCount,
		RetryCount:    e.retryCount,
		DeadCount:     e.deadCount,
		BusyWorkers:   busy,
		Processed:     processed,
		Failed:        failed,
		WALSize:       walSize,
		WALSeq:        walSeq,
		DLQSize:       dlqSize,
	}
}

// EngineStats holds engine-level metrics.
type EngineStats struct {
	QueueDepth    int    `json:"queue_depth"`
	HeapSize      int    `json:"heap_size"`
	TotalTasks    int    `json:"total_tasks"`
	EnqueueCount  int64  `json:"enqueue_count"`
	CompleteCount int64  `json:"complete_count"`
	FailCount     int64  `json:"fail_count"`
	RetryCount    int64  `json:"retry_count"`
	DeadCount     int64  `json:"dead_count"`
	BusyWorkers   int    `json:"busy_workers"`
	Processed     int64  `json:"processed"`
	Failed        int64  `json:"failed"`
	WALSize       int64  `json:"wal_size"`
	WALSeq        uint64 `json:"wal_seq"`
	DLQSize       int    `json:"dlq_size"`
}

// Metrics returns the metrics collector.
func (e *Engine) Metrics() *metrics.Metrics {
	return e.metricCollector
}

// --- Internal methods ---

// wrapHandler creates a worker.HandlerFunc that integrates WAL logging,
// state transitions, and retry management.
func (e *Engine) wrapHandler(userHandler worker.HandlerFunc) worker.HandlerFunc {
	return func(ctx context.Context, task *queue.Task) error {
		// WAL: mark task as started
		e.mu.Lock()
		task.ForceSetState(queue.TaskStateRunning)
		task.StartedAt = time.Now()
		e.mu.Unlock()

		if _, err := e.walLog.Append(&wal.Entry{
			Type:   wal.EntryStart,
			TaskID: task.ID,
		}); err != nil {
			logger.Error("engine: WAL start entry failed", "task_id", task.ID, "error", err)
		}

		e.metricCollector.RecordDequeue()
		execStart := time.Now()
		logger.Info("task_started", "task_id", task.ID, "attempt", task.RetryCount)

		// Execute with timeout and panic recovery
		execCtx := ctx
		var cancel context.CancelFunc
		if task.Timeout > 0 {
			execCtx, cancel = context.WithTimeout(ctx, task.Timeout)
		} else {
			execCtx, cancel = context.WithCancel(ctx)
		}

		var execErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					execErr = fmt.Errorf("panic: %v", r)
				}
			}()
			execErr = userHandler(execCtx, task)
		}()
		cancel()

		if execErr != nil {
			e.metricCollector.RecordFail()
			return e.handleFailure(task, execErr)
		}

		e.metricCollector.RecordComplete(time.Since(execStart))
		return e.handleSuccess(task)
	}
}

func (e *Engine) handleSuccess(task *queue.Task) error {
	e.mu.Lock()
	task.ForceSetState(queue.TaskStateCompleted)
	task.CompletedAt = time.Now()
	e.mu.Unlock()

	logger.Info("task_completed", "task_id", task.ID, "attempt", task.RetryCount)

	if _, err := e.walLog.Append(&wal.Entry{
		Type:   wal.EntryComplete,
		TaskID: task.ID,
	}); err != nil {
		logger.Error("engine: WAL complete entry failed", "task_id", task.ID, "error", err)
	}

	e.metricsMu.Lock()
	e.completeCount++
	e.metricsMu.Unlock()

	return nil
}

func (e *Engine) handleFailure(task *queue.Task, execErr error) error {
	e.metricsMu.Lock()
	e.failCount++
	e.metricsMu.Unlock()

	e.mu.Lock()
	task.LastError = execErr.Error()
	task.RetryCount++

	isNonRetryable := retry.IsNonRetryable(execErr)
	if isNonRetryable || task.RetryCount > task.MaxRetries {
		// Exhausted retries or non-retryable — move to DLQ
		task.ForceSetState(queue.TaskStateDead)
		task.CompletedAt = time.Now()
		e.mu.Unlock()

		logger.Error("task_sent_to_dlq", "task_id", task.ID, "total_attempts", task.RetryCount, "error", execErr.Error())

		if _, err := e.walLog.Append(&wal.Entry{
			Type:   wal.EntryDead,
			TaskID: task.ID,
			Data:   []byte(execErr.Error()),
		}); err != nil {
			logger.Error("engine: WAL dead entry failed", "task_id", task.ID, "error", err)
		}

		e.dlqMu.Lock()
		e.dlq = append(e.dlq, task)
		e.dlqMu.Unlock()

		e.metricsMu.Lock()
		e.deadCount++
		e.metricsMu.Unlock()
		e.metricCollector.RecordDead()

		return execErr
	}

	// Schedule retry with backoff
	delay := retry.NextDelay(e.retryCfg, task.RetryCount-1)
	task.ForceSetState(queue.TaskStateScheduled)
	task.ScheduledAt = time.Now().Add(delay)
	e.mu.Unlock()

	logger.Warn("task_retry_scheduled", "task_id", task.ID, "attempt", task.RetryCount, "delay_ms", delay.Milliseconds(), "error", execErr.Error())

	// WAL
	retryData, _ := json.Marshal(map[string]interface{}{
		"error":       execErr.Error(),
		"retry_count": task.RetryCount,
		"next_at":     task.ScheduledAt.UnixNano(),
	})
	if _, err := e.walLog.Append(&wal.Entry{
		Type:   wal.EntryRetry,
		TaskID: task.ID,
		Data:   retryData,
	}); err != nil {
		logger.Error("engine: WAL retry entry failed", "task_id", task.ID, "error", err)
	}

	// Re-enqueue via delayed heap
	e.heap.Push(task)
	e.scheduler.NotifyNewTask()

	e.metricsMu.Lock()
	e.retryCount++
	e.metricsMu.Unlock()
	e.metricCollector.RecordRetry()

	return execErr
}

// applyEntry reconstructs state from a single WAL entry during recovery.
func (e *Engine) applyEntry(entry *wal.Entry) error {
	switch entry.Type {
	case wal.EntryEnqueue:
		var task queue.Task
		if err := json.Unmarshal(entry.Data, &task); err != nil {
			return fmt.Errorf("unmarshal enqueue: %w", err)
		}
		e.tasks[task.ID] = &task
		if task.IdempotencyKey != "" {
			e.idempKeys[task.IdempotencyKey] = task.ID
		}

	case wal.EntryStart:
		if t, ok := e.tasks[entry.TaskID]; ok {
			t.ForceSetState(queue.TaskStateRunning)
			t.StartedAt = time.Unix(0, entry.Timestamp)
		}

	case wal.EntryComplete:
		if t, ok := e.tasks[entry.TaskID]; ok {
			t.ForceSetState(queue.TaskStateCompleted)
			t.CompletedAt = time.Unix(0, entry.Timestamp)
		}

	case wal.EntryFail:
		if t, ok := e.tasks[entry.TaskID]; ok {
			t.ForceSetState(queue.TaskStateFailed)
			if len(entry.Data) > 0 {
				t.LastError = string(entry.Data)
			}
		}

	case wal.EntryRetry:
		if t, ok := e.tasks[entry.TaskID]; ok {
			var rd map[string]interface{}
			if err := json.Unmarshal(entry.Data, &rd); err == nil {
				if rc, ok := rd["retry_count"].(float64); ok {
					t.RetryCount = int(rc)
				}
				if na, ok := rd["next_at"].(float64); ok {
					t.ScheduledAt = time.Unix(0, int64(na))
				}
				if errStr, ok := rd["error"].(string); ok {
					t.LastError = errStr
				}
			}
			t.ForceSetState(queue.TaskStateScheduled)
		}

	case wal.EntryDead:
		if t, ok := e.tasks[entry.TaskID]; ok {
			t.ForceSetState(queue.TaskStateDead)
			t.CompletedAt = time.Unix(0, entry.Timestamp)
			if len(entry.Data) > 0 {
				t.LastError = string(entry.Data)
			}
			e.dlq = append(e.dlq, t)
		}

	case wal.EntryCancel:
		if t, ok := e.tasks[entry.TaskID]; ok {
			t.ForceSetState(queue.TaskStateCancelled)
			t.CompletedAt = time.Unix(0, entry.Timestamp)
		}
	}

	return nil
}

// rebuildQueues places recovered tasks into the appropriate queues.
func (e *Engine) rebuildQueues() {
	for _, task := range e.tasks {
		switch task.GetState() {
		case queue.TaskStatePending:
			// Re-enqueue into ring buffer
			if err := e.buffer.Enqueue(task); err != nil {
				logger.Error("engine: recovery re-enqueue failed", "task_id", task.ID, "error", err)
			}

		case queue.TaskStateScheduled:
			// Place in delayed heap
			e.heap.Push(task)

		case queue.TaskStateRunning:
			// Running tasks that weren't completed = crashed mid-execution
			// Treat as failed, re-enqueue as pending
			task.ForceSetState(queue.TaskStatePending)
			task.RetryCount++
			if err := e.buffer.Enqueue(task); err != nil {
				logger.Error("engine: recovery re-enqueue running task failed", "task_id", task.ID, "error", err)
			}
		}
		// Completed, Dead, Cancelled, Failed tasks stay in registry but aren't re-queued
	}
}

// checkpointLoop periodically creates snapshots and truncates the WAL.
func (e *Engine) checkpointLoop() {
	ticker := time.NewTicker(e.cfg.Checkpoint.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.checkpoint(); err != nil {
				logger.Error("engine: checkpoint failed", "error", err)
			}
		}
	}
}

// checkpoint creates a snapshot and rotates the WAL.
func (e *Engine) checkpoint() error {
	e.mu.RLock()
	snap := &wal.Snapshot{
		LastSeq: e.walLog.LastSeq(),
		Tasks:   make([]*wal.SnapshotTask, 0, len(e.tasks)),
	}
	for _, t := range e.tasks {
		snap.Tasks = append(snap.Tasks, taskToSnapshotTask(t))
	}
	e.mu.RUnlock()

	// Write snapshot atomically
	if err := wal.WriteSnapshot(e.walDir, snap); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}

	// Rotate WAL: close current, rename to .old, open new
	if err := e.rotateWAL(); err != nil {
		return fmt.Errorf("rotate WAL: %w", err)
	}

	logger.Info("engine: checkpoint complete", "task_count", len(snap.Tasks), "last_seq", snap.LastSeq)
	return nil
}

func (e *Engine) rotateWAL() error {
	walPath := e.walDir + "/wal.bin"
	oldPath := e.walDir + "/wal.old"

	// Close current WAL
	if err := e.walLog.Close(); err != nil {
		return fmt.Errorf("close old WAL: %w", err)
	}

	// Remove previous old WAL
	os.Remove(oldPath)

	// Rename current to old
	if err := os.Rename(walPath, oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("rename WAL: %w", err)
	}

	// Open fresh WAL
	syncPolicy := wal.ParseSyncPolicy(e.cfg.WAL.SyncPolicy)
	w, err := wal.Open(walPath, syncPolicy)
	if err != nil {
		return fmt.Errorf("open new WAL: %w", err)
	}
	w.SetSeq(e.walLog.LastSeq())
	e.walLog = w

	return nil
}

// --- Helpers ---

func taskToSnapshotTask(t *queue.Task) *wal.SnapshotTask {
	state := uint8(0)
	if t.State >= 0 && t.State <= 255 {
		// #nosec G115 -- TaskState is a bounded enum
		state = uint8(t.State)
	}

	return &wal.SnapshotTask{
		ID:             t.ID,
		IdempotencyKey: t.IdempotencyKey,
		Payload:        t.Payload,
		State:          state,
		Priority:       t.Priority,
		CreatedAt:      t.CreatedAt.UnixNano(),
		ScheduledAt:    t.ScheduledAt.UnixNano(),
		StartedAt:      t.StartedAt.UnixNano(),
		CompletedAt:    t.CompletedAt.UnixNano(),
		RetryCount:     t.RetryCount,
		MaxRetries:     t.MaxRetries,
		LastError:      t.LastError,
		TimeoutNs:      int64(t.Timeout),
	}
}

func snapshotTaskToTask(st *wal.SnapshotTask) *queue.Task {
	return &queue.Task{
		ID:             st.ID,
		IdempotencyKey: st.IdempotencyKey,
		Payload:        st.Payload,
		State:          queue.TaskState(st.State),
		Priority:       st.Priority,
		CreatedAt:      time.Unix(0, st.CreatedAt),
		ScheduledAt:    time.Unix(0, st.ScheduledAt),
		StartedAt:      time.Unix(0, st.StartedAt),
		CompletedAt:    time.Unix(0, st.CompletedAt),
		RetryCount:     st.RetryCount,
		MaxRetries:     st.MaxRetries,
		LastError:      st.LastError,
		Timeout:        time.Duration(st.TimeoutNs),
	}
}

var idCounter uint64
var idMu sync.Mutex

func generateID() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), idCounter)
}
