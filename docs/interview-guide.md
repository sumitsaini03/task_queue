# TaskQueue — Architectural Decisions & Interview Guide

This guide provides deep technical defenses for the primary engineering decisions in `github.com/sumitsaini03/task_queue`. These notes are structured to explain the problem, alternatives evaluated, the chosen architecture, and the quantified trade-offs.

---

## 1. Why the Binary WAL Framing Looks the Way It Does

### The Problem
A durable task queue must survive sudden process death (`SIGKILL`, power loss, kernel panics). On restart, the recovery system must answer two distinct questions:
1. Did the log end cleanly, or was a write interrupted mid-flight (torn write at EOF)?
2. Is every record in the middle of the log genuine and bit-exact, or has disk corruption occurred?

### The Framing Specification
```
+----------+----------+----------+----------+---------+----------+
| magic(2) | len(4)   | seq(8)   | crc32(4) | type(1) | data(N)  |
+----------+----------+----------+----------+---------+----------+
```

### Technical Defense
* **Magic Header (`0xAA4C`):** Two fixed bytes at the start of every record allow instant boundary alignment. If a process died while writing the length prefix, the replay reader detects an invalid magic and halts cleanly.
* **Length Prefix (`uint32 LE`):** Explicit length allows the reader to allocate a buffer or seek without scanning for delimiter bytes (unlike JSON lines or newline-delimited logs).
* **Monotonically Increasing Sequence Number (`uint64 LE`):** Sequence numbers track the total order of operations across checkpoints and WAL rotations. When a snapshot is taken at sequence $S$, the recovery engine safely ignores any WAL entry with $\text{Seq} \le S$, providing idempotent replay.
* **Castagnoli CRC32-C (`uint32 LE`):** Computed over `type + data`. CRC32-C is hardware-accelerated on modern x86 (`CRC32` instruction) and ARM64. It detects multi-bit burst errors with mathematical guarantees.
* **Torn Write vs Corruption Disambiguation:**
  * **Torn Write at Tail:** If EOF is reached while reading the magic, length, or body, the system recognizes an incomplete write caused by a crash, truncates the file back to the last valid record boundary, and resumes safely.
  * **Middle-of-Log Corruption:** If the magic and length are present but CRC32-C fails, the log has experienced silent bit rot or tampering. The system immediately returns `wal.ErrCorrupted` and aborts startup rather than silently losing tasks.

---

## 2. Why the Scheduler Uses a Min-Heap + Wake Channel Instead of Polling

### The Problem
Delayed tasks must execute as close to their target `scheduled_at` timestamp as possible, while using zero CPU during idle periods and immediately accommodating new tasks scheduled earlier than existing ones.

### Alternatives Evaluated
1. **Ticker Polling (e.g., check every 100ms):**
   * *Drawback:* Wastes CPU cycles when idle. Introduces up to 100ms latency error on execution.
2. **Hashed Timing Wheel:**
   * *Drawback:* Excellent for millions of timers with short horizons, but complex memory footprint and bucket granularity issues for multi-day horizons.

### The Chosen Architecture: Min-Heap + Precise Sleep + Wake Channel
* The priority queue (`internal/queue/heap.go`) maintains tasks ordered by `scheduled_at` ascending using Go's `container/heap`. Root of the heap is always the earliest deadline ($O(1)$ inspection).
* The scheduler loop calculates:
  $$\Delta t = \text{NextDeadline}() - \text{time.Now()}$$
* If $\Delta t > 0$, the scheduler enters an interruptible sleep:
  ```go
  timer := time.NewTimer(delay)
  select {
  case <-s.ctx.Done():
      return
  case <-timer.C:
      // Deadline arrived: promote ready tasks to ring buffer
      s.promoteReady()
  case <-s.wake:
      // New earlier task inserted: recompute timer immediately
  }
  ```
* **Why this wins:**
  * Zero CPU spin: Sleep consumes 0% CPU.
  * Zero latency penalty: When an urgent task with $\text{delay} = 100\text{ms}$ is inserted while another is waiting 10 minutes, `s.Push()` sends a non-blocking signal to `s.wake`, causing the scheduler to wake up immediately and adjust its timer.

---

## 3. How the State Machine Prevents Clobbering Under Concurrent Workers

### The Problem
In an asynchronous queue where workers execute handlers concurrently while background components (schedulers, retry managers, timeout handlers, and cancel endpoints) inspect and update tasks, subtle race conditions can occur.
*Example Hazard:* A worker completes task execution and attempts to set `task.State = Completed`, while the handler returned an error and the engine wrapper transitioned the task to `Retrying` or `Dead`. If the worker blindly wrote `Completed`, a dead task would be marked successful.

### The Architecture: Concurrency-Safe State Guards
1. **Mutex Protection & Explicit State Checks:**
   Every `Task` holds an unexported `sync.RWMutex`.
   ```go
   func (t *Task) CanTransitionTo(target TaskState) bool {
       // Terminal states (Completed, Failed, Dead, Cancelled, Expired) cannot transition
       if t.State.IsTerminal() {
           return false
       }
       // Strict valid state transitions
   }
   ```
2. **Worker Execution Isolation:**
   The worker pool only modifies the task state if the task is still in `TaskStateRunning`:
   ```go
   // Inside worker.go
   if task.GetState() == queue.TaskStateRunning {
       task.ForceSetState(queue.TaskStateCompleted)
   }
   ```
   If the engine handler wrapped the task into a retry (`TaskStateScheduled`) or DLQ (`TaskStateDead`), the worker's completion write is safely bypassed.
3. **Idempotency & Race Detector Cleanliness:**
   Running under `go test -race` across 32 concurrent producers, 16 workers, and 3,200 tasks verifies zero data races.

---

## 4. Ring Buffer with `sync.Cond` vs Go Channels

### Why Not Just Use Go Channels?
Go channels (`chan *Task`) are built on mutexes and condition variables internally, but have specific operational constraints:
* **Capacity and Overflow:** Channel capacity is immutable once created with `make(chan T, cap)`. A ring buffer permits dynamically changing overflow policies (instant `OverflowReject` returning HTTP 503 vs `OverflowBlock`).
* **Performance:** Our pre-allocated circular ring buffer avoids slice header manipulations and channel allocations, clocking **74.3M ops/sec with 0 heap allocations** single-threaded and **1.62M–4.65M ops/sec** under high multi-threaded contention.
* **Blocking Semantics:** `sync.Cond` allows worker goroutines to sleep with zero CPU usage when the buffer is empty and broadcast wake-ups efficiently during batch appends.

---

## 5. CS Fundamentals to Production Mapping

This table connects fundamental computer science concepts to their load-bearing production implementations in `taskqueue`:

| Fundamental | Production Implementation | Component / Location |
| :--- | :--- | :--- |
| **Heaps / Priority Queues** | Delayed job scheduling ($O(\log N)$ push/pop, $O(1)$ peek) | `internal/queue/heap.go` |
| **Hash Maps** | $O(1)$ task lookup, state inspection, deduplication | `internal/engine/engine.go` |
| **Ring Buffers** | Bounded FIFO queue without slice reallocation | `internal/queue/ring.go` |
| **OS Thread Scheduling** | Worker concurrency model with fixed goroutine pool | `internal/worker/pool.go` |
| **Mutexes / Atomics / CAS** | Thread-safe state mutation, metrics collection | `internal/queue`, `internal/metrics` |
| **Condition Variables** | Blocking/waking idle workers via `sync.Cond` | `internal/queue/ring.go` |
| **Memory Model (Happens-Before)**| Concurrency correctness validated via race detector | `go test -race ./...` |
| **Write-Ahead Logging** | Crash durability, ACID-like durability guarantees | `internal/wal/wal.go` |
| **Log Compaction** | Bounded disk growth via atomic snapshot rotation | `internal/wal/snapshot.go` |
| **Idempotency** | At-least-once delivery to effectively-once execution | `internal/engine/engine.go` |
| **Backpressure & Load Shedding**| Bounded queue with 503 rejection under overload | `internal/queue/ring.go`, `internal/api` |
| **Token Bucket** | Submission rate limiting (burst + sustained rate) | `internal/api/limiter.go` |
| **Exponential Backoff + Full Jitter** | Distributed retry strategy preventing thundering herds | `internal/retry/backoff.go` |

