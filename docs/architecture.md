# Concurrent Task Queue — Architecture Overview

## 1. System Overview

`github.com/sumitsaini/taskqueue` is a production-grade, crash-resilient, concurrent task queue implemented in Go with zero external runtime dependencies. It combines in-memory ring-buffer performance with single-node durability guaranteed by a binary write-ahead log (WAL) and checkpointing.

```
                         HTTP API Layer (:8080)
           [POST /tasks] [GET /tasks/:id] [DELETE /tasks/:id] [GET /dlq] [GET /metrics]
                                      |
                           +----------v-----------+
                           |     TokenBucket      | Rate Limiter
                           +----------+-----------+
                                      |
                           +----------v-----------+
                           |    Engine Core       |<----+
                           +----+-----+-----+-----+     |
                                |     |     |           |
            +-------------------+     |     +-----+     |
            |                         |           |     | Replay &
            v                         v           v     | Rebuild
     +-------------+           +-------------+  +-------------+
     | RingBuffer  |           | DelayedHeap |  |   WAL Log   |
     | (sync.Cond) |           |  (Min-Heap) |  | (CRC32/Fsync|
     +------+------+           +------+------+  +------+------+
            |                         |                |
            |     +-------------------+                | Periodic
            |     | (PromoteReady)                     v Snapshots
            v     v                             +-------------+
     +-------------+                            |  Snapshots  |
     | Worker Pool |                            | (Atomic JSON|
     | (Goroutines)|                            +-------------+
     +------+------+
            | Execute Handler
            v
     [Success / Retry / DLQ]
```

---

## 2. Concurrency Model

1. **Bounded Ring Buffer (`internal/queue/queue.go`):**
   * Preallocated circular slice of pointers with `head`, `tail`, and `size`.
   * Synchronization via `sync.Mutex` and `sync.Cond`.
   * When empty, worker goroutines block on `cond.Wait()`, consuming 0% CPU.
   * Overflow policy: `OverflowReject` (instant 503) or `OverflowBlock` (producer blocks until capacity frees).
   * Benchmark: **~74.3M ops/sec** single-threaded with **0 allocations**, **1.27M - 4.65M ops/sec** multi-threaded under lock contention.

2. **Fixed-Size Worker Pool (`internal/worker/worker.go`):**
   * Configurable goroutine count.
   * Non-blocking context cancellation and graceful ring-buffer drain.
   * Per-task timeout enforcement via `context.WithTimeout`.
   * Two-level panic recovery (worker pool level + engine handler level).

3. **Background Scheduler (`internal/queue/scheduler.go`):**
   * Thread-safe min-heap (`container/heap`) ordered by `ScheduledAt`.
   * Precise sleep timer based on `heap.NextDeadline()`; no polling spin-loops.
   * Wake-up channel (`s.wake`) interrupts sleep immediately when earlier tasks arrive.

---

## 3. Durability & Recovery (WAL)

### Binary Wire Format

```
+----------+----------+----------+----------+---------+----------+
| magic(2) | len(4)   | seq(8)   | crc32(4) | type(1) | data(N)  |
+----------+----------+----------+----------+---------+----------+
```

* **Durability Guarantee:** Under `sync_policy: "every"`, `wal.Append()` flushes the 64KB write buffer and invokes `os.File.Sync()`. The queue does not acknowledge an HTTP submission until disk persistence succeeds.
* **Deterministic Recovery Pipeline:**
  1. Load point-in-time state from `snapshot.json` (if present).
  2. Replay all WAL records with `Seq > snapshot.LastSeq`.
  3. Validate CRC32-C on every entry. Middle-of-log corruption returns `ErrCorrupted` and halts startup.
  4. Truncated tail writes from sudden crashes are detected at EOF and repaired automatically by truncating the file to the last valid boundary.
  5. In-flight tasks (`RUNNING` at crash time) are re-enqueued as `PENDING`.

---

## 4. Checkpointing & Rotation

To prevent unbounded log growth:
1. Engine writes point-in-time task registry to `snapshot.tmp`.
2. Atomically renames `snapshot.tmp` to `snapshot.json`.
3. WAL is rotated: `wal.bin` -> `wal.old`, fresh `wal.bin` opened with sequence continuity.
4. Old files safely cleaned without interrupting active workloads.

---

## 5. Retries & Dead Letter Queue (DLQ)

* **Exponential Backoff with Jitter:**
  $$\text{Delay} = \min(\text{Base} \times \text{Multiplier}^{\text{attempt}}, \text{Max}) \pm \text{Jitter}$$
* **Non-Retryable Errors:** Handlers can return `retry.MarkNonRetryable(err)` to immediately bypass retry backoff and route to DLQ.
* **DLQ Persistence:** Tasks that exceed `max_retries` transition to `TaskStateDead`, append an `EntryDead` record to the WAL, and are stored in DLQ with error context and timestamps.

---

## 6. HTTP API & Observability

* Strictly complies with `docs/api-contract.md`.
* **Rate Limiting:** In-memory Token Bucket per client request burst/refill.
* **Prometheus Metrics (`GET /metrics`):**
  * `taskqueue_depth` (gauge)
  * `taskqueue_enqueue_total` (counter)
  * `taskqueue_dequeue_total` (counter)
  * `taskqueue_completed_total` (counter)
  * `taskqueue_failed_total` (counter)
  * `taskqueue_retry_total` (counter)
  * `taskqueue_dead_total` (counter)
  * `taskqueue_worker_busy` (gauge)
  * `taskqueue_enqueue_duration_p50_seconds` / `p99`
  * `taskqueue_execute_duration_p50_seconds` / `p99`
