# Release Notes — TaskQueue v1.0.0

**Release Tag:** `v1.0.0`  
**Date:** 2026-09-03  
**Status:** Production Ready  

---

## Executive Summary

`TaskQueue v1.0.0` is a high-performance, single-node concurrent task queue in Go with **zero external dependencies**. Engineered to balance extreme in-memory throughput with crash-resilient durability, TaskQueue achieves **74.3M ops/sec** on queue operations and **6,550 req/sec** over the network while guaranteeing crash durability via a Castagnoli CRC32-C binary Write-Ahead Log (WAL) and atomic snapshots.

---

## Key Highlights & Capabilities

### 1. High-Throughput In-Memory Core
* **Ring Buffer Queue:** Preallocated circular array guarded by `sync.Cond` condition variables. Reaches **74.3M ops/sec** single-threaded with **0 memory allocations**; 1.62M–4.65M ops/sec under multi-threaded contention.
* **Non-Busy Waiting:** Worker goroutines consume 0% CPU during queue starvation.
* **Overflow Protection:** Configurable overflow policies (`OverflowReject` returning instant HTTP 503 vs `OverflowBlock`).

### 2. Durability & Crash Recovery
* **Binary-Framed WAL:** Low-overhead record layout (`magic(2) | len(4) | seq(8) | crc32(4) | type(1) | data(N)`).
* **Castagnoli CRC32-C Integrity:** Hardware-accelerated checksumming distinguishing mid-file bit rot (`wal.ErrCorrupted`) from torn tail writes caused by abrupt power loss.
* **Automatic Tail Repair:** Silently truncates uncommitted writes at EOF on crash recovery.
* **Point-in-Time Checkpointing:** Atomic snapshot creation via temp-file replacement (`snapshot.tmp` -> `snapshot.json`) with safe WAL file rotation (`wal.bin` -> `wal.old`).
* **Recovery Speed:** Reconstructs and verifies 1,000 tasks in **6.09 ms** (~**164,000 tasks/sec**).

### 3. Task Lifecycle & Concurrency Model
* **Concurrency-Safe State Machine:** Governed by state transitions: `PENDING`, `SCHEDULED`, `RUNNING`, `RETRYING`, `COMPLETED`, `FAILED`, `DEAD`, `CANCELLED`, `EXPIRED`.
* **Zero Race Conditions:** Validated under `go test -race` across 32 concurrent producers submitting 3,200 tasks across 16 workers.
* **Priority Min-Heap Scheduler:** Non-busy interruptible timer sleeping until `NextDeadline()` with instant wake-up channel on earlier task insertions.

### 4. Fault Tolerance & Dead-Letter Queue
* **Exponential Backoff with Jitter:** Configurable multiplier and jitter to prevent thundering herds on downstream services.
* **Non-Retryable Error Bypassing:** Handlers can mark unrecoverable errors with `retry.MarkNonRetryable` to route directly to DLQ.
* **DLQ Inspection:** Paginated HTTP endpoint (`GET /dlq`) for inspection of permanently failed tasks.

### 5. Production Observability & REST API
* **Native Prometheus Metrics (`GET /metrics`):** Gauges, counters, and p50/p99 latency percentiles exposed in Prometheus text format.
* **Ready-to-Use Grafana Dashboard:** Complete dashboard JSON in `docs/grafana-dashboard.json`.
* **Structured JSON Logging:** End-to-end trace correlation with `request_id` and `task_id` threaded from HTTP ingestion to worker completion.
* **Rate Limiting:** Token-bucket rate limiting protecting against traffic bursts.

### 6. Containerization & Tooling
* **Minimal Distroless Container:** Multi-stage `Dockerfile` producing a **~9.5 MB** final non-root container image.
* **One-Command Boot:** `docker-compose.yml` with persistent WAL volume mount.
* **OpenAPI 3.0.3 Contract:** Validated API spec in `docs/openapi.yaml`.
* **Complete Test Coverage:** 78 unit/integration tests, chaos tests, and standalone load testing CLI.
