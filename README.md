# TaskQueue — Concurrent Crash-Resilient Task Queue in Go

[![CI & Security](https://github.com/sumitsaini/taskqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/sumitsaini/taskqueue/actions)
[![Go Version](https://img.shields.io/badge/Go-1.24%20%7C%201.27-00ADD8?logo=go)](go.mod)
[![Race Detector](https://img.shields.io/badge/Race%20Detector-Clean%20(0%20races)-success)](PROOF_OF_WORK.md)
[![OpenAPI 3.0](https://img.shields.io/badge/OpenAPI-3.0.3-brightgreen.svg)](docs/openapi.yaml)
[![Proof of Work](https://img.shields.io/badge/Verified-Proof%20of%20Work-blueviolet)](PROOF_OF_WORK.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A production-grade, crash-resilient concurrent task queue built in Go with **zero external dependencies**. Engineered for single-node durability and extreme throughput, it pairs an in-memory ring-buffer (~**74.3M ops/sec**, 0 allocations) with a Castagnoli CRC32-C binary Write-Ahead Log (WAL), atomic snapshot checkpointing, min-heap delayed scheduling, exponential backoff retries with DLQ, token-bucket rate limiting, and native Prometheus exposition. Full live verification outputs documented in [PROOF_OF_WORK.md](PROOF_OF_WORK.md).

---

## Architecture at a Glance

```
                         HTTP API Layer (:8080)
           [POST /tasks] [GET /tasks/:id] [DELETE /tasks/:id] [GET /dlq] [GET /metrics]
                                      |
                           +----------v-----------+
                           |  Token Bucket Limiter| 100 burst, 50 req/sec
                           +----------+-----------+
                                      |
                           +----------v-----------+
                           |    Engine Core       |<----+ Replay & Recover
                           +----+-----+-----+-----+     |
                                |     |     |           |
            +-------------------+     |     +-----+     |
            |                         |           |     |
            v                         v           v     |
     +-------------+           +-------------+  +-------------+
     | RingBuffer  |           | DelayedHeap |  |   WAL Log   |
     | (sync.Cond) |           |  (Min-Heap) |  | (CRC32-C LE)|
     +------+------+           +------+------+  +------+------+
            |                         |                |
            |     +-------------------+                | Periodic Snapshots
            |     | (PromoteReady)                     v (atomic rename)
            v     v                             +-------------+
     +-------------+                            |  Snapshots  |
     | Worker Pool | (Fixed-Size Goroutines)    | (Atomic JSON|
     +------+------+                            +-------------+
            | Timeout & Panic Recover
            v
     [Success / Retry / DLQ]
```

---

## 5-Minute Quickstart

### 1. Run with Docker (Under 10MB Container Image)

```bash
# Clone and boot with persistent volume
docker-compose up -d
```

*Or build and run locally:*
```bash
go build -o bin/taskqueue ./cmd/server && ./bin/taskqueue
```

### 2. Submit a Task

```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"action": "email", "to": "alex@example.com"},
    "priority": 1,
    "delay_seconds": 0,
    "max_retries": 3,
    "timeout_seconds": 10,
    "idempotency_key": "welcome-user-123"
  }'
```

```json
{
  "id": "task_1772719200000000_1",
  "state": "pending",
  "created_at": "2026-09-03T14:40:00Z"
}
```

### 3. Check Task Lifecycle

```bash
curl http://localhost:8080/tasks/task_1772719200000000_1
```

### 4. Inspect Prometheus Metrics

```bash
curl http://localhost:8080/metrics
```

### 5. Run Interactive Walkthrough

```bash
# Linux/macOS:
./examples/walkthrough.sh

# Windows PowerShell:
.\examples\walkthrough.ps1
```

---

## Key Performance Benchmarks

*Measured on AMD Ryzen 5 7235HS (8 virtual cores, Go 1.27)*

| Benchmark Subsystem | Ops / Sec | Latency | Memory Allocs |
| :--- | :--- | :--- | :--- |
| **RingBuffer Enqueue + Dequeue** | **74.3M ops/sec** | **16.83 ns/op** | **0 B/op (0 allocs)** |
| **RingBuffer Multi-Threaded** (1 worker) | **4.65M ops/sec** | 251.4 ns/op | 310 B/op (3 allocs) |
| **RingBuffer Multi-Threaded** (16 workers) | **1.62M ops/sec** | 745.2 ns/op | 307 B/op (3 allocs) |
| **Priority Min-Heap (Push + Pop)** | **1.45M ops/sec** | 748.7 ns/op | 355 B/op (2 allocs) |
| **WAL Append** (`sync_policy: none`) | **1.60M ops/sec** | 749.5 ns/op | 448 B/op (3 allocs) |
| **WAL Append** (`sync_policy: every`, fsync) | **2,752 ops/sec** | 0.37 ms/op | 449 B/op (3 allocs) |
| **Engine Submit** (WAL + Queue) | **172,034 ops/sec** | 6.52 µs/op | 5.1 KB/op (10 allocs) |
| **Engine Throughput** (Submit + Worker Run) | **89,118 ops/sec** | 11.3 µs/op | 6.7 KB/op (29 allocs) |
| **Engine Recovery Speed** | **~164,000 tasks/sec** | 6.09 ms / 1k tasks | Full state rebuild |

Run benchmarks yourself:
```bash
go test -bench=. -benchmem ./internal/...
```

---

## Testing & Quality Guarantees

* **Unit & Integration Suite:** 78 tests across 9 packages.
* **Race Detector:** **0 data races** under `go test -race -v -count=1 ./...`.
* **Static Analysis:** Clean passes on `go vet`, `staticcheck`, `govulncheck`, and `gosec`.
* **Stress Tested:** Verified under high-load contention: 32 concurrent producers, 3,200 tasks across 16 workers, simultaneous task cancellations, and worker panics.
* **Failure Simulation:** Validated under simulated mid-write crashes, torn writes at EOF, and middle-of-file bit corruptions.

---

## Core Documentation & Technical Defenses

* **[OpenAPI 3.0 Specification](docs/openapi.yaml)**: Complete API definitions, schemas, and error codes.
* **[Architecture Overview](docs/architecture.md)**: Deep dive into the concurrency model, WAL format, and snapshots.
* **[Architecture Decisions & Interview Guide](docs/interview-guide.md)**: Deep defenses for WAL framing, min-heap scheduler, and state machine anti-clobbering rules.
* **[API Contract](docs/api-contract.md)**: REST endpoints specification.
* **[ADRs](docs/adr/)**: Architectural Decision Records (ADRs 001–005).
* **[Examples](examples/)**: Minimal Go client and bash/PowerShell walkthrough scripts.
