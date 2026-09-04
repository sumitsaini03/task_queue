# TaskQueue — Concurrent Crash-Resilient Task Queue in Go

[![CI & Security](https://github.com/sumitsaini03/task_queue/actions/workflows/ci.yml/badge.svg)](.github/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.24%20%7C%201.27-00ADD8?logo=go)](go.mod)
[![Race Detector](https://img.shields.io/badge/Race%20Detector-Clean%20(0%20races)-success)](PROOF_OF_WORK.md)
[![OpenAPI 3.0](https://img.shields.io/badge/OpenAPI-3.0.3-brightgreen.svg)](docs/openapi.yaml)
[![Proof of Work](https://img.shields.io/badge/Verified-Proof%20of%20Work-blueviolet)](PROOF_OF_WORK.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A production-grade, crash-resilient concurrent task queue built in Go with **zero external runtime dependencies**. Engineered for single-node durability and high-throughput execution, TaskQueue pairs an in-memory circular ring buffer (**74.3M ops/sec**, 0 heap allocations) with a Castagnoli CRC32-C binary Write-Ahead Log (WAL), atomic snapshot checkpointing, min-heap delayed task scheduling, exponential backoff retries with Dead Letter Queue (DLQ) quarantine, token-bucket rate limiting, and native Prometheus exposition format. Live verification outputs and benchmark reproducibility commands are documented in [PROOF_OF_WORK.md](PROOF_OF_WORK.md).

---

## Table of Contents

- [Architecture](#architecture)
- [Quickstart (5 Minutes)](#quickstart-5-minutes)
- [Empirical Benchmarks](#empirical-benchmarks)
- [Testing & Reliability Guarantees](#testing--reliability-guarantees)
- [Technical Documentation & Architecture Guides](#technical-documentation--architecture-guides)
- [License](#license)

---

## Architecture

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion & HTTP Layer (:8080)"]
        HTTP["HTTP REST API<br/>/tasks | /tasks/:id | /dlq | /metrics | /health"]
        Limiter["Token Bucket Rate Limiter<br/>100 Burst, 50 req/sec"]
    end

    subgraph Core["Coordination & State Engine"]
        Engine["Engine Core & Registry<br/>O(1) Memory Lookup & Idempotency"]
    end

    subgraph Storage["Crash-Resilient Durability"]
        WAL[("Write-Ahead Log (WAL)<br/>CRC32-C Framed, Append-Only")]
        Snap[("Atomic Snapshots<br/>Periodic Compaction")]
    end

    subgraph Queues["Queuing & Scheduling Tier"]
        Ring["Circular Ring Buffer<br/>sync.Cond Wakeup (74.3M ops/sec, 0 allocs)"]
        Heap["Delayed Min-Heap<br/>Interruptible Timer Sleep"]
    end

    subgraph Execution["Worker Execution Tier"]
        Workers["Worker Pool<br/>Fixed Goroutines with Graceful Drain"]
        Outcome{"Execution Result"}
        Success["Completed"]
        Retry["Exponential Backoff<br/>Full Jitter"]
        DLQ[("Dead Letter Queue<br/>Quarantine")]
    end

    HTTP --> Limiter
    Limiter --> Engine
    Engine -->|"1. Durable Append"| WAL
    WAL -.->|"Compact"| Snap
    Snap -.->|"Replay Baseline"| Engine
    WAL -.->|"Replay Tail"| Engine
    Engine -->|"2a. Immediate FIFO"| Ring
    Engine -->|"2b. Scheduled At"| Heap
    Heap -->|"Promote Ready"| Ring
    Ring --> Workers
    Workers --> Outcome
    Outcome -->|OK| Success
    Outcome -->|Fail (Attempt <= Max)| Retry
    Outcome -->|Fail (Exhausted)| DLQ
    Retry -->|"Reschedule"| Heap
    Success -->|"Record Complete"| WAL
    DLQ -->|"Record Dead"| WAL
```

---

## Quickstart (5 Minutes)

### 1. Boot with Docker (Distroless Image, ~9.5 MB Footprint)

```bash
docker-compose up -d
```

*Or build and run natively with Go:*
```bash
go build -o bin/taskqueue ./cmd/server
./bin/taskqueue
```

### 2. Submit a Task

```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"action": "send_notification", "user_id": "usr_9482"},
    "priority": 1,
    "delay_seconds": 0,
    "max_retries": 3,
    "timeout_seconds": 10,
    "idempotency_key": "welcome-user-9482"
  }'
```

```json
{
  "id": "task_1788448942988636200_1",
  "state": "pending",
  "created_at": "2026-09-04T01:00:00Z"
}
```

### 3. Check Task Lifecycle

```bash
curl http://localhost:8080/tasks/task_1788448942988636200_1
```

```json
{
  "id": "task_1788448942988636200_1",
  "state": "completed",
  "payload": "eyJhY3Rpb24iOiJzZW5kX25vdGlmaWNhdGlvbiIsInVzZXJfaWQiOiJ1c3JfOTQ4MiJ9",
  "priority": 1,
  "created_at": "2026-09-04T01:00:00Z",
  "started_at": "2026-09-04T01:00:00.002Z",
  "completed_at": "2026-09-04T01:00:00.015Z",
  "retry_count": 0,
  "max_retries": 3
}
```

### 4. Inspect Prometheus Metrics

```bash
curl http://localhost:8080/metrics
```

### 5. Run Interactive Walkthrough Scripts

```bash
# Linux / macOS:
./examples/walkthrough.sh

# Windows PowerShell:
.\examples\walkthrough.ps1
```

---

## Empirical Benchmarks

*Measured on AMD Ryzen 5 7235HS (4 physical cores / 8 logical threads @ 3.2 GHz, Go 1.27 on NVMe PCIe Gen 4 SSD)*

| Subsystem / Operation | Throughput | Latency | Memory Allocations |
| :--- | :--- | :--- | :--- |
| **In-Memory RingBuffer (Enqueue + Dequeue)** | **74.3M ops/sec** | **16.83 ns/op** | **0 B/op (0 allocs)** |
| **RingBuffer Multi-Threaded** (1 worker) | **4.65M ops/sec** | 251.4 ns/op | 310 B/op (3 allocs) |
| **RingBuffer Multi-Threaded** (16 workers) | **1.62M ops/sec** | 745.2 ns/op | 307 B/op (3 allocs) |
| **Priority Min-Heap (Push + Pop)** | **1.45M ops/sec** | 748.7 ns/op | 355 B/op (2 allocs) |
| **WAL Append** (`sync_policy: none`, buffered) | **1.60M ops/sec** | 749.5 ns/op | 448 B/op (3 allocs) |
| **WAL Append** (`sync_policy: every`, fsync) | **2,752 ops/sec** | 0.37 ms/op | 449 B/op (3 allocs) |
| **Engine In-Process Submit** (WAL + Queue) | **172,034 ops/sec** | 6.52 µs/op | 5.1 KB/op (10 allocs) |
| **Engine Full Throughput** (Submit + Worker Run)| **89,118 ops/sec** | 11.3 µs/op | 6.7 KB/op (29 allocs) |
| **Crash Recovery Speed** (WAL Replay) | **~164,000 tasks/sec** | 6.09 ms / 1k tasks | Full state rebuild |
| **Network HTTP API Saturation** (32 workers) | **6,550.7 req/sec** | 4.59 ms p50, 8.70 ms p99 | Over TCP socket |

Reproduce benchmarks locally:
```bash
# In-memory and storage subsystem microbenchmarks
go test -bench=. -benchmem ./internal/...

# HTTP API saturation load test
go run ./cmd/loadtest -c 32 -n 5000
```

---

## Testing & Reliability Guarantees

* **Full Test Suite:** 79 tests across all subsystems passing with **0 data races** under `go test -race -v -count=1 ./...`.
* **Zero External Dependencies:** Built entirely with standard library primitives (`sync`, `sync/atomic`, `container/heap`, `hash/crc32`, `net/http`, `log/slog`).
* **Static Analysis & Security:** Verified with `gofmt -s -l`, `go vet`, `staticcheck`, `govulncheck`, and `gosec` (clean exit code 0).
* **Crash Chaos Verification:** Validated under mid-write `SIGKILL` termination (`test/chaos/chaos_test.go`), confirming 100% state restoration on restart.
* **Torn Write Auto-Repair:** Automatic detection and truncation of partial records at the tail of the log without losing committed entries.

---

## Technical Documentation & Architecture Guides

* **[OpenAPI 3.0.3 Specification](docs/openapi.yaml)**: Formal API contract for all endpoints, request schemas, and error codes.
* **[Project Walkthrough & Phase History](PROJECT_WALKTHROUGH.md)**: Canonical build history, architectural milestones (Phases 1–20), and post-MVP stretch goals.
* **[Architecture Decisions & Interview Guide](docs/interview-guide.md)**: Deep technical defenses covering binary WAL framing, scheduler min-heap vs. polling, and concurrency isolation models.
* **[Empirical Proof of Work](PROOF_OF_WORK.md)**: Unedited live verification commands and raw benchmark terminal output.
* **[Observability & Monitoring Guide](docs/observability.md)**: Metrics catalog, Prometheus queries, and included [11-panel Grafana dashboard](docs/grafana-dashboard.json).
* **[Architectural Decision Records (ADRs)](docs/adr/)**: Architectural records (ADR 001–005) documenting structural decisions.
* **[Examples](examples/)**: Go client SDK example, bash walkthrough, and PowerShell scripts.
* **[Portfolio Positioning](context.md)**: System positioning, engineering constraints, and ATS resume pitch.

---

## License

This project is licensed under the [MIT License](LICENSE).
