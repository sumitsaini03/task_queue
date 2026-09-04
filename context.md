# Context: Concurrent Task Queue

> **Roadmap & Architecture Notice:** For the canonical build history, phase-by-phase verification, and benchmarks across all completed phases (Phases 1–20), refer to [PROJECT_WALKTHROUGH.md](PROJECT_WALKTHROUGH.md) and [PROOF_OF_WORK.md](PROOF_OF_WORK.md). This document focuses on portfolio positioning, engineering constraints, and interview framing.

---

## What This Is
A production-grade, single-node concurrent task queue — the kind of system that sits behind Celery, Sidekiq, or Bull in a real backend. Workers pull jobs, execute them, retry on failure, and survive a crash without losing work.

## Why This Project
The goal is one system where CS fundamentals, DSA, and system design aren't decorative — they're load-bearing. A task queue forces real decisions in exactly the areas top-tier systems engineering interviews probe hardest: concurrency correctness, durability, and graceful failure under load.

## Portfolio Fit
This is one of three projects in the portfolio. To avoid overlap, each owns a distinct domain layer:

| Project | Core Topic | Scope & Focus |
| :--- | :--- | :--- |
| **CodePilot / DevPilot** | Service Architecture | RAG, multi-agent orchestration, LLM application engineering |
| **Distributed KV Store** | Distributed Systems | Consensus (Raft), leader election, replication logs |
| **Concurrent Task Queue** | **Systems & Concurrency** | **OS concurrency, durability, binary WAL, single-node production hardening** |

## Core Goals
- Build a task queue that could plausibly run in production, not a toy demo.
- Make every design decision defensible in a 30–45 min system design interview.
- Cover DSA, OS, DBMS, and system design concepts through genuine use, not superficial mentions.
- Ship something benchmarked, containerized, and documented with empirical proof of work.

## Non-Goals (Scope Boundaries)
- **Distributed / multi-node operation:** Deliberately single-node. Multi-node would duplicate the Raft/consensus ground already covered by the KV store project. (Possible v2: use the KV store's Raft layer for leader election across queue nodes — noted as a post-MVP stretch goal).
- **Full message-broker feature parity:** No pub/sub topics, no consumer groups, no multi-tenant routing. This is a job queue, not a Kafka clone.
- **A UI/dashboard:** A native `/metrics` Prometheus exposition endpoint is in scope; a custom built frontend is not.

## Engineering Constraints
- Solo project built from scratch.
- Zero external runtime dependencies: every component is implemented using the Go standard library (`sync`, `sync/atomic`, `container/heap`, `hash/crc32`, `net/http`, `log/slog`).
- Every component should be something you personally built and can explain line-by-line — no third-party queue or database wrappers.

## Interview & Resume Framing

### Verified Resume Pitch
*"Built a production-grade, single-node concurrent task queue in Go with zero external runtime dependencies. Engineered an in-memory ring buffer (74.3M ops/sec, 0 heap allocations), a Castagnoli CRC32-C binary Write-Ahead Log (WAL) with atomic snapshot rotation for crash durability, token-bucket rate limiting, and native Prometheus exposition, sustaining 6,550 tasks/sec with 8.70 ms p99 network latency and surviving mid-write SIGKILL process crashes."*

### Key Technical Topics & ATS Keywords
* **Concurrency:** Goroutines, mutexes (`sync.RWMutex`), condition variables (`sync.Cond`), atomics, race detection (`go test -race`).
* **Durability & Storage:** Write-Ahead Logging (WAL), binary framing, CRC32-C checksums, atomic file rotation, log compaction, crash recovery.
* **Scheduling & Systems:** Priority min-heap, token-bucket rate limiter, backpressure load shedding, exponential backoff with full jitter, dead letter queues (DLQ).
* **Observability & Operations:** Prometheus metric exposition, structured logging (`log/slog`), request-ID tracing, distroless Docker containers.