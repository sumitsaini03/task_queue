# Context: Concurrent Task Queue

## What This Is
A production-grade, single-node concurrent task queue — the kind of system that sits behind Celery, Sidekiq, or Bull in a real backend. Workers pull jobs, execute them, retry on failure, and survive a crash without losing work.

## Why This Project
The goal is one system where CS fundamentals, DSA, and system design aren't decorative — they're load-bearing. A task queue forces real decisions in exactly the areas FAANG interviews probe hardest: concurrency correctness, durability, and graceful failure under load.

## Portfolio Fit
This is one of three projects in the current portfolio. To avoid overlap, each owns a different layer:

| Project | Core Topic |
|---|---|
| CodePilot/DevPilot | Service architecture, RAG, multi-agent orchestration |
| Distributed KV Store | Consensus (Raft), distributed systems theory |
| **Concurrent Task Queue** | **OS-level concurrency, durability, single-node production hardening** |

## Goals
- Build a task queue that could plausibly run in production, not a toy demo
- Make every design decision defensible in a 30–45 min system design interview
- Cover DSA, OS, DBMS, and system design concepts through genuine use, not superficial mentions
- Ship something benchmarked and documented, not just "working"

## Non-Goals (for MVP)
- **Distributed / multi-node operation.** Deliberately single-node. Multi-node would duplicate the Raft/consensus ground already covered by the KV store project. (Possible v2: use the KV store's Raft layer for leader election across queue nodes — noted as a stretch goal, not committed.)
- **Full message-broker feature parity.** No pub/sub topics, no consumer groups, no multi-tenant routing. This is a job queue, not a Kafka clone.
- **A UI/dashboard.** A `/metrics` endpoint is in scope; a built frontend is not.

## Constraints
- Timeline: 2–3 months, part-time alongside coursework
- Solo project
- Every component should be something you personally built and can explain line-by-line — no "and then I imported a queue library"

## Interview & Resume Framing
Draft pitch — fill in the real numbers once benchmarked (Phase 6):

*"Built a production-grade concurrent task queue in Go with a hand-rolled write-ahead log for durability, backpressure-aware load shedding, and [p99 latency] enqueue latency at [N] tasks/sec."*

Resume/ATS keywords this project earns: concurrency, goroutines, mutexes, lock-free data structures, write-ahead logging, crash recovery, backpressure, circuit breaker, exponential backoff, idempotency, Prometheus, observability, Go.