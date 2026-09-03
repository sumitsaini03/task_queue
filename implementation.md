# Implementation Plan: Concurrent Task Queue

Each phase has a goal, concrete tasks, and a verification check. Don't move to the next phase until the current one's verification passes — that's what keeps "make it work" from turning into a vague, unfinishable goal.

## Phase 0 — Setup & Design Validation (Week 1)
**Goal:** A scaffolded repo with CI, and the key architecture decisions locked before code depends on them.

- [ ] Repo scaffolding, Go modules, `Makefile` (build/test/run)
- [ ] GitHub Actions CI: lint (`golangci-lint`), `go test -race ./...`
- [ ] Write 3 short ADRs: language choice, WAL vs. embedded DB for persistence, channel- vs. `sync.Cond`-based blocking for the ring buffer
- [ ] Define the API contract (request/response shapes for submit/status/cancel)

**Verify:** `go build ./...` succeeds; CI is green on the empty scaffold; ADRs committed to `/docs/adr/`.

---

## Phase 1 — Core Queue + Concurrency (Weeks 2–3)
**Goal:** Tasks can be enqueued and executed by concurrent workers, safely.

- [ ] Hand-rolled ring buffer (array + head/tail indices) for immediate tasks, with blocking consumer wake-up
- [ ] Min-heap for scheduled/delayed tasks + background promoter goroutine
- [ ] Worker pool: configurable N workers, pull-execute loop
- [ ] Graceful shutdown: `context.Context` cancellation, SIGTERM handling, drain in-flight tasks before exit

**Verify:**
- Unit tests for enqueue/dequeue, heap ordering
- `go test -race ./...` passes with concurrent producers/consumers
- Benchmark: throughput at 1, 4, 16, 64 workers — confirm it scales, find the knee in the curve

---

## Phase 2 — Durability: WAL & Recovery (Weeks 3–4)
**Goal:** The queue survives a crash without losing or duplicating work.

- [ ] Append-only WAL: every state transition written before being acted on
- [ ] Periodic snapshot + WAL compaction (don't replay from the beginning of time forever)
- [ ] Recovery on startup: replay snapshot + WAL tail, rebuild in-memory state

**Verify:**
- Test that `SIGKILL`s the process mid-run (not graceful) and confirms recovery restores the exact pending-task set
- Measure recovery time for 10k / 100k pending tasks — know your own numbers

---

## Phase 3 — Retry, DLQ, Idempotency (Week 5)
**Goal:** Failures are handled the way production systems handle them, not by crashing.

- [ ] Exponential backoff + jitter on task failure
- [ ] Max-retry threshold → move to dead-letter queue
- [ ] Idempotency key deduplication on task submission

**Verify:**
- Test with an injected-failure task handler confirms correct retry count and eventual DLQ placement
- Duplicate-submission test confirms exactly one execution

---

## Phase 4 — API + Backpressure (Week 6)
**Goal:** The queue is usable over the network and degrades gracefully under overload instead of falling over.

- [ ] HTTP API: submit, status, cancel, list DLQ
- [ ] Bounded queue capacity + configurable overload policy (reject `503` vs. block)
- [ ] Token-bucket rate limiter on the submit endpoint

**Verify:**
- Load test (vegeta/k6) at 2–5x capacity confirms rejection/backpressure kicks in — no OOM, no crash, no silent drops
- p50/p99 latency recorded at normal and overload conditions

---

## Phase 5 — Observability (Week 6–7)
**Goal:** You can answer "how do you know it's healthy in production" with a real dashboard, not "I'd check the logs."

- [ ] Prometheus metrics: queue depth, latency histograms, throughput, worker utilization, error rate
- [ ] Structured logging with per-task correlation IDs

**Verify:** `/metrics` endpoint shows correct values under synthetic load; spot-check a few metrics against known test scenarios (e.g., queue depth after enqueuing 100 tasks with 10 slow workers).

---

## Phase 6 — Polish, Docs, Benchmarks (Week 7–8)
**Goal:** A stranger (or interviewer) can clone the repo and understand it in 10 minutes.

- [ ] README: architecture diagram, quickstart, design rationale, the CS-mapping table from `plan.md`
- [ ] Documented benchmark results (throughput, p50/p99 latency, recovery time) — real numbers, not estimates
- [ ] Fresh-clone test: `git clone` → `make run` → works with no undocumented steps

**Verify:** Have someone else (or you, after a day away) clone and run it cold, following only the README.

---

## Stretch Goals (post-MVP, optional — don't let these creep into the 2–3 month window)
- Lock-free queue variant (CAS-based) benchmarked against the mutex-based one — a strong "I understand the tradeoff, not just the buzzword" story
- Work-stealing across worker pools
- gRPC API alongside HTTP
- Time-wheel scheduler for high-volume delayed tasks (replaces the heap at scale)
- Multi-node mode using the KV store project's Raft layer for leader election — genuinely interesting if time allows, but explicitly out of MVP scope