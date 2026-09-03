# Resume Ready Bullets — TaskQueue

Copy and paste these directly onto your resume or LinkedIn. Each bullet is mathematically grounded in real benchmarks from the test suite and follows the standard FAANG impact formula: **what you built → nontrivial technical decision → quantified result.**

---

### Option 1: Distributed Systems / Backend Focus
* **Engineered a crash-resilient concurrent task queue in Go** with zero external dependencies, implementing a binary Write-Ahead Log (WAL) with Castagnoli CRC32-C checksumming and atomic snapshot rotation; achieved **1.6M log writes/sec** and recovered **~164,000 tasks/sec** from disk with automated torn-write repair.
* **Designed an in-memory bounded ring buffer using `sync.Cond` condition variables** and an interruptible priority min-heap scheduler; sustained **74.3M ops/sec** (16.8 ns/op, **0 heap allocations**) and handled **3,200 concurrent tasks across 16 workers** with zero data races under `-race`.
* **Built a REST API with token-bucket rate limiting and structured JSON tracing**, sustaining **6,550 req/sec** over HTTP with **4.6 ms median latency** (p50) and **8.7 ms 99th percentile latency** (p99) under saturation load testing with 0% error rate.
* **Developed an automated external-process chaos testing suite** exercising violent `SIGKILL` termination mid-write; validated 100% durability of acknowledged tasks on restart, coupled with exponential backoff retries and Dead-Letter Queue (DLQ) routing.
* **Containerized with multi-stage Go build onto distroless static runtime**, producing a minimal **9.5 MB production image** with integrated Prometheus exposition metrics and importable Grafana dashboards.

---

### Option 2: Concise 3-Bullet Summary
* **Built a crash-resilient concurrent task queue in Go** featuring a custom binary WAL (CRC32-C framing, atomic snapshot checkpointing, and torn-write auto-repair), sustaining **74.3M ops/sec** on in-memory ring-buffer operations and recovering **~164K tasks/sec** upon cold restart.
* **Architected a lock-safe task state machine and priority min-heap scheduler** managing delayed execution and exponential backoff retries with DLQ, verified clean across 32 concurrent producers under `go test -race` with 0 data races.
* **Developed an HTTP REST service with token-bucket rate limiting and native Prometheus metrics**, delivering **6,550 req/sec** sustained network throughput at **4.6 ms median latency** and packaged into a **9.5 MB distroless container image**.
