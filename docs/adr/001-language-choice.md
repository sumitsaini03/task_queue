# ADR 001: Language Choice — Go

**Status:** Accepted  
**Date:** 2026-09-03  
**Context:** We need a language for a production-grade, single-node concurrent task queue.

## Decision

**Go.**

## Rationale

1. **Native concurrency primitives.** Goroutines, channels, `sync.Mutex`, `sync.Cond`, and `sync.WaitGroup` map directly onto the concurrency concepts this project is designed to demonstrate. No external threading library, no runtime config — concurrency is a first-class citizen.

2. **Production pedigree.** Kubernetes, Docker, etcd, CockroachDB, NATS — the systems this project draws inspiration from are all written in Go. Using Go means the project speaks the same language as the ecosystem it references.

3. **Performance floor.** Go compiles to native machine code with a lightweight garbage collector. For a task queue that needs to benchmark throughput and p99 latency, we need a language where the runtime doesn't dominate the profile. Go gives us that without the compile-time complexity of Rust.

4. **Buildability within timeline.** Rust would produce a more impressive artifact but significantly increases implementation time (borrow checker, lifetimes, async runtime choices). Given the 2–3 month part-time constraint, Go hits the sweet spot of "production-realistic without timeline risk."

5. **Interview alignment.** Go's concurrency model (goroutines ≈ OS threads, channels ≈ IPC, mutexes ≈ kernel synchronization) maps cleanly onto OS and systems interview questions. Every design decision in this project has a direct analog in the concepts FAANG interviews probe.

## Alternatives Considered

| Language | Pros | Cons |
|----------|------|------|
| Rust | Memory safety, zero-cost abstractions, impressive resume signal | Steep learning curve, async ecosystem fragmentation, timeline risk |
| Java | Mature concurrency libraries, common in enterprise | Verbose, JVM startup/memory overhead, less "systems" feel |
| C++ | Maximum control, impressive | Unsafe by default, much slower to build correctly, overkill for this scope |

## Consequences

- All code uses Go idioms: table-driven tests, `error` returns, `context.Context` for cancellation.
- The project is buildable by anyone with `go 1.21+` installed.
- We lose Rust's memory-safety guarantees but gain Go's race detector (`go test -race`) as a substitute for concurrency correctness verification.
