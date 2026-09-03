# ADR 002: WAL vs. Embedded DB for Persistence

**Status:** Accepted  
**Date:** 2026-09-03  
**Context:** The task queue needs crash-durable state persistence. Tasks must survive `SIGKILL` without loss or duplication.

## Decision

**Hand-rolled write-ahead log (WAL),** not an embedded database (BoltDB, BadgerDB, SQLite).

## Rationale

1. **The WAL is the point.** This project exists to demonstrate understanding of durability mechanics — WAL, fsync, crash recovery, log compaction. Using BoltDB's built-in transactions would hide exactly the concepts this project is meant to surface. An interviewer asking "how do you guarantee durability?" deserves an answer that starts with "I wrote the WAL," not "I imported a library that handles it."

2. **Simpler mental model.** A WAL is an append-only file. State transitions are serialized entries. Recovery is a sequential replay. There's no query language, no B-tree balancing, no page cache to reason about. For a task queue (which only needs key-based lookup, not range queries), a WAL + in-memory hash map is the correct level of complexity.

3. **Predictable performance.** Append-only writes are sequential I/O — the fastest pattern for spinning disks and still optimal for SSDs. An embedded DB introduces compaction pauses, page splits, and GC cycles that are harder to predict and benchmark.

4. **Bounded disk growth via snapshotting.** Periodic snapshots of the full in-memory state + WAL truncation keep disk usage bounded without the complexity of an embedded DB's compaction algorithm.

## Alternatives Considered

| Option | Pros | Cons |
|--------|------|------|
| BoltDB | Mature, battle-tested, ACID transactions | Hides durability mechanics; read-heavy optimization is irrelevant for a write-heavy queue |
| BadgerDB | Fast writes (LSM-tree), Go-native | Adds a large dependency; compaction behavior is opaque |
| SQLite | SQL interface, well-understood | CGo dependency, overkill for key-based task lookup |

## Consequences

- We own the entire persistence stack: encoding, append, fsync, replay, snapshot, compaction.
- We must handle corruption detection (checksums per entry) and truncated-write recovery ourselves.
- WAL format is a custom binary/JSON-lines format — not portable, but doesn't need to be.
- Recovery correctness is verified via crash tests (`SIGKILL` mid-write → restart → verify state).

## WAL Entry Format (preliminary)

```
[4 bytes: entry length][N bytes: JSON payload][4 bytes: CRC32 checksum]
```

Each entry contains: `{type, task_id, timestamp, data}`. The `data` field carries the full task on enqueue and the error string on failure.
