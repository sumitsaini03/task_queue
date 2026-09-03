# ADR 005: Concurrency-Safe Task State Machine & Transition Rules

**Status:** Accepted  
**Date:** 2026-09-03  
**Context:** In a concurrent queue with background workers, delayed schedulers, retries, cancellations, and WAL logging, multiple goroutines inspect and transition task states. Without strict rules, race conditions (e.g. worker overwriting a retried task as completed) can occur.

## Decision

We establish an explicit **Task State Machine** governed by `TaskState.CanTransitionTo()` and `Task.SetState()` / `Task.GetState()` under mutex lock.

### States

* `PENDING`: Task waiting in ring buffer for worker pickup
* `SCHEDULED`: Task waiting in min-heap for delay to elapse
* `RUNNING`: Task currently executing in worker pool
* `RETRYING`: Task failed execution; waiting for backoff delay calculation
* `COMPLETED`: Terminal. Handler succeeded
* `FAILED`: Terminal. Non-retryable failure or unrecoverable error
* `DEAD`: Terminal. Exhausted retries; moved to Dead Letter Queue (DLQ)
* `CANCELLED`: Terminal. Cancelled by user before execution
* `EXPIRED`: Terminal. Execution deadline elapsed before execution

### Transition Diagram

```
                 +---------------> CANCELLED / EXPIRED
                 |                     ^
[SUBMIT] -> PENDING <-------+          |
               |            |          |
               v            |          |
            RUNNING      SCHEDULED ----+
             /   \          ^
            /     \         |
           v       v        |
      COMPLETED  RETRYING --+
                     |
                     v
               DEAD / FAILED
```

### Invariants

1. **Terminal State Immutability:** Once in `COMPLETED`, `FAILED`, `DEAD`, `CANCELLED`, or `EXPIRED`, no transitions are permitted.
2. **Cancellation Restriction:** Tasks can only be cancelled from `PENDING` or `SCHEDULED`. A running task cannot be cancelled (avoids indeterminate state in external execution).
3. **Execution Ownership:** Workers only transition `RUNNING` tasks to terminal states if the engine's handler wrapper did not already transition them (e.g. to `SCHEDULED` for retry or `DEAD` for DLQ).
