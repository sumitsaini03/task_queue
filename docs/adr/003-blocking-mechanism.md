# ADR 003: Blocking Mechanism — `sync.Cond` vs. Channels

**Status:** Accepted  
**Date:** 2026-09-03  
**Context:** When the ring buffer is empty, worker goroutines must block efficiently until new tasks are available. Two Go-idiomatic options exist: `sync.Cond` and buffered channels.

## Decision

**`sync.Cond` for the ring buffer's consumer wake-up mechanism,** with channels reserved for shutdown signaling.

## Rationale

1. **Direct mapping to OS concepts.** `sync.Cond` is Go's condition variable — the same primitive used in pthreads (`pthread_cond_wait`/`pthread_cond_signal`). Using it here creates a direct line from this code to OS interview questions about condition variables, monitors, and spurious wake-ups. A channel-based approach would work but would obscure this mapping.

2. **Broadcast semantics.** `sync.Cond.Broadcast()` wakes all waiting workers — useful for shutdown. With channels, you'd need to close the channel (single-use) or send N signals (error-prone). `Broadcast` is purpose-built for this.

3. **Tight coupling with the mutex.** The ring buffer already needs a `sync.Mutex` for head/tail manipulation. `sync.Cond` wraps that same mutex, so blocking and state checks happen atomically. With channels, you'd need to release the lock, send/receive on the channel, then re-acquire — introducing a window for races.

4. **No channel buffer sizing.** A buffered channel requires choosing a buffer size, which creates a subtle second capacity (channel buffer size vs. ring buffer capacity). `sync.Cond` has no such parameter — it's purely a signaling mechanism.

## Trade-offs

| Aspect | `sync.Cond` | Channel |
|--------|-------------|---------|
| Idiomatic Go | Less common, more "systems" feel | More Go-idiomatic |
| Spurious wake-ups | Must use `for` loop (not `if`) around Wait | Not applicable |
| Select/timeout | Not directly compatible with `select` | Works with `select` and `context.Done()` |
| Interview value | Maps to OS condition variables | Maps to CSP/message passing |

## Handling the `select` Limitation

`sync.Cond.Wait()` cannot be used inside a `select` statement, which means we can't directly combine "wait for task" with "wait for shutdown." The solution:

```go
// Worker loop pattern:
for {
    rb.mu.Lock()
    for rb.size == 0 && !rb.closed {
        rb.cond.Wait()
    }
    if rb.closed && rb.size == 0 {
        rb.mu.Unlock()
        return // clean shutdown
    }
    task := rb.dequeue()
    rb.mu.Unlock()
    
    // execute task with context cancellation for timeout
    execute(ctx, task)
}
```

On shutdown, we set `rb.closed = true` and call `rb.cond.Broadcast()` to wake all blocked workers.

## Consequences

- Workers use a `for` loop around `Wait()` to handle spurious wake-ups correctly.
- Shutdown uses a `closed` flag + `Broadcast()` instead of channel close.
- Context-based task timeout is handled separately from the blocking mechanism.
- This approach is less "Go-idiomatic" but more "systems-correct" — a deliberate trade-off given the project's goals.
