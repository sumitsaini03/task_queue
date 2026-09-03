# ADR 004: Binary Framing and Checksummed Records for Write-Ahead Log (WAL)

**Status:** Accepted  
**Date:** 2026-09-03  
**Context:** The task queue requires single-node crash durability where enqueued tasks and state transitions survive abnormal termination (`SIGKILL`, power failure). In Phase 0/1, stubs existed. For Phase 2, we needed a robust on-disk format distinguishing clean records, torn writes at EOF, and middle-of-log bit corruption.

## Decision

We implement a custom **binary-framed record format** with CRC32-C checksums and 64-bit sequence numbers:

```
+----------+----------+----------+----------+---------+----------+
| magic(2) | len(4)   | seq(8)   | crc32(4) | type(1) | data(N)  |
+----------+----------+----------+----------+---------+----------+
```

1. **Magic Header (2 bytes - `0xAA4C`):** Enables record boundary detection and corrupted head detection.
2. **Length Prefix (4 bytes uint32 LE):** Total byte length of body (`seq + crc32 + type + data`).
3. **Monotonic Sequence (8 bytes uint64 LE):** Tracks ordering across WAL rotations and snapshot offsets.
4. **CRC32-C Checksum (4 bytes uint32 LE):** Castagnoli polynomial checksum computed over `type + data`.
5. **Type (1 byte uint8):** Enqueue, Start, Complete, Fail, Retry, Dead, Cancel, Snapshot.
6. **Data (N bytes):** JSON-encoded payload.

## Recovery Guarantees

* **Torn Write at Tail:** If the process dies mid-write, replay detects an incomplete magic, length, or body, stops without error, and truncates the file back to the last valid boundary (repairing the log).
* **Mid-File Corruption:** A CRC mismatch or corrupted magic before EOF returns `ErrCorrupted` and aborts recovery. The queue does NOT silently lose corrupted history.
* **Durability Guarantee:** Under `sync_policy: every`, `Append()` performs `writer.Flush()` and `os.File.Sync()` before returning. An HTTP `201 Created` is never returned until the WAL append has been persisted to disk.

## Alternatives Considered

1. **Pure JSON Lines (`.jsonl`):** Human readable, but parsing line breaks is slow, lacks checksumming against flipped bits, and has ambiguous behavior on partial writes.
2. **SQLite / BadgerDB / BoltDB:** Adds large external dependencies and obscures the core log-structured persistence learning objectives.
