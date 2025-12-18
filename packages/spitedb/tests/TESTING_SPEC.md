# SpiteDB Testing Spec (First Principles)

This document defines what SpiteDB **must** do, independent of current implementation. Tests should be written to this spec (TDD), not to “whatever the code happens to do today”.

## Core invariants (must never be violated)

### Ordering + durability
- **Single logical writer**: all commits serialize through one writer connection.
- **Durable ordering**: `global_pos` is strictly increasing, never reused, and has **no gaps** across committed events.
- **Per-stream correctness**: for any `(tenant, stream_id)`, `stream_rev` is strictly increasing, starts at 1, and has **no gaps**.
- **Atomicity**: a successful append makes *all* its events visible; a failed append makes *none* visible.

### Exactly-once semantics
- **Idempotency**: repeating an append with the same `command_id` must never create new events.
- **Crash window safety**: if the writer commits but the client doesn’t receive the response (timeout/cancel), retrying the same `command_id` must return the original result.

### Reads
- **Stream reads**: `read_stream(stream, from_rev, limit)` returns events in revision order, inclusive of `from_rev`, and returns at most `limit` events.
- **Global reads**: `read_global(from_pos, limit)` returns events in global order, inclusive of `from_pos`, and returns at most `limit` events.
- **Corruption handling**: corrupted indexes or blobs must yield a structured error (never UB/panic, never silently wrong bytes).

### GDPR deletion (tombstones)
- **Immediate filtering**: a tombstoned revision range is filtered from *all* reads immediately after the tombstone commit.
- **Tenant deletion**: a tombstoned tenant is filtered from *all* reads immediately.
- **No retroactive mutation**: tombstones do not rewrite history; they only hide data from reads.
- **Idempotent deletes**: repeating the same delete `command_id` returns the original delete result.

## Test categories

### Integration tests (black-box)
Use the public surface (writer handle + reader functions, or async `SpiteDB`) and validate the invariants end-to-end against SQLite.

Required coverage:
- Global ordering + no gaps (mixed event counts, sequential + concurrent).
- Per-stream monotonic revisions + no gaps.
- Retry semantics when the client times out/cancels.
- Transaction isolation: conflicts in one command must not break others.
- Restart recovery: positions and revisions continue after reopening.
- Tombstone filtering for stream ranges and tenants (including overlaps).
- Data corruption and tamper detection (bad byte offsets, modified ciphertext).

### Unit tests (white-box)
Use module-level tests to validate pure logic and boundary handling:
- Tombstone range containment + filtering.
- Codec offsets and round-trip encode/decode.
- Cryptor integrity (wrong key/batch id/tamper fails).
- Bounds checks for event extraction (invalid offsets/lengths).

## Future/Deferred tests (should exist but may be `#[ignore]` until implemented)
- Epoch fencing correctness (split-brain prevention).
- Rolling deploy leader handoff behavior.

