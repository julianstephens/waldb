# WALDB

WALDB is a small, embedded, **single-writer** key–value store with **durable commits** backed by a write-ahead log (WAL).

---

## Features

1. **Durable commit:** If `Put/Delete/Commit` returns success, the mutation is preserved after process crash and restart (subject to `FsyncOnCommit` configuration).
2. **Atomic batches:** A batch commit is all-or-nothing across keys after restart. No partial batch effects are ever visible after recovery.
3. **Write-ahead rule:** WAL records for a commit are persisted before the commit is acknowledged to the caller.
4. **Deterministic recovery:** Given the same WAL prefix, recovery produces exactly the same final state.
5. **Idempotent replay:** Replaying the same WAL records multiple times yields the same state (no double-apply effects).
6. **No silent corruption:** WAL record corruption/truncation is detected. WALDB never “guesses”; recovery stops at the last valid boundary.
7. **Single writer:** Only one WALDB process may own a DB directory for write access at a time.
8. **Repair is explicit:** WALDB does not automatically discard or rewrite user data except through explicit repair commands.

---

## Supported Failure Model

WALDB explicitly handles:

- process crash (`kill -9`)
- power loss (equivalent to sudden crash)
- torn/partial writes (truncated WAL tail)
- disk full (`ENOSPC`) during write
- corrupted WAL bytes (checksum mismatch)
- concurrent open attempts (lock failure)

Out of scope:

- Byzantine storage/hardware faults beyond detectable corruption
- bit rot prevention over long periods (no scrubber)
- multi-process writes or shared directory usage
- guarantees under filesystem bugs

---

## DB Directory Layout (On-Disk Contract)

WALDB stores all files in a single directory (`<dir>`). v0 supports segmented WAL files, with no compaction/snapshots yet.

```text
<dir>/
  LOCK
  MANIFEST.json
  wal/
    wal-000001.log
    wal-000002.log
    ...
  wal/backup/         (created by repair operations)
```

Rules:

- WAL segments are append-only.
- WAL segments are ordered by monotonically increasing segment id.
- WALDB replays WAL segments in segment-id order.
- v0 does not delete WAL segments automatically (they accumulate until snapshots/compaction exist).

---

## Manifest Contract and Versioning

WALDB maintains a small manifest file at `<dir>/MANIFEST.json`.

### Manifest Purpose

- Stores format versioning information and critical options required to correctly interpret on-disk data.
- Enables `doctor` to validate compatibility and prevents “opening with wrong assumptions”.

### Manifest Fields (v0 minimum)

- `format_version` (int): initial value `1`
- `fsync_on_commit` (bool)
- `max_key_bytes` (int)
- `max_value_bytes` (int)
- `wal_segment_max_bytes` (int)
- `wal_next_segment_id` (int) (optional; used only for naming convenience)
- `wal_app_dir` (string) (optional; defaults to `~/.waldb`)
- `wal_log_max_size` (int) (optional; defaults to `10 MiB`)
- `wal_log_max_backups` (int) (optional; defaults to `3`)

### Manifest Write Rule (Atomic)

MANIFEST updates must be atomic:

1. write `<dir>/MANIFEST.json.tmp`
2. `fsync` the temp file
3. rename temp to `<dir>/MANIFEST.json`
4. `fsync` the parent directory `<dir>`

### Compatibility Rules

- If `MANIFEST.json` is missing: **error** (user should run `waldb init`).
- If `format_version` is newer than the running binary supports: **error** (refuse to open).
- If `format_version` is older: WALDB may open if it explicitly supports migration, otherwise error.

---

## Limits and Input Validation

WALDB enforces hard limits on keys/values to keep WAL parsing safe and deterministic.

Defaults (configurable through Options + persisted in manifest):

- `MaxKeyBytes = 4096` (4 KiB)
- `MaxValueBytes = 4_194_304` (4 MiB)

Rules:

- Empty key: **not allowed**
- Empty value: **allowed**
- Oversize key/value: operation fails and **no WAL records are written**

---

## API

### Get(key)

`Get(key)` returns the most recent **committed** value for the key, excluding uncommitted or incomplete transactions.

Read-your-writes: within a process, after `Put/Delete/Commit` returns success, subsequent `Get` will observe the new value.

### Put/Delete

`Put(key,value)` and `Delete(key)` are implemented as **single-operation transactions** (i.e., they commit immediately).

On success they guarantee:

- the mutation is committed according to the durability configuration (see Durability Model)
- it will be visible after restart under the same durability configuration

### Batch / Commit(Batch)

A batch commit:

- is **atomic** (all-or-nothing)
- applies operations in the order provided (for repeated writes to same key within a batch, last operation wins)
- will never partially apply after restart

Within a batch/txn:

- repeated PUTs to the same key: last PUT wins
- `PUT` then `DEL`: deleted
- `DEL` then `PUT`: put wins

`Commit(batch)` returns success iff the transaction’s COMMIT record is written and the durability boundary is satisfied.

`Commit(batch)` may fail due to:

- invalid key/value size
- I/O errors (including `ENOSPC`)
- failure to acquire DB write lock
- corruption detected in existing DB files (open-time checks)

---

## Durability Model and fsync Boundary

WALDB supports configurable durability:

- If `FsyncOnCommit=true` (default), a successful commit is durable across power loss.
- If `FsyncOnCommit=false`, commits are durable only to OS buffers; recent commits may be lost on power loss, but should survive a graceful process exit.

### Semantics

**If `FsyncOnCommit=true`, success means:**

- all WAL bytes for the commit (including `COMMIT_TXN`) have been written and `fsync`’d to stable storage.

**If `FsyncOnCommit=false`, success means:**

- WAL bytes have been written to the file descriptor (kernel buffers), but may not survive power loss.

### Ordering Rule (Write Path)

For any successful commit, WALDB performs:

1. validate inputs (key/value size limits)
2. append WAL records for the transaction:
   - `BEGIN_TXN`
   - `PUT/DEL` records
   - `COMMIT_TXN`

3. flush WAL file to OS (buffered write completion)
4. if `FsyncOnCommit=true`, call `WALFile.Sync()` (fsync)
5. apply mutation(s) to the in-memory memtable
6. return success

The commit is acknowledged **only after** step 4 (if enabled) and step 5 completes.

---

## WAL Specification

### WAL Segments

- WAL segments live in `<dir>/wal/` and are named `wal-%06d.log` (zero-padded).
- WAL segments are created in increasing order, starting at `wal-000001.log`.
- WAL segment rotation occurs when the active segment exceeds `wal_segment_max_bytes` (legacy default: 256 MiB).
- Rotation creates a new segment file; subsequent records append to the new segment.

### Record Types

- `BEGIN_TXN(txn_id)`
- `PUT(txn_id, key, value)`
- `DEL(txn_id, key)`
- `COMMIT_TXN(txn_id)`

### Transaction ID Rules

- `txn_id` is generated by WALDB (not user-provided).
- `txn_id` is a monotonically increasing integer within the DB, starting at 1.
- `txn_id` reuse is forbidden.
- WAL replay assumes `txn_id` uniqueness.

### WAL Record Framing Rules (Required for Implementability)

WAL records must be sequentially parseable and safely truncatable.

**Record framing:**

Each record is encoded as:

- `len` (u32, little-endian): byte length of `[type + payload]`
- `type` (u8): record type enum
- `payload` (len-1 bytes): type-specific payload
- `crc` (u32, little-endian): checksum over `[type + payload]`

**Checksum:**

- Algorithm: CRC32-C (Castagnoli)
- Computed over `[type + payload]` only (does not include `len` or `crc` field)

**Constraints:**

- `len` MUST be `>= 1`
- `len` MUST be `<= MaxRecordBytes` (hard limit; default 16 MiB)
- record is invalid if:
  - `len` cannot be read (EOF)
  - `len` exceeds MaxRecordBytes
  - cannot read `type+payload+crc` fully (EOF)
  - checksum mismatch

**Stop rule:**

- On the first invalid or truncated record, recovery stops. Remaining WAL tail is ignored.

### Orphan Record Policy

Records outside a well-formed transaction are treated as corruption:

- `PUT/DEL` with no active `BEGIN_TXN` for that `txn_id`: **invalid WAL** → stop recovery
- `COMMIT_TXN` with no prior `BEGIN_TXN`: **invalid WAL** → stop recovery
- Any record following a committed `COMMIT_TXN` for the same `txn_id`: **invalid WAL** → stop recovery

---

## WAL Replay / Recovery Contract

Recovery uses the WAL only. On open, WALDB scans and replays WAL records sequentially from the beginning of the WAL stream (segment-id order), rebuilding the materialized state in memory.

### Replay Rule

Replay applies **only committed transactions** to rebuild the final state. Replay requires the start boundary’s segment to exist, even if the log is empty.

- `BEGIN_TXN` opens a transaction context
- `PUT/DEL` are associated to the open txn
- `COMMIT_TXN` finalizes and applies the txn’s mutations

### Idempotency Rule

WALDB recovery must be idempotent:

- a committed transaction is applied exactly once in the recovered state
- applying the same WAL prefix multiple times yields the same final state

This is achieved by basing state solely on WAL order and commit boundaries (not on “already applied” markers).

### Replay State Machine and Invariants

#### Canonical per-txn states

For each `txn_id`, recovery tracks one of:

- **Absent**: no `BEGIN_TXN(txn_id)` has been seen
- **Open**: `BEGIN_TXN(txn_id)` has been seen; accumulating ops
- **Committed**: `COMMIT_TXN(txn_id)` has been seen and applied
- (**Ignored** is not a runtime state; it’s the outcome for any `Open` txn when replay ends before its COMMIT)

#### Global rule

Recovery processes records in **WAL order** (segment-id order, then increasing offset). It is not permitted to reorder or “skip invalid and continue.” (“Stop-at-first-invalid.”)

#### Transition and validity table

##### Table A — Allowed transitions and effects

| Current state (for `txn_id`) | Next record          | Allowed? | New state      | Effect                                        |
| ---------------------------- | -------------------- | -------: | -------------- | --------------------------------------------- |
| Absent                       | `BEGIN_TXN(txn_id)`  |        ✅ | Open           | Start txn context                             |
| Open                         | `PUT(txn_id, …)`     |        ✅ | Open           | Buffer op                                     |
| Open                         | `DEL(txn_id, …)`     |        ✅ | Open           | Buffer op                                     |
| Open                         | `COMMIT_TXN(txn_id)` |        ✅ | Committed      | **Apply buffered ops atomically to memtable** |
| Committed                    | (end of log)         |        ✅ | Committed      | No effect                                     |
| Open                         | (end of log)         |        ✅ | Open → Ignored | **Ignore txn entirely** (no apply)            |

##### Table B — Invalid sequences (semantic corruption)

| Current state | Next record          | Classification          | Required behavior               |
| ------------- | -------------------- | ----------------------- | ------------------------------- |
| Absent        | `PUT/DEL(txn_id, …)` | **Orphan op**           | **Stop recovery** (invalid WAL) |
| Absent        | `COMMIT_TXN(txn_id)` | **Orphan op**           | **Stop recovery**               |
| Open          | `BEGIN_TXN(txn_id)`  | **Duplicate begin**     | **Stop recovery**               |
| Committed     | `PUT/DEL(txn_id, …)` | **After-commit record** | **Stop recovery**               |
| Committed     | `COMMIT_TXN(txn_id)` | **Double commit**       | **Stop recovery**               |

#### Monotonicity invariants (global txn_id rules)

##### Table C — txn_id monotonicity expectations

Let `lastCommittedTxnID` be the highest txn applied so far.

| Event                            | Constraint                           | If violated                     |
| -------------------------------- | ------------------------------------ | ------------------------------- |
| `BEGIN_TXN(txn_id)` encountered  | `txn_id > lastCommittedTxnID`        | Stop recovery (**corrupt WAL**) |
| `COMMIT_TXN(txn_id)` encountered | must match the currently Open txn id | Stop recovery (**corrupt WAL**) |
| Replay completes                 | `NextTxnID = lastCommittedTxnID + 1` | (output invariant)              |

This is the minimal set that prevents replay from “accepting” replays that would break the allocator contract.

---

## Transaction Semantics (Atomicity)

### What makes a transaction committed?

A transaction is committed iff:

- a `BEGIN_TXN(txn_id)` record is present
- followed by zero or more `PUT/DEL` records for the same `txn_id`
- followed by a valid `COMMIT_TXN(txn_id)` record
- all records are valid (length + checksum) and fully readable

If a transaction has `BEGIN_TXN` but no valid `COMMIT_TXN`, it is **ignored entirely** during recovery.

Records for a txn_id after COMMIT:

- invalid by construction; WALDB treats these as corruption and recovery stops at first invalid boundary.

### Crash Scenarios

| Crash point                                         | Example                               | Post-restart result                                                      |
| --------------------------------------------------- | ------------------------------------- | ------------------------------------------------------------------------ |
| Before BEGIN_TXN                                    | no records written                    | no effect                                                                |
| After BEGIN_TXN, before PUT/DEL                     | `BEGIN_TXN` only                      | txn ignored                                                              |
| After some PUT/DEL, before COMMIT_TXN               | `BEGIN + PUT...`                      | txn ignored                                                              |
| After COMMIT_TXN written, before fsync (if enabled) | `BEGIN + PUT + COMMIT`, not fsynced   | if power loss: commit may be lost; if process crash only: likely present |
| After fsync, before memtable apply                  | WAL durable but in-memory not updated | commit applied during recovery                                           |
| Mid-record (torn write)                             | truncated payload/checksum            | stop at last valid record; incomplete txn ignored                        |

---

## Corruption / Truncation Policy

### What counts as an invalid record?

A record is invalid if any of the following holds:

- length prefix is missing, truncated, or exceeds configured maximum record size
- checksum does not match payload bytes
- record type is unknown
- required fields are missing/truncated (e.g., key length exceeds available bytes)

### Recovery behavior on invalid record

Policy: **Stop-at-first-invalid.**

Recovery scans sequentially and stops at the first invalid or truncated record; the remainder of the WAL tail is ignored.

Rationale:

- safest behavior (never risks mis-parsing)
- supports precise repair by truncating at last valid boundary
- avoids “skip-invalid” complexity and ambiguous resynchronization

### Repair stance

WALDB does **not** auto-truncate WAL on open.

- `doctor` is read-only.
- truncation and rebuild actions happen only via `repair`.

---

## Concurrency and Locking

### Single-writer enforcement

WALDB uses a `LOCK` file with an advisory OS file lock.

Behavior:

- if lock acquisition fails: `Open` returns an error indicating the DB is already in use
- on crash, OS releases lock automatically
- WALDB does not implement “stale lock detection”

### In-process concurrency

- `Get` may be called concurrently
- `Put/Delete/Commit` are serialized behind a single write mutex
- reads during writes are permitted; reads see the latest applied in-memory state

Read-only multi-process access is out of scope for v0 (no `OpenReadOnly` yet).

---

## Operational Tools Contract

### doctor

Doctor checks:

- `MANIFEST.json` parse and version compatibility (required)
- WAL segment continuity and naming scheme
- WAL scan integrity:
  - fast scan: headers + checksums
  - full scan: validates payload fields and lengths

Doctor exit codes:

- `0` OK (no issues)
- `1` warnings (non-fatal anomalies, e.g., truncated tail record, temp files present)
- `2` errors (corruption, missing required files, invalid checksums, incompatible manifest)

Severity guide:

- Truncated tail record: **warning** (common after crash)
- Checksum mismatch in middle of WAL stream: **error**
- Missing manifest: **error**
- Unknown WAL segment naming: **error**

Doctor is **read-only**.

### repair

Repair modes:

- `truncate-wal`: truncate WAL tail to the last valid record boundary

Safety rules:

- repair must require explicit user confirmation for destructive actions
- before truncation, WALDB must rename/copy the original WAL file/segment(s) to a backup location (e.g., `wal/backup/`)
- repair should output the boundary (segment + offset) used for truncation

---

## Examples

### Example A — Single put survives crash

Steps:

1. `Put("a","1")` returns success with `FsyncOnCommit=true`.
2. process crashes immediately after returning.

Expected after restart:

- `Get("a")` returns `"1"`.

### Example B — Partial txn does not apply

Steps:

1. Begin a batch with `Put("a","1")` and `Put("b","2")`.
2. Crash occurs after WAL has written BEGIN and PUT records but before COMMIT record is fully written.

Expected after restart:

- neither `"a"` nor `"b"` is present (batch ignored).

---

## Glossary

- **WAL (Write-Ahead Log):** append-only log of mutations written before they are applied to the materialized state.
- **fsync boundary:** the point at which WAL bytes are forced to stable storage via `File.Sync()` and may survive power loss.
- **torn write:** a partially written record (often due to crash mid-write), detected by invalid length/checksum.
- **tombstone:** a marker representing deletion of a key (represented as a delete record in WAL).
