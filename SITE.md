---
complete: false
---

# WalDB

WalDB is a key-value store built on top of a write-ahead log (WAL). The repo provides a simple CLI for initializing and managing a WalDB instance. I also used this project as an opportunity to experiment with the role of AI in my learning process. I used ChatGPT to create the DESIGN.md document, which outlines the architecture and design decisions behind WalDB. I also had ChatGPT break the development work into GitHub issues, which I then implemented and committed to the repo. This approach allowed me to focus on writing code and learning by doing, while still benefiting from the guidance and structure provided by the design document and issue tracking.

## Goals

- Understand the design and implementation of a WAL-based key-value store.
- Practice writing Go code that interacts with the filesystem and handles errors gracefully.

## Architecture

See the [design document](https://github.com/julianstephens/waldb/blob/main/docs/DESIGN.md) for an in-depth overview of the architecture and design decisions behind WalDB. The codebase is organized into several packages:

- `internal/waldb`: Provides top-level DB initialization function.
- `internal/waldb/db`: Database management and operations.
- `internal/waldb/wal`: Write-ahead log management.
- `internal/waldb/manifest`: Manifest file management for user configuration and metadata.
- `internal/waldb/recovery`: Recovery logic for replaying the WAL and restoring DB state.
- `internal/waldb/txn`: Transaction management for batching operations and ensuring atomicity.
- `internal/waldb/memtable`: In-memory data structure for storing key-value pairs before flushing to disk.
- `internal/cli`: Command-line interface for interacting with WalDB.

```mermaid
graph TD
    subgraph CLI["cmd/waldb — CLI"]
        CMD["commands\n(init, put, get, delete, doctor)"]
    end

    subgraph DB["internal/waldb/db — DB"]
        DB_CORE["DB\n• Open / Init / Close\n• Put / Get / Delete\n• File lock (LOCK)\n• writeMu / lifecycleMu"]
    end

    subgraph TXN["internal/waldb/txn — Transaction Layer"]
        BATCH["Batch\n(Put / Delete ops)"]
        WRITER["Writer\n• Commit(batch)\n• Allocates TxnID\n• Encodes WAL records"]
        ID["IDAllocator\n(monotonic TxnID)"]
        WRITER --> BATCH
        WRITER --> ID
    end

    subgraph WAL["internal/waldb/wal — Write-Ahead Log"]
        LOG["Log\n• Manages segments\n• Active segment rotation"]
        SEG_APP["SegmentAppender\n• Buffered append\n• Flush / FSync"]
        SEG_READ["SegmentReader\n• SeekTo / Reader"]
        LOG --> SEG_APP
        LOG --> SEG_READ
        subgraph RECORD["wal/record — Record Layer"]
            FRAME["FrameCodec\n(encode / decode)"]
            PAYLOAD["PayloadCodec\n(BeginTxn, KV, CommitTxn)"]
            CRC["Checksum\n(CRC-32)"]
            FRAME --> CRC
            FRAME --> PAYLOAD
        end
        SEG_APP --> RECORD
        SEG_READ --> RECORD
    end

    subgraph RECOVERY["internal/waldb/recovery — Recovery"]
        REPLAY["Replay\n• Reads WAL segments in order\n• Rebuilds memtable state\n• Detects tail status\n  (valid/corrupt/truncated)"]
        STATE["ReplayState\n• In-flight txn tracking\n• Apply / discard logic"]
        REPLAY --> STATE
    end

    subgraph MEM["internal/waldb/memtable — Memtable"]
        TABLE["Table\n• RWMutex map[string]Entry\n• Put / Get / Delete\n• Tombstones"]
    end

    subgraph MANIFEST["internal/waldb/manifest — Manifest"]
        MF["Manifest\n• MANIFEST.json\n• Format version\n• Config options\n• Atomic write (tmp → rename)"]
    end

    subgraph KV["internal/waldb/kv — KV Types"]
        OP["Op\n(OpPut / OpDelete)"]
    end

    subgraph LOGGER["internal/logger — Logger"]
        LG["Logger interface\n(NoOpLogger / impl)"]
    end

    %% Top-level flow
    CMD --> DB_CORE
    DB_CORE --> MF
    DB_CORE --> WRITER
    DB_CORE --> MEM
    DB_CORE --> LOG
    DB_CORE --> REPLAY

    WRITER --> LOG
    BATCH --> OP
    REPLAY --> MEM
    REPLAY --> LOG

    DB_CORE --> LG
    WRITER --> LG
    REPLAY --> LG
    LOG --> LG

    %% On-disk artifacts (styled as notes)
    LOCK[(LOCK\nfile)]
    MANIFEST_FILE[(MANIFEST.json)]
    SEG_FILES[(wal/segment-XXXXXXXX.wal\n...)]

    DB_CORE -.acquires.-> LOCK
    MF -.reads/writes.-> MANIFEST_FILE
    SEG_APP -.appends.-> SEG_FILES
    SEG_READ -.reads.-> SEG_FILES
```
