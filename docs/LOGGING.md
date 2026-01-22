# WALDB Logging Strategy

This document outlines the logging strategy and conventions for the WALDB project, ensuring consistent, meaningful, and efficient logging across all components.

## Design Principles

- **Observability-First**: Strategic logging at key decision points and state transitions
- **Zero Overhead**: NoOpLogger pattern means production deployments can run with zero-cost logging when disabled
- **Structured Logging**: Variadic field-based logging enables rich filtering and aggregation
- **Single-Writer Pattern**: Only one goroutine (txn writer) modifies WAL to simplify reasoning about state

## Logger Architecture

The logger abstraction provides a unified interface across all waldb components:

```go
// Logger defines logging operations with structured fields
type Logger interface {
    Debug(msg string, fields ...interface{})
    Info(msg string, fields ...interface{})
    Warn(msg string, fields ...interface{})
    Error(msg string, err error, fields ...interface{})
}

// Closeable optional interface for graceful shutdown
type Closeable interface {
    Close() error
}
```

### Implementations

| Logger | Purpose | Output |
|--------|---------|--------|
| **ConsoleLogger** | CLI/development logging | stdout/stderr with timestamps |
| **FileLogger** | Persistent rotating logs | Files (configurable max size and backup count) |
| **MultiLogger** | Composite logging | Broadcasts to multiple loggers |
| **NoOpLogger** | Testing/disabled | Discards all messages (zero cost) |

## Log Levels & Field Conventions

### Debug
**Purpose**: Low-volume operational details for debugging and performance analysis.

**Strategic Points**:
- Segment processing (segment ID, frame count)
- Transaction lifecycle (TXN allocation, operation processing)
- Get operations (key size, found status)
- Record appends to WAL

**Field Naming**:
```
seg         uint64      Segment ID
txn         uint64      Transaction ID
op_index    int         Operation index in batch
key_size    int         Size of key in bytes
value_size  int         Size of value in bytes
offset      int64       Byte offset in file/segment
payload_size int        Size of encoded record payload
count       int         Item/frame count
found       bool        Boolean existence flag
index       int         Index in array/sequence
total       int         Total count for progress
frames_processed int   Total frames read from segment
```

### Info
**Purpose**: Significant milestones and lifecycle events.

**Strategic Points**:
- Database open/close/initialization
- Recovery completion
- Transaction commit success
- Segment operations

**Sample Events**:
```
"opening database"              path, fsync_on_commit
"database opened successfully"  path
"closing database"              path
"starting recovery"             seg_count
"recovery complete"             next_txn_id, last_committed_txn_id
"commit successful"             txn, count
"appended record to segment"    seg, record_type, offset, payload_size
```

### Warn 
**Purpose**: Recoverable issues and degraded states.

**Strategic Points**:
- Segment open failures (attempt recovery)
- Batch validation failures
- Checksum/truncation handling

**Sample Events**:
```
"batch validation failed"       count, reason="invalid_batch"
"failed to open segment"        seg, reason="open_error"
```

### Error
**Purpose**: Operation failures with root cause and context.

**Strategic Points**:
- All error paths include error object + coordinates
- Include file path/segment ID for context
- Preserve operation stage information

**Sample Events**:
```
"failed to initialize database"         path
"commit failed"                         path, count
"failed to apply batch to memtable"     path, txn
"recovery failed"                       path
"failed to encode/append record"        txn, op_index, key_size
"failed to open WAL log"               path
"failed to close WAL log"               path
"failed to create transaction allocator" path, next_txn_id
"start segment not found"              seg
```

## Best Practices

1. **Never log inside loops** - Log summary metrics instead (frame count, op count)
2. **Correlate by transaction/segment ID** - Use `txn`, `seg` fields consistently
3. **Include coordinates in errors** - Segment ID, offset, operation index
4. **Log at origin** - Report errors where they occur, not in every layer
5. **Use variadic fields** - Pairs of key/value for structured logging
6. **Minimize allocation** - NoOpLogger has zero overhead

## Usage Patterns

### Database Layer
```go
db, err := OpenWithOptions(path, opts, logger)
if err != nil {
    logger.Error("failed to open database", err, "path", path)
    return err
}
defer db.Close()

value, ok := db.Get(key)
logger.Debug("get operation", "key_size", len(key), "found", ok)
```

### CLI with Multi-Logger
```go
// Create console logger from CLI options
consoleLogger := logger.NewConsoleLogger(opts.Level)

// Create file logger if configured
var multiLogger logger.Logger = consoleLogger
if !opts.Stream {
    fileLogger, _ := logger.NewFileLogger("logs", "waldb.log", 100, 5)
    multiLogger = logger.NewMultiLogger(fileLogger, consoleLogger)
}

// Pass to database - caller owns lifecycle
db, _ := db.OpenWithOptions(path, opts, multiLogger)
defer func() {
    if c, ok := multiLogger.(logger.Closeable); ok {
        c.Close()
    }
}()
```

## Integration Example

Full commit path with strategic logging:

```
DB.Commit(batch)
  * Info: "opening database" [path, fsync_on_commit]
  
  txn_writer.Commit(batch)
    * Warn: "batch validation failed" [count, reason]
    * Debug: "allocated txn id" [txn, ops_count]
    * Error: "failed to encode begin txn record" [txn]
    * Debug: "processing put operation" [txn, op_index, key_size]
    * Error: "failed to append put operation" [txn, op_index]
    * Debug: "processing delete operation" [txn, op_index, key_size]
    * Info: "commit successful" [txn, count]
    
  WAL.Append(record)
    * Debug: "appended record to segment" [seg, record_type, offset, payload_size]
    
  memtable.Apply(ops)
    * Error: "failed to apply batch to memtable" [txn]

DB.Close()
  * Info: "closing database" [path]
  * Error: "failed to close WAL log" [path]
```