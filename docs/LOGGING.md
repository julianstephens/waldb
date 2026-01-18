# Logging Guidelines

This document outlines the logging strategy and conventions for the WALDB project, ensuring consistent, meaningful, and efficient logging across all components.

## 📋 Overview

1. **Logging Philosophy** - Design principles (observability-first, zero overhead, structured logging ready)

2. **Logging Conventions** - Detailed log level semantics:
   - **Debug**: Low-volume operational details (segments, frame codec)
   - **Info**: Significant milestones (DB open/close, recovery complete, commit success)
   - **Warn**: Recoverable issues (truncated WAL, checksum mismatches)
   - **Error**: Operation failures with root causes

3. **Strategic Logging Points** organized by layer:
   - **Database**: Open/Close/Initialize operations
   - **WAL**: Segment operations, rotations, frame codec
   - **Recovery**: Start/progress/corruption detection/completion
   - **Transactions**: Commit stages and error handling
   - **Memtable**: Get/Set operations (minimal)

4. **Best Practices**:
   - Never log inside loops
   - Correlate operations by transaction/segment ID
   - Include coordinates in error logs
   - Use String() for enums
   - Log errors at origin, not in every layer

5. **Implementation Roadmap**:
   - Phase 1 (High): Core logging points (Info/Error levels)
   - Phase 2 (Medium): Debug-level logging for operational visibility
   - Phase 3 (Medium): CLI integration with `--verbose` flag
   - Phase 4 (Low): Performance metrics and SLO tracking

6. **Field Conventions** table with consistent names (seg, at, txn, op, key, size, duration, count, reason)

7. **Integration Example** showing full commit path with logs

## 🎯 Key Takeaways:

- **NoOpLogger pattern** means all logging is zero-cost in tests and non-instrumented deployments
- **Structured fields** enable rich filtering and aggregation when using real logger implementations
- **Error context preservation** through the stack using error `Coordinates`
- **Consistent field names** across the codebase for easy operator filtering/grepping
