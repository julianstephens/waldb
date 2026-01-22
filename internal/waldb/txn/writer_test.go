package txn_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/testutil"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestWriterCommitScenarios_TableDriven tests various writer commit scenarios (happy path, errors, etc)
func TestWriterCommitScenarios_TableDriven(t *testing.T) {
	testCases := []struct {
		name          string
		initialID     uint64
		fsyncOnCommit bool
		failOnAppend  int // Append failure index (-1 = no failure)
		failOnFlush   bool
		failOnFSync   bool
		batch         func() *txn.Batch
		expectError   bool
		verifyCall    func(calls []testutil.RecordedCall) bool // Validates the call sequence
	}{
		{
			name:          "HappyPathWithFSync",
			initialID:     100,
			fsyncOnCommit: true,
			failOnAppend:  -1,
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Put([]byte("key2"), []byte("value2"))
				batch.Delete([]byte("key3"))
				return batch
			},
			expectError: false,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Should be: BEGIN, PUT, PUT, DELETE, COMMIT, Flush, FSync
				if len(calls) != 7 {
					return false
				}
				if calls[0].RecordType != record.RecordTypeBeginTransaction {
					return false
				}
				if calls[4].RecordType != record.RecordTypeCommitTransaction {
					return false
				}
				return calls[5].Method == "Flush" && calls[6].Method == "FSync"
			},
		},
		{
			name:          "HappyPathWithoutFSync",
			initialID:     200,
			fsyncOnCommit: false,
			failOnAppend:  -1,
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Delete([]byte("key2"))
				return batch
			},
			expectError: false,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Should be: BEGIN, PUT, DELETE, COMMIT, Flush (no FSync)
				if len(calls) != 5 {
					return false
				}
				if calls[4].Method != "Flush" {
					return false
				}
				// Verify FSync not called
				for _, call := range calls {
					if call.Method == "FSync" {
						return false
					}
				}
				return true
			},
		},
		{
			name:         "FailOnOpAppend",
			initialID:    300,
			failOnAppend: 1, // Fail on second append (first operation)
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Put([]byte("key2"), []byte("value2"))
				return batch
			},
			expectError: true,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Should only have BEGIN (first append succeeded, second failed)
				appendCount := 0
				for _, call := range calls {
					if call.Method == "Append" {
						appendCount++
					}
					// No Flush or FSync on failure
					if call.Method == "Flush" || call.Method == "FSync" {
						return false
					}
				}
				return appendCount == 1
			},
		},
		{
			name:         "FailOnCommitAppend",
			initialID:    400,
			failOnAppend: 4, // Fail on fifth append (COMMIT after BEGIN + 3 ops)
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Delete([]byte("key2"))
				batch.Put([]byte("key3"), []byte("value3"))
				return batch
			},
			expectError: true,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Should have BEGIN + 3 operations, but no COMMIT
				appendCount := 0
				hasCommit := false
				for _, call := range calls {
					if call.Method == "Append" {
						appendCount++
						if call.RecordType == record.RecordTypeCommitTransaction {
							hasCommit = true
						}
					}
				}
				return appendCount == 4 && !hasCommit
			},
		},
		{
			name:          "FailOnFlush",
			initialID:     500,
			failOnAppend:  -1,
			failOnFlush:   true,
			fsyncOnCommit: true,
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), []byte("value"))
				return batch
			},
			expectError: true,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Should have appended all records (BEGIN, PUT, COMMIT), then called Flush which fails
				appendCount := 0
				flushCalled := false
				fsyncCalled := false

				for _, call := range calls {
					if call.Method == "Append" {
						appendCount++
					}
					if call.Method == "Flush" {
						flushCalled = true
					}
					if call.Method == "FSync" {
						fsyncCalled = true
					}
				}
				return appendCount == 3 && flushCalled && !fsyncCalled
			},
		},
		{
			name:          "FailOnFSync",
			initialID:     600,
			failOnAppend:  -1,
			failOnFSync:   true,
			fsyncOnCommit: true,
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), []byte("value"))
				return batch
			},
			expectError: true,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Should have appended all records, called Flush and FSync (which fails)
				appendCount := 0
				flushCalled := false
				fsyncCalled := false
				for _, call := range calls {
					if call.Method == "Append" {
						appendCount++
					}
					if call.Method == "Flush" {
						flushCalled = true
					}
					if call.Method == "FSync" {
						fsyncCalled = true
					}
				}
				return appendCount == 3 && flushCalled && fsyncCalled
			},
		},
		{
			name:         "InvalidBatchValidationBeforeWALWrite",
			initialID:    700,
			failOnAppend: -1,
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte(""), []byte("value")) // Invalid: empty key
				return batch
			},
			expectError: true,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Validation should happen before any WAL writes
				return len(calls) == 0
			},
		},
		{
			name:          "AppendOrderingBeginOpsCommit",
			initialID:     800,
			fsyncOnCommit: false,
			failOnAppend:  -1,
			batch: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Delete([]byte("key2"))
				batch.Put([]byte("key3"), []byte("value3"))
				batch.Delete([]byte("key4"))
				return batch
			},
			expectError: false,
			verifyCall: func(calls []testutil.RecordedCall) bool {
				// Exact ordering: BEGIN → PUT → DELETE → PUT → DELETE → COMMIT → Flush
				appendCalls := []testutil.RecordedCall{}
				for _, call := range calls {
					if call.Method == "Append" {
						appendCalls = append(appendCalls, call)
					}
				}

				expectedOrdering := []record.RecordType{
					record.RecordTypeBeginTransaction,
					record.RecordTypePutOperation,
					record.RecordTypeDeleteOperation,
					record.RecordTypePutOperation,
					record.RecordTypeDeleteOperation,
					record.RecordTypeCommitTransaction,
				}

				if len(appendCalls) != len(expectedOrdering) {
					return false
				}

				for i, expected := range expectedOrdering {
					if appendCalls[i].RecordType != expected {
						return false
					}
				}
				return true
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocator := testutil.NewIDAllocator(tc.initialID)
			appender := testutil.NewLogAppender()

			if tc.failOnAppend >= 0 {
				appender.SetFailOnAppend(tc.failOnAppend)
			}
			if tc.failOnFlush {
				appender.SetFailOnFlush(true)
			}
			if tc.failOnFSync {
				appender.SetFailOnFSync(true)
			}

			opts := txn.WriterOpts{FsyncOnCommit: tc.fsyncOnCommit}
			writer := txn.NewWriter(allocator, appender, opts, logger.NoOpLogger{})

			batch := tc.batch()
			txnID, err := writer.Commit(batch)

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error for %s", tc.name)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if txnID != tc.initialID {
					t.Errorf("expected txnID %d, got %d", tc.initialID, txnID)
				}
			}

			if tc.verifyCall != nil && !tc.verifyCall(appender.Calls()) {
				t.Errorf("call sequence verification failed for %s", tc.name)
				for i, call := range appender.Calls() {
					t.Logf("  call %d: %s(%v)", i, call.Method, call.RecordType)
				}
			}
		})
	}
}
