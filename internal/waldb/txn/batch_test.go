package txn_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/kv"
	"github.com/julianstephens/waldb/internal/waldb/txn"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestNewBatch creates a new batch and verifies it's empty
func TestNewBatch(t *testing.T) {
	batch := txn.NewBatch()
	if batch == nil {
		t.Fatal("expected non-nil batch")
	}

	ops := batch.Ops()
	if len(ops) != 0 {
		t.Errorf("expected empty batch, got %d operations", len(ops))
	}
}

// TestBatchOperations_TableDriven tests basic batch operations (put, delete, multiple)
func TestBatchOperations_TableDriven(t *testing.T) {
	testCases := []struct {
		name           string
		setup          func() *txn.Batch
		expectedOpKind kv.OpKind
		expectedCount  int
	}{
		{
			name: "SinglePutOperation",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("mykey"), []byte("myvalue"))
				return batch
			},
			expectedOpKind: kv.OpPut,
			expectedCount:  1,
		},
		{
			name: "SingleDeleteOperation",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Delete([]byte("keyToDelete"))
				return batch
			},
			expectedOpKind: kv.OpDelete,
			expectedCount:  1,
		},
		{
			name: "MultipleOperations",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Delete([]byte("key2"))
				batch.Put([]byte("key3"), []byte("value3"))
				return batch
			},
			expectedOpKind: kv.OpPut, // Last operation kind
			expectedCount:  3,
		},
		{
			name: "MultipleOpsAllPuts",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				for i := 0; i < 10; i++ {
					batch.Put([]byte("key"), []byte("value"))
				}
				return batch
			},
			expectedOpKind: kv.OpPut,
			expectedCount:  10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := tc.setup()
			if batch == nil {
				t.Fatal("expected non-nil batch")
			}

			ops := batch.Ops()
			if len(ops) != tc.expectedCount {
				t.Fatalf("expected %d operations, got %d", tc.expectedCount, len(ops))
			}

			if tc.expectedCount > 0 && ops[0].Kind != tc.expectedOpKind {
				t.Errorf("expected first op kind %v, got %v", tc.expectedOpKind, ops[0].Kind)
			}
		})
	}
}

// TestValidateOperations_TableDriven tests various batch validation scenarios
func TestValidateOperations_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		setup       func() *txn.Batch
		expectError bool
	}{
		{
			name: "EmptyBatch",
			setup: func() *txn.Batch {
				return txn.NewBatch()
			},
			expectError: true,
		},
		{
			name: "ValidPutOperation",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), []byte("value"))
				return batch
			},
			expectError: false,
		},
		{
			name: "ValidDeleteOperation",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Delete([]byte("key"))
				return batch
			},
			expectError: false,
		},
		{
			name: "ValidMixedOperations",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key1"), []byte("value1"))
				batch.Delete([]byte("key2"))
				batch.Put([]byte("key3"), []byte("value3"))
				return batch
			},
			expectError: false,
		},
		{
			name: "NilKey",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put(nil, []byte("value"))
				return batch
			},
			expectError: true,
		},
		{
			name: "EmptyKey",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte{}, []byte("value"))
				return batch
			},
			expectError: true,
		},
		{
			name: "KeyTooLarge",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put(make([]byte, record.MaxKeySize+1), []byte("value"))
				return batch
			},
			expectError: true,
		},
		{
			name: "ValueTooLarge",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), make([]byte, record.MaxValueSize+1))
				return batch
			},
			expectError: true,
		},
		{
			name: "DeleteWithOversizeKey",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Delete(make([]byte, record.MaxKeySize+1))
				return batch
			},
			expectError: true,
		},
		{
			name: "AtMaxKeySizeBoundary",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put(make([]byte, record.MaxKeySize), []byte("value"))
				return batch
			},
			expectError: false,
		},
		{
			name: "AtMaxValueSizeBoundary",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), make([]byte, record.MaxValueSize))
				return batch
			},
			expectError: false,
		},
		{
			name: "EmptyValueForPut",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("key"), []byte{})
				return batch
			},
			expectError: false,
		},
		{
			name: "BinaryData",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte{0x00, 0x01, 0x02, 0xFF, 0xFE}, []byte{0xAA, 0xBB, 0xCC, 0xDD})
				return batch
			},
			expectError: false,
		},
		{
			name: "UnicodeData",
			setup: func() *txn.Batch {
				batch := txn.NewBatch()
				batch.Put([]byte("🔑"), []byte("📝"))
				return batch
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := tc.setup()
			err := batch.Validate()

			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error for %s", tc.name)
				}
				validErr, ok := err.(*txn.BatchValidationError)
				if !ok {
					t.Fatalf("expected BatchValidationError, got %T", err)
				}
				if validErr.Err == nil {
					t.Error("expected non-nil inner error in BatchValidationError")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestBatchErrorHandling_TableDriven tests large batches and error message formatting
func TestBatchErrorHandling_TableDriven(t *testing.T) {
	testCases := []struct {
		name              string
		setup             func() (*txn.Batch, int) // Returns batch and expected op count
		expectValidateErr bool
		checkErrorMessage func(msg string) bool // Validates error message (if applicable)
		checkErrorUnwrap  func(err error) bool  // Validates unwrapped error (if applicable)
	}{
		{
			name: "LargeBatchManyOperations",
			setup: func() (*txn.Batch, int) {
				batch := txn.NewBatch()
				for i := 0; i < 1000; i++ {
					batch.Put([]byte("key"), []byte("value"))
				}
				return batch, 1000
			},
			expectValidateErr: false,
		},
		{
			name: "ErrorMessageFormatWithOpIndex",
			setup: func() (*txn.Batch, int) {
				batch := txn.NewBatch()
				batch.Put([]byte{}, []byte("value")) // Empty key error
				return batch, 0
			},
			expectValidateErr: true,
			checkErrorMessage: func(msg string) bool {
				// Should mention operation index
				return contains(msg, "op")
			},
			checkErrorUnwrap: func(err error) bool {
				// Should be emptykey error
				validErr := err.(*txn.BatchValidationError)
				return validErr.Unwrap() == txn.ErrEmptyKey
			},
		},
		{
			name: "ErrorMessageFormatEmptyBatch",
			setup: func() (*txn.Batch, int) {
				batch := txn.NewBatch()
				return batch, 0
			},
			expectValidateErr: true,
			checkErrorMessage: func(msg string) bool {
				// Empty batch error should not mention "op " (with space, indicating op index)
				return !contains(msg, "op ")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch, expectedOpCount := tc.setup()
			err := batch.Validate()

			if tc.expectValidateErr {
				if err == nil {
					t.Fatalf("expected validation error for %s", tc.name)
				}
				validErr, ok := err.(*txn.BatchValidationError)
				if !ok {
					t.Fatalf("expected BatchValidationError, got %T", err)
				}

				if tc.checkErrorMessage != nil {
					if !tc.checkErrorMessage(validErr.Error()) {
						t.Errorf("error message validation failed: %q", validErr.Error())
					}
				}

				if tc.checkErrorUnwrap != nil {
					if !tc.checkErrorUnwrap(err) {
						t.Errorf("error unwrap validation failed")
					}
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				ops := batch.Ops()
				if len(ops) != expectedOpCount {
					t.Errorf("expected %d ops, got %d", expectedOpCount, len(ops))
				}
			}
		})
	}
}

// contains is a helper function to check if substr is in s
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > len(substr) && (s[0:len(substr)] == substr || contains(s[1:], substr)))
}
