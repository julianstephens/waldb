package txn_test

import (
	"testing"

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

// TestBatchPut adds a put operation to the batch
func TestBatchPut(t *testing.T) {
	batch := txn.NewBatch()
	key := []byte("mykey")
	value := []byte("myvalue")

	batch.Put(key, value)

	ops := batch.Ops()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}

	if ops[0].Kind != txn.OpPut {
		t.Errorf("expected OpPut, got %v", ops[0].Kind)
	}
	if string(ops[0].Key) != string(key) {
		t.Errorf("expected key %v, got %v", key, ops[0].Key)
	}
	if string(ops[0].Value) != string(value) {
		t.Errorf("expected value %v, got %v", value, ops[0].Value)
	}
}

// TestBatchDelete adds a delete operation to the batch
func TestBatchDelete(t *testing.T) {
	batch := txn.NewBatch()
	key := []byte("keyToDelete")

	batch.Delete(key)

	ops := batch.Ops()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}

	if ops[0].Kind != txn.OpDelete {
		t.Errorf("expected OpDelete, got %v", ops[0].Kind)
	}
	if string(ops[0].Key) != string(key) {
		t.Errorf("expected key %v, got %v", key, ops[0].Key)
	}
}

// TestBatchMultipleOperations adds multiple operations to the batch
func TestBatchMultipleOperations(t *testing.T) {
	batch := txn.NewBatch()

	batch.Put([]byte("key1"), []byte("value1"))
	batch.Delete([]byte("key2"))
	batch.Put([]byte("key3"), []byte("value3"))

	ops := batch.Ops()
	if len(ops) != 3 {
		t.Fatalf("expected 3 operations, got %d", len(ops))
	}

	if ops[0].Kind != txn.OpPut {
		t.Errorf("op 0: expected OpPut, got %v", ops[0].Kind)
	}
	if ops[1].Kind != txn.OpDelete {
		t.Errorf("op 1: expected OpDelete, got %v", ops[1].Kind)
	}
	if ops[2].Kind != txn.OpPut {
		t.Errorf("op 2: expected OpPut, got %v", ops[2].Kind)
	}
}

// TestBatchOpsReturnsAllOperations verifies Ops returns all accumulated operations
func TestBatchOpsReturnsAllOperations(t *testing.T) {
	batch := txn.NewBatch()
	expectedCount := 10

	for i := 0; i < expectedCount; i++ {
		batch.Put([]byte("key"), []byte("value"))
	}

	ops := batch.Ops()
	if len(ops) != expectedCount {
		t.Errorf("expected %d operations, got %d", expectedCount, len(ops))
	}
}

// TestValidateEmptyBatch validates that empty batch returns error
func TestValidateEmptyBatch(t *testing.T) {
	batch := txn.NewBatch()

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error for empty batch")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	if validErr.Err != txn.ErrEmptyBatch {
		t.Errorf("expected ErrEmptyBatch, got %v", validErr.Err)
	}
}

// TestValidateValidPutOperation validates a valid put operation
func TestValidateValidPutOperation(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte("value"))

	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateValidDeleteOperation validates a valid delete operation
func TestValidateValidDeleteOperation(t *testing.T) {
	batch := txn.NewBatch()
	batch.Delete([]byte("key"))

	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateValidMixedOperations validates a batch with both put and delete
func TestValidateValidMixedOperations(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Delete([]byte("key2"))
	batch.Put([]byte("key3"), []byte("value3"))

	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateNilKey validates that nil key returns error
func TestValidateNilKey(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put(nil, []byte("value"))

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error for nil key")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	if validErr.Err != txn.ErrEmptyKey {
		t.Errorf("expected ErrNilKey, got %v", validErr.Err)
	}
	if validErr.OpIndex != 0 {
		t.Errorf("expected OpIndex 0, got %d", validErr.OpIndex)
	}
	if validErr.OpKind != txn.OpPut {
		t.Errorf("expected OpPut, got %v", validErr.OpKind)
	}
}

// TestValidateEmptyKey validates that empty key returns error
func TestValidateEmptyKey(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte{}, []byte("value"))

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error for empty key")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	if validErr.Err != txn.ErrEmptyKey {
		t.Errorf("expected ErrNilKey, got %v", validErr.Err)
	}
}

// TestValidateKeyTooLarge validates that oversized key returns error
func TestValidateKeyTooLarge(t *testing.T) {
	batch := txn.NewBatch()
	oversizeKey := make([]byte, record.MaxKeySize+1)
	batch.Put(oversizeKey, []byte("value"))

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error for key too large")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	if validErr.Err != txn.ErrKeyTooLarge {
		t.Errorf("expected ErrKeyTooLarge, got %v", validErr.Err)
	}
	if validErr.KeyLen != len(oversizeKey) {
		t.Errorf("expected KeyLen %d, got %d", len(oversizeKey), validErr.KeyLen)
	}
	if validErr.OpIndex != 0 {
		t.Errorf("expected OpIndex 0, got %d", validErr.OpIndex)
	}
}

// TestValidateValueTooLarge validates that oversized value returns error
func TestValidateValueTooLarge(t *testing.T) {
	batch := txn.NewBatch()
	oversizeValue := make([]byte, record.MaxValueSize+1)
	batch.Put([]byte("key"), oversizeValue)

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error for value too large")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	if validErr.Err != txn.ErrValueTooLarge {
		t.Errorf("expected ErrValueTooLarge, got %v", validErr.Err)
	}
	if validErr.ValueLen != len(oversizeValue) {
		t.Errorf("expected ValueLen %d, got %d", len(oversizeValue), validErr.ValueLen)
	}
	if validErr.OpIndex != 0 {
		t.Errorf("expected OpIndex 0, got %d", validErr.OpIndex)
	}
}

// TestValidateDeleteWithOversizeKey validates delete with large key
func TestValidateDeleteWithOversizeKey(t *testing.T) {
	batch := txn.NewBatch()
	oversizeKey := make([]byte, record.MaxKeySize+1)
	batch.Delete(oversizeKey)

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error for delete with key too large")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	if validErr.Err != txn.ErrKeyTooLarge {
		t.Errorf("expected ErrKeyTooLarge, got %v", validErr.Err)
	}
	if validErr.OpKind != txn.OpDelete {
		t.Errorf("expected OpDelete, got %v", validErr.OpKind)
	}
}

// TestValidateMultipleOpsErrorOnFirstInvalid validates that validation stops at first error
func TestValidateMultipleOpsErrorOnFirstInvalid(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))            // valid
	batch.Put([]byte{}, []byte("value2"))                  // invalid - empty key
	batch.Put(make([]byte, record.MaxKeySize+1), []byte{}) // invalid - key too large

	err := batch.Validate()
	if err == nil {
		t.Fatal("expected error")
	}

	validErr, ok := err.(*txn.BatchValidationError)
	if !ok {
		t.Fatalf("expected BatchValidationError, got %T", err)
	}

	// Should error on second operation (index 1), not the third
	if validErr.OpIndex != 1 {
		t.Errorf("expected error at OpIndex 1, got %d", validErr.OpIndex)
	}
	if validErr.Err != txn.ErrEmptyKey {
		t.Errorf("expected ErrNilKey, got %v", validErr.Err)
	}
}

// TestValidateAtMaxKeySizeBoundary validates key at exact max size
func TestValidateAtMaxKeySizeBoundary(t *testing.T) {
	batch := txn.NewBatch()
	maxKey := make([]byte, record.MaxKeySize)
	batch.Put(maxKey, []byte("value"))

	err := batch.Validate()
	if err != nil {
		t.Fatalf("expected no error for key at max size, got %v", err)
	}
}

// TestValidateAtMaxValueSizeBoundary validates value at exact max size
func TestValidateAtMaxValueSizeBoundary(t *testing.T) {
	batch := txn.NewBatch()
	maxValue := make([]byte, record.MaxValueSize)
	batch.Put([]byte("key"), maxValue)

	err := batch.Validate()
	if err != nil {
		t.Fatalf("expected no error for value at max size, got %v", err)
	}
}

// TestValidateDeleteIgnoresValueSize validates delete doesn't check value size
func TestValidateDeleteIgnoresValueSize(t *testing.T) {
	batch := txn.NewBatch()
	batch.Delete([]byte("key"))

	// Even if we somehow set a large value on delete, it shouldn't be checked
	ops := batch.Ops()
	ops[0].Value = make([]byte, record.MaxValueSize+1)

	// Validate should still pass because delete ops don't validate value size
	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateEmptyValueForPut validates put with empty value is allowed
func TestValidateEmptyValueForPut(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte{})

	err := batch.Validate()
	if err != nil {
		t.Fatalf("expected no error for empty value in put, got %v", err)
	}
}

// TestBatchPutWithEmptyValue verifies put with empty value is added correctly
func TestBatchPutWithEmptyValue(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte("key"), []byte{})

	ops := batch.Ops()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}

	if len(ops[0].Value) != 0 {
		t.Errorf("expected empty value, got length %d", len(ops[0].Value))
	}
}

// TestValidateBinaryData validates batch with binary key and value data
func TestValidateBinaryData(t *testing.T) {
	batch := txn.NewBatch()
	binaryKey := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	binaryValue := []byte{0xAA, 0xBB, 0xCC, 0xDD}

	batch.Put(binaryKey, binaryValue)

	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error for binary data: %v", err)
	}
}

// TestValidateUnicodeData validates batch with unicode key and value
func TestValidateUnicodeData(t *testing.T) {
	batch := txn.NewBatch()
	unicodeKey := []byte("🔑")
	unicodeValue := []byte("📝")

	batch.Put(unicodeKey, unicodeValue)

	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error for unicode data: %v", err)
	}
}

// TestValidateLargeBatchManyOps validates batch with many operations
func TestValidateLargeBatchManyOps(t *testing.T) {
	batch := txn.NewBatch()
	numOps := 1000

	for i := 0; i < numOps; i++ {
		batch.Put([]byte("key"), []byte("value"))
	}

	err := batch.Validate()
	if err != nil {
		t.Fatalf("unexpected error for large batch: %v", err)
	}

	ops := batch.Ops()
	if len(ops) != numOps {
		t.Errorf("expected %d operations, got %d", numOps, len(ops))
	}
}

// TestBatchValidationErrorMessage validates error string formatting
func TestBatchValidationErrorMessage(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte{}, []byte("value"))

	err := batch.Validate()
	validErr := err.(*txn.BatchValidationError)

	errMsg := validErr.Error()
	if errMsg == "" {
		t.Error("expected non-empty error message")
	}

	// Message should include operation index
	if !contains(errMsg, "op") {
		t.Errorf("expected error message to mention operation, got: %s", errMsg)
	}
}

// TestBatchValidationErrorUnwrap validates error unwrapping
func TestBatchValidationErrorUnwrap(t *testing.T) {
	batch := txn.NewBatch()
	batch.Put([]byte{}, []byte("value"))

	err := batch.Validate()
	validErr := err.(*txn.BatchValidationError)

	unwrappedErr := validErr.Unwrap()
	if unwrappedErr != txn.ErrEmptyKey {
		t.Errorf("expected ErrNilKey from Unwrap, got %v", unwrappedErr)
	}
}

// TestValidateEmptyBatchErrorMessage validates error string for empty batch
func TestValidateEmptyBatchErrorMessage(t *testing.T) {
	batch := txn.NewBatch()

	err := batch.Validate()
	validErr := err.(*txn.BatchValidationError)

	errMsg := validErr.Error()
	if errMsg == "" {
		t.Error("expected non-empty error message")
	}

	// Empty batch error does not mention operation index (OpIndex is -1)
	if contains(errMsg, "op ") {
		t.Errorf("empty batch error should not mention operation: %s", errMsg)
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
