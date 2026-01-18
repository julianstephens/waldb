package record_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestValidateRecordLengthValid tests valid record lengths
func TestValidateRecordLengthValid(t *testing.T) {
	testCases := []struct {
		name   string
		length uint32
	}{
		{"minimum length", 1},
		{"small length", 10},
		{"medium length", 1000},
		{"large length", record.MaxRecordSize},
	}

	for _, tc := range testCases {
		err := record.ValidateRecordLength(tc.length)
		if err != nil {
			t.Errorf("%s: expected no error, got %v", tc.name, err)
		}
	}
}

// TestValidateRecordLengthZero tests zero record length (invalid)
func TestValidateRecordLengthZero(t *testing.T) {
	err := record.ValidateRecordLength(0)
	if err == nil {
		t.Fatal("expected error for zero length")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}

	if parseErr.Kind != record.KindInvalidLength {
		t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
	}
	if parseErr.DeclaredLen != 0 {
		t.Errorf("expected DeclaredLen 0, got %d", parseErr.DeclaredLen)
	}
	if parseErr.Err != record.ErrInvalidLength {
		t.Errorf("expected ErrInvalidLength, got %v", parseErr.Err)
	}
}

// TestValidateRecordLengthTooLarge tests record length exceeding MaxRecordSize
func TestValidateRecordLengthTooLarge(t *testing.T) {
	tooLargeLength := uint32(record.MaxRecordSize + 1)

	err := record.ValidateRecordLength(tooLargeLength)
	if err == nil {
		t.Fatal("expected error for oversized length")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}

	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
	if parseErr.DeclaredLen != tooLargeLength {
		t.Errorf("expected DeclaredLen %d, got %d", tooLargeLength, parseErr.DeclaredLen)
	}
	if parseErr.Want != record.MaxRecordSize {
		t.Errorf("expected Want %d, got %d", record.MaxRecordSize, parseErr.Want)
	}
	if parseErr.Have != int(tooLargeLength) {
		t.Errorf("expected Have %d, got %d", tooLargeLength, parseErr.Have)
	}
	if parseErr.Err != record.ErrTooLarge {
		t.Errorf("expected ErrTooLarge, got %v", parseErr.Err)
	}
}

// TestValidateRecordLengthBoundaries tests boundary conditions
func TestValidateRecordLengthBoundaries(t *testing.T) {
	testCases := []struct {
		name       string
		length     uint32
		shouldFail bool
	}{
		{"one byte", 1, false},
		{"MaxRecordSize - 1", record.MaxRecordSize - 1, false},
		{"MaxRecordSize", record.MaxRecordSize, false},
		{"MaxRecordSize + 1", record.MaxRecordSize + 1, true},
	}

	for _, tc := range testCases {
		err := record.ValidateRecordLength(tc.length)
		if tc.shouldFail && err == nil {
			t.Errorf("%s: expected error, got none", tc.name)
		}
		if !tc.shouldFail && err != nil {
			t.Errorf("%s: expected no error, got %v", tc.name, err)
		}
	}
}

// TestValidateRecordFrameBeginTransaction tests validation of BeginTransaction record
func TestValidateRecordFrameBeginTransaction(t *testing.T) {
	// Valid: TxnIdSize bytes payload
	validPayload := make([]byte, record.TxnIdSize)
	err := record.ValidateRecordFrame(record.RecordTypeBeginTransaction, validPayload)
	if err != nil {
		t.Errorf("valid BeginTransaction: expected no error, got %v", err)
	}

	// Invalid: wrong payload size
	invalidPayload := make([]byte, record.TxnIdSize+1)
	err = record.ValidateRecordFrame(record.RecordTypeBeginTransaction, invalidPayload)
	if err == nil {
		t.Fatal("invalid BeginTransaction: expected error for wrong payload size")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidLength {
		t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
	}
}

// TestValidateRecordFrameCommitTransaction tests validation of CommitTransaction record
func TestValidateRecordFrameCommitTransaction(t *testing.T) {
	// Valid: TxnIdSize bytes payload
	validPayload := make([]byte, record.TxnIdSize)
	err := record.ValidateRecordFrame(record.RecordTypeCommitTransaction, validPayload)
	if err != nil {
		t.Errorf("valid CommitTransaction: expected no error, got %v", err)
	}

	// Invalid: wrong payload size
	invalidPayload := make([]byte, record.TxnIdSize-1)
	err = record.ValidateRecordFrame(record.RecordTypeCommitTransaction, invalidPayload)
	if err == nil {
		t.Fatal("invalid CommitTransaction: expected error for wrong payload size")
	}
}

// TestValidateRecordFramePutOperation tests validation of PutOperation record
func TestValidateRecordFramePutOperation(t *testing.T) {
	// Valid: minimum size (txn_id + key_len + value_len)
	minPayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize+record.PayloadHeaderSize)
	err := record.ValidateRecordFrame(record.RecordTypePutOperation, minPayload)
	if err != nil {
		t.Errorf("valid minimum PutOperation: expected no error, got %v", err)
	}

	// Valid: with key and value
	validPayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize+10+record.PayloadHeaderSize+20)
	err = record.ValidateRecordFrame(record.RecordTypePutOperation, validPayload)
	if err != nil {
		t.Errorf("valid PutOperation with data: expected no error, got %v", err)
	}

	// Invalid: too short (missing value_len)
	tooShortPayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize)
	err = record.ValidateRecordFrame(record.RecordTypePutOperation, tooShortPayload)
	if err == nil {
		t.Fatal("invalid PutOperation: expected error for too short payload")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidLength {
		t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
	}
}

// TestValidateRecordFramePutOperationMaxSizes tests PutOperation with max sizes
func TestValidateRecordFramePutOperationMaxSizes(t *testing.T) {
	// Valid: at max boundaries
	maxSize := record.TxnIdSize + record.PayloadHeaderSize + record.MaxKeySize +
		record.PayloadHeaderSize + record.MaxValueSize
	maxPayload := make([]byte, maxSize)
	err := record.ValidateRecordFrame(record.RecordTypePutOperation, maxPayload)
	if err != nil {
		t.Errorf("PutOperation at max size: expected no error, got %v", err)
	}

	// Invalid: exceeds max
	overSize := record.TxnIdSize + record.PayloadHeaderSize + record.MaxKeySize +
		record.PayloadHeaderSize + record.MaxValueSize + 1
	oversizePayload := make([]byte, overSize)
	err = record.ValidateRecordFrame(record.RecordTypePutOperation, oversizePayload)
	if err == nil {
		t.Fatal("PutOperation over max: expected error")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
}

// TestValidateRecordFrameDeleteOperation tests validation of DeleteOperation record
func TestValidateRecordFrameDeleteOperation(t *testing.T) {
	// Valid: minimum size (txn_id + key_len)
	minPayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize)
	err := record.ValidateRecordFrame(record.RecordTypeDeleteOperation, minPayload)
	if err != nil {
		t.Errorf("valid minimum DeleteOperation: expected no error, got %v", err)
	}

	// Valid: with key data
	validPayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize+10)
	err = record.ValidateRecordFrame(record.RecordTypeDeleteOperation, validPayload)
	if err != nil {
		t.Errorf("valid DeleteOperation with key: expected no error, got %v", err)
	}

	// Invalid: too short (missing key_len)
	tooShortPayload := make([]byte, record.TxnIdSize)
	err = record.ValidateRecordFrame(record.RecordTypeDeleteOperation, tooShortPayload)
	if err == nil {
		t.Fatal("invalid DeleteOperation: expected error for too short payload")
	}
}

// TestValidateRecordFrameDeleteOperationMaxSize tests DeleteOperation with max key size
func TestValidateRecordFrameDeleteOperationMaxSize(t *testing.T) {
	// Valid: at max boundary
	maxPayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize+record.MaxKeySize)
	err := record.ValidateRecordFrame(record.RecordTypeDeleteOperation, maxPayload)
	if err != nil {
		t.Errorf("DeleteOperation at max size: expected no error, got %v", err)
	}

	// Invalid: exceeds max
	oversizePayload := make([]byte, record.TxnIdSize+record.PayloadHeaderSize+record.MaxKeySize+1)
	err = record.ValidateRecordFrame(record.RecordTypeDeleteOperation, oversizePayload)
	if err == nil {
		t.Fatal("DeleteOperation over max: expected error")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
}

// TestValidateRecordFrameInvalidType tests validation with invalid record type
func TestValidateRecordFrameInvalidType(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	invalidType := record.RecordType(99) // Non-existent type

	err := record.ValidateRecordFrame(invalidType, payload)
	if err == nil {
		t.Fatal("expected error for invalid record type")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidType {
		t.Errorf("expected KindInvalidType, got %v", parseErr.Kind)
	}
	if parseErr.Err != record.ErrInvalidType {
		t.Errorf("expected ErrInvalidType, got %v", parseErr.Err)
	}
}

// TestValidateRecordFramePayloadTooLarge tests payload exceeding MaxRecordSize
func TestValidateRecordFramePayloadTooLarge(t *testing.T) {
	// Create payload that when +1 (for record type) exceeds MaxRecordSize
	tooLargePayload := make([]byte, record.MaxRecordSize)
	err := record.ValidateRecordFrame(record.RecordTypeBeginTransaction, tooLargePayload)
	if err == nil {
		t.Fatal("expected error for payload exceeding MaxRecordSize")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
}

// TestEncodedRecordSize tests calculation of encoded record size
func TestEncodedRecordSize(t *testing.T) {
	testCases := []struct {
		name        string
		payloadLen  int
		expectedLen int64
	}{
		{
			"zero payload",
			0,
			int64(record.RecordHeaderSize + 1 + record.RecordCRCSize),
		},
		{
			"small payload",
			10,
			int64(record.RecordHeaderSize + 1 + 10 + record.RecordCRCSize),
		},
		{
			"medium payload",
			1000,
			int64(record.RecordHeaderSize + 1 + 1000 + record.RecordCRCSize),
		},
		{
			"large payload",
			record.MaxValueSize,
			int64(record.RecordHeaderSize + 1 + record.MaxValueSize + record.RecordCRCSize),
		},
	}

	for _, tc := range testCases {
		result := record.EncodedRecordSize(tc.payloadLen)
		if result != tc.expectedLen {
			t.Errorf("%s: expected %d, got %d", tc.name, tc.expectedLen, result)
		}
	}
}

// TestEncodedRecordSizeComponents verifies the calculation includes all components
func TestEncodedRecordSizeComponents(t *testing.T) {
	payloadLen := 100

	result := record.EncodedRecordSize(payloadLen)

	// Should equal: header(4) + type(1) + payload(100) + crc(4) = 109
	expected := int64(record.RecordHeaderSize + 1 + payloadLen + record.RecordCRCSize)

	if result != expected {
		t.Errorf("expected %d (header:%d + type:1 + payload:%d + crc:%d), got %d",
			expected, record.RecordHeaderSize, payloadLen, record.RecordCRCSize, result)
	}
}

// TestEncodedRecordSizeMaxPayload tests with maximum payload size
func TestEncodedRecordSizeMaxPayload(t *testing.T) {
	result := record.EncodedRecordSize(record.MaxRecordSize - 1)

	expected := int64(record.RecordHeaderSize + 1 + (record.MaxRecordSize - 1) + record.RecordCRCSize)

	if result != expected {
		t.Errorf("expected %d, got %d", expected, result)
	}
}

// TestValidateRecordFrameAllTypes iterates through all valid record types
func TestValidateRecordFrameAllTypes(t *testing.T) {
	testCases := []struct {
		recordType record.RecordType
		payloadLen int
		shouldFail bool
	}{
		{record.RecordTypeBeginTransaction, record.TxnIdSize, false},
		{record.RecordTypeCommitTransaction, record.TxnIdSize, false},
		{record.RecordTypePutOperation, record.TxnIdSize + record.PayloadHeaderSize + record.PayloadHeaderSize, false},
		{record.RecordTypeDeleteOperation, record.TxnIdSize + record.PayloadHeaderSize, false},
	}

	for _, tc := range testCases {
		payload := make([]byte, tc.payloadLen)
		err := record.ValidateRecordFrame(tc.recordType, payload)

		if tc.shouldFail && err == nil {
			t.Errorf("type %v: expected error, got none", tc.recordType)
		}
		if !tc.shouldFail && err != nil {
			t.Errorf("type %v: expected no error, got %v", tc.recordType, err)
		}
	}
}
