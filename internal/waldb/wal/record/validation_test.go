package record_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestValidateRecordLength_TableDriven tests record length validation across all cases
func TestValidateRecordLength_TableDriven(t *testing.T) {
	testCases := []struct {
		name              string
		length            uint32
		expectError       bool
		checkErrorDetails func(*testing.T, error)
	}{
		{
			name:        "ValidMinimum",
			length:      1,
			expectError: false,
		},
		{
			name:        "ValidSmall",
			length:      10,
			expectError: false,
		},
		{
			name:        "ValidMedium",
			length:      1000,
			expectError: false,
		},
		{
			name:        "ValidMaximum",
			length:      record.MaxRecordSize,
			expectError: false,
		},
		{
			name:        "ValidBoundaryMin",
			length:      1,
			expectError: false,
		},
		{
			name:        "ValidBoundaryMax-1",
			length:      record.MaxRecordSize - 1,
			expectError: false,
		},
		{
			name:        "InvalidZero",
			length:      0,
			expectError: true,
			checkErrorDetails: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindInvalidLength {
					t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
				}
				if parseErr.Err != record.ErrInvalidLength {
					t.Errorf("expected ErrInvalidLength, got %v", parseErr.Err)
				}
			},
		},
		{
			name:        "InvalidTooLarge",
			length:      record.MaxRecordSize + 1,
			expectError: true,
			checkErrorDetails: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindTooLarge {
					t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
				}
				if parseErr.Err != record.ErrTooLarge {
					t.Errorf("expected ErrTooLarge, got %v", parseErr.Err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := record.ValidateRecordLength(tc.length)
			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tc.expectError && err != nil && tc.checkErrorDetails != nil {
				tc.checkErrorDetails(t, err)
			}
		})
	}
}

// TestValidateRecordFrame_TableDriven tests record frame validation for all record types
func TestValidateRecordFrame_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		recordType   record.RecordType
		payloadLen   int
		payloadSetup func() []byte
		expectError  bool
		checkError   func(*testing.T, error)
	}{
		{
			name:        "BeginTransactionValid",
			recordType:  record.RecordTypeBeginTransaction,
			payloadLen:  record.TxnIdSize,
			expectError: false,
		},
		{
			name:        "BeginTransactionInvalidSize",
			recordType:  record.RecordTypeBeginTransaction,
			payloadLen:  record.TxnIdSize + 1,
			expectError: true,
			checkError: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindInvalidLength {
					t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
				}
			},
		},
		{
			name:        "CommitTransactionValid",
			recordType:  record.RecordTypeCommitTransaction,
			payloadLen:  record.TxnIdSize,
			expectError: false,
		},
		{
			name:        "CommitTransactionInvalidSize",
			recordType:  record.RecordTypeCommitTransaction,
			payloadLen:  record.TxnIdSize - 1,
			expectError: true,
		},
		{
			name:        "PutOperationMinValid",
			recordType:  record.RecordTypePutOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize + record.PayloadHeaderSize,
			expectError: false,
		},
		{
			name:        "PutOperationWithData",
			recordType:  record.RecordTypePutOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize + 10 + record.PayloadHeaderSize + 20,
			expectError: false,
		},
		{
			name:        "PutOperationTooShort",
			recordType:  record.RecordTypePutOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize,
			expectError: true,
			checkError: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindInvalidLength {
					t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
				}
			},
		},
		{
			name:       "PutOperationMaxSize",
			recordType: record.RecordTypePutOperation,
			payloadLen: record.TxnIdSize + record.PayloadHeaderSize + record.MaxKeySize +
				record.PayloadHeaderSize + record.MaxValueSize,
			expectError: false,
		},
		{
			name:       "PutOperationOverMaxSize",
			recordType: record.RecordTypePutOperation,
			payloadLen: record.TxnIdSize + record.PayloadHeaderSize + record.MaxKeySize +
				record.PayloadHeaderSize + record.MaxValueSize + 1,
			expectError: true,
			checkError: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindTooLarge {
					t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
				}
			},
		},
		{
			name:        "DeleteOperationMinValid",
			recordType:  record.RecordTypeDeleteOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize,
			expectError: false,
		},
		{
			name:        "DeleteOperationWithKey",
			recordType:  record.RecordTypeDeleteOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize + 10,
			expectError: false,
		},
		{
			name:        "DeleteOperationTooShort",
			recordType:  record.RecordTypeDeleteOperation,
			payloadLen:  record.TxnIdSize,
			expectError: true,
		},
		{
			name:        "DeleteOperationMaxSize",
			recordType:  record.RecordTypeDeleteOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize + record.MaxKeySize,
			expectError: false,
		},
		{
			name:        "DeleteOperationOverMaxSize",
			recordType:  record.RecordTypeDeleteOperation,
			payloadLen:  record.TxnIdSize + record.PayloadHeaderSize + record.MaxKeySize + 1,
			expectError: true,
			checkError: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindTooLarge {
					t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
				}
			},
		},
		{
			name:        "InvalidType",
			recordType:  record.RecordType(99),
			payloadLen:  10,
			expectError: true,
			checkError: func(t *testing.T, err error) {
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
			},
		},
		{
			name:        "PayloadExceedsMaxRecordSize",
			recordType:  record.RecordTypeBeginTransaction,
			payloadLen:  record.MaxRecordSize,
			expectError: true,
			checkError: func(t *testing.T, err error) {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Fatalf("expected ParseError, got %T", err)
				}
				if parseErr.Kind != record.KindTooLarge {
					t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload := make([]byte, tc.payloadLen)
			if tc.payloadSetup != nil {
				payload = tc.payloadSetup()
			}

			err := record.ValidateRecordFrame(tc.recordType, payload)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tc.expectError && err != nil && tc.checkError != nil {
				tc.checkError(t, err)
			}
		})
	}
}

// TestEncodedRecordSize_TableDriven tests calculation of encoded record size
func TestEncodedRecordSize_TableDriven(t *testing.T) {
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
		{
			"components calculation",
			100,
			int64(record.RecordHeaderSize + 1 + 100 + record.RecordCRCSize),
		},
		{
			"max record size",
			record.MaxRecordSize - 1,
			int64(record.RecordHeaderSize + 1 + (record.MaxRecordSize - 1) + record.RecordCRCSize),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := record.EncodedRecordSize(tc.payloadLen)
			if result != tc.expectedLen {
				t.Errorf("expected %d, got %d", tc.expectedLen, result)
			}
		})
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
