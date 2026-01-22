package record_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

func TestComputeChecksum_TableDriven(t *testing.T) {
	// Test that ComputeChecksum produces consistent results and different data produces different checksums
	testCases := []struct {
		name            string
		data            []byte
		expectDifferent bool // Whether this should differ from first test case
	}{
		{"consistent_basic", []byte("test data"), false},
		{"empty", []byte{}, false},
		{"different_data", []byte("different data"), true},
		{"single_byte", []byte{0x42}, false},
		{"text", []byte("hello world"), false},
		{"binary", []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}, false},
		{"large", make([]byte, 10000), false},
	}

	baselineChecksum := uint32(0)
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Compute checksum twice to test determinism
			crc1 := record.ComputeChecksum(tc.data)
			crc2 := record.ComputeChecksum(tc.data)

			if crc1 != crc2 {
				t.Errorf("checksum not deterministic: %d != %d", crc1, crc2)
			}

			// Track baseline for comparison
			if i == 0 {
				baselineChecksum = crc1
			} else if tc.expectDifferent {
				if crc1 == baselineChecksum {
					t.Errorf("expected different checksum from baseline, but got %d == %d", crc1, baselineChecksum)
				}
			}
		})
	}
}

func TestVerifyChecksum_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		setupRecord func() *record.Record
		expectValid bool
		description string
	}{
		{
			name: "ValidRecord",
			setupRecord: func() *record.Record {
				rec := &record.Record{
					Type:    record.RecordTypePutOperation,
					Payload: []byte("test payload"),
				}
				record.UpdateChecksum(rec)
				return rec
			},
			expectValid: true,
			description: "valid record with correct checksum",
		},
		{
			name: "InvalidCRC",
			setupRecord: func() *record.Record {
				return &record.Record{
					Type:    record.RecordTypePutOperation,
					Payload: []byte("test payload"),
					CRC:     0xDEADBEEF,
				}
			},
			expectValid: false,
			description: "record with corrupted CRC",
		},
		{
			name: "NilRecord",
			setupRecord: func() *record.Record {
				return nil
			},
			expectValid: false,
			description: "nil record",
		},
		{
			name: "PayloadMutation",
			setupRecord: func() *record.Record {
				rec := &record.Record{
					Type:    record.RecordTypeBeginTransaction,
					Payload: []byte("original payload"),
				}
				record.UpdateChecksum(rec)
				rec.Payload = []byte("modified payload")
				return rec
			},
			expectValid: false,
			description: "record with mutated payload",
		},
		{
			name: "TypeMutation",
			setupRecord: func() *record.Record {
				rec := &record.Record{
					Type:    record.RecordTypePutOperation,
					Payload: []byte("test"),
				}
				record.UpdateChecksum(rec)
				rec.Type = record.RecordTypeDeleteOperation
				return rec
			},
			expectValid: false,
			description: "record with mutated type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := tc.setupRecord()
			valid := record.VerifyChecksum(rec)

			if valid != tc.expectValid {
				t.Errorf("%s: expected valid=%v, got=%v", tc.description, tc.expectValid, valid)
			}
		})
	}
}

func TestUpdateChecksum_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		setupRecord func() *record.Record
		verify      func(*testing.T, *record.Record)
	}{
		{
			name: "BasicUpdate",
			setupRecord: func() *record.Record {
				return &record.Record{
					Type:    record.RecordTypeCommitTransaction,
					Payload: []byte("commit data"),
					CRC:     0,
				}
			},
			verify: func(t *testing.T, rec *record.Record) {
				if !record.VerifyChecksum(rec) {
					t.Error("expected valid checksum after UpdateChecksum")
				}
				if rec.CRC == 0 {
					t.Error("expected non-zero CRC after UpdateChecksum")
				}
			},
		},
		{
			name: "NilRecord",
			setupRecord: func() *record.Record {
				return nil
			},
			verify: func(t *testing.T, rec *record.Record) {
				// Should not panic, no verification needed
			},
		},
		{
			name: "MultipleUpdates",
			setupRecord: func() *record.Record {
				return &record.Record{
					Type:    record.RecordTypePutOperation,
					Payload: []byte("test"),
				}
			},
			verify: func(t *testing.T, rec *record.Record) {
				record.UpdateChecksum(rec)
				crc1 := rec.CRC

				record.UpdateChecksum(rec)
				crc2 := rec.CRC

				if crc1 != crc2 {
					t.Errorf("UpdateChecksum not idempotent: %d != %d", crc1, crc2)
				}
				if !record.VerifyChecksum(rec) {
					t.Error("expected valid checksum after multiple updates")
				}
			},
		},
		{
			name: "AllRecordTypes",
			setupRecord: func() *record.Record {
				// This will be tested for each type
				return &record.Record{
					Type:    record.RecordTypeBeginTransaction,
					Payload: []byte("test-data"),
				}
			},
			verify: func(t *testing.T, rec *record.Record) {
				types := []record.RecordType{
					record.RecordTypeBeginTransaction,
					record.RecordTypeCommitTransaction,
					record.RecordTypePutOperation,
					record.RecordTypeDeleteOperation,
				}
				for _, recordType := range types {
					rec := &record.Record{
						Type:    recordType,
						Payload: []byte("test-data"),
					}
					record.UpdateChecksum(rec)

					if !record.VerifyChecksum(rec) {
						t.Errorf("expected valid checksum for type %v", recordType)
					}
					if rec.CRC == 0 {
						t.Errorf("expected non-zero CRC for type %v", recordType)
					}
				}
			},
		},
		{
			name: "EmptyPayload",
			setupRecord: func() *record.Record {
				return &record.Record{
					Type:    record.RecordTypeBeginTransaction,
					Payload: []byte{},
				}
			},
			verify: func(t *testing.T, rec *record.Record) {
				record.UpdateChecksum(rec)
				if !record.VerifyChecksum(rec) {
					t.Error("expected valid checksum for empty payload")
				}
				if rec.CRC == 0 {
					t.Error("expected non-zero CRC for empty payload")
				}
			},
		},
		{
			name: "LargePayload",
			setupRecord: func() *record.Record {
				payload := make([]byte, 100000)
				for i := range payload {
					payload[i] = byte(i % 256)
				}
				return &record.Record{
					Type:    record.RecordTypePutOperation,
					Payload: payload,
				}
			},
			verify: func(t *testing.T, rec *record.Record) {
				record.UpdateChecksum(rec)
				if !record.VerifyChecksum(rec) {
					t.Error("expected valid checksum for large payload")
				}
			},
		},
		{
			name: "BinaryData",
			setupRecord: func() *record.Record {
				payload := make([]byte, 256)
				for i := 0; i < 256; i++ {
					payload[i] = byte(i)
				}
				return &record.Record{
					Type:    record.RecordTypeDeleteOperation,
					Payload: payload,
				}
			},
			verify: func(t *testing.T, rec *record.Record) {
				record.UpdateChecksum(rec)
				if !record.VerifyChecksum(rec) {
					t.Error("expected valid checksum for binary payload")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := tc.setupRecord()
			record.UpdateChecksum(rec)
			tc.verify(t, rec)
		})
	}
}
