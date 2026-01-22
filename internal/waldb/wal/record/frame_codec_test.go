package record_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestEncodeFrame_TableDriven consolidates encode tests
func TestEncodeFrame_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		recordType  record.RecordType
		payload     []byte
		expectError bool
	}{
		{"basic", record.RecordTypePutOperation, []byte("test-payload"), false},
		{"empty", record.RecordTypeBeginTransaction, []byte{}, false},
		{"large", record.RecordTypeCommitTransaction, make([]byte, 10000), false},
		{"oversized", record.RecordTypePutOperation, make([]byte, record.MaxRecordSize+1), true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := record.EncodeFrame(tc.recordType, tc.payload)
			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.expectError {
				rec, err := record.DecodeFrame(encoded)
				if err != nil || rec.Record.Type != tc.recordType || !bytes.Equal(rec.Record.Payload, tc.payload) {
					t.Errorf("roundtrip failed")
				}
			}
		})
	}
}

func TestEncodeFrameMultiple(t *testing.T) {
	// Test encoding multiple records
	records := []struct {
		typ     record.RecordType
		payload []byte
	}{
		{record.RecordTypeBeginTransaction, []byte("txn-start")},
		{record.RecordTypePutOperation, []byte("key=value")},
		{record.RecordTypeDeleteOperation, []byte("old-key")},
		{record.RecordTypeCommitTransaction, []byte("txn-end")},
	}

	buf := new(bytes.Buffer)

	// Encode all records
	for i, rec := range records {
		encoded, err := record.EncodeFrame(rec.typ, rec.payload)
		if err != nil {
			t.Fatalf("record %d: unexpected error encoding: %v", i, err)
		}
		buf.Write(encoded)
	}

	// Read them all back
	reader := record.NewFrameReader(bytes.NewReader(buf.Bytes()))
	for i, expected := range records {
		rec, err := reader.Next()
		if err != nil {
			t.Fatalf("record %d: unexpected error reading: %v", i, err)
		}

		if rec.Record.Type != expected.typ {
			t.Errorf("record %d: expected type %v, got %v", i, expected.typ, rec.Record.Type)
		}
		if !bytes.Equal(rec.Record.Payload, expected.payload) {
			t.Errorf("record %d: expected payload %v, got %v", i, expected.payload, rec.Record.Payload)
		}
	}
}

func TestEncodeFrameByteFormat(t *testing.T) {
	// Test that Encode produces bytes in correct wire format:
	// [4-byte LE length][1-byte type][N-byte payload][4-byte LE CRC]
	// where length = 1 + len(payload)
	payload := []byte("hello")
	encoded, err := record.EncodeFrame(record.RecordTypePutOperation, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding: %v", err)
	}

	// Expected structure:
	expectedLen := uint32(1 + len(payload)) //nolint:gosec
	expectedSize := 4 + expectedLen + 4     // header + (type+payload) + crc

	if len(encoded) != int(expectedSize) {
		t.Errorf("expected encoded size %d, got %d", expectedSize, len(encoded))
	}

	// Verify length field (bytes 0-3, little-endian)
	decodedLen := binary.LittleEndian.Uint32(encoded[0:4])
	if decodedLen != expectedLen {
		t.Errorf("expected length field %d, got %d", expectedLen, decodedLen)
	}

	// Verify type byte (byte 4)
	if encoded[4] != byte(record.RecordTypePutOperation) {
		t.Errorf("expected type %d at byte 4, got %d", record.RecordTypePutOperation, encoded[4])
	}

	// Verify payload (bytes 5 to 5+len(payload))
	payloadStart := 5
	payloadEnd := payloadStart + len(payload)
	if !bytes.Equal(encoded[payloadStart:payloadEnd], payload) {
		t.Errorf("expected payload %v at bytes [5:%d], got %v", payload, payloadEnd, encoded[payloadStart:payloadEnd])
	}

	// Verify CRC (last 4 bytes, little-endian)
	crcStart := int(4 + expectedLen)
	encodedCRC := binary.LittleEndian.Uint32(encoded[crcStart : crcStart+4])
	if encodedCRC == 0 {
		t.Error("expected non-zero CRC")
	}

	// Verify the CRC is correct by decoding and checking
	rec, err := record.DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding: %v", err)
	}
	if rec.Record.CRC != encodedCRC {
		t.Errorf("expected CRC %d from decoded record, got %d", encodedCRC, rec.Record.CRC)
	}

	// Verify it round-trips correctly
	if rec.Record.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
	}
}

// TestDecodeFrame_TableDriven consolidates all decode tests
func TestDecodeFrame_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		payload      []byte
		encodeType   record.RecordType
		prepare      func([]byte) []byte
		expectError  bool
		expectedKind record.ParseErrorKind
	}{
		{"basic", []byte("test-payload"), record.RecordTypePutOperation, nil, false, 0},
		{"empty", []byte{}, record.RecordTypeBeginTransaction, nil, false, 0},
		{"large", make([]byte, 10000), record.RecordTypeCommitTransaction, nil, false, 0},
		{"truncated data", []byte{0x05, 0x00}, 0, nil, true, record.KindTruncated},
		{"data too short", []byte{0x01}, 0, nil, true, record.KindTruncated},
		{"zero length", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0, nil, true, record.KindInvalidLength},
		{
			"oversized length",
			func() []byte {
				data := make([]byte, 8)
				binary.LittleEndian.PutUint32(data[:4], uint32(record.MaxRecordSize+1))
				return data
			}(),
			0,
			nil,
			true,
			record.KindTooLarge,
		},
		{
			"checksum mismatch",
			[]byte("test"),
			record.RecordTypePutOperation,
			func(b []byte) []byte {
				b[len(b)-1] ^= 0xFF
				return b
			},
			true,
			record.KindChecksumMismatch,
		},
		{
			"extra data",
			[]byte("test"),
			record.RecordTypePutOperation,
			func(b []byte) []byte { return append(b, 0xFF, 0xFF) },
			true,
			record.KindCorrupt,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var data []byte
			if tc.encodeType != 0 {
				encoded, err := record.EncodeFrame(tc.encodeType, tc.payload)
				if err != nil {
					t.Fatalf("unexpected error encoding: %v", err)
				}
				data = encoded
			} else {
				data = tc.payload
			}

			if tc.prepare != nil {
				data = tc.prepare(data)
			}

			rec, err := record.DecodeFrame(data)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.expectError && err != nil {
				parseErr, ok := err.(*record.ParseError)
				if !ok {
					t.Errorf("expected ParseError, got %T", err)
				} else if parseErr.Kind != tc.expectedKind {
					t.Errorf("expected %v, got %v", tc.expectedKind, parseErr.Kind)
				}
			}
			if !tc.expectError && rec.Record.Type != 0 && tc.encodeType != 0 {
				if rec.Record.Type != tc.encodeType {
					t.Errorf("type mismatch")
				}
			}
		})
	}
}

// TestDecodeFrameValidAllTypes tests all valid record types
func TestDecodeFrameValidAllTypes(t *testing.T) {
	types := []record.RecordType{
		record.RecordTypeBeginTransaction,
		record.RecordTypeCommitTransaction,
		record.RecordTypePutOperation,
		record.RecordTypeDeleteOperation,
	}
	payload := []byte("test-payload")

	for _, recordType := range types {
		t.Run(fmt.Sprintf("type_%v", recordType), func(t *testing.T) {
			encoded, err := record.EncodeFrame(recordType, payload)
			if err != nil {
				t.Fatalf("unexpected error encoding: %v", err)
			}

			rec, err := record.DecodeFrame(encoded)
			if err != nil {
				t.Fatalf("unexpected error decoding: %v", err)
			}

			if rec.Record.Type != recordType {
				t.Errorf("expected type %v, got %v", recordType, rec.Record.Type)
			}
			if !bytes.Equal(rec.Record.Payload, payload) {
				t.Errorf("payload mismatch")
			}
		})
	}
}

// TestDecodeFrameRoundtrip_TableDriven tests encode/decode roundtrip
func TestDecodeFrameRoundtrip_TableDriven(t *testing.T) {
	testCases := []struct {
		name    string
		typ     record.RecordType
		payload []byte
	}{
		{"empty", record.RecordTypeBeginTransaction, []byte{}},
		{"small", record.RecordTypePutOperation, []byte("key=value")},
		{"binary", record.RecordTypeDeleteOperation, []byte{0x00, 0x01, 0x02, 0xFF}},
		{"large", record.RecordTypeCommitTransaction, make([]byte, 10000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := record.EncodeFrame(tc.typ, tc.payload)
			if err != nil {
				t.Fatalf("unexpected error encoding: %v", err)
			}

			rec, err := record.DecodeFrame(encoded)
			if err != nil {
				t.Fatalf("unexpected error decoding: %v", err)
			}

			if rec.Record.Type != tc.typ {
				t.Errorf("expected type %v, got %v", tc.typ, rec.Record.Type)
			}
			if !bytes.Equal(rec.Record.Payload, tc.payload) {
				t.Errorf("payload mismatch")
			}
		})
	}
}
