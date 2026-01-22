package record_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// helper to construct a valid encoded record
func encodeRecord(recordType record.RecordType, payload []byte) []byte {
	recordLen := uint32(len(payload)) + 1 //nolint:gosec

	data := make([]byte, recordLen)
	data[0] = byte(recordType)
	copy(data[1:], payload)
	table := crc32.MakeTable(crc32.Castagnoli)
	crc := crc32.Checksum(data, table)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, recordLen)
	buf.Write(data)
	_ = binary.Write(buf, binary.LittleEndian, crc)

	return buf.Bytes()
}

// TestNextRoundtrip_TableDriven consolidates all roundtrip tests for different record types
func TestNextRoundtrip_TableDriven(t *testing.T) {
	testCases := []struct {
		name       string
		recordType record.RecordType
		payload    []byte
	}{
		{"BeginTransaction", record.RecordTypeBeginTransaction, []byte("transaction-id-123")},
		{"CommitTransaction", record.RecordTypeCommitTransaction, []byte("commit-data")},
		{"PutOperation", record.RecordTypePutOperation, []byte("key=value")},
		{"DeleteOperation", record.RecordTypeDeleteOperation, []byte("key-to-delete")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodeRecord(tc.recordType, tc.payload)
			reader := record.NewFrameReader(bytes.NewReader(encoded))
			rec, err := reader.Next()

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rec.Record.Type != tc.recordType {
				t.Errorf("expected type %v, got %v", tc.recordType, rec.Record.Type)
			}
			if !bytes.Equal(rec.Record.Payload, tc.payload) {
				t.Errorf("payload mismatch")
			}
		})
	}
}

func TestNextErrorDetection_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		prepare      func() io.Reader
		expectedKind record.ParseErrorKind
	}{
		{"truncated_tail", func() io.Reader {
			payload := []byte("some-data")
			encoded := encodeRecord(record.RecordTypePutOperation, payload)
			return &io.LimitedReader{
				R: bytes.NewReader(encoded),
				N: int64(len(encoded) - 2),
			}
		}, record.KindTruncated},
		{"truncated_length", func() io.Reader {
			return bytes.NewReader([]byte{0x05, 0x00})
		}, record.KindTruncated},
		{"checksum_mismatch", func() io.Reader {
			payload := []byte("test-data")
			encoded := encodeRecord(record.RecordTypePutOperation, payload)
			corrupted := make([]byte, len(encoded))
			copy(corrupted, encoded)
			corrupted[len(corrupted)-1] ^= 0xFF
			return bytes.NewReader(corrupted)
		}, record.KindChecksumMismatch},
		{"invalid_length", func() io.Reader {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint32(0))
			return buf
		}, record.KindInvalidLength},
		{"too_large_length", func() io.Reader {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint32(record.MaxRecordSize+1))
			return buf
		}, record.KindTooLarge},
		{"invalid_type", func() io.Reader {
			payload := []byte("test")
			return bytes.NewReader(encodeRecord(record.RecordTypeUnknown, payload))
		}, record.KindInvalidType},
		{"partial_header_1byte", func() io.Reader {
			return bytes.NewReader([]byte{0x05})
		}, record.KindTruncated},
		{"partial_header_3bytes", func() io.Reader {
			return bytes.NewReader([]byte{0x05, 0x00, 0x00})
		}, record.KindTruncated},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := record.NewFrameReader(tc.prepare())
			_, err := reader.Next()

			if err == nil {
				t.Errorf("expected error, got none")
			}

			parseErr, ok := err.(*record.ParseError)
			if !ok {
				t.Errorf("expected ParseError, got %T", err)
			} else if parseErr.Kind != tc.expectedKind {
				t.Errorf("expected Kind=%v, got %v", tc.expectedKind, parseErr.Kind)
			}
		})
	}
}

func TestNextMultipleRecords(t *testing.T) {
	// Test reading multiple sequential records
	records := []struct {
		typ     record.RecordType
		payload []byte
	}{
		{record.RecordTypeBeginTransaction, []byte("txn-1")},
		{record.RecordTypePutOperation, []byte("key1=value1")},
		{record.RecordTypePutOperation, []byte("key2=value2")},
		{record.RecordTypeCommitTransaction, []byte("commit-1")},
	}

	buf := new(bytes.Buffer)
	for _, rec := range records {
		buf.Write(encodeRecord(rec.typ, rec.payload))
	}

	reader := record.NewFrameReader(buf)
	for i, expected := range records {
		rec, err := reader.Next()
		if err != nil {
			t.Fatalf("record %d: unexpected error: %v", i, err)
		}
		if rec.Record.Type != expected.typ {
			t.Errorf("record %d: expected type %v, got %v", i, expected.typ, rec.Record.Type)
		}
		if !bytes.Equal(rec.Record.Payload, expected.payload) {
			t.Errorf("record %d: expected payload %v, got %v", i, expected.payload, rec.Record.Payload)
		}
	}
}

func TestNextEOF(t *testing.T) {
	// Test reading from empty stream should return io.EOF exactly
	reader := record.NewFrameReader(bytes.NewReader([]byte{}))
	_, err := reader.Next()

	if err != io.EOF {
		t.Fatalf("expected io.EOF for empty stream, got %v", err)
	}
}

func TestOffset(t *testing.T) {
	// Test that offset is tracked correctly
	records := []struct {
		typ     record.RecordType
		payload []byte
	}{
		{record.RecordTypeBeginTransaction, []byte("txn-1")},
		{record.RecordTypePutOperation, []byte("key1=value1")},
		{record.RecordTypeDeleteOperation, []byte("key2")},
	}

	buf := new(bytes.Buffer)
	offsets := []int64{}
	for _, rec := range records {
		offsets = append(offsets, int64(buf.Len()))
		buf.Write(encodeRecord(rec.typ, rec.payload))
	}

	reader := record.NewFrameReader(buf)

	// Initial offset should be 0
	if reader.Offset() != 0 {
		t.Errorf("expected initial offset 0, got %d", reader.Offset())
	}

	for i := range records {
		rec, err := reader.Next()
		if err != nil {
			t.Fatalf("record %d: unexpected error: %v", i, err)
		}

		// After reading a record, offset should point to the next record position
		expectedOffset := offsets[i] + int64(4+len(rec.Record.Payload)+1+4) // len header + payload + type + crc
		if reader.Offset() != expectedOffset {
			t.Errorf("record %d: expected offset %d, got %d", i, expectedOffset, reader.Offset())
		}
	}
}
