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
	recordLen := uint32(uint32(len(payload)) + 1) //nolint:gosec

	data := make([]byte, recordLen)
	data[0] = byte(recordType)
	copy(data[1:], payload)
	crc := crc32.ChecksumIEEE(data)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, recordLen)
	buf.Write(data)
	_ = binary.Write(buf, binary.LittleEndian, crc)

	return buf.Bytes()
}

func TestNextRoundtripBeginTransaction(t *testing.T) {
	payload := []byte("transaction-id-123")
	encoded := encodeRecord(record.RecordTypeBeginTransaction, payload)

	reader := record.NewRecordReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Type != record.RecordTypeBeginTransaction {
		t.Errorf("expected type %v, got %v", record.RecordTypeBeginTransaction, rec.Type)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Payload)
	}
}

func TestNextRoundtripCommitTransaction(t *testing.T) {
	payload := []byte("commit-data")
	encoded := encodeRecord(record.RecordTypeCommitTransaction, payload)

	reader := record.NewRecordReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Type != record.RecordTypeCommitTransaction {
		t.Errorf("expected type %v, got %v", record.RecordTypeCommitTransaction, rec.Type)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Payload)
	}
}

func TestNextRoundtripPutOperation(t *testing.T) {
	payload := []byte("key=value")
	encoded := encodeRecord(record.RecordTypePutOperation, payload)

	reader := record.NewRecordReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Type)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Payload)
	}
}

func TestNextRoundtripDeleteOperation(t *testing.T) {
	payload := []byte("key-to-delete")
	encoded := encodeRecord(record.RecordTypeDeleteOperation, payload)

	reader := record.NewRecordReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Type != record.RecordTypeDeleteOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypeDeleteOperation, rec.Type)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Payload)
	}
}

func TestNextTruncatedTailDetection(t *testing.T) {
	payload := []byte("some-data")
	encoded := encodeRecord(record.RecordTypePutOperation, payload)

	// When we truncate the record, the CRC bytes get corrupted/incomplete
	// This results in a checksum mismatch rather than a read error
	limitedReader := io.LimitedReader{
		R: bytes.NewReader(encoded),
		N: int64(len(encoded) - 2),
	}

	reader := record.NewRecordReader(&limitedReader)
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for truncated record")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	// When the tail is truncated during ReadFull, we get a truncation error
	if parseErr.Kind != record.KindTruncated {
		t.Errorf("expected KindTruncated for truncated tail, got %v", parseErr.Kind)
	}
}

func TestNextTruncatedLengthDetection(t *testing.T) {
	// Create a buffer with only 2 bytes (incomplete length header)
	buf := bytes.NewReader([]byte{0x05, 0x00})

	reader := record.NewRecordReader(buf)
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for truncated length header")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTruncated {
		t.Errorf("expected KindTruncated, got %v", parseErr.Kind)
	}
}

func TestNextChecksumMismatchDetection(t *testing.T) {
	payload := []byte("test-data")
	encoded := encodeRecord(record.RecordTypePutOperation, payload)

	// Corrupt the checksum (last 4 bytes)
	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[len(corrupted)-1] ^= 0xFF // Flip bits in last byte of checksum

	reader := record.NewRecordReader(bytes.NewReader(corrupted))
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for checksum mismatch")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindChecksumMismatch {
		t.Errorf("expected KindChecksumMismatch, got %v", parseErr.Kind)
	}
}

func TestNextInvalidLengthDetection(t *testing.T) {
	buf := new(bytes.Buffer)
	// Write length of 0 (invalid)
	_ = binary.Write(buf, binary.LittleEndian, uint32(0))

	reader := record.NewRecordReader(buf)
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for invalid (zero) length")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidLength {
		t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
	}
}

func TestNextTooLargeLengthDetection(t *testing.T) {
	buf := new(bytes.Buffer)
	// Write length larger than MaxRecordSize
	_ = binary.Write(buf, binary.LittleEndian, uint32(record.MaxRecordSize+1))

	reader := record.NewRecordReader(buf)
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for too large length")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
}

func TestNextInvalidTypeDetection(t *testing.T) {
	// Use RecordTypeUnknown which is invalid
	payload := []byte("test")
	encoded := encodeRecord(record.RecordTypeUnknown, payload)

	reader := record.NewRecordReader(bytes.NewReader(encoded))
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for invalid record type")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidType {
		t.Errorf("expected KindInvalidType, got %v", parseErr.Kind)
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

	reader := record.NewRecordReader(buf)
	for i, expected := range records {
		rec, err := reader.Next()
		if err != nil {
			t.Fatalf("record %d: unexpected error: %v", i, err)
		}
		if rec.Type != expected.typ {
			t.Errorf("record %d: expected type %v, got %v", i, expected.typ, rec.Type)
		}
		if !bytes.Equal(rec.Payload, expected.payload) {
			t.Errorf("record %d: expected payload %v, got %v", i, expected.payload, rec.Payload)
		}
	}
}

func TestNextEOF(t *testing.T) {
	// Test reading from empty stream
	reader := record.NewRecordReader(bytes.NewReader([]byte{}))
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for empty stream")
	}
	if err != io.EOF && err.Error() != "EOF" {
		parseErr, ok := err.(*record.ParseError)
		if !ok {
			t.Errorf("expected EOF or ParseError, got %T: %v", err, err)
		} else if parseErr.Kind != record.KindTruncated {
			t.Errorf("expected KindTruncated for EOF, got %v", parseErr.Kind)
		}
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

	reader := record.NewRecordReader(buf)

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
		expectedOffset := offsets[i] + int64(4+len(rec.Payload)+1+4) // len header + payload + type + crc
		if reader.Offset() != expectedOffset {
			t.Errorf("record %d: expected offset %d, got %d", i, expectedOffset, reader.Offset())
		}
	}
}

func TestEncode(t *testing.T) {
	// Test encoding a record
	payload := []byte("test-payload")
	encoded, err := record.Encode(record.RecordTypePutOperation, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding record: %v", err)
	}

	// Verify it can be decoded
	rec, err := record.Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding record: %v", err)
	}

	if rec.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Type)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Payload)
	}
}

func TestEncodeMultiple(t *testing.T) {
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
		encoded, err := record.Encode(rec.typ, rec.payload)
		if err != nil {
			t.Fatalf("record %d: unexpected error encoding: %v", i, err)
		}
		buf.Write(encoded)
	}

	// Read them all back
	reader := record.NewRecordReader(bytes.NewReader(buf.Bytes()))
	for i, expected := range records {
		rec, err := reader.Next()
		if err != nil {
			t.Fatalf("record %d: unexpected error reading: %v", i, err)
		}

		if rec.Type != expected.typ {
			t.Errorf("record %d: expected type %v, got %v", i, expected.typ, rec.Type)
		}
		if !bytes.Equal(rec.Payload, expected.payload) {
			t.Errorf("record %d: expected payload %v, got %v", i, expected.payload, rec.Payload)
		}
	}
}

func TestEncodeInvalidLength(t *testing.T) {
	// Test that Encode rejects payloads larger than MaxRecordSize
	// Create a payload larger than MaxRecordSize
	largePayload := make([]byte, record.MaxRecordSize+1)

	_, err := record.Encode(record.RecordTypePutOperation, largePayload)
	if err == nil {
		t.Fatal("expected error for oversized payload")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
}

func TestEncodeEmptyPayload(t *testing.T) {
	// Test encoding with empty payload
	encoded, err := record.Encode(record.RecordTypeBeginTransaction, []byte{})
	if err != nil {
		t.Fatalf("unexpected error encoding empty payload: %v", err)
	}

	// Decode it back
	rec, err := record.Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding record: %v", err)
	}

	if rec.Type != record.RecordTypeBeginTransaction {
		t.Errorf("expected type %v, got %v", record.RecordTypeBeginTransaction, rec.Type)
	}
	if len(rec.Payload) != 0 {
		t.Errorf("expected empty payload, got %v", rec.Payload)
	}
}

func TestDecode(t *testing.T) {
	// Test decoding a record
	payload := []byte("test-payload")
	encoded, err := record.Encode(record.RecordTypePutOperation, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding record: %v", err)
	}

	// Decode it
	rec, err := record.Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding record: %v", err)
	}

	if rec.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Type)
	}
	if !bytes.Equal(rec.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Payload)
	}
}

func TestDecodeInvalidData(t *testing.T) {
	// Test decoding truncated data
	_, err := record.Decode([]byte{0x05, 0x00})
	if err == nil {
		t.Fatal("expected error for truncated data")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTruncated {
		t.Errorf("expected KindTruncated, got %v", parseErr.Kind)
	}
}
