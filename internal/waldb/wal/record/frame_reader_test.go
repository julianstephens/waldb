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

func TestNextRoundtripBeginTransaction(t *testing.T) {
	payload := []byte("transaction-id-123")
	encoded := encodeRecord(record.RecordTypeBeginTransaction, payload)

	reader := record.NewFrameReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Record.Type != record.RecordTypeBeginTransaction {
		t.Errorf("expected type %v, got %v", record.RecordTypeBeginTransaction, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
	}
}

func TestNextRoundtripCommitTransaction(t *testing.T) {
	payload := []byte("commit-data")
	encoded := encodeRecord(record.RecordTypeCommitTransaction, payload)

	reader := record.NewFrameReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Record.Type != record.RecordTypeCommitTransaction {
		t.Errorf("expected type %v, got %v", record.RecordTypeCommitTransaction, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
	}
}

func TestNextRoundtripPutOperation(t *testing.T) {
	payload := []byte("key=value")
	encoded := encodeRecord(record.RecordTypePutOperation, payload)

	reader := record.NewFrameReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Record.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
	}
}

func TestNextRoundtripDeleteOperation(t *testing.T) {
	payload := []byte("key-to-delete")
	encoded := encodeRecord(record.RecordTypeDeleteOperation, payload)

	reader := record.NewFrameReader(bytes.NewReader(encoded))
	rec, err := reader.Next()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.Record.Type != record.RecordTypeDeleteOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypeDeleteOperation, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
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

	reader := record.NewFrameReader(&limitedReader)
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
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.Want == 0 || parseErr.Have == 0 {
		t.Errorf("expected non-zero Want/Have for truncation, got Want=%d Have=%d", parseErr.Want, parseErr.Have)
	}
}

func TestNextTruncatedLengthDetection(t *testing.T) {
	// Create a buffer with only 2 bytes (incomplete length header)
	buf := bytes.NewReader([]byte{0x05, 0x00})

	reader := record.NewFrameReader(buf)
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
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.Want == 0 || parseErr.Have == 0 {
		t.Errorf("expected non-zero Want/Have for truncation, got Want=%d Have=%d", parseErr.Want, parseErr.Have)
	}
}

func TestNextChecksumMismatchDetection(t *testing.T) {
	payload := []byte("test-data")
	encoded := encodeRecord(record.RecordTypePutOperation, payload)

	// Corrupt the checksum (last 4 bytes)
	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[len(corrupted)-1] ^= 0xFF // Flip bits in last byte of checksum

	reader := record.NewFrameReader(bytes.NewReader(corrupted))
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
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for checksum error, got %d", parseErr.DeclaredLen)
	}
}

func TestNextInvalidLengthDetection(t *testing.T) {
	buf := new(bytes.Buffer)
	// Write length of 0 (invalid)
	_ = binary.Write(buf, binary.LittleEndian, uint32(0))

	reader := record.NewFrameReader(buf)
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
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen != 0 {
		t.Errorf("expected DeclaredLen 0 for invalid length error, got %d", parseErr.DeclaredLen)
	}
}

func TestNextTooLargeLengthDetection(t *testing.T) {
	buf := new(bytes.Buffer)
	// Write length larger than MaxRecordSize
	_ = binary.Write(buf, binary.LittleEndian, uint32(record.MaxRecordSize+1))

	reader := record.NewFrameReader(buf)
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
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for too large error, got %d", parseErr.DeclaredLen)
	}
}

func TestNextInvalidTypeDetection(t *testing.T) {
	// Use RecordTypeUnknown which is invalid
	payload := []byte("test")
	encoded := encodeRecord(record.RecordTypeUnknown, payload)

	reader := record.NewFrameReader(bytes.NewReader(encoded))
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
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for invalid type error, got %d", parseErr.DeclaredLen)
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

func TestNextPartialHeaderOneByte(t *testing.T) {
	// Test reading 1 byte then EOF - should be truncated
	buf := bytes.NewReader([]byte{0x05})
	reader := record.NewFrameReader(buf)
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for 1-byte truncated header")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTruncated {
		t.Errorf("expected KindTruncated, got %v", parseErr.Kind)
	}
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.Want != 4 {
		t.Errorf("expected Want=4, got %d", parseErr.Want)
	}
	if parseErr.Have != 1 {
		t.Errorf("expected Have=1, got %d", parseErr.Have)
	}
}

func TestNextPartialHeaderThreeBytes(t *testing.T) {
	// Test reading 3 bytes then EOF - should be truncated
	buf := bytes.NewReader([]byte{0x05, 0x00, 0x00})
	reader := record.NewFrameReader(buf)
	_, err := reader.Next()

	if err == nil {
		t.Fatal("expected error for 3-byte truncated header")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTruncated {
		t.Errorf("expected KindTruncated, got %v", parseErr.Kind)
	}
	if *parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", *parseErr.Offset)
	}
	if parseErr.Want != 4 {
		t.Errorf("expected Want=4, got %d", parseErr.Want)
	}
	if parseErr.Have != 3 {
		t.Errorf("expected Have=3, got %d", parseErr.Have)
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
