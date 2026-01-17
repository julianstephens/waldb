package record_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

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

	if rec.Record.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
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
	reader := wal.NewRecordReader(bytes.NewReader(buf.Bytes()))
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

	if rec.Record.Type != record.RecordTypeBeginTransaction {
		t.Errorf("expected type %v, got %v", record.RecordTypeBeginTransaction, rec.Record.Type)
	}
	if len(rec.Record.Payload) != 0 {
		t.Errorf("expected empty payload, got %v", rec.Record.Payload)
	}
}

func TestEncodeByteFormat(t *testing.T) {
	// Test that Encode produces bytes in correct wire format:
	// [4-byte LE length][1-byte type][N-byte payload][4-byte LE CRC]
	// where length = 1 + len(payload)
	payload := []byte("hello")
	encoded, err := record.Encode(record.RecordTypePutOperation, payload)
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
	rec, err := record.Decode(encoded)
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

	if rec.Record.Type != record.RecordTypePutOperation {
		t.Errorf("expected type %v, got %v", record.RecordTypePutOperation, rec.Record.Type)
	}
	if !bytes.Equal(rec.Record.Payload, payload) {
		t.Errorf("expected payload %v, got %v", payload, rec.Record.Payload)
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

func TestDecodeTooShort(t *testing.T) {
	// Test decoding data shorter than minimum header + crc
	_, err := record.Decode([]byte{0x01})
	if err == nil {
		t.Fatal("expected error for data too short")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTruncated {
		t.Errorf("expected KindTruncated, got %v", parseErr.Kind)
	}
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.Want == 0 || parseErr.Have == 0 {
		t.Errorf("expected non-zero Want/Have for truncation, got Want=%d Have=%d", parseErr.Want, parseErr.Have)
	}
}

func TestDecodeZeroLength(t *testing.T) {
	// Test decoding with zero record length
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	_, err := record.Decode(data)
	if err == nil {
		t.Fatal("expected error for zero record length")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidLength {
		t.Errorf("expected KindInvalidLength, got %v", parseErr.Kind)
	}
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen != 0 {
		t.Errorf("expected DeclaredLen 0 for invalid length, got %d", parseErr.DeclaredLen)
	}
}

func TestDecodeTooLarge(t *testing.T) {
	// Test decoding with record length > MaxRecordSize
	// Encode length as 17MB (16MB + 1)
	largeLen := uint32(record.MaxRecordSize + 1)
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[:4], largeLen)

	_, err := record.Decode(data)
	if err == nil {
		t.Fatal("expected error for oversized record length")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindTooLarge {
		t.Errorf("expected KindTooLarge, got %v", parseErr.Kind)
	}
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for too large error, got %d", parseErr.DeclaredLen)
	}
}

func TestDecodeUnknownType(t *testing.T) {
	// Test decoding with RecordTypeUnknown
	// Create a record with type 0 (Unknown)
	data := []byte{
		0x02, 0x00, 0x00, 0x00, // length = 2
		0x00,                   // type = 0 (Unknown)
		0xFF,                   // payload
		0x00, 0x00, 0x00, 0x00, // crc
	}

	_, err := record.Decode(data)
	if err == nil {
		t.Fatal("expected error for unknown record type")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindInvalidType {
		t.Errorf("expected KindInvalidType, got %v", parseErr.Kind)
	}
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for invalid type error, got %d", parseErr.DeclaredLen)
	}
}

func TestDecodeInvalidType(t *testing.T) {
	// Test decoding with invalid record type (beyond DeleteOperation)
	// RecordTypeDeleteOperation = 4, so 5 is invalid
	data := []byte{
		0x02, 0x00, 0x00, 0x00, // length = 2
		0x05,                   // type = 5 (invalid)
		0xFF,                   // payload
		0x00, 0x00, 0x00, 0x00, // crc
	}

	_, err := record.Decode(data)
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
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for invalid type error, got %d", parseErr.DeclaredLen)
	}
}

func TestDecodeChecksumMismatch(t *testing.T) {
	// Test decoding with mismatched checksum
	payload := []byte("test")
	encoded, err := record.Encode(record.RecordTypePutOperation, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding: %v", err)
	}

	// Corrupt the CRC bytes (last 4 bytes)
	encoded[len(encoded)-1] ^= 0xFF

	_, err = record.Decode(encoded)
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
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for checksum error, got %d", parseErr.DeclaredLen)
	}
}

func TestDecodeExtraData(t *testing.T) {
	// Test decoding with extra data beyond expected record length
	payload := []byte("test")
	encoded, err := record.Encode(record.RecordTypePutOperation, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding: %v", err)
	}

	// Append extra bytes
	encoded = append(encoded, 0xFF, 0xFF)

	_, err = record.Decode(encoded)
	if err == nil {
		t.Fatal("expected error for extra data")
	}

	parseErr, ok := err.(*record.ParseError)
	if !ok {
		t.Errorf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != record.KindCorrupt {
		t.Errorf("expected KindCorrupt, got %v", parseErr.Kind)
	}
	if parseErr.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", parseErr.Offset)
	}
	if parseErr.SafeTruncateOffset != 0 {
		t.Errorf("expected SafeTruncateOffset 0, got %d", parseErr.SafeTruncateOffset)
	}
	if parseErr.DeclaredLen == 0 {
		t.Errorf("expected non-zero DeclaredLen for corrupt error, got %d", parseErr.DeclaredLen)
	}
}

func TestDecodeValidAllTypes(t *testing.T) {
	// Test decoding valid records for all valid types
	types := []record.RecordType{
		record.RecordTypeBeginTransaction,
		record.RecordTypeCommitTransaction,
		record.RecordTypePutOperation,
		record.RecordTypeDeleteOperation,
	}
	payload := []byte("test-payload")

	for _, recordType := range types {
		encoded, err := record.Encode(recordType, payload)
		if err != nil {
			t.Fatalf("unexpected error encoding type %v: %v", recordType, err)
		}

		rec, err := record.Decode(encoded)
		if err != nil {
			t.Fatalf("unexpected error decoding type %v: %v", recordType, err)
		}

		if rec.Record.Type != recordType {
			t.Errorf("type %v: expected type %v, got %v", recordType, recordType, rec.Record.Type)
		}
		if !bytes.Equal(rec.Record.Payload, payload) {
			t.Errorf("type %v: expected payload %v, got %v", recordType, payload, rec.Record.Payload)
		}
	}
}

func TestDecodeRoundtrip(t *testing.T) {
	// Test encode/decode roundtrip for various payloads
	testCases := []struct {
		name    string
		typ     record.RecordType
		payload []byte
	}{
		{"empty payload", record.RecordTypeBeginTransaction, []byte{}},
		{"small payload", record.RecordTypePutOperation, []byte("key=value")},
		{"binary payload", record.RecordTypeDeleteOperation, []byte{0x00, 0x01, 0x02, 0xFF}},
		{"large payload", record.RecordTypeCommitTransaction, make([]byte, 10000)},
	}

	for _, tc := range testCases {
		encoded, err := record.Encode(tc.typ, tc.payload)
		if err != nil {
			t.Fatalf("%s: unexpected error encoding: %v", tc.name, err)
		}

		rec, err := record.Decode(encoded)
		if err != nil {
			t.Fatalf("%s: unexpected error decoding: %v", tc.name, err)
		}

		if rec.Record.Type != tc.typ {
			t.Errorf("%s: expected type %v, got %v", tc.name, tc.typ, rec.Record.Type)
		}
		if !bytes.Equal(rec.Record.Payload, tc.payload) {
			t.Errorf("%s: payload mismatch", tc.name)
		}
	}
}
