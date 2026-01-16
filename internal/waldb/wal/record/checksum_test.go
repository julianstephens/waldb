package record_test

import (
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

func TestComputeChecksum(t *testing.T) {
	// Test that ComputeChecksum produces consistent results
	data := []byte("test data")
	crc1 := record.ComputeChecksum(data)
	crc2 := record.ComputeChecksum(data)

	if crc1 != crc2 {
		t.Errorf("checksum not deterministic: %d != %d", crc1, crc2)
	}

	// Test empty data produces zero (valid for CRC algorithms)
	emptyCRC := record.ComputeChecksum([]byte{})
	if emptyCRC != 0 {
		t.Errorf("expected checksum of empty data to be 0, got %d", emptyCRC)
	}

	// Test different data produces different checksums
	data2 := []byte("different data")
	crc3 := record.ComputeChecksum(data2)
	if crc1 == crc3 {
		t.Error("different data should produce different checksums")
	}
}

func TestComputeChecksumDeterministic(t *testing.T) {
	// Test with various payloads to ensure consistency
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"single byte", []byte{0x42}},
		{"text", []byte("hello world")},
		{"binary", []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}},
		{"large", make([]byte, 10000)},
	}

	for _, tc := range testCases {
		crc1 := record.ComputeChecksum(tc.data)
		crc2 := record.ComputeChecksum(tc.data)
		if crc1 != crc2 {
			t.Errorf("%s: checksum not deterministic: %d != %d", tc.name, crc1, crc2)
		}
	}
}

func TestVerifyChecksum(t *testing.T) {
	// Test with valid record
	rec := &record.Record{
		Type:    record.RecordTypePutOperation,
		Payload: []byte("test payload"),
	}
	record.UpdateChecksum(rec)

	if !record.VerifyChecksum(rec) {
		t.Error("expected VerifyChecksum to return true for valid record")
	}
}

func TestVerifyChecksumInvalid(t *testing.T) {
	// Test with corrupted CRC
	rec := &record.Record{
		Type:    record.RecordTypePutOperation,
		Payload: []byte("test payload"),
		CRC:     0xDEADBEEF, // Invalid CRC
	}

	if record.VerifyChecksum(rec) {
		t.Error("expected VerifyChecksum to return false for invalid record")
	}
}

func TestVerifyChecksumNil(t *testing.T) {
	// Test with nil record
	if record.VerifyChecksum(nil) {
		t.Error("expected VerifyChecksum to return false for nil record")
	}
}

func TestVerifyChecksumPayloadMutation(t *testing.T) {
	// Create a record with valid checksum
	rec := &record.Record{
		Type:    record.RecordTypeBeginTransaction,
		Payload: []byte("original payload"),
	}
	record.UpdateChecksum(rec)

	// Verify it's valid
	if !record.VerifyChecksum(rec) {
		t.Fatal("expected valid checksum initially")
	}

	// Mutate the payload
	rec.Payload = []byte("modified payload")

	// Should now be invalid
	if record.VerifyChecksum(rec) {
		t.Error("expected VerifyChecksum to return false after payload mutation")
	}
}

func TestVerifyChecksumTypeMutation(t *testing.T) {
	// Create a record with valid checksum
	rec := &record.Record{
		Type:    record.RecordTypePutOperation,
		Payload: []byte("test"),
	}
	record.UpdateChecksum(rec)

	// Verify it's valid
	if !record.VerifyChecksum(rec) {
		t.Fatal("expected valid checksum initially")
	}

	// Change the type
	rec.Type = record.RecordTypeDeleteOperation

	// Should now be invalid
	if record.VerifyChecksum(rec) {
		t.Error("expected VerifyChecksum to return false after type mutation")
	}
}

func TestUpdateChecksum(t *testing.T) {
	// Create a record
	rec := &record.Record{
		Type:    record.RecordTypeCommitTransaction,
		Payload: []byte("commit data"),
		CRC:     0, // Invalid initially
	}

	// Update the checksum
	record.UpdateChecksum(rec)

	// Should now be valid
	if !record.VerifyChecksum(rec) {
		t.Error("expected VerifyChecksum to return true after UpdateChecksum")
	}

	// CRC should be non-zero
	if rec.CRC == 0 {
		t.Error("expected non-zero CRC after UpdateChecksum")
	}
}

func TestUpdateChecksumNil(t *testing.T) {
	// Should not panic
	record.UpdateChecksum(nil)
}

func TestUpdateChecksumMultiple(t *testing.T) {
	// Update checksum multiple times should be idempotent
	rec := &record.Record{
		Type:    record.RecordTypePutOperation,
		Payload: []byte("test"),
	}

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
}

func TestChecksumAllRecordTypes(t *testing.T) {
	// Test checksum computation for all record types
	types := []record.RecordType{
		record.RecordTypeBeginTransaction,
		record.RecordTypeCommitTransaction,
		record.RecordTypePutOperation,
		record.RecordTypeDeleteOperation,
	}
	payload := []byte("test-data")

	for _, recordType := range types {
		rec := &record.Record{
			Type:    recordType,
			Payload: payload,
		}
		record.UpdateChecksum(rec)

		if !record.VerifyChecksum(rec) {
			t.Errorf("expected valid checksum for type %v", recordType)
		}

		if rec.CRC == 0 {
			t.Errorf("expected non-zero CRC for type %v", recordType)
		}
	}
}

func TestChecksumEmptyPayload(t *testing.T) {
	// Test checksum with empty payload
	rec := &record.Record{
		Type:    record.RecordTypeBeginTransaction,
		Payload: []byte{},
	}
	record.UpdateChecksum(rec)

	if !record.VerifyChecksum(rec) {
		t.Error("expected valid checksum for empty payload")
	}

	if rec.CRC == 0 {
		t.Error("expected non-zero CRC for empty payload")
	}
}

func TestChecksumLargePayload(t *testing.T) {
	// Test checksum with large payload
	payload := make([]byte, 100000)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	rec := &record.Record{
		Type:    record.RecordTypePutOperation,
		Payload: payload,
	}
	record.UpdateChecksum(rec)

	if !record.VerifyChecksum(rec) {
		t.Error("expected valid checksum for large payload")
	}
}

func TestChecksumBinaryData(t *testing.T) {
	// Test checksum with binary data containing all byte values
	payload := make([]byte, 256)
	for i := 0; i < 256; i++ {
		payload[i] = byte(i)
	}

	rec := &record.Record{
		Type:    record.RecordTypeDeleteOperation,
		Payload: payload,
	}
	record.UpdateChecksum(rec)

	if !record.VerifyChecksum(rec) {
		t.Error("expected valid checksum for binary payload")
	}
}
