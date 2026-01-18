package record_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestEncodeBeginTxnPayload tests encoding BeginTransaction payload
func TestEncodeBeginTxnPayload(t *testing.T) {
	txnId := uint64(12345)
	encoded, err := record.EncodeBeginTxnPayload(txnId)
	tst.RequireNoError(t, err)

	tst.RequireDeepEqual(t, len(encoded), record.TxnIdSize)

	// Verify txn_id
	decodedTxnId := binary.LittleEndian.Uint64(encoded[0:])
	tst.RequireDeepEqual(t, decodedTxnId, txnId)
}

// TestEncodeBeginTxnPayloadZero tests with txn_id 0
func TestEncodeBeginTxnPayloadZero(t *testing.T) {
	txnId := uint64(0)
	encoded, err := record.EncodeBeginTxnPayload(txnId)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedTxnId := binary.LittleEndian.Uint64(encoded[0:])
	if decodedTxnId != txnId {
		t.Errorf("expected txn_id %d, got %d", txnId, decodedTxnId)
	}
}

// TestEncodeBeginTxnPayloadMaxUint64 tests with max uint64 value
func TestEncodeBeginTxnPayloadMaxUint64(t *testing.T) {
	txnId := uint64(^uint64(0)) // max uint64
	encoded, err := record.EncodeBeginTxnPayload(txnId)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decodedTxnId := binary.LittleEndian.Uint64(encoded[0:])
	if decodedTxnId != txnId {
		t.Errorf("expected txn_id %d, got %d", txnId, decodedTxnId)
	}
}

// TestDecodeBeginTxnPayload tests decoding BeginTransaction payload
func TestDecodeBeginTxnPayload(t *testing.T) {
	txnId := uint64(98765)
	encoded, _ := record.EncodeBeginTxnPayload(txnId)

	decoded, err := record.DecodeBeginTxnPayload(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.TxnID != txnId {
		t.Errorf("expected txn_id %d, got %d", txnId, decoded.TxnID)
	}
}

// TestDecodeBeginTxnPayloadTruncated tests truncated payload
func TestDecodeBeginTxnPayloadTruncated(t *testing.T) {
	truncated := []byte{0x02, 0x03} // only 2 bytes instead of txn_id (8 bytes)

	_, err := record.DecodeBeginTxnPayload(truncated)
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodeBeginTxnPayloadExtraData tests extra data beyond expected size
func TestDecodeBeginTxnPayloadExtraData(t *testing.T) {
	txnId := uint64(12345)
	encoded, _ := record.EncodeBeginTxnPayload(txnId)
	// Append extra bytes
	encoded = append(encoded, 0xFF, 0xFF)

	_, err := record.DecodeBeginTxnPayload(encoded)
	if err == nil {
		t.Fatal("expected error for extra data")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecCorrupt {
		t.Errorf("expected CodecCorrupt, got %v", codecErr.Kind)
	}
}

// TestEncodePutOpPayload tests encoding PutOperation payload
func TestEncodePutOpPayload(t *testing.T) {
	txnId := uint64(111)
	key := []byte("mykey")
	value := []byte("myvalue")

	encoded, err := record.EncodePutOpPayload(txnId, key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify structure: txn_id (8) + key_len (4) + key + value_len (4) + value
	expectedLen := record.TxnIdSize + record.PayloadHeaderSize + len(key) + record.PayloadHeaderSize + len(value)
	if len(encoded) != expectedLen {
		t.Errorf("expected encoded length %d, got %d", expectedLen, len(encoded))
	}

	// Verify we can decode it back
	decoded, err := record.DecodePutOpPayload(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding: %v", err)
	}

	if decoded.TxnID != txnId {
		t.Errorf("expected txn_id %d, got %d", txnId, decoded.TxnID)
	}
	if !bytes.Equal(decoded.Key, key) {
		t.Errorf("expected key %v, got %v", key, decoded.Key)
	}
	if !bytes.Equal(decoded.Value, value) {
		t.Errorf("expected value %v, got %v", value, decoded.Value)
	}
}

// TestEncodePutOpPayloadEmptyValue tests with empty value
func TestEncodePutOpPayloadEmptyValue(t *testing.T) {
	txnId := uint64(222)
	key := []byte("key")
	value := []byte{}

	encoded, err := record.EncodePutOpPayload(txnId, key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decoded, err := record.DecodePutOpPayload(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding: %v", err)
	}

	if !bytes.Equal(decoded.Key, key) {
		t.Errorf("expected key %v, got %v", key, decoded.Key)
	}
	if len(decoded.Value) != 0 {
		t.Errorf("expected empty value, got %v", decoded.Value)
	}
}

// TestEncodePutOpPayloadOversizeKey tests oversized key
func TestEncodePutOpPayloadOversizeKey(t *testing.T) {
	txnId := uint64(333)
	key := make([]byte, record.MaxKeySize+1)
	value := []byte("value")

	_, err := record.EncodePutOpPayload(txnId, key, value)
	if err == nil {
		t.Fatal("expected error for oversized key")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecInvalid {
		t.Errorf("expected CodecInvalid, got %v", codecErr.Kind)
	}
	if codecErr.Field != "key_len" {
		t.Errorf("expected field 'key_len', got %s", codecErr.Field)
	}
}

// TestEncodePutOpPayloadOversizeValue tests oversized value
func TestEncodePutOpPayloadOversizeValue(t *testing.T) {
	txnId := uint64(444)
	key := []byte("key")
	value := make([]byte, record.MaxValueSize+1)

	_, err := record.EncodePutOpPayload(txnId, key, value)
	if err == nil {
		t.Fatal("expected error for oversized value")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecInvalid {
		t.Errorf("expected CodecInvalid, got %v", codecErr.Kind)
	}
	if codecErr.Field != "value_len" {
		t.Errorf("expected field 'value_len', got %s", codecErr.Field)
	}
}

// TestDecodePutOpPayloadTruncatedKeyLen tests truncated key length field
func TestDecodePutOpPayloadTruncatedKeyLen(t *testing.T) {
	// Only txn_id (8 bytes), missing key_len - will fail at key_len read
	data := make([]byte, record.TxnIdSize+2)
	binary.LittleEndian.PutUint64(data[0:], uint64(111))

	_, err := record.DecodePutOpPayload(data)
	if err == nil {
		t.Fatal("expected error for truncated key_len")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodeCommitTxnPayloadEmpty tests empty payload returns CodecTruncated
func TestDecodeCommitTxnPayloadEmpty(t *testing.T) {
	emptyPayload := []byte{}

	_, err := record.DecodeCommitTxnPayload(emptyPayload)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodePutOpPayloadTruncatedKey tests truncated key data
func TestDecodePutOpPayloadTruncatedKey(t *testing.T) {
	txnId := uint64(555)
	keyLen := uint32(10)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, txnId)
	_ = binary.Write(buf, binary.LittleEndian, keyLen)
	buf.Write([]byte("short")) // only 5 bytes instead of 10

	_, err := record.DecodePutOpPayload(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for truncated key")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodePutOpPayloadTruncatedValueLen tests truncated value length field
func TestDecodePutOpPayloadTruncatedValueLen(t *testing.T) {
	txnId := uint64(666)
	key := []byte("mykey")

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, txnId)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(key))) //nolint:gosec
	buf.Write(key)
	// Missing value_len

	_, err := record.DecodePutOpPayload(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for truncated value_len")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodePutOpPayloadExtraData tests extra data beyond payload
func TestDecodePutOpPayloadExtraData(t *testing.T) {
	txnId := uint64(777)
	key := []byte("key")
	value := []byte("value")

	encoded, _ := record.EncodePutOpPayload(txnId, key, value)
	encoded = append(encoded, 0xFF, 0xFF) // append extra bytes

	_, err := record.DecodePutOpPayload(encoded)
	if err == nil {
		t.Fatal("expected error for extra data")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecCorrupt {
		t.Errorf("expected CodecCorrupt, got %v", codecErr.Kind)
	}
}

// TestDecodePutOpPayloadOversizeKey tests oversized key/value in payload
func TestDecodePutOpPayloadOversizeKey(t *testing.T) {
	txnId := uint64(888)
	keyLen := uint32(record.MaxKeySize + 1)
	value := []byte("value")

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, txnId)
	_ = binary.Write(buf, binary.LittleEndian, keyLen)
	// key data omitted to trigger size check before truncation check
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(value))) //nolint:gosec
	buf.Write(value)

	_, err := record.DecodePutOpPayload(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for oversized key")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecInvalid {
		t.Errorf("expected CodecInvalid, got %v", codecErr.Kind)
	}
}

// TestDecodePutOpPayloadOversizeValue tests oversized value in payload
func TestDecodePutOpPayloadOversizeValue(t *testing.T) {
	txnId := uint64(999)
	key := []byte("key")
	valueLen := uint32(record.MaxValueSize + 1)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, txnId)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(key))) //nolint:gosec
	buf.Write(key)
	_ = binary.Write(buf, binary.LittleEndian, valueLen) //nolint:gosec

	_, err := record.DecodePutOpPayload(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for oversized value")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecInvalid {
		t.Errorf("expected CodecInvalid, got %v", codecErr.Kind)
	}
}

// TestEncodeDeleteOpPayload tests encoding DeleteOperation payload
func TestEncodeDeleteOpPayload(t *testing.T) {
	txnId := uint64(1001)
	key := []byte("deletekey")

	encoded, err := record.EncodeDeleteOpPayload(txnId, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify structure: txn_id (8) + key_len (4) + key
	expectedLen := record.TxnIdSize + record.PayloadHeaderSize + len(key)
	if len(encoded) != expectedLen {
		t.Errorf("expected encoded length %d, got %d", expectedLen, len(encoded))
	}

	// Verify we can decode it back
	decoded, err := record.DecodeDeleteOpPayload(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding: %v", err)
	}

	if decoded.TxnID != txnId {
		t.Errorf("expected txn_id %d, got %d", txnId, decoded.TxnID)
	}
	if !bytes.Equal(decoded.Key, key) {
		t.Errorf("expected key %v, got %v", key, decoded.Key)
	}
}

// TestEncodeDeleteOpPayloadOversizeKey tests oversized key for delete
func TestEncodeDeleteOpPayloadOversizeKey(t *testing.T) {
	txnId := uint64(1002)
	key := make([]byte, record.MaxKeySize+1)

	_, err := record.EncodeDeleteOpPayload(txnId, key)
	if err == nil {
		t.Fatal("expected error for oversized key")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecInvalid {
		t.Errorf("expected CodecInvalid, got %v", codecErr.Kind)
	}
}

// TestDecodeDeleteOpPayloadTruncatedKeyLen tests truncated key length field
func TestDecodeDeleteOpPayloadTruncatedKeyLen(t *testing.T) {
	// Only txn_id (8 bytes), missing key_len
	data := make([]byte, record.TxnIdSize+2)
	binary.LittleEndian.PutUint64(data[0:], uint64(1003))

	_, err := record.DecodeDeleteOpPayload(data)
	if err == nil {
		t.Fatal("expected error for truncated key_len")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodeDeleteOpPayloadTruncatedKey tests truncated key data
func TestDecodeDeleteOpPayloadTruncatedKey(t *testing.T) {
	txnId := uint64(1004)
	keyLen := uint32(10)

	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, txnId)
	_ = binary.Write(buf, binary.LittleEndian, keyLen)
	buf.Write([]byte("short")) // only 5 bytes instead of 10

	_, err := record.DecodeDeleteOpPayload(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for truncated key")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodeDeleteOpPayloadExtraData tests extra data beyond payload
func TestDecodeDeleteOpPayloadExtraData(t *testing.T) {
	txnId := uint64(1005)
	key := []byte("key")

	encoded, _ := record.EncodeDeleteOpPayload(txnId, key)
	encoded = append(encoded, 0xFF, 0xFF) // append extra bytes

	_, err := record.DecodeDeleteOpPayload(encoded)
	if err == nil {
		t.Fatal("expected error for extra data")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecCorrupt {
		t.Errorf("expected CodecCorrupt, got %v", codecErr.Kind)
	}
}

// TestDecodePutOpPayloadEmpty tests empty payload returns CodecTruncated
func TestDecodePutOpPayloadEmpty(t *testing.T) {
	emptyPayload := []byte{}

	_, err := record.DecodePutOpPayload(emptyPayload)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodeDeleteOpPayloadEmpty tests empty payload returns CodecTruncated
func TestDecodeDeleteOpPayloadEmpty(t *testing.T) {
	emptyPayload := []byte{}

	_, err := record.DecodeDeleteOpPayload(emptyPayload)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestDecodeBeginTxnPayloadEmpty tests empty payload returns CodecTruncated
func TestDecodeBeginTxnPayloadEmpty(t *testing.T) {
	emptyPayload := []byte{}

	_, err := record.DecodeBeginTxnPayload(emptyPayload)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Errorf("expected CodecError, got %T", err)
	}
	if codecErr.Kind != record.CodecTruncated {
		t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
	}
}

// TestEncodePutOpPayloadRoundtrip tests encode/decode roundtrip for various payloads
func TestEncodePutOpPayloadRoundtrip(t *testing.T) {
	testCases := []struct {
		name  string
		txnId uint64
		key   []byte
		value []byte
	}{
		{"simple", 1, []byte("key"), []byte("value")},
		{"empty value", 2, []byte("key"), []byte{}},
		{"binary data", 3, []byte{0x00, 0x01, 0x02}, []byte{0xFF, 0xFE, 0xFD}},
		{"large key", 4, make([]byte, record.MaxKeySize), []byte("value")},
		{"large value", 5, []byte("key"), make([]byte, record.MaxValueSize)},
		{"unicode", 6, []byte("🔑"), []byte("📝")},
	}

	for _, tc := range testCases {
		encoded, err := record.EncodePutOpPayload(tc.txnId, tc.key, tc.value)
		if err != nil {
			t.Fatalf("%s: unexpected error encoding: %v", tc.name, err)
		}

		decoded, err := record.DecodePutOpPayload(encoded)
		if err != nil {
			t.Fatalf("%s: unexpected error decoding: %v", tc.name, err)
		}

		if decoded.TxnID != tc.txnId {
			t.Errorf("%s: expected txn_id %d, got %d", tc.name, tc.txnId, decoded.TxnID)
		}
		if !bytes.Equal(decoded.Key, tc.key) {
			t.Errorf("%s: key mismatch", tc.name)
		}
		if !bytes.Equal(decoded.Value, tc.value) {
			t.Errorf("%s: value mismatch", tc.name)
		}
	}
}

// TestEncodeDeleteOpPayloadRoundtrip tests encode/decode roundtrip for delete payloads
func TestEncodeDeleteOpPayloadRoundtrip(t *testing.T) {
	testCases := []struct {
		name  string
		txnId uint64
		key   []byte
	}{
		{"simple", 1, []byte("key")},
		{"binary data", 2, []byte{0x00, 0x01, 0x02, 0xFF}},
		{"large key", 3, make([]byte, record.MaxKeySize)},
		{"unicode", 4, []byte("🗑️")},
	}

	for _, tc := range testCases {
		encoded, err := record.EncodeDeleteOpPayload(tc.txnId, tc.key)
		if err != nil {
			t.Fatalf("%s: unexpected error encoding: %v", tc.name, err)
		}

		decoded, err := record.DecodeDeleteOpPayload(encoded)
		if err != nil {
			t.Fatalf("%s: unexpected error decoding: %v", tc.name, err)
		}

		if decoded.TxnID != tc.txnId {
			t.Errorf("%s: expected txn_id %d, got %d", tc.name, tc.txnId, decoded.TxnID)
		}
		if !bytes.Equal(decoded.Key, tc.key) {
			t.Errorf("%s: key mismatch", tc.name)
		}
	}
}

// TestEncodeBeginTxnPayloadRoundtrip tests roundtrip for begin payloads
func TestEncodeBeginTxnPayloadRoundtrip(t *testing.T) {
	testCases := []struct {
		name  string
		txnId uint64
	}{
		{"small", 1},
		{"medium", 1000000},
		{"large", ^uint64(0)},
	}

	for _, tc := range testCases {
		encoded, err := record.EncodeBeginTxnPayload(tc.txnId)
		if err != nil {
			t.Fatalf("%s: unexpected error encoding: %v", tc.name, err)
		}

		decoded, err := record.DecodeBeginTxnPayload(encoded)
		if err != nil {
			t.Fatalf("%s: unexpected error decoding: %v", tc.name, err)
		}

		if decoded.TxnID != tc.txnId {
			t.Errorf("%s: expected txn_id %d, got %d", tc.name, tc.txnId, decoded.TxnID)
		}
	}
}

// TestCodecErrorIs tests CodecError Is() method
func TestCodecErrorIs(t *testing.T) {
	testCases := []struct {
		name        string
		err         error
		targetErr   error
		shouldMatch bool
	}{
		{"truncated", &record.CodecError{Kind: record.CodecTruncated}, record.ErrCodecTruncated, true},
		{"corrupt", &record.CodecError{Kind: record.CodecCorrupt}, record.ErrCodecCorrupt, true},
		{"invalid", &record.CodecError{Kind: record.CodecInvalid}, record.ErrCodecInvalid, true},
		{"truncated non-match", &record.CodecError{Kind: record.CodecTruncated}, record.ErrCodecCorrupt, false},
	}

	for _, tc := range testCases {
		if (tc.err.(*record.CodecError).Is(tc.targetErr)) != tc.shouldMatch {
			t.Errorf("%s: Is() mismatch", tc.name)
		}
	}
}

// TestCodecErrorString tests CodecError Error() string representation
func TestCodecErrorString(t *testing.T) {
	err := &record.CodecError{
		Kind:  record.CodecTruncated,
		Field: "test_field",
		At:    10,
		Want:  20,
		Have:  15,
	}

	errStr := err.Error()
	if errStr == "" {
		t.Error("expected non-empty error string")
	}

	// Verify the error string contains key information
	if !bytes.Contains([]byte(errStr), []byte("truncated")) {
		t.Error("expected error string to contain 'truncated'")
	}
	if !bytes.Contains([]byte(errStr), []byte("test_field")) {
		t.Error("expected error string to contain field name")
	}
}

// TestDecodePutOpPayloadMaxSizes tests with maximum allowed key and value sizes
func TestDecodePutOpPayloadMaxSizes(t *testing.T) {
	txnId := uint64(2000)
	key := make([]byte, record.MaxKeySize)
	value := make([]byte, record.MaxValueSize)

	encoded, err := record.EncodePutOpPayload(txnId, key, value)
	if err != nil {
		t.Fatalf("unexpected error encoding: %v", err)
	}

	decoded, err := record.DecodePutOpPayload(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding: %v", err)
	}

	if uint32(len(decoded.Key)) != uint32(record.MaxKeySize) { //nolint:gosec
		t.Errorf("expected key len %d, got %d", record.MaxKeySize, len(decoded.Key))
	}
	if uint32(len(decoded.Value)) != uint32(record.MaxValueSize) { //nolint:gosec
		t.Errorf("expected value size %d, got %d", record.MaxValueSize, len(decoded.Value))
	}
}

// TestDecodePutOpPayloadKeyLenValidation tests the keyLen validation logic in detail
// This covers the specific code path in DecodePutOpPayload that validates keyLen > MaxKeySize
func TestDecodePutOpPayloadKeyLenValidation(t *testing.T) {
	txnId := uint64(2001)
	oversizeKeyLen := uint32(record.MaxKeySize + 100)
	value := []byte("value")

	// Build payload with oversized key length but incomplete key data
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, txnId)
	_ = binary.Write(buf, binary.LittleEndian, oversizeKeyLen)
	// key data omitted intentionally to test size validation before truncation check
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(value))) //nolint:gosec
	buf.Write(value)

	_, err := record.DecodePutOpPayload(buf.Bytes())
	if err == nil {
		t.Fatal("expected error for key length exceeding MaxKeySize")
	}

	codecErr, ok := err.(*record.CodecError)
	if !ok {
		t.Fatalf("expected CodecError, got %T", err)
	}

	// Verify all error details
	if codecErr.Kind != record.CodecInvalid {
		t.Errorf("expected Kind=CodecInvalid, got %v", codecErr.Kind)
	}
	if codecErr.Field != "key_len" {
		t.Errorf("expected Field='key_len', got '%s'", codecErr.Field)
	}
	if codecErr.Want != int(record.MaxKeySize) {
		t.Errorf("expected Want=%d, got %d", record.MaxKeySize, codecErr.Want)
	}
	if codecErr.Have != int(oversizeKeyLen) {
		t.Errorf("expected Have=%d, got %d", oversizeKeyLen, codecErr.Have)
	}
	if codecErr.At != record.TxnIdSize {
		t.Errorf("expected At=%d, got %d", record.TxnIdSize, codecErr.At)
	}
	if codecErr.Err != record.ErrCodecInvalid {
		t.Errorf("expected Err=ErrCodecInvalid, got %v", codecErr.Err)
	}
}
