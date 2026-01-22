package record_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// TestEncodeBeginTxnPayload_TableDriven consolidates encoding tests for BeginTransaction payloads
func TestEncodeBeginTxnPayload_TableDriven(t *testing.T) {
	testCases := []struct {
		name  string
		txnId uint64
	}{
		{"normal", 12345},
		{"zero", 0},
		{"max", ^uint64(0)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := record.EncodeBeginTxnPayload(tc.txnId)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(encoded) != record.TxnIdSize {
				t.Errorf("expected size %d, got %d", record.TxnIdSize, len(encoded))
			}

			decodedTxnId := binary.LittleEndian.Uint64(encoded[0:])
			if decodedTxnId != tc.txnId {
				t.Errorf("expected txn_id %d, got %d", tc.txnId, decodedTxnId)
			}
		})
	}
}

// TestDecodeBeginTxnPayload_TableDriven consolidates decoding tests for BeginTransaction payloads
func TestDecodeBeginTxnPayload_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		txnId        uint64
		prepare      func([]byte) []byte
		expectError  bool
		expectedKind record.CodecErrorKind
	}{
		{"valid", 98765, nil, false, 0},
		{"zero", 0, nil, false, 0},
		{"truncated", 12345, func(b []byte) []byte { return []byte{0x02, 0x03} }, true, record.CodecTruncated},
		{
			"extra data",
			12345,
			func(b []byte) []byte { return append(b, 0xFF, 0xFF) },
			true,
			record.CodecTruncated,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var payload []byte

			if !tc.expectError {
				encoded, _ := record.EncodeBeginTxnPayload(tc.txnId)
				payload = encoded
			} else if tc.prepare != nil {
				payload = tc.prepare(nil)
			}

			decoded, err := record.DecodeBeginTxnPayload(payload)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tc.expectError && err != nil {
				codecErr, ok := err.(*record.CodecError)
				if !ok {
					t.Errorf("expected CodecError, got %T", err)
				} else if codecErr.Kind != tc.expectedKind {
					t.Errorf("expected %v, got %v", tc.expectedKind, codecErr.Kind)
				}
			}
			if !tc.expectError && decoded.TxnID != tc.txnId {
				t.Errorf("expected txn_id %d, got %d", tc.txnId, decoded.TxnID)
			}
		})
	}
}

// TestEncodePutOpPayload tests encoding PutOperation payload
func TestEncodePutOpPayload_TableDriven(t *testing.T) {
	testCases := []struct {
		name          string
		txnId         uint64
		key           []byte
		value         []byte
		expectError   bool
		expectedKind  record.CodecErrorKind
		expectedField string
	}{
		{"normal", 111, []byte("mykey"), []byte("myvalue"), false, 0, ""},
		{"empty_value", 222, []byte("key"), []byte{}, false, 0, ""},
		{
			"oversized_key",
			333,
			make([]byte, record.MaxKeySize+1),
			[]byte("value"),
			true,
			record.CodecInvalid,
			"key_len",
		},
		{
			"oversized_value",
			444,
			[]byte("key"),
			make([]byte, record.MaxValueSize+1),
			true,
			record.CodecInvalid,
			"value_len",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := record.EncodePutOpPayload(tc.txnId, tc.key, tc.value)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.expectError && err != nil {
				codecErr, ok := err.(*record.CodecError)
				if !ok {
					t.Errorf("expected CodecError, got %T", err)
				} else {
					if codecErr.Kind != tc.expectedKind {
						t.Errorf("expected kind %v, got %v", tc.expectedKind, codecErr.Kind)
					}
					if codecErr.Field != tc.expectedField {
						t.Errorf("expected field %s, got %s", tc.expectedField, codecErr.Field)
					}
				}
			}

			if !tc.expectError {
				expectedLen := record.TxnIdSize + record.PayloadHeaderSize + len(
					tc.key,
				) + record.PayloadHeaderSize + len(
					tc.value,
				)
				if len(encoded) != expectedLen {
					t.Errorf("expected encoded length %d, got %d", expectedLen, len(encoded))
				}

				decoded, err := record.DecodePutOpPayload(encoded)
				if err != nil {
					t.Errorf("unexpected error decoding: %v", err)
				}
				if decoded.TxnID != tc.txnId {
					t.Errorf("expected txn_id %d, got %d", tc.txnId, decoded.TxnID)
				}
				if !bytes.Equal(decoded.Key, tc.key) {
					t.Errorf("expected key %v, got %v", tc.key, decoded.Key)
				}
				if !bytes.Equal(decoded.Value, tc.value) {
					t.Errorf("expected value %v, got %v", tc.value, decoded.Value)
				}
			}
		})
	}
}

func TestDecodePutOpPayload_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		prepare      func() []byte
		expectError  bool
		expectedKind record.CodecErrorKind
	}{
		{"valid", func() []byte {
			data, _ := record.EncodePutOpPayload(111, []byte("key"), []byte("value"))
			return data
		}, false, 0},
		{"empty", func() []byte {
			return []byte{}
		}, true, record.CodecTruncated},
		{"truncated_keylen", func() []byte {
			data := make([]byte, record.TxnIdSize+2)
			binary.LittleEndian.PutUint64(data[0:], 111)
			return data
		}, true, record.CodecTruncated},
		{"truncated_key", func() []byte {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint64(111))
			_ = binary.Write(buf, binary.LittleEndian, uint32(10))
			buf.Write([]byte("short"))
			return buf.Bytes()
		}, true, record.CodecTruncated},
		{"truncated_valuelen", func() []byte {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint64(111))
			_ = binary.Write(buf, binary.LittleEndian, uint32(5))
			buf.Write([]byte("mykey"))
			return buf.Bytes()
		}, true, record.CodecTruncated},
		{"extra_data", func() []byte {
			data, _ := record.EncodePutOpPayload(111, []byte("key"), []byte("value"))
			return append(data, 0xFF, 0xFF)
		}, true, record.CodecCorrupt},
		{"oversized_key", func() []byte {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint64(111))
			_ = binary.Write(buf, binary.LittleEndian, uint32(record.MaxKeySize+1))
			_ = binary.Write(buf, binary.LittleEndian, uint32(5))
			buf.Write([]byte("value"))
			return buf.Bytes()
		}, true, record.CodecInvalid},
		{"oversized_value", func() []byte {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint64(111))
			_ = binary.Write(buf, binary.LittleEndian, uint32(3))
			buf.Write([]byte("key"))
			_ = binary.Write(buf, binary.LittleEndian, uint32(record.MaxValueSize+1))
			return buf.Bytes()
		}, true, record.CodecInvalid},
		{"max_sizes", func() []byte {
			data, _ := record.EncodePutOpPayload(
				111,
				make([]byte, record.MaxKeySize),
				make([]byte, record.MaxValueSize),
			)
			return data
		}, false, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.prepare()
			decoded, err := record.DecodePutOpPayload(payload)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.expectError && err != nil {
				codecErr, ok := err.(*record.CodecError)
				if !ok {
					t.Errorf("expected CodecError, got %T", err)
				} else if codecErr.Kind != tc.expectedKind {
					t.Errorf("expected %v, got %v", tc.expectedKind, codecErr.Kind)
				}
			}

			if !tc.expectError && tc.name == "valid" {
				if decoded.TxnID != 111 {
					t.Errorf("expected txn_id 111, got %d", decoded.TxnID)
				}
				if !bytes.Equal(decoded.Key, []byte("key")) {
					t.Errorf("expected key %v, got %v", []byte("key"), decoded.Key)
				}
			}
			if !tc.expectError && tc.name == "max_sizes" {
				if len(decoded.Key) != int(record.MaxKeySize) {
					t.Errorf("expected key len %d, got %d", record.MaxKeySize, len(decoded.Key))
				}
				if len(decoded.Value) != int(record.MaxValueSize) {
					t.Errorf("expected value size %d, got %d", record.MaxValueSize, len(decoded.Value))
				}
			}
		})
	}
}

func TestEncodeDeleteOpPayload_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		txnId        uint64
		key          []byte
		expectError  bool
		expectedKind record.CodecErrorKind
	}{
		{"normal", 1001, []byte("deletekey"), false, 0},
		{"oversized_key", 1002, make([]byte, record.MaxKeySize+1), true, record.CodecInvalid},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := record.EncodeDeleteOpPayload(tc.txnId, tc.key)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.expectError && err != nil {
				codecErr, ok := err.(*record.CodecError)
				if !ok {
					t.Errorf("expected CodecError, got %T", err)
				} else if codecErr.Kind != tc.expectedKind {
					t.Errorf("expected %v, got %v", tc.expectedKind, codecErr.Kind)
				}
			}

			if !tc.expectError {
				expectedLen := record.TxnIdSize + record.PayloadHeaderSize + len(tc.key)
				if len(encoded) != expectedLen {
					t.Errorf("expected encoded length %d, got %d", expectedLen, len(encoded))
				}

				decoded, err := record.DecodeDeleteOpPayload(encoded)
				if err != nil {
					t.Errorf("unexpected error decoding: %v", err)
				}
				if decoded.TxnID != tc.txnId {
					t.Errorf("expected txn_id %d, got %d", tc.txnId, decoded.TxnID)
				}
				if !bytes.Equal(decoded.Key, tc.key) {
					t.Errorf("expected key %v, got %v", tc.key, decoded.Key)
				}
			}
		})
	}
}

func TestDecodeDeleteOpPayload_TableDriven(t *testing.T) {
	testCases := []struct {
		name         string
		prepare      func() []byte
		expectError  bool
		expectedKind record.CodecErrorKind
	}{
		{"valid", func() []byte {
			data, _ := record.EncodeDeleteOpPayload(1003, []byte("key"))
			return data
		}, false, 0},
		{"empty", func() []byte {
			return []byte{}
		}, true, record.CodecTruncated},
		{"truncated_keylen", func() []byte {
			data := make([]byte, record.TxnIdSize+2)
			binary.LittleEndian.PutUint64(data[0:], 1004)
			return data
		}, true, record.CodecTruncated},
		{"truncated_key", func() []byte {
			buf := new(bytes.Buffer)
			_ = binary.Write(buf, binary.LittleEndian, uint64(1005))
			_ = binary.Write(buf, binary.LittleEndian, uint32(10))
			buf.Write([]byte("short"))
			return buf.Bytes()
		}, true, record.CodecTruncated},
		{"extra_data", func() []byte {
			data, _ := record.EncodeDeleteOpPayload(1006, []byte("key"))
			return append(data, 0xFF, 0xFF)
		}, true, record.CodecCorrupt},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.prepare()
			decoded, err := record.DecodeDeleteOpPayload(payload)

			if tc.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tc.expectError && err != nil {
				codecErr, ok := err.(*record.CodecError)
				if !ok {
					t.Errorf("expected CodecError, got %T", err)
				} else if codecErr.Kind != tc.expectedKind {
					t.Errorf("expected %v, got %v", tc.expectedKind, codecErr.Kind)
				}
			}

			if !tc.expectError && tc.name == "valid" {
				if !bytes.Equal(decoded.Key, []byte("key")) {
					t.Errorf("expected key %v, got %v", []byte("key"), decoded.Key)
				}
			}
		})
	}
}

func TestEmptyPayloads_TableDriven(t *testing.T) {
	testCases := []struct {
		name   string
		decode func([]byte) error
	}{
		{"begin_txn", func(payload []byte) error {
			_, err := record.DecodeBeginTxnPayload(payload)
			return err
		}},
		{"put_op", func(payload []byte) error {
			_, err := record.DecodePutOpPayload(payload)
			return err
		}},
		{"delete_op", func(payload []byte) error {
			_, err := record.DecodeDeleteOpPayload(payload)
			return err
		}},
		{"commit_txn", func(payload []byte) error {
			_, err := record.DecodeCommitTxnPayload(payload)
			return err
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			emptyPayload := []byte{}
			err := tc.decode(emptyPayload)

			if err == nil {
				t.Errorf("expected error for empty payload")
			}

			codecErr, ok := err.(*record.CodecError)
			if !ok {
				t.Errorf("expected CodecError, got %T", err)
			} else if codecErr.Kind != record.CodecTruncated {
				t.Errorf("expected CodecTruncated, got %v", codecErr.Kind)
			}
		})
	}
}

func TestRoundtrip_TableDriven(t *testing.T) {
	t.Run("put_op", func(t *testing.T) {
		testCases := []struct {
			name  string
			txnId uint64
			key   []byte
			value []byte
		}{
			{"simple", 1, []byte("key"), []byte("value")},
			{"empty_value", 2, []byte("key"), []byte{}},
			{"binary_data", 3, []byte{0x00, 0x01, 0x02}, []byte{0xFF, 0xFE, 0xFD}},
			{"large_key", 4, make([]byte, record.MaxKeySize), []byte("value")},
			{"large_value", 5, []byte("key"), make([]byte, record.MaxValueSize)},
			{"unicode", 6, []byte("🔑"), []byte("📝")},
		}

		for _, tc := range testCases {
			encoded, err := record.EncodePutOpPayload(tc.txnId, tc.key, tc.value)
			if err != nil {
				t.Errorf("%s: unexpected error encoding: %v", tc.name, err)
			}

			decoded, err := record.DecodePutOpPayload(encoded)
			if err != nil {
				t.Errorf("%s: unexpected error decoding: %v", tc.name, err)
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
	})

	t.Run("delete_op", func(t *testing.T) {
		testCases := []struct {
			name  string
			txnId uint64
			key   []byte
		}{
			{"simple", 1, []byte("key")},
			{"binary_data", 2, []byte{0x00, 0x01, 0x02, 0xFF}},
			{"large_key", 3, make([]byte, record.MaxKeySize)},
			{"unicode", 4, []byte("🗑️")},
		}

		for _, tc := range testCases {
			encoded, err := record.EncodeDeleteOpPayload(tc.txnId, tc.key)
			if err != nil {
				t.Errorf("%s: unexpected error encoding: %v", tc.name, err)
			}

			decoded, err := record.DecodeDeleteOpPayload(encoded)
			if err != nil {
				t.Errorf("%s: unexpected error decoding: %v", tc.name, err)
			}

			if decoded.TxnID != tc.txnId {
				t.Errorf("%s: expected txn_id %d, got %d", tc.name, tc.txnId, decoded.TxnID)
			}
			if !bytes.Equal(decoded.Key, tc.key) {
				t.Errorf("%s: key mismatch", tc.name)
			}
		}
	})

	t.Run("begin_txn", func(t *testing.T) {
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
				t.Errorf("%s: unexpected error encoding: %v", tc.name, err)
			}

			decoded, err := record.DecodeBeginTxnPayload(encoded)
			if err != nil {
				t.Errorf("%s: unexpected error decoding: %v", tc.name, err)
			}

			if decoded.TxnID != tc.txnId {
				t.Errorf("%s: expected txn_id %d, got %d", tc.name, tc.txnId, decoded.TxnID)
			}
		}
	})
}

func TestCodecError_TableDriven(t *testing.T) {
	t.Run("Is", func(t *testing.T) {
		testCases := []struct {
			name        string
			err         error
			targetErr   error
			shouldMatch bool
		}{
			{"truncated", &record.CodecError{Kind: record.CodecTruncated}, record.ErrCodecTruncated, true},
			{"corrupt", &record.CodecError{Kind: record.CodecCorrupt}, record.ErrCodecCorrupt, true},
			{"invalid", &record.CodecError{Kind: record.CodecInvalid}, record.ErrCodecInvalid, true},
			{"truncated_non_match", &record.CodecError{Kind: record.CodecTruncated}, record.ErrCodecCorrupt, false},
		}

		for _, tc := range testCases {
			if (tc.err.(*record.CodecError).Is(tc.targetErr)) != tc.shouldMatch {
				t.Errorf("%s: Is() mismatch", tc.name)
			}
		}
	})

	t.Run("Error", func(t *testing.T) {
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
	})
}

func TestDecodePutOpPayloadKeyLenValidation_TableDriven(t *testing.T) {
	testCases := []struct {
		name              string
		txnId             uint64
		oversizeKeyLen    uint32
		value             []byte
		expectedKind      record.CodecErrorKind
		expectedField     string
		expectedWantValue int
	}{
		{
			"key_len_validation",
			2001,
			uint32(record.MaxKeySize + 100),
			[]byte("value"),
			record.CodecInvalid,
			"key_len",
			int(record.MaxKeySize),
		},
		{"max_sizes", 2000, 0, []byte(""), record.CodecInvalid, "", 0}, // special case handled separately
	}

	// Test key len validation in detail
	{
		txnId := uint64(2001)
		oversizeKeyLen := uint32(record.MaxKeySize + 100)
		value := []byte("value")

		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.LittleEndian, txnId)
		_ = binary.Write(buf, binary.LittleEndian, oversizeKeyLen)
		// nolint:gosec // len(value) is small constant, safe cast
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(value)))
		buf.Write(value)

		_, err := record.DecodePutOpPayload(buf.Bytes())
		if err == nil {
			t.Fatal("expected error for key length exceeding MaxKeySize")
		}

		codecErr, ok := err.(*record.CodecError)
		if !ok {
			t.Fatalf("expected CodecError, got %T", err)
		}

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

	// Test max sizes case
	{
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

		if len(decoded.Key) != int(record.MaxKeySize) {
			t.Errorf("expected key len %d, got %d", record.MaxKeySize, len(decoded.Key))
		}
		if len(decoded.Value) != int(record.MaxValueSize) {
			t.Errorf("expected value size %d, got %d", record.MaxValueSize, len(decoded.Value))
		}
	}

	_ = testCases // suppress unused warning
}
