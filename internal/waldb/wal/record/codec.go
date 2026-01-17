package record

import (
	"encoding/binary"
	"fmt"
)

// Helpers

func need(data []byte, at, want int, field string) error {
	if at < 0 {
		at = 0
	}
	have := len(data) - at
	if have >= want {
		return nil
	}
	return &CodecError{
		Kind:  CodecTruncated,
		Field: field,
		At:    at,
		Want:  want,
		Have:  have,
		Err:   ErrCodecTruncated,
	}
}

func u32le(data []byte, at int, field string) (uint32, error) {
	if err := need(data, at, PayloadHeaderSize, field); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[at : at+PayloadHeaderSize]), nil
}

func u64le(data []byte, at int, field string) (uint64, error) {
	if err := need(data, at, TxnIdSize, field); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data[at : at+TxnIdSize]), nil
}

func rejectTrailing(data []byte, expectedLen int, field string) error {
	if len(data) == expectedLen {
		return nil
	}
	return &CodecError{
		Kind:  CodecCorrupt,
		Field: field,
		At:    expectedLen,
		Want:  expectedLen,
		Have:  len(data),
		Err:   fmt.Errorf("%w: trailing bytes", ErrCodecCorrupt),
	}
}

// Begin / Commit payloads

// EncodeBeginTxnPayload encodes the payload for a BeginTransaction record.
// Format: [txn_id (8)]
func EncodeBeginTxnPayload(txnId uint64) ([]byte, error) {
	payload := make([]byte, TxnIdSize)
	binary.LittleEndian.PutUint64(payload[:TxnIdSize], txnId)
	return payload, nil
}

// DecodeBeginTxnPayload decodes the payload for a BeginTransaction record.
// Format: [txn_id (8)]
func DecodeBeginTxnPayload(data []byte) (*BeginCommitTransactionPayload, error) {
	txnID, err := u64le(data, 0, "txn_id")
	if err != nil {
		return nil, err
	}
	if err := rejectTrailing(data, TxnIdSize, "payload_length"); err != nil {
		return nil, err
	}
	return &BeginCommitTransactionPayload{TransactionID: txnID}, nil
}

// EncodeCommitTxnPayload encodes the payload for a CommitTransaction record.
// Format: [txn_id (8)]
func EncodeCommitTxnPayload(txnId uint64) ([]byte, error) {
	payload := make([]byte, TxnIdSize)
	binary.LittleEndian.PutUint64(payload[:TxnIdSize], txnId)
	return payload, nil
}

// DecodeCommitTxnPayload decodes the payload for a CommitTransaction record.
// Format: [txn_id (8)]
func DecodeCommitTxnPayload(data []byte) (*BeginCommitTransactionPayload, error) {
	txnID, err := u64le(data, 0, "txn_id")
	if err != nil {
		return nil, err
	}
	if err := rejectTrailing(data, TxnIdSize, "payload_length"); err != nil {
		return nil, err
	}
	return &BeginCommitTransactionPayload{TransactionID: txnID}, nil
}

// PUT payloads

// EncodePutOpPayload encodes the payload for a PutOperation record.
// Format: [txn_id (8)][key_len (4)][key][value_len (4)][value]
func EncodePutOpPayload(txnId uint64, key, value []byte) ([]byte, error) {
	if uint32(len(key)) > MaxKeySize { //nolint:gosec
		return nil, &CodecError{
			Kind:  CodecInvalid,
			Field: "key_len",
			At:    0,
			Want:  int(MaxKeySize),
			Have:  len(key),
			Err:   ErrCodecInvalid,
		}
	}
	if uint32(len(value)) > MaxValueSize { //nolint:gosec
		return nil, &CodecError{
			Kind:  CodecInvalid,
			Field: "value_len",
			At:    0,
			Want:  int(MaxValueSize),
			Have:  len(value),
			Err:   ErrCodecInvalid,
		}
	}

	data := make([]byte, TxnIdSize+PayloadHeaderSize+len(key)+PayloadHeaderSize+len(value))
	off := 0

	binary.LittleEndian.PutUint64(data[off:off+TxnIdSize], txnId)
	off += TxnIdSize

	binary.LittleEndian.PutUint32(data[off:off+PayloadHeaderSize], uint32(len(key))) //nolint:gosec
	off += PayloadHeaderSize
	copy(data[off:off+len(key)], key)
	off += len(key)

	binary.LittleEndian.PutUint32(data[off:off+PayloadHeaderSize], uint32(len(value))) //nolint:gosec
	off += PayloadHeaderSize
	copy(data[off:off+len(value)], value)

	return data, nil
}

// DecodePutOpPayload decodes the payload for a PutOperation record.
// Format: [txn_id (8)][key_len (4)][key][value_len (4)][value]
func DecodePutOpPayload(data []byte) (*PutOpPayload, error) {
	off := 0

	txnID, err := u64le(data, off, "txn_id")
	if err != nil {
		return nil, err
	}
	off += TxnIdSize

	keyLen, err := u32le(data, off, "key_len")
	if err != nil {
		return nil, err
	}
	off += PayloadHeaderSize

	if keyLen > MaxKeySize {
		return nil, &CodecError{
			Kind:  CodecInvalid,
			Field: "key_len",
			At:    off - PayloadHeaderSize,
			Want:  int(MaxKeySize),
			Have:  int(keyLen),
			Err:   ErrCodecInvalid,
		}
	}

	if err := need(data, off, int(keyLen), "key"); err != nil {
		return nil, err
	}
	keyStart := off
	keyEnd := off + int(keyLen)
	off = keyEnd

	valueLen, err := u32le(data, off, "value_len")
	if err != nil {
		return nil, err
	}
	off += PayloadHeaderSize

	if valueLen > MaxValueSize {
		return nil, &CodecError{
			Kind:  CodecInvalid,
			Field: "value_len",
			At:    off - PayloadHeaderSize,
			Want:  int(MaxValueSize),
			Have:  int(valueLen),
			Err:   ErrCodecInvalid,
		}
	}

	if err := need(data, off, int(valueLen), "value"); err != nil {
		return nil, err
	}
	valueStart := off
	valueEnd := off + int(valueLen)
	off = valueEnd

	if err := rejectTrailing(data, off, "payload_length"); err != nil {
		return nil, err
	}

	return &PutOpPayload{
		TransactionID: txnID,
		Key:           data[keyStart:keyEnd],
		Value:         data[valueStart:valueEnd],
	}, nil
}

// DELETE payloads

// EncodeDeleteOpPayload encodes the payload for a DeleteOperation record.
// Format: [txn_id (8)][key_len (4)][key]
func EncodeDeleteOpPayload(txnId uint64, key []byte) ([]byte, error) {
	if uint32(len(key)) > MaxKeySize { //nolint:gosec
		return nil, &CodecError{
			Kind:  CodecInvalid,
			Field: "key_len",
			At:    0,
			Want:  int(MaxKeySize),
			Have:  len(key),
			Err:   ErrCodecInvalid,
		}
	}

	data := make([]byte, TxnIdSize+PayloadHeaderSize+len(key))
	off := 0

	binary.LittleEndian.PutUint64(data[off:off+TxnIdSize], txnId)
	off += TxnIdSize

	binary.LittleEndian.PutUint32(data[off:off+PayloadHeaderSize], uint32(len(key))) //nolint:gosec
	off += PayloadHeaderSize
	copy(data[off:off+len(key)], key)

	return data, nil
}

// DecodeDeleteOpPayload decodes the payload for a DeleteOperation record.
// Format: [txn_id (8)][key_len (4)][key]
func DecodeDeleteOpPayload(data []byte) (*DeleteOpPayload, error) {
	off := 0

	txnID, err := u64le(data, off, "txn_id")
	if err != nil {
		return nil, err
	}
	off += TxnIdSize

	keyLen, err := u32le(data, off, "key_len")
	if err != nil {
		return nil, err
	}
	off += PayloadHeaderSize

	if keyLen > MaxKeySize {
		return nil, &CodecError{
			Kind:  CodecInvalid,
			Field: "key_len",
			At:    off - PayloadHeaderSize,
			Want:  int(MaxKeySize),
			Have:  int(keyLen),
			Err:   ErrCodecInvalid,
		}
	}

	if err := need(data, off, int(keyLen), "key"); err != nil {
		return nil, err
	}
	keyStart := off
	keyEnd := off + int(keyLen)
	off = keyEnd

	if err := rejectTrailing(data, off, "payload_length"); err != nil {
		return nil, err
	}

	return &DeleteOpPayload{
		TransactionID: txnID,
		Key:           data[keyStart:keyEnd],
	}, nil
}
