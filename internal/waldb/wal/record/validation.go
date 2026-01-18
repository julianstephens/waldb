package record

const (
	RecordHeaderSize     = 4                // Length of the record length field
	RecordTypeHeaderSize = 1                // Length of the record type field
	RecordCRCSize        = 4                // Length of the CRC32 field
	MaxKeySize           = 4 * 1024         // 4 KB
	MaxValueSize         = 4 * 1024 * 1024  // 4 MB
	MaxRecordSize        = 16 * 1024 * 1024 // 16 MB
	TxnIdSize            = 8                // Size of Transaction ID field (uint64)
	PayloadHeaderSize    = 4                // Size of payload header (e.g., for key/value lengths)
)

// ValidateRecordLength checks if the given record length is within valid bounds.
func ValidateRecordLength(length uint32) error {
	if length < 1 {
		return &ParseError{
			Kind:        KindInvalidLength,
			DeclaredLen: length,
			Err:         ErrInvalidLength,
		}
	}

	if length > MaxRecordSize {
		return &ParseError{
			Kind:        KindTooLarge,
			DeclaredLen: length,
			Want:        MaxRecordSize,
			Have:        int(length),
			Err:         ErrTooLarge,
		}
	}
	return nil
}

// ValidateRecordFrame validates the record type and payload according to predefined rules.
func ValidateRecordFrame(recordType RecordType, payload []byte) error {
	if err := ValidateRecordLength(uint32(len(payload)) + 1); err != nil { //nolint:gosec
		return err
	}
	switch recordType {
	case RecordTypeBeginTransaction:
		if len(payload) != TxnIdSize {
			return &ParseError{
				Kind:       KindInvalidLength,
				RecordType: recordType,
				Err:        ErrInvalidLength,
			}
		}
	case RecordTypeCommitTransaction:
		if len(payload) != TxnIdSize {
			return &ParseError{
				Kind:       KindInvalidLength,
				RecordType: recordType,
				Err:        ErrInvalidLength,
			}
		}
	case RecordTypePutOperation:
		if len(payload) < TxnIdSize+PayloadHeaderSize+PayloadHeaderSize {
			return &ParseError{
				Kind:       KindInvalidLength,
				Have:       len(payload),
				Want:       TxnIdSize + PayloadHeaderSize + PayloadHeaderSize,
				RecordType: recordType,
				Err:        ErrInvalidLength,
			}
		}
		if len(payload) > TxnIdSize+PayloadHeaderSize+MaxKeySize+PayloadHeaderSize+MaxValueSize {
			return &ParseError{
				Kind:       KindTooLarge,
				RecordType: recordType,
				Want:       TxnIdSize + PayloadHeaderSize + MaxKeySize + PayloadHeaderSize + MaxValueSize,
				Have:       len(payload),
				Err:        ErrTooLarge,
			}
		}
	case RecordTypeDeleteOperation:
		if len(payload) < TxnIdSize+PayloadHeaderSize {
			return &ParseError{
				Kind:       KindInvalidLength,
				Have:       len(payload),
				Want:       TxnIdSize + PayloadHeaderSize,
				RecordType: recordType,
				Err:        ErrInvalidLength,
			}
		}
		if len(payload) > TxnIdSize+PayloadHeaderSize+MaxKeySize {
			return &ParseError{
				Kind:       KindTooLarge,
				RecordType: recordType,
				Want:       TxnIdSize + PayloadHeaderSize + MaxKeySize,
				Have:       len(payload),
				Err:        ErrTooLarge,
			}
		}
	default:
		return &ParseError{
			Kind:       KindInvalidType,
			RecordType: recordType,
			Err:        ErrInvalidType,
		}
	}

	return nil
}

func EncodedRecordSize(payloadLen int) int64 {
	return RecordHeaderSize + 1 + int64(payloadLen) + RecordCRCSize
}
