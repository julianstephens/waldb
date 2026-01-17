package record

import (
	"encoding/binary"
	"errors"
	"io"
)

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

// Encode encodes a record with the given type and payload.
// It returns the encoded byte slice or an error.
func Encode(recordType RecordType, payload []byte) ([]byte, error) {
	recordLen := uint32(len(payload)) + 1 //nolint:gosec
	err := ValidateRecordLength(recordLen)
	if err != nil {
		return nil, err
	}

	data := make([]byte, RecordHeaderSize+recordLen+RecordCRCSize)

	binary.LittleEndian.PutUint32(data[:RecordHeaderSize], recordLen)

	data[4] = byte(recordType)
	copy(data[5:], payload)

	crc := ComputeChecksum(data[RecordHeaderSize : RecordHeaderSize+recordLen])
	crcIndex := RecordHeaderSize + recordLen
	binary.LittleEndian.PutUint32(data[crcIndex:], crc)

	return data, nil
}

// Decode decodes a record from the given byte slice.
// It returns the decoded Record or an error.
func Decode(data []byte) (FramedRecord, error) {
	if len(data) < RecordHeaderSize+RecordCRCSize {
		return FramedRecord{}, &ParseError{
			Kind:               KindTruncated,
			Offset:             0,
			SafeTruncateOffset: 0,
			Want:               RecordHeaderSize + RecordCRCSize,
			Have:               len(data),
			Err:                io.ErrUnexpectedEOF,
		}
	}

	recordLen := binary.LittleEndian.Uint32(data[:RecordHeaderSize])
	if err := ValidateRecordLength(recordLen); err != nil {
		if pe, ok := AsParseError(err); ok {
			pe.Offset = 0
			pe.SafeTruncateOffset = 0
			return FramedRecord{}, pe
		}
		return FramedRecord{}, err
	}

	wantTotal := RecordHeaderSize + int(recordLen) + RecordCRCSize
	if len(data) < wantTotal {
		return FramedRecord{}, &ParseError{
			Kind:               KindTruncated,
			Offset:             0,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			Want:               wantTotal,
			Have:               len(data),
			Err:                io.ErrUnexpectedEOF,
		}
	}
	if len(data) != wantTotal {
		return FramedRecord{}, &ParseError{
			Kind:               KindCorrupt,
			Offset:             0,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			Want:               wantTotal,
			Have:               len(data),
			Err:                errors.New("extra data beyond expected record length"),
		}
	}

	rawType := data[RecordHeaderSize]
	recordType := RecordType(rawType)
	if recordType <= RecordTypeUnknown || recordType > RecordTypeDeleteOperation {
		return FramedRecord{}, &ParseError{
			Kind:               KindInvalidType,
			Offset:             0,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			RawType:            rawType,
			RecordType:         recordType,
			Err:                errors.New("unknown record type"),
		}
	}

	rec := FramedRecord{
		Offset: 0,
		Size:   int64(RecordHeaderSize + recordLen + RecordCRCSize),
		Record: Record{
			Len:     recordLen,
			Type:    recordType,
			Payload: data[RecordHeaderSize+1 : RecordHeaderSize+recordLen],
			CRC: binary.LittleEndian.Uint32(
				data[RecordHeaderSize+recordLen : wantTotal],
			),
		},
	}

	if !VerifyChecksum(&rec.Record) {
		return FramedRecord{}, &ParseError{
			Kind:               KindChecksumMismatch,
			Offset:             0,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			RawType:            rawType,
			RecordType:         recordType,
			Err:                errors.New("checksum mismatch"),
		}
	}

	return rec, nil
}

func ValidateRecordLength(length uint32) error {
	if length < 1 {
		return &ParseError{
			Kind:        KindInvalidLength,
			DeclaredLen: length,
			Err:         errors.New("record length must be >= 1"),
		}
	}

	if length > MaxRecordSize {
		return &ParseError{
			Kind:        KindTooLarge,
			DeclaredLen: length,
			Want:        int(length),
			Have:        MaxRecordSize,
			Err:         errors.New("record length exceeds maximum allowed size"),
		}
	}
	return nil
}
