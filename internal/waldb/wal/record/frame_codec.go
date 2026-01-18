package record

import (
	"encoding/binary"
	"io"

	"github.com/julianstephens/waldb/internal/waldb/errorutil"
)

// EncodeFrame encodes a record with the given type and payload.
// It returns the encoded byte slice or an error.
func EncodeFrame(recordType RecordType, payload []byte) ([]byte, error) {
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

// DecodeFrame decodes a record from the given byte slice.
// It returns the decoded Record or an error.
func DecodeFrame(data []byte) (FramedRecord, error) {
	if len(data) < RecordHeaderSize+RecordCRCSize {
		zero := int64(0)
		return FramedRecord{}, &ParseError{
			Coordinates: &errorutil.Coordinates{
				Offset: &zero,
			},
			Kind:               KindTruncated,
			SafeTruncateOffset: 0,
			Want:               RecordHeaderSize + RecordCRCSize,
			Have:               len(data),
			Err:                io.ErrUnexpectedEOF,
		}
	}

	recordLen := binary.LittleEndian.Uint32(data[:RecordHeaderSize])
	if err := ValidateRecordLength(recordLen); err != nil {
		if pe, ok := AsParseError(err); ok {
			zero := int64(0)
			pe.Coordinates = &errorutil.Coordinates{
				Offset: &zero,
			}
			pe.SafeTruncateOffset = 0
			return FramedRecord{}, pe
		}
		return FramedRecord{}, err
	}

	wantTotal := RecordHeaderSize + int(recordLen) + RecordCRCSize
	if len(data) < wantTotal {
		zero := int64(0)
		return FramedRecord{}, &ParseError{
			Coordinates: &errorutil.Coordinates{
				Offset: &zero,
			},
			Kind:               KindTruncated,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			Want:               wantTotal,
			Have:               len(data),
			Err:                io.ErrUnexpectedEOF,
		}
	}
	if len(data) != wantTotal {
		zero := int64(0)
		return FramedRecord{}, &ParseError{
			Coordinates: &errorutil.Coordinates{
				Offset: &zero,
			},
			Kind:               KindCorrupt,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			Want:               wantTotal,
			Have:               len(data),
			Err:                ErrInvalidLength,
		}
	}

	rawType := data[RecordHeaderSize]
	recordType := RecordType(rawType)
	if recordType <= RecordTypeUnknown || recordType > RecordTypeDeleteOperation {
		zero := int64(0)
		return FramedRecord{}, &ParseError{
			Coordinates: &errorutil.Coordinates{
				Offset: &zero,
			},
			Kind:               KindInvalidType,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			RawType:            rawType,
			RecordType:         recordType,
			Err:                ErrInvalidType,
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
		zero := int64(0)
		return FramedRecord{}, &ParseError{
			Coordinates: &errorutil.Coordinates{
				Offset: &zero,
			},
			Kind:               KindChecksumMismatch,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			RawType:            rawType,
			RecordType:         recordType,
			Err:                ErrChecksumMismatch,
		}
	}

	return rec, nil
}
