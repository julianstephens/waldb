package record

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	RecordHeaderSize = 4                // Length of the record length field
	RecordCRCSize    = 4                // Length of the CRC32 field
	MaxRecordSize    = 16 * 1024 * 1024 // 16 MB
)

// NewRecordReader creates a new RecordReader that reads records from the given io.Reader.
func NewRecordReader(r io.Reader) *RecordReader {
	return &RecordReader{
		r:      r,
		offset: 0,
	}
}

// Next reads the next record from the underlying reader.
// It returns the record and any error encountered.
func (rr *RecordReader) Next() (Record, error) {
	recordStart := rr.offset

	hdr := make([]byte, RecordHeaderSize)
	n, err := io.ReadFull(rr.r, hdr)
	if err != nil {
		rr.offset += int64(n)
		if err == io.EOF && n == 0 {
			return Record{}, io.EOF
		}

		return Record{}, &ParseError{
			Kind:               KindTruncated,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			Want:               RecordHeaderSize,
			Have:               n,
			Err:                io.ErrUnexpectedEOF,
		}
	}

	recordLen := binary.LittleEndian.Uint32(hdr)
	if err = validateRecordLength(recordLen); err != nil {
		if pe, ok := AsParseError(err); ok {
			pe.Offset = recordStart
			pe.SafeTruncateOffset = recordStart
			return Record{}, pe
		}
		return Record{}, err
	}

	// Read the rest of the record based on the length
	body := make([]byte, recordLen+RecordCRCSize)
	n, err = io.ReadFull(rr.r, body)
	if err != nil {
		rr.offset += int64(RecordHeaderSize + n)
		return Record{}, &ParseError{
			Kind:               KindTruncated,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			Want:               int(recordLen) + RecordCRCSize,
			Have:               n,
			Err:                io.ErrUnexpectedEOF,
		}
	}
	rr.offset += int64(RecordHeaderSize + len(body))

	// Parse the record type and payload
	recordTypeRaw := body[0]
	recordType := RecordType(recordTypeRaw)
	if recordType <= RecordTypeUnknown || recordType > RecordTypeDeleteOperation {
		return Record{}, &ParseError{
			Kind:               KindInvalidType,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			RawType:            recordTypeRaw,
			RecordType:         recordType,
			Err:                errors.New("unknown record type"),
		}
	}

	rec := Record{
		Len:     recordLen,
		Type:    recordType,
		Payload: body[1:int(recordLen)],
		CRC: binary.LittleEndian.Uint32(
			body[recordLen : recordLen+RecordCRCSize],
		),
	}

	if !VerifyChecksum(&rec) {
		return Record{}, &ParseError{
			Kind:               KindChecksumMismatch,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			RawType:            recordTypeRaw,
			RecordType:         recordType,
			Err:                errors.New("checksum mismatch"),
		}
	}

	return rec, nil
}

// Offset returns the current offset in the underlying reader.
func (rr *RecordReader) Offset() int64 {
	return rr.offset
}

// Encode encodes a record with the given type and payload.
// It returns the encoded byte slice or an error.
func Encode(recordType RecordType, payload []byte) ([]byte, error) {
	recordLen := uint32(len(payload)) + 1 //nolint:gosec
	err := validateRecordLength(recordLen)
	if err != nil {
		return nil, err
	}

	data := make([]byte, RecordHeaderSize+recordLen+RecordCRCSize)

	data[0] = byte(recordLen)
	data[1] = byte(recordLen >> 8)
	data[2] = byte(recordLen >> 16)
	data[3] = byte(recordLen >> 24)

	data[4] = byte(recordType)
	copy(data[5:], payload)

	crc := ComputeChecksum(data[RecordHeaderSize : RecordHeaderSize+recordLen])
	crcIndex := RecordHeaderSize + recordLen
	data[crcIndex] = byte(crc)
	data[crcIndex+1] = byte(crc >> 8)
	data[crcIndex+2] = byte(crc >> 16)
	data[crcIndex+3] = byte(crc >> 24)

	return data, nil
}

// Decode decodes a record from the given byte slice.
// It returns the decoded Record or an error.
func Decode(data []byte) (Record, error) {
	if len(data) < RecordHeaderSize+RecordCRCSize {
		return Record{}, &ParseError{
			Kind:               KindTruncated,
			Offset:             0,
			SafeTruncateOffset: 0,
			Want:               RecordHeaderSize + RecordCRCSize,
			Have:               len(data),
			Err:                io.ErrUnexpectedEOF,
		}
	}

	recordLen := binary.LittleEndian.Uint32(data[:RecordHeaderSize])
	if err := validateRecordLength(recordLen); err != nil {
		if pe, ok := AsParseError(err); ok {
			pe.Offset = 0
			pe.SafeTruncateOffset = 0
			return Record{}, pe
		}
		return Record{}, err
	}

	wantTotal := RecordHeaderSize + int(recordLen) + RecordCRCSize
	if len(data) < wantTotal {
		return Record{}, &ParseError{
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
		return Record{}, &ParseError{
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
		return Record{}, &ParseError{
			Kind:               KindInvalidType,
			Offset:             0,
			SafeTruncateOffset: 0,
			DeclaredLen:        recordLen,
			RawType:            rawType,
			RecordType:         recordType,
			Err:                errors.New("unknown record type"),
		}
	}

	rec := Record{
		Len:     recordLen,
		Type:    recordType,
		Payload: data[RecordHeaderSize+1 : RecordHeaderSize+recordLen],
		CRC: binary.LittleEndian.Uint32(
			data[RecordHeaderSize+recordLen : wantTotal],
		),
	}

	if !VerifyChecksum(&rec) {
		return Record{}, &ParseError{
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

func validateRecordLength(length uint32) error {
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
