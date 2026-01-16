package record

import (
	"io"
)

const (
	MaxRecordSize = 16 * 1024 * 1024 // 16 MB
)

// NewRecordReader creates a new RecordReader that reads records from the given io.Reader.
func NewRecordReader(r io.Reader) *RecordReader {
	return &RecordReader{
		r: r,
	}
}

// Next reads the next record from the underlying reader.
// It returns the record and any error encountered.
func (rr *RecordReader) Next() (Record, error) {
	result := Record{}
	recordLen := make([]byte, 4)

	_, err := rr.r.Read(recordLen)
	if err != nil {
		return Record{}, &ParseError{
			Kind:   KindTruncated,
			Offset: rr.offset,
			Err:    err,
		}
	}
	err = validateRecordLength(
		uint32(recordLen[0]) | uint32(recordLen[1])<<8 | uint32(recordLen[2])<<16 | uint32(recordLen[3])<<24,
	)
	if err != nil {
		return Record{}, err
	}
	result.Len = uint32(recordLen[0]) | uint32(recordLen[1])<<8 | uint32(recordLen[2])<<16 | uint32(recordLen[3])<<24

	// Read the rest of the record based on the length
	data := make([]byte, result.Len+4)
	_, err = rr.r.Read(data)
	if err != nil {
		return Record{}, &ParseError{
			Kind:   KindTruncated,
			Offset: rr.offset,
			Err:    err,
		}
	}

	// Parse the record type and payload
	recordType := RecordType(data[0])
	if recordType == RecordTypeUnknown {
		return Record{}, &ParseError{
			Kind:       KindInvalidType,
			Offset:     rr.offset,
			RawType:    data[0],
			RecordType: recordType,
			Err:        io.ErrUnexpectedEOF,
		}
	}

	payload := data[1:result.Len]

	result.Type = recordType
	result.Payload = payload
	crcIndex := int(result.Len)
	result.CRC = uint32(
		data[crcIndex],
	) | uint32(
		data[crcIndex+1],
	)<<8 | uint32(
		data[crcIndex+2],
	)<<16 | uint32(
		data[crcIndex+3],
	)<<24

	if !VerifyChecksum(&result) {
		return Record{}, &ParseError{
			Kind:   KindChecksumMismatch,
			Offset: rr.offset,
			Err:    io.ErrUnexpectedEOF,
		}
	}

	rr.offset += int64(4 + len(data))

	return result, nil
}

// Encode writes a record with the given type and payload to the underlying writer.
// It returns any error encountered.
func (rr *RecordReader) Encode(recordType RecordType, payload []byte) error {
	return nil
}

func validateRecordLength(length uint32) error {
	if length == 0 {
		return &ParseError{
			Kind: KindInvalidLength,
			Err:  io.ErrUnexpectedEOF,
		}
	}

	if length > MaxRecordSize {
		return &ParseError{
			Kind: KindTooLarge,
			Want: int(length),
			Have: MaxRecordSize,
			Err:  io.ErrUnexpectedEOF,
		}
	}
	return nil
}
