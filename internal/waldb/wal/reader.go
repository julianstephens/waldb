package wal

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type RecordReader struct {
	r      io.Reader
	offset int64
}

// NewRecordReader creates a new RecordReader that reads records from the given io.Reader.
func NewRecordReader(r io.Reader) *RecordReader {
	return &RecordReader{
		r:      r,
		offset: 0,
	}
}

// Next reads the next record from the underlying reader.
// It returns the record and any error encountered.
func (rr *RecordReader) Next() (record.FramedRecord, error) {
	recordStart := rr.offset

	hdr := make([]byte, record.RecordHeaderSize)
	n, err := io.ReadFull(rr.r, hdr)
	if err != nil {
		rr.offset += int64(n)
		if err == io.EOF && n == 0 {
			return record.FramedRecord{}, io.EOF
		}

		return record.FramedRecord{}, &record.ParseError{
			Kind:               record.KindTruncated,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			Want:               record.RecordHeaderSize,
			Have:               n,
			Err:                io.ErrUnexpectedEOF,
		}
	}

	recordLen := binary.LittleEndian.Uint32(hdr)
	if err = record.ValidateRecordLength(recordLen); err != nil {
		if pe, ok := record.AsParseError(err); ok {
			pe.Offset = recordStart
			pe.SafeTruncateOffset = recordStart
			return record.FramedRecord{}, pe
		}
		return record.FramedRecord{}, err
	}

	// Read the rest of the record based on the length
	body := make([]byte, recordLen+record.RecordCRCSize)
	n, err = io.ReadFull(rr.r, body)
	if err != nil {
		rr.offset += int64(record.RecordHeaderSize + n)
		return record.FramedRecord{}, &record.ParseError{
			Kind:               record.KindTruncated,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			Want:               int(recordLen) + record.RecordCRCSize,
			Have:               n,
			Err:                io.ErrUnexpectedEOF,
		}
	}
	rr.offset += int64(record.RecordHeaderSize + len(body))
	// Parse the record type and payload
	recordTypeRaw := body[0]
	recordType := record.RecordType(recordTypeRaw)
	if recordType <= record.RecordTypeUnknown || recordType > record.RecordTypeDeleteOperation {
		return record.FramedRecord{}, &record.ParseError{
			Kind:               record.KindInvalidType,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			RawType:            recordTypeRaw,
			RecordType:         recordType,
			Err:                errors.New("unknown record type"),
		}
	}

	rec := record.FramedRecord{
		Offset: recordStart,
		Size:   int64(record.RecordHeaderSize + recordLen + record.RecordCRCSize),
		Record: record.Record{
			Len:     recordLen,
			Type:    recordType,
			Payload: body[1:int(recordLen)],
			CRC: binary.LittleEndian.Uint32(
				body[recordLen : recordLen+record.RecordCRCSize],
			),
		},
	}

	if !record.VerifyChecksum(&rec.Record) {
		return record.FramedRecord{}, &record.ParseError{
			Kind:               record.KindChecksumMismatch,
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
