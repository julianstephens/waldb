package record

import (
	"encoding/binary"
	"io"
)

type FrameReader struct {
	r      io.Reader
	offset int64
}

// NewFrameReader creates a new FrameReader that reads framed records from the given io.Reader.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{
		r:      r,
		offset: 0,
	}
}

// Next reads the next record from the underlying reader.
// It returns the record and any error encountered.
func (rr *FrameReader) Next() (FramedRecord, error) {
	recordStart := rr.offset

	hdr := make([]byte, RecordHeaderSize)
	n, err := io.ReadFull(rr.r, hdr)
	if err != nil {
		rr.offset += int64(n)
		if err == io.EOF && n == 0 {
			return FramedRecord{}, io.EOF
		}

		return FramedRecord{}, &ParseError{
			Kind:               KindTruncated,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			Want:               RecordHeaderSize,
			Have:               n,
			Err:                io.ErrUnexpectedEOF,
		}
	}

	recordLen := binary.LittleEndian.Uint32(hdr)
	if err = ValidateRecordLength(recordLen); err != nil {
		if pe, ok := AsParseError(err); ok {
			pe.Offset = recordStart
			pe.SafeTruncateOffset = recordStart
			return FramedRecord{}, pe
		}
		return FramedRecord{}, err
	}

	// Read the rest of the record based on the length
	body := make([]byte, recordLen+RecordCRCSize)
	n, err = io.ReadFull(rr.r, body)
	if err != nil {
		rr.offset += int64(RecordHeaderSize + n)
		return FramedRecord{}, &ParseError{
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
		return FramedRecord{}, &ParseError{
			Kind:               KindInvalidType,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			RawType:            recordTypeRaw,
			RecordType:         recordType,
			Err:                ErrInvalidType,
		}
	}

	rec := FramedRecord{
		Offset: recordStart,
		Size:   int64(RecordHeaderSize + recordLen + RecordCRCSize),
		Record: Record{
			Len:     recordLen,
			Type:    recordType,
			Payload: body[1:int(recordLen)],
			CRC: binary.LittleEndian.Uint32(
				body[recordLen : recordLen+RecordCRCSize],
			),
		},
	}

	if !VerifyChecksum(&rec.Record) {
		return FramedRecord{}, &ParseError{
			Kind:               KindChecksumMismatch,
			Offset:             recordStart,
			SafeTruncateOffset: recordStart,
			DeclaredLen:        recordLen,
			RawType:            recordTypeRaw,
			RecordType:         recordType,
			Err:                ErrChecksumMismatch,
		}
	}

	return rec, nil
}

func (rr *FrameReader) SkipTo(offset int64) error {
	if offset < rr.offset {
		return &ReaderError{
			Kind:    ReaderInvalidSeek,
			Current: rr.offset,
			Want:    offset,
			Err:     ErrReaderInvalidSeek,
		}
	}

	skipBytes := offset - rr.offset
	buf := make([]byte, 4096)
	for skipBytes > 0 {
		toRead := int64(len(buf))
		if skipBytes < toRead {
			toRead = skipBytes
		}
		n, err := rr.r.Read(buf[:toRead])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.EOF
		}
		skipBytes -= int64(n)
		rr.offset += int64(n)
	}

	return nil
}

// Offset returns the current offset in the underlying reader.
func (rr *FrameReader) Offset() int64 {
	return rr.offset
}
