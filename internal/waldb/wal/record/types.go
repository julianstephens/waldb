package record

import "io"

type RecordType uint8

const (
	RecordTypeUnknown RecordType = iota
	RecordTypeBeginTransaction
	RecordTypeCommitTransaction
	RecordTypePutOperation
	RecordTypeDeleteOperation
)

type Record struct {
	Type    RecordType `json:"type"`
	Payload []byte     `json:"payload"`
	CRC     uint32     `json:"crc"`
	// The length of the record type + payload (excluding CRC)
	Len uint32 `json:"len"`
}

type RecordReader struct {
	r      io.Reader
	offset int64
}
