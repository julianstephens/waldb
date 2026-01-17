package record

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

type FramedRecord struct {
	Record Record `json:"record"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

type BeginCommitTransactionPayload struct {
	TxnID uint64 `json:"txn_id"`
}

type PutOpPayload struct {
	TxnID uint64 `json:"txn_id"`
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type DeleteOpPayload struct {
	TxnID uint64 `json:"txn_id"`
	Key   []byte `json:"key"`
}
