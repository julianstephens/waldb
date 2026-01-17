package recovery

import (
	"context"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type Result struct {
	LastTxnID uint64
}

func Replay(ctx context.Context, rec record.FramedRecord) error {
	return replayOne(rec)
}

func replayOne(rec record.FramedRecord) error {
	switch rec.Record.Type {
	case record.RecordTypeBeginTransaction:
		_, err := record.DecodeBeginTxnPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				RecordType:   rec.Record.Type,
				RecordOffset: rec.Offset,
				DeclaredLen:  rec.Record.Len,
				Err:          err,
			}
		}
	case record.RecordTypeCommitTransaction:
		_, err := record.DecodeCommitTxnPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				RecordType:   rec.Record.Type,
				RecordOffset: rec.Offset,
				DeclaredLen:  rec.Record.Len,
				Err:          err,
			}
		}
	case record.RecordTypePutOperation:
		_, err := record.DecodePutOpPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				RecordType:   rec.Record.Type,
				RecordOffset: rec.Offset,
				DeclaredLen:  rec.Record.Len,
				Err:          err,
			}
		}
	case record.RecordTypeDeleteOperation:
		_, err := record.DecodeDeleteOpPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				RecordType:   rec.Record.Type,
				RecordOffset: rec.Offset,
				DeclaredLen:  rec.Record.Len,
				Err:          err,
			}
		}
	default:
		return &ReplayDecodeError{
			RecordType:   record.RecordTypeUnknown,
			RecordOffset: rec.Offset,
			DeclaredLen:  rec.Record.Len,
		}
	}

	return nil
}
