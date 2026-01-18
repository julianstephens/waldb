package recovery

import (
	"io"

	"github.com/julianstephens/go-utils/validator"
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/errorutil"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type TailStatus int

const (
	TailStatusValid TailStatus = iota
	TailStatusCorrupt
	TailStatusMissing
)

func (ts TailStatus) String() string {
	switch ts {
	case TailStatusValid:
		return "valid"
	case TailStatusCorrupt:
		return "corrupt"
	case TailStatusMissing:
		return "missing"
	default:
		return "unknown"
	}
}

type ReplayResult struct {
	NextTxnId          uint64
	LastCommittedTxnId uint64
	LastValid          wal.Boundary
	TailStatus         TailStatus
}

// Replay replays WAL segments from the given starting boundary into the provided memtable.
// It returns a ReplayResult indicating the next transaction ID and last valid boundary.
func Replay(p wal.SegmentProvider, start wal.Boundary, mem *memtable.Table, lg logger.Logger) (*ReplayResult, error) {
	state := newReplayState(mem)
	truncateTo := start

	ids := p.SegmentIDs()

	startIdx := -1
	for i, segId := range ids {
		if segId == start.SegId {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		return nil, &ReplayLogicError{
			Coordinates: &errorutil.Coordinates{
				SegId: &start.SegId,
			},
			Type: record.RecordTypeUnknown,
			Err:  ErrSegmentNotFound,
		}
	}

	for i := startIdx; i < len(ids); i++ {
		segId := ids[i]
		sr, err := p.OpenSegment(segId)
		if err != nil {
			return &ReplayResult{
					NextTxnId: state.maxCommitted + 1,
					LastValid: truncateTo,
				}, &ReplayLogicError{
					Coordinates: &errorutil.Coordinates{
						SegId: &segId,
					},
					Err: err,
				}
		}
		if i == startIdx {
			if err := sr.SeekTo(start.Offset); err != nil {
				return &ReplayResult{
						NextTxnId: state.maxCommitted + 1,
						LastValid: truncateTo,
					}, &ReplayLogicError{
						Coordinates: &errorutil.Coordinates{
							SegId:  &segId,
							Offset: &start.Offset,
						},
						Type: record.RecordTypeUnknown,
						Err:  err,
					}
			}
		} else {
			if err := sr.SeekTo(0); err != nil {
				return &ReplayResult{
						NextTxnId: state.maxCommitted + 1,
						LastValid: truncateTo,
					}, &ReplayLogicError{
						Coordinates: &errorutil.Coordinates{
							SegId: &segId,
						},
						Type: record.RecordTypeUnknown,
						Err:  err,
					}
			}
		}

		rr := record.NewFrameReader(sr.Reader())
		for {
			rec, err := rr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return &ReplayResult{
						NextTxnId: state.maxCommitted + 1,
						LastValid: truncateTo,
					}, &ReplayDecodeError{
						Coordinates: &errorutil.Coordinates{
							SegId: &segId,
						},
						RecordType: record.RecordTypeUnknown,
						SafeOffset: truncateTo.Offset,
						Err:        err,
					}
			}

			if err := replayOne(rec, segId, state); err != nil {
				return &ReplayResult{
					NextTxnId: state.maxCommitted + 1,
					LastValid: truncateTo,
				}, err
			}

			truncateTo = wal.Boundary{
				SegId:  segId,
				Offset: rec.Offset + rec.Size,
			}

		}

		if err := sr.Close(); err != nil {
			return &ReplayResult{
					NextTxnId: state.maxCommitted + 1,
					LastValid: truncateTo,
				}, &ReplayLogicError{
					Coordinates: &errorutil.Coordinates{
						SegId:  &segId,
						Offset: &truncateTo.Offset,
					},
					Type: record.RecordTypeUnknown,
					Err:  err,
				}
		}
	}

	return &ReplayResult{
		NextTxnId: state.maxCommitted + 1,
		LastValid: truncateTo,
	}, nil
}

func replayOne(rec record.FramedRecord, segId uint64, state *replayState) error {
	switch rec.Record.Type {
	case record.RecordTypeBeginTransaction:
		payload, err := record.DecodeBeginTxnPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId: &segId,
				},
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		return state.onBegin(*payload, recordCtx{
			offset:      rec.Offset,
			segId:       segId,
			declaredLen: rec.Record.Len,
			recordType:  rec.Record.Type,
		})

	case record.RecordTypeCommitTransaction:
		payload, err := record.DecodeCommitTxnPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId: &segId,
				},
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		return state.onCommit(*payload, recordCtx{
			offset:      rec.Offset,
			segId:       segId,
			declaredLen: rec.Record.Len,
			recordType:  rec.Record.Type,
		})
	case record.RecordTypePutOperation:
		payload, err := record.DecodePutOpPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId: &segId,
				},
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		return state.onPut(*payload, recordCtx{
			offset:      rec.Offset,
			segId:       segId,
			declaredLen: rec.Record.Len,
			recordType:  rec.Record.Type,
		})
	case record.RecordTypeDeleteOperation:
		payload, err := record.DecodeDeleteOpPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId: &segId,
				},
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		return state.onDel(*payload, recordCtx{
			offset:      rec.Offset,
			segId:       segId,
			declaredLen: rec.Record.Len,
			recordType:  rec.Record.Type,
		})
	default:
		return &ReplayDecodeError{
			Coordinates: &errorutil.Coordinates{
				SegId: &segId,
			},
			RecordType:  record.RecordTypeUnknown,
			DeclaredLen: rec.Record.Len,
			Err:         record.ErrInvalidType,
		}
	}
}

// validateSegments checks that the given segment IDs are well-ordered, consecutive, and non-empty.
func validateSegments(ids []uint64) error {
	v := validator.Numbers[uint64]()

	if len(ids) == 0 {
		return nil
	}

	for i := 1; i < len(ids); i++ {
		if err := v.ValidateNonZero(ids[i]); err != nil {
			return err
		}

	}

	return nil
}
