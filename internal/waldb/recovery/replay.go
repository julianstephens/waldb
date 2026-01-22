package recovery

import (
	"errors"
	"io"

	"github.com/julianstephens/go-utils/generic"
	"github.com/julianstephens/go-utils/validator"
	"github.com/julianstephens/waldb/internal/logger"
	"github.com/julianstephens/waldb/internal/waldb/errorutil"
	"github.com/julianstephens/waldb/internal/waldb/memtable"
	"github.com/julianstephens/waldb/internal/waldb/wal"
	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

type TailStatus int

const (
	// TailStatusValid indicates the tail of the WAL is valid with no issues.
	TailStatusValid TailStatus = iota
	// TailStatusCorrupt indicates an undecodable or invalid record was found in the WAL.
	TailStatusCorrupt
	// TailStatusMissing indicates the WAL segment containing the starting boundary is missing.
	TailStatusMissing
	// TailStatusTruncated indicates the WAL was truncated but in a recoverable manner.
	TailStatusTruncated
)

func (ts TailStatus) String() string {
	switch ts {
	case TailStatusValid:
		return "valid"
	case TailStatusCorrupt:
		return "corrupt"
	case TailStatusMissing:
		return "missing"
	case TailStatusTruncated:
		return "truncated"
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
	if lg == nil {
		lg = logger.NoOpLogger{}
	}

	state := newReplayState(mem)
	lastValidBoundary := start

	result := &ReplayResult{
		NextTxnId:          0,
		LastCommittedTxnId: 0,
		LastValid:          lastValidBoundary,
	}

	ids := p.SegmentIDs()
	if err := validateSegments(ids); err != nil {
		lg.Error("segment validation failed", err)
		result.NextTxnId = state.maxCommitted + 1
		result.LastCommittedTxnId = state.maxCommitted
		result.LastValid = lastValidBoundary
		result.TailStatus = TailStatusCorrupt
		return result, &ReplaySourceError{
			Kind: ReplaySourceSegmentOrder,
			Coordinates: &errorutil.Coordinates{
				SegId:  &start.SegId,
				Offset: &lastValidBoundary.Offset,
			},
			Cause: err,
			Err:   ErrSegmentOrder,
		}
	}

	startIdx := -1
	for i, segId := range ids {
		if segId == start.SegId {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		lg.Error("start segment not found", nil, "seg", start.SegId)
		result.NextTxnId = state.maxCommitted + 1
		result.LastCommittedTxnId = state.maxCommitted
		result.LastValid = lastValidBoundary
		result.TailStatus = TailStatusMissing
		return result, &ReplaySourceError{
			Kind: ReplaySourceSegmentMissing,
			Coordinates: &errorutil.Coordinates{
				SegId:  &start.SegId,
				Offset: &lastValidBoundary.Offset,
			},
			Err: ErrSegmentMissing,
		}
	}

	lg.Info("starting WAL replay", "start_seg", start.SegId, "start_offset", start.Offset, "total_segs", len(ids))

	sawRecoverableTruncation := false
	for i := startIdx; i < len(ids); i++ {
		segId := ids[i]
		lg.Debug("processing segment", "seg", segId, "index", i-startIdx, "total", len(ids)-startIdx)
		sr, err := p.OpenSegment(segId)
		if err != nil {
			lg.Warn("failed to open segment", "seg", segId, "reason", "open_error")
			result.NextTxnId = state.maxCommitted + 1
			result.LastCommittedTxnId = state.maxCommitted
			result.LastValid = lastValidBoundary
			result.TailStatus = TailStatusCorrupt
			if sr != nil {
				if closeErr := sr.Close(); closeErr != nil {
					lg.Error("failed to close segment", closeErr, "seg", segId)
				}
			}
			return result, &ReplaySourceError{
				Kind: ReplaySourceSegmentOpen,
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &lastValidBoundary.Offset,
				},
				Cause: err,
				Err:   ErrSegmentOpen,
			}
		}

		if i == startIdx {
			if err := sr.SeekTo(start.Offset); err != nil {
				result.NextTxnId = state.maxCommitted + 1
				result.LastCommittedTxnId = state.maxCommitted
				result.LastValid = lastValidBoundary
				result.TailStatus = TailStatusCorrupt
				if closeErr := sr.Close(); closeErr != nil {
					lg.Error("failed to close segment", closeErr, "seg", segId)
				}
				return result, &ReplaySourceError{
					Kind: ReplaySourceSegmentOpen,
					Coordinates: &errorutil.Coordinates{
						SegId:  &segId,
						Offset: &lastValidBoundary.Offset,
					},
					Cause: err,
					Err:   ErrSegmentOpen,
				}
			}
		} else {
			if err := sr.SeekTo(0); err != nil {
				result.NextTxnId = state.maxCommitted + 1
				result.LastCommittedTxnId = state.maxCommitted
				result.LastValid = lastValidBoundary
				result.TailStatus = TailStatusCorrupt
				if closeErr := sr.Close(); closeErr != nil {
					lg.Error("failed to close segment", closeErr, "seg", segId)
				}
				return result, &ReplaySourceError{
					Kind: ReplaySourceSegmentOpen,
					Coordinates: &errorutil.Coordinates{
						SegId:  &segId,
						Offset: &lastValidBoundary.Offset,
					},
					Cause: err,
					Err:   ErrSegmentOpen,
				}
			}
		}

		rr := record.NewFrameReader(sr.Reader())
		frameCount := 0
		for {
			rec, err := rr.Next()
			if err != nil {
				if err == io.EOF {
					lg.Debug("segment read complete", "seg", segId, "frames_processed", frameCount)
					break
				}
				var pe *record.ParseError
				pe, ok := err.(*record.ParseError)
				if errors.Is(err, record.ErrTruncated) {
					lg.Warn(
						"segment read truncated",
						"seg",
						segId,
						"frames_processed",
						frameCount,
						"reason",
						"truncation",
					)
					atBeyondBoundary := true
					lastSegment := (i == len(ids)-1)
					if ok && pe.Coordinates != nil && pe.Coordinates.Offset != nil {
						atBeyondBoundary = generic.If(*pe.Coordinates.Offset >= lastValidBoundary.Offset, true, false)
					}
					if atBeyondBoundary && lastSegment {
						lg.Info(
							"truncation at or beyond last valid boundary in final segment; treating as clean EOF",
							"seg",
							segId,
							"offset",
							generic.If(
								ok && pe.Coordinates != nil && pe.Coordinates.Offset != nil,
								*pe.Coordinates.Offset,
								int64(0),
							),
							"boundary_offset",
							lastValidBoundary.Offset,
						)
						sawRecoverableTruncation = true
						if closeErr := sr.Close(); closeErr != nil {
							lg.Error("failed to close segment", closeErr, "seg", segId)
						}
						break
					}
				}
				result.NextTxnId = state.maxCommitted + 1
				result.LastCommittedTxnId = state.maxCommitted
				result.LastValid = lastValidBoundary
				result.TailStatus = TailStatusCorrupt
				if closeErr := sr.Close(); closeErr != nil {
					lg.Error("failed to close segment", closeErr, "seg", segId)
				}
				return result, &ReplayDecodeError{
					Coordinates: &errorutil.Coordinates{
						SegId:  &segId,
						Offset: &lastValidBoundary.Offset,
					},
					SafeOffset:  generic.If(ok, pe.SafeTruncateOffset, lastValidBoundary.Offset),
					DeclaredLen: generic.If(ok, pe.DeclaredLen, 0),
					RecordType:  generic.If(ok, pe.RecordType, record.RecordTypeUnknown),
					Err:         err,
				}
			}

			if err := replayOne(rec, segId, state); err != nil {
				result.NextTxnId = state.maxCommitted + 1
				result.LastValid = lastValidBoundary
				result.LastCommittedTxnId = state.maxCommitted
				result.TailStatus = TailStatusCorrupt
				if closeErr := sr.Close(); closeErr != nil {
					lg.Error("failed to close segment", closeErr, "seg", segId)
				}
				de, ok := err.(*ReplayDecodeError)
				if ok {
					de.SafeOffset = lastValidBoundary.Offset
					return result, de
				}
				return result, err
			}
			frameCount++

			lastValidBoundary = wal.Boundary{
				SegId:  segId,
				Offset: rec.Offset + rec.Size,
			}

		}

		if sr != nil {
			if closeErr := sr.Close(); closeErr != nil {
				lg.Error("failed to close segment", closeErr, "seg", segId)
			}
		}

	}

	result.NextTxnId = state.maxCommitted + 1
	result.LastCommittedTxnId = state.maxCommitted
	result.LastValid = lastValidBoundary
	result.TailStatus = generic.If(sawRecoverableTruncation, TailStatusTruncated, TailStatusValid)

	lg.Info(
		"WAL replay complete",
		"next_txn_id",
		result.NextTxnId,
		"last_committed_txn_id",
		result.LastCommittedTxnId,
		"last_valid_seg",
		lastValidBoundary.SegId,
		"last_valid_offset",
		lastValidBoundary.Offset,
	)
	return result, nil
}

func replayOne(rec record.FramedRecord, segId uint64, state *replayState) error {
	switch rec.Record.Type {
	case record.RecordTypeBeginTransaction:
		payload, err := record.DecodeBeginTxnPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
				},
				SafeOffset:  rec.Offset,
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		if err := state.onBegin(*payload); err != nil {
			return &ReplayLogicError{
				Kind: ReplayLogicBegin,
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
					TxnID:  &payload.TxnID,
				},
				RecordType: rec.Record.Type,
				Err:        err,
			}
		}
	case record.RecordTypeCommitTransaction:
		payload, err := record.DecodeCommitTxnPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
				},
				SafeOffset:  rec.Offset,
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		if err := state.onCommit(*payload); err != nil {
			errKind := ReplayLogicCommit
			if errors.Is(err, memtable.ErrInvalidOpKind) || errors.Is(err, memtable.ErrNilKey) {
				errKind = ReplayLogicApply
			}
			return &ReplayLogicError{
				Kind: errKind,
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
					TxnID:  &payload.TxnID,
				},
				RecordType: rec.Record.Type,
				Err:        err,
			}
		}
	case record.RecordTypePutOperation:
		payload, err := record.DecodePutOpPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
				},
				SafeOffset:  rec.Offset,
				RecordType:  rec.Record.Type,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		if err := state.onPut(*payload); err != nil {
			return &ReplayLogicError{
				Kind: ReplayLogicPut,
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
					TxnID:  &payload.TxnID,
				},
				RecordType: rec.Record.Type,
				Err:        err,
			}
		}
	case record.RecordTypeDeleteOperation:
		payload, err := record.DecodeDeleteOpPayload(rec.Record.Payload)
		if err != nil {
			return &ReplayDecodeError{
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
				},
				RecordType:  rec.Record.Type,
				SafeOffset:  rec.Offset,
				DeclaredLen: rec.Record.Len,
				Err:         err,
			}
		}
		if err := state.onDel(*payload); err != nil {
			return &ReplayLogicError{
				Kind: ReplayLogicDel,
				Coordinates: &errorutil.Coordinates{
					SegId:  &segId,
					Offset: &rec.Offset,
					TxnID:  &payload.TxnID,
				},
				RecordType: rec.Record.Type,
				Err:        err,
			}
		}
	default:
		return &ReplayDecodeError{
			Coordinates: &errorutil.Coordinates{
				SegId:  &segId,
				Offset: &rec.Offset,
			},
			SafeOffset:  rec.Offset,
			RecordType:  record.RecordTypeUnknown,
			DeclaredLen: rec.Record.Len,
			Err:         record.ErrInvalidType,
		}
	}

	return nil
}

// validateSegments checks that the given segment IDs are well-ordered, consecutive, and non-empty.
func validateSegments(ids []uint64) error {
	v := validator.Numbers[uint64]()

	if len(ids) == 0 {
		return nil
	}

	if err := v.ValidateNonZero(ids[0]); err != nil {
		return err
	}

	for i := 1; i < len(ids); i++ {
		if err := v.ValidateNonZero(ids[i]); err != nil {
			return err
		}
		if err := v.ValidateConsecutive(ids[i-1], ids[i]); err != nil {
			return err
		}
	}

	return nil
}
