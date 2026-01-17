package wal

import "errors"

var (
	ErrNilSegmentFile = errors.New("wal: nil segment file provided")
)
