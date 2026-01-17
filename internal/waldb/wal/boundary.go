package wal

type Boundary struct {
	SegId  uint64
	Offset int64
}
