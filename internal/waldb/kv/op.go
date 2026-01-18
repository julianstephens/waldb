package kv

type OpKind uint8

const (
	OpPut OpKind = iota + 1
	OpDelete
)

type Op struct {
	Kind  OpKind
	Key   []byte
	Value []byte // nil for delete
}
