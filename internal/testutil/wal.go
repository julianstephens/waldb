package testutil

import (
	"fmt"

	"github.com/julianstephens/waldb/internal/waldb/wal/record"
)

// RecordedCall represents a recorded method call to a log appender
type RecordedCall struct {
	Method     string // "Append", "Flush", or "FSync"
	RecordType record.RecordType
	Payload    []byte
}

// LogAppender is a test implementation that records all calls
type LogAppender struct {
	calls             []RecordedCall
	failOnAppendIndex int // -1 means no failure
	failOnFlush       bool
	failOnFSync       bool
}

// NewLogAppender creates a new test log appender
func NewLogAppender() *LogAppender {
	return &LogAppender{
		calls:             make([]RecordedCall, 0),
		failOnAppendIndex: -1,
	}
}

// SetFailOnAppend sets the append operation to fail at the given index
func (f *LogAppender) SetFailOnAppend(index int) {
	f.failOnAppendIndex = index
}

// SetFailOnFlush sets the flush operation to fail
func (f *LogAppender) SetFailOnFlush(fail bool) {
	f.failOnFlush = fail
}

// SetFailOnFSync sets the fsync operation to fail
func (f *LogAppender) SetFailOnFSync(fail bool) {
	f.failOnFSync = fail
}

// Append records an append call and returns success or failure based on configuration
func (f *LogAppender) Append(typ record.RecordType, payload []byte) (int64, error) {
	appendIndex := 0
	for _, call := range f.calls {
		if call.Method == "Append" {
			appendIndex++
		}
	}

	if f.failOnAppendIndex == appendIndex {
		return 0, fmt.Errorf("append failed at index %d", appendIndex)
	}

	// Copy payload to avoid issues with reused buffers
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	f.calls = append(f.calls, RecordedCall{
		Method:     "Append",
		RecordType: typ,
		Payload:    payloadCopy,
	})
	return int64(appendIndex), nil
}

// Flush records a flush call and returns success or failure based on configuration
func (f *LogAppender) Flush() error {
	f.calls = append(f.calls, RecordedCall{Method: "Flush"})
	if f.failOnFlush {
		return fmt.Errorf("flush failed")
	}
	return nil
}

// FSync records an fsync call and returns success or failure based on configuration
func (f *LogAppender) FSync() error {
	f.calls = append(f.calls, RecordedCall{Method: "FSync"})
	if f.failOnFSync {
		return fmt.Errorf("fsync failed")
	}
	return nil
}

// Close closes the appender
func (f *LogAppender) Close() error {
	return nil
}

// Calls returns the recorded calls for inspection
func (f *LogAppender) Calls() []RecordedCall {
	return f.calls
}

// CallCount returns the number of recorded calls
func (f *LogAppender) CallCount() int {
	return len(f.calls)
}

// CallSequence returns the method names in call order
func (f *LogAppender) CallSequence() []string {
	seq := make([]string, len(f.calls))
	for i, call := range f.calls {
		seq[i] = call.Method
	}
	return seq
}
