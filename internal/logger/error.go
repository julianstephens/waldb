package logger

import "errors"

var (
	ErrLogCreate = errors.New("logger: create error")
	ErrLogOpen   = errors.New("logger: open error")
	ErrLogClose  = errors.New("logger: close error")
)

type LoggerError struct {
	Op    string // operation being performed, e.g., "open", "write"
	Err   error  // underlying error
	Cause error  // optional cause error for more context
	Path  string // optional path related to the error
}

func (e *LoggerError) Error() string {
	if e.Path != "" {
		return e.Op + " error on " + e.Path + ": " + e.Err.Error()
	}
	return e.Op + " error: " + e.Err.Error()
}

func (e *LoggerError) Unwrap() error {
	return e.Err
}

func wrapLoggerErr(op string, err, cause error, path string) error {
	return &LoggerError{
		Op:    op,
		Err:   err,
		Cause: cause,
		Path:  path,
	}
}
