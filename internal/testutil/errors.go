package testutil

// Error is a simple test error implementation
type Error struct {
	Message string
}

// Error returns the error message
func (e *Error) Error() string {
	return e.Message
}

// NewError creates a new test error with the given message
func NewError(msg string) *Error {
	return &Error{Message: msg}
}
