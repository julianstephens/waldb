package errorutil

import "fmt"

// Coordinates holds positional information (segment ID, offset, transaction ID)
// used in error formatting across different error types in the waldb package.
type Coordinates struct {
	// SegId is the WAL segment ID where the error occurred.
	SegId *uint64

	// Offset is the byte offset within a segment or file where the error occurred.
	Offset *int64

	// TxnID is the transaction ID associated with the error.
	TxnID *uint64
}

// FormatCoordinates returns a formatted string representation of the error coordinates.
// It includes only non-nil values in the format: "seg=X at=Y txn=Z".
// Returns an empty string if all coordinates are nil.
func (c *Coordinates) FormatCoordinates() string {
	if c == nil {
		return ""
	}

	var parts []string

	if c.SegId != nil {
		parts = append(parts, fmt.Sprintf("seg=%d", *c.SegId))
	}
	if c.Offset != nil {
		parts = append(parts, fmt.Sprintf("at=%d", *c.Offset))
	}
	if c.TxnID != nil {
		parts = append(parts, fmt.Sprintf("txn=%d", *c.TxnID))
	}

	if len(parts) == 0 {
		return ""
	}

	result := ""
	for i, part := range parts {
		if i > 0 {
			result += " "
		}
		result += part
	}
	return result
}

// String implements the Stringer interface for Coordinates.
func (c *Coordinates) String() string {
	return c.FormatCoordinates()
}
