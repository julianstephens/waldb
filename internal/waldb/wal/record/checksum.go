package record

import "hash/crc32"

// ComputeChecksum computes the CRC32 checksum for the given data.
func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// VerifyChecksum verifies the checksum of the given record.
// It returns true if the checksum is valid, false otherwise.
// The checksum is calculated over the Type and Payload fields.
func VerifyChecksum(record *Record) bool {
	if record == nil {
		return false
	}

	data := make([]byte, 1+len(record.Payload))
	data[0] = byte(record.Type)
	copy(data[1:], record.Payload)

	calculatedCRC := ComputeChecksum(data)
	return calculatedCRC == record.CRC
}

// UpdateChecksum updates the checksum of the given record.
// It recalculates the checksum based on the current Type and Payload fields.
func UpdateChecksum(record *Record) {
	if record == nil {
		return
	}

	data := make([]byte, 1+len(record.Payload))
	data[0] = byte(record.Type)
	copy(data[1:], record.Payload)

	record.CRC = ComputeChecksum(data)
}
