package wal_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	tst "github.com/julianstephens/go-utils/tests"
	"github.com/julianstephens/waldb/internal/waldb/wal"
)

// TestNewFileSegmentReader tests creating a new file segment reader
func TestNewFileSegmentReader(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file
	content := []byte("test data")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(42, file)
	tst.AssertNotNil(t, reader, "expected non-nil reader")
}

// TestSegmentReaderSegID tests getting the segment ID
func TestSegmentReaderSegID(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file
	err := os.WriteFile(testFile, []byte("test data"), 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	segId := uint64(123)
	reader := wal.NewFileSegmentReader(segId, file)

	tst.RequireDeepEqual(t, reader.SegID(), segId)
}

// TestFileSegmentReaderSeekOperations tests seeking to various offsets
func TestFileSegmentReaderSeekOperations(t *testing.T) {
	testCases := []struct {
		name     string
		content  []byte
		offset   int64
		shouldOk bool
	}{
		{
			name:     "seek_within_file",
			content:  []byte("0123456789ABCDEF"),
			offset:   5,
			shouldOk: true,
		},
		{
			name:     "seek_to_beyond_eof",
			content:  []byte("small"),
			offset:   1000,
			shouldOk: true,
		},
		{
			name:     "seek_to_zero",
			content:  []byte("0123456789"),
			offset:   0,
			shouldOk: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			testFile := filepath.Join(tempDir, "test.wal")

			err := os.WriteFile(testFile, tc.content, 0o600) //nolint:gosec
			tst.RequireNoError(t, err)

			file, err := os.Open(testFile) //nolint:gosec
			tst.RequireNoError(t, err)
			defer file.Close() //nolint:errcheck

			reader := wal.NewFileSegmentReader(1, file)
			err = reader.SeekTo(tc.offset)

			if tc.shouldOk {
				tst.RequireNoError(t, err)
			} else {
				if err == nil {
					t.Fatal("expected error")
				}
			}
		})
	}
}

// TestFileSegmentReaderSeekTo tests seeking to an offset (deprecated - use TestFileSegmentReaderSeekOperations)
func TestFileSegmentReaderSeekTo(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file with known content
	content := []byte("0123456789ABCDEF")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)

	// Seek to offset 5
	err = reader.SeekTo(5)
	tst.RequireNoError(t, err)
}

// TestFileSegmentReaderSeekToInvalid tests seeking to invalid offset (deprecated - use TestFileSegmentReaderSeekOperations)
func TestFileSegmentReaderSeekToInvalid(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a small test file
	err := os.WriteFile(testFile, []byte("small"), 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)

	// Seeking to a valid position should succeed even if beyond EOF
	err = reader.SeekTo(1000)
	tst.RequireNoError(t, err)
}

// TestFileSegmentReaderReadOperations tests various read scenarios
func TestFileSegmentReaderReadOperations(t *testing.T) {
	testCases := []struct {
		name         string
		content      []byte
		seekOffset   int64
		expectedRead []byte
		readSize     int
		expectError  error
	}{
		{
			name:         "read_from_start",
			content:      []byte("test content"),
			seekOffset:   -1, // no seek
			expectedRead: []byte("test content"),
			readSize:     12,
			expectError:  nil,
		},
		{
			name:         "read_after_seek",
			content:      []byte("0123456789"),
			seekOffset:   5,
			expectedRead: []byte("56789"),
			readSize:     5,
			expectError:  nil,
		},
		{
			name:         "read_all_then_eof",
			content:      []byte("short"),
			seekOffset:   -1,
			expectedRead: []byte("short"),
			readSize:     5,
			expectError:  nil,
		},
		{
			name:         "read_from_empty_file",
			content:      []byte{},
			seekOffset:   -1,
			expectedRead: nil,
			readSize:     10,
			expectError:  io.EOF,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			testFile := filepath.Join(tempDir, "test.wal")

			err := os.WriteFile(testFile, tc.content, 0o600) //nolint:gosec
			tst.RequireNoError(t, err)

			file, err := os.Open(testFile) //nolint:gosec
			tst.RequireNoError(t, err)
			defer file.Close() //nolint:errcheck

			reader := wal.NewFileSegmentReader(1, file)
			r := reader.Reader()

			// Seek if specified
			if tc.seekOffset >= 0 {
				err = reader.SeekTo(tc.seekOffset)
				tst.RequireNoError(t, err)
				r = reader.Reader() // Get updated reader after seek
			}

			// Read
			buf := make([]byte, tc.readSize)
			n, err := r.Read(buf)

			// Check results
			if tc.expectError != nil {
				tst.RequireDeepEqual(t, err, tc.expectError)
				tst.RequireDeepEqual(t, n, 0)
			} else {
				tst.RequireNoError(t, err)
				if tc.expectedRead != nil {
					tst.RequireDeepEqual(t, buf[:n], tc.expectedRead)
				}
				// For the "read_all_then_eof" case, verify EOF on next read
				if tc.name == "read_all_then_eof" {
					buf2 := make([]byte, 10)
					n2, err2 := r.Read(buf2)
					tst.RequireDeepEqual(t, n2, 0)
					tst.RequireDeepEqual(t, err2, io.EOF)
				}
			}
		})
	}
}

// TestFileSegmentReaderGetReader tests getting the io.Reader (deprecated - use TestFileSegmentReaderReadOperations)
func TestFileSegmentReaderGetReader(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file with known content
	content := []byte("test content")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)

	// Get the io.Reader
	r := reader.Reader()
	tst.AssertNotNil(t, r, "expected non-nil reader")

	// Read from the reader
	buf := make([]byte, len(content))
	n, err := r.Read(buf)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, len(content))
	tst.RequireDeepEqual(t, buf, content)
}

// TestFileSegmentReaderReaderAfterSeek tests reading after seeking (deprecated - use TestFileSegmentReaderReadOperations)
func TestFileSegmentReaderReaderAfterSeek(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file with known content
	content := []byte("0123456789")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)

	// Seek to offset 5
	err = reader.SeekTo(5)
	tst.RequireNoError(t, err)

	// Read from the reader
	r := reader.Reader()
	buf := make([]byte, 5)
	n, err := r.Read(buf)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, 5)
	tst.RequireDeepEqual(t, buf, []byte("56789"))
}

// TestFileSegmentReaderClose tests closing the reader
func TestFileSegmentReaderClose(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file
	err := os.WriteFile(testFile, []byte("test"), 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)

	reader := wal.NewFileSegmentReader(1, file)

	// Close should succeed
	err = reader.Close()
	tst.RequireNoError(t, err)
}

// TestFileSegmentReaderMultipleReads tests reading in chunks (deprecated - use TestFileSegmentReaderReadOperations)
func TestFileSegmentReaderMultipleReads(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file with known content
	content := []byte("abcdefghijklmnop")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)
	r := reader.Reader()

	// Read in chunks
	chunk1 := make([]byte, 5)
	n, err := r.Read(chunk1)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, 5)
	tst.RequireDeepEqual(t, chunk1, []byte("abcde"))

	chunk2 := make([]byte, 5)
	n, err = r.Read(chunk2)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, 5)
	tst.RequireDeepEqual(t, chunk2, []byte("fghij"))

	chunk3 := make([]byte, 10)
	n, err = r.Read(chunk3)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, 6)
	tst.RequireDeepEqual(t, chunk3[:n], []byte("klmnop"))
}

// TestFileSegmentReaderReadEOF tests EOF after reading all content (deprecated - use TestFileSegmentReaderReadOperations)
func TestFileSegmentReaderReadEOF(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a small test file
	content := []byte("short")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)
	r := reader.Reader()

	// Read all content
	buf := make([]byte, len(content))
	n, err := r.Read(buf)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, len(content))

	// Next read should return EOF
	buf2 := make([]byte, 10)
	n, err = r.Read(buf2)
	tst.RequireDeepEqual(t, n, 0)
	tst.RequireDeepEqual(t, err, io.EOF)
}

// TestFileSegmentReaderSeekToBeginning tests seeking back to beginning (deprecated - use TestFileSegmentReaderSeekOperations)
func TestFileSegmentReaderSeekToBeginning(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create a test file
	content := []byte("test content")
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)
	r := reader.Reader()

	// Read some data
	buf1 := make([]byte, 4)
	n, err := r.Read(buf1)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, 4)
	tst.RequireDeepEqual(t, buf1, []byte("test"))

	// Seek back to beginning
	err = reader.SeekTo(0)
	tst.RequireNoError(t, err)

	// Read again should get the same data
	buf2 := make([]byte, 4)
	n, err = r.Read(buf2)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, 4)
	tst.RequireDeepEqual(t, buf2, []byte("test"))
}

// TestFileSegmentReaderDataTypes tests different data types (binary, text, special cases)
func TestFileSegmentReaderDataTypes(t *testing.T) {
	testCases := []struct {
		name    string
		content []byte
	}{
		{
			name:    "binary_data",
			content: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
		},
		{
			name:    "text_data",
			content: []byte("test content"),
		},
		{
			name:    "empty_file",
			content: []byte{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			testFile := filepath.Join(tempDir, "test.wal")

			err := os.WriteFile(testFile, tc.content, 0o600) //nolint:gosec
			tst.RequireNoError(t, err)

			file, err := os.Open(testFile) //nolint:gosec
			tst.RequireNoError(t, err)
			defer file.Close() //nolint:errcheck

			reader := wal.NewFileSegmentReader(1, file)
			r := reader.Reader()

			if len(tc.content) == 0 {
				// Empty file should return EOF
				buf := make([]byte, 10)
				n, err := r.Read(buf)
				tst.RequireDeepEqual(t, n, 0)
				tst.RequireDeepEqual(t, err, io.EOF)
			} else {
				// Read the content
				buf := make([]byte, len(tc.content))
				n, err := r.Read(buf)
				tst.RequireNoError(t, err)
				tst.RequireDeepEqual(t, n, len(tc.content))
				tst.RequireDeepEqual(t, buf, tc.content)
			}
		})
	}
}

// TestFileSegmentReaderBinaryData tests reading binary data (deprecated - use TestFileSegmentReaderDataTypes)
func TestFileSegmentReaderBinaryData(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.wal")

	// Create binary test data
	content := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)
	r := reader.Reader()

	// Read the binary data
	buf := make([]byte, len(content))
	n, err := r.Read(buf)
	tst.RequireNoError(t, err)
	tst.RequireDeepEqual(t, n, len(content))
	tst.RequireDeepEqual(t, buf, content)
}

// TestFileSegmentReaderEmptyFile tests reading from empty file (deprecated - use TestFileSegmentReaderDataTypes)
func TestFileSegmentReaderEmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "empty.wal")

	// Create an empty file
	err := os.WriteFile(testFile, []byte{}, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)
	r := reader.Reader()

	// Reading from empty file should return EOF immediately
	buf := make([]byte, 10)
	n, err := r.Read(buf)
	tst.RequireDeepEqual(t, n, 0)
	tst.RequireDeepEqual(t, err, io.EOF)
}

// TestFileSegmentReaderLargeFile tests reading from a larger file
func TestFileSegmentReaderLargeFile(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "large.wal")

	// Create a large test file (1MB)
	size := 1024 * 1024
	content := bytes.Repeat([]byte("x"), size)
	err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
	tst.RequireNoError(t, err)

	file, err := os.Open(testFile) //nolint:gosec
	tst.RequireNoError(t, err)
	defer file.Close() //nolint:errcheck

	reader := wal.NewFileSegmentReader(1, file)
	r := reader.Reader()

	// Read the content in chunks
	totalRead := 0
	chunkSize := 4096
	for totalRead < size {
		buf := make([]byte, chunkSize)
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}
		totalRead += n
		if err == io.EOF {
			break
		}
	}

	tst.RequireDeepEqual(t, totalRead, size)
}

// TestFileSegmentReaderMultipleSegments tests multiple readers with different segment IDs
func TestFileSegmentReaderMultipleSegments(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple test files
	for i := 1; i <= 3; i++ {
		testNum := strconv.Itoa(i)
		testFile := filepath.Join(tempDir, "test"+testNum+".wal")
		content := []byte("segment " + testNum)
		err := os.WriteFile(testFile, content, 0o600) //nolint:gosec
		tst.RequireNoError(t, err)
	}

	// Open and read from multiple files
	for i := 1; i <= 3; i++ {
		testNum := strconv.Itoa(i)
		testFile := filepath.Join(tempDir, "test"+testNum+".wal")
		file, err := os.Open(testFile) //nolint:gosec
		tst.RequireNoError(t, err)
		defer file.Close() //nolint:errcheck

		reader := wal.NewFileSegmentReader(uint64(i), file) //nolint:gosec
		tst.RequireDeepEqual(t, reader.SegID(), uint64(i))  //nolint:gosec
	}
}
