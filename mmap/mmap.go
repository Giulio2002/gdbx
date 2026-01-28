// Package mmap provides cross-platform memory mapping functionality.
package mmap

// Map represents a memory-mapped file region.
// This type wraps platform-specific mmap implementations.
type Map struct {
	data     []byte // Mapped memory region
	fd       int    // File descriptor
	size     int64  // Current mapped size
	capacity int64  // Reserved address space (can be larger than size)
	writable bool   // True if mapped with write permission
	// Windows-specific handles (only used on Windows, zero on Unix)
	handle  uintptr // File handle (Windows only)
	mapping uintptr // Mapping handle (Windows only)
}

// Data returns the mapped byte slice.
func (m *Map) Data() []byte {
	return m.data
}

// Size returns the current mapped size.
func (m *Map) Size() int64 {
	return m.size
}

// Capacity returns the reserved address space capacity.
func (m *Map) Capacity() int64 {
	return m.capacity
}

// Writable returns true if the mapping is writable.
func (m *Map) Writable() bool {
	return m.writable
}

// Fd returns the file descriptor.
func (m *Map) Fd() int {
	return m.fd
}

// Error represents an mmap error.
type Error struct {
	Op  string
	Err error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return "mmap: " + e.Op + ": " + e.Err.Error()
	}
	return "mmap: " + e.Op
}

func (e *Error) Unwrap() error {
	return e.Err
}

// Common errors
var (
	ErrInvalidSize  = &Error{Op: "invalid size"}
	ErrInvalidRange = &Error{Op: "invalid range"}
	ErrNotMapped    = &Error{Op: "not mapped"}
	ErrEmptyFile    = &Error{Op: "empty file"}
)
