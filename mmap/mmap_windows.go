//go:build windows

package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// New creates a new memory mapping for the given file descriptor.
func New(fd int, offset int64, length int, writable bool) (*Map, error) {
	if length <= 0 {
		return nil, ErrInvalidSize
	}

	handle := windows.Handle(fd)

	// Create file mapping
	prot := uint32(windows.PAGE_READONLY)
	access := uint32(windows.FILE_MAP_READ)
	if writable {
		prot = windows.PAGE_READWRITE
		access = windows.FILE_MAP_WRITE
	}

	maxSizeHigh := uint32(uint64(length) >> 32)
	maxSizeLow := uint32(length)

	mapping, err := windows.CreateFileMapping(handle, nil, prot, maxSizeHigh, maxSizeLow, nil)
	if err != nil {
		return nil, &Error{Op: "CreateFileMapping", Err: err}
	}

	// Map view of file
	offsetHigh := uint32(uint64(offset) >> 32)
	offsetLow := uint32(offset)

	addr, err := windows.MapViewOfFile(mapping, access, offsetHigh, offsetLow, uintptr(length))
	if err != nil {
		windows.CloseHandle(mapping)
		return nil, &Error{Op: "MapViewOfFile", Err: err}
	}

	// Create slice from mapped memory
	var data []byte
	sh := (*struct {
		Data uintptr
		Len  int
		Cap  int
	})(unsafe.Pointer(&data))
	sh.Data = addr
	sh.Len = length
	sh.Cap = length

	return &Map{
		data:     data,
		fd:       fd,
		size:     int64(length),
		capacity: int64(length),
		writable: writable,
		handle:   uintptr(handle),
		mapping:  uintptr(mapping),
	}, nil
}

// MapFile opens a file and creates a memory mapping.
func MapFile(path string, writable bool) (*Map, error) {
	flag := os.O_RDONLY
	if writable {
		flag = os.O_RDWR
	}

	f, err := os.OpenFile(path, flag, 0)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := fi.Size()
	if size == 0 {
		f.Close()
		return nil, ErrEmptyFile
	}

	m, err := New(int(f.Fd()), 0, int(size), writable)
	if err != nil {
		f.Close()
		return nil, err
	}

	return m, nil
}

// Sync flushes changes to disk.
func (m *Map) Sync() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return windows.FlushViewOfFile(uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.size))
}

// SyncAsync flushes changes to disk asynchronously (same as sync on Windows).
func (m *Map) SyncAsync() error {
	return m.Sync()
}

// SyncRange flushes a specific range to disk.
func (m *Map) SyncRange(offset, length int64) error {
	if m.data == nil {
		return ErrNotMapped
	}
	if offset < 0 || length < 0 || offset+length > m.size {
		return ErrInvalidRange
	}
	return windows.FlushViewOfFile(uintptr(unsafe.Pointer(&m.data[offset])), uintptr(length))
}

// Close releases the memory mapping.
func (m *Map) Close() error {
	if m.data == nil {
		return nil
	}

	addr := uintptr(unsafe.Pointer(&m.data[0]))

	if err := windows.UnmapViewOfFile(addr); err != nil {
		return &Error{Op: "UnmapViewOfFile", Err: err}
	}

	if m.mapping != 0 {
		windows.CloseHandle(windows.Handle(m.mapping))
		m.mapping = 0
	}

	m.data = nil
	m.size = 0
	m.capacity = 0
	return nil
}

// Remap changes the size of the mapping.
// Windows doesn't support mremap, so we always unmap and remap.
func (m *Map) Remap(newSize int64) error {
	if m.data == nil {
		return ErrNotMapped
	}

	if newSize <= 0 {
		return ErrInvalidSize
	}

	if newSize == m.size {
		return nil
	}

	// Unmap current view
	addr := uintptr(unsafe.Pointer(&m.data[0]))
	if err := windows.UnmapViewOfFile(addr); err != nil {
		return &Error{Op: "UnmapViewOfFile for remap", Err: err}
	}

	if m.mapping != 0 {
		windows.CloseHandle(windows.Handle(m.mapping))
	}

	// Create new mapping
	prot := uint32(windows.PAGE_READONLY)
	access := uint32(windows.FILE_MAP_READ)
	if m.writable {
		prot = windows.PAGE_READWRITE
		access = windows.FILE_MAP_WRITE
	}

	maxSizeHigh := uint32(uint64(newSize) >> 32)
	maxSizeLow := uint32(newSize)

	mapping, err := windows.CreateFileMapping(windows.Handle(m.handle), nil, prot, maxSizeHigh, maxSizeLow, nil)
	if err != nil {
		m.data = nil
		m.size = 0
		m.mapping = 0
		return &Error{Op: "CreateFileMapping for remap", Err: err}
	}

	newAddr, err := windows.MapViewOfFile(mapping, access, 0, 0, uintptr(newSize))
	if err != nil {
		windows.CloseHandle(mapping)
		m.data = nil
		m.size = 0
		m.mapping = 0
		return &Error{Op: "MapViewOfFile for remap", Err: err}
	}

	// Update slice
	var newData []byte
	sh := (*struct {
		Data uintptr
		Len  int
		Cap  int
	})(unsafe.Pointer(&newData))
	sh.Data = newAddr
	sh.Len = int(newSize)
	sh.Cap = int(newSize)

	m.data = newData
	m.size = newSize
	m.capacity = newSize
	m.mapping = uintptr(mapping)
	return nil
}

// Lock locks the mapped pages in memory (prevents swapping).
func (m *Map) Lock() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return windows.VirtualLock(uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.size))
}

// Unlock unlocks the mapped pages.
func (m *Map) Unlock() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return windows.VirtualUnlock(uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.size))
}

// Advise provides hints to the kernel about memory usage patterns.
// Windows doesn't have madvise, so these are no-ops.
func (m *Map) Advise(advice int) error {
	if m.data == nil {
		return ErrNotMapped
	}
	// No-op on Windows
	return nil
}

// AdviseSequential hints that pages will be accessed sequentially.
func (m *Map) AdviseSequential() error {
	return m.Advise(0)
}

// AdviseRandom hints that pages will be accessed randomly.
func (m *Map) AdviseRandom() error {
	return m.Advise(0)
}

// AdviseWillNeed hints that pages will be needed soon.
func (m *Map) AdviseWillNeed() error {
	return m.Advise(0)
}

// AdviseDontNeed hints that pages won't be needed soon.
func (m *Map) AdviseDontNeed() error {
	return m.Advise(0)
}

// tryMremap is not available on Windows, always returns error to trigger fallback.
func (m *Map) tryMremap(newSize int) ([]byte, error) {
	return nil, &Error{Op: "mremap not available on windows"}
}
