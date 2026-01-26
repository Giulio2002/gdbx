//go:build windows

package gdbx

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// mmap represents a memory-mapped file region.
type mmap struct {
	data     []byte         // Mapped memory region
	fd       int            // File descriptor (not used on Windows, kept for compatibility)
	size     int64          // Current mapped size
	capacity int64          // Reserved address space
	writable bool           // True if mapped with write permission
	handle   windows.Handle // File handle
	mapping  windows.Handle // Mapping handle
}

// mmapMap creates a new memory mapping for the given file.
func mmapMap(fd int, offset int64, length int, writable bool) (*mmap, error) {
	if length <= 0 {
		return nil, errMmapInvalidSize
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
		return nil, &mmapError{"CreateFileMapping", err}
	}

	// Map view of file
	offsetHigh := uint32(uint64(offset) >> 32)
	offsetLow := uint32(offset)

	addr, err := windows.MapViewOfFile(mapping, access, offsetHigh, offsetLow, uintptr(length))
	if err != nil {
		windows.CloseHandle(mapping)
		return nil, &mmapError{"MapViewOfFile", err}
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

	return &mmap{
		data:     data,
		fd:       fd,
		size:     int64(length),
		capacity: int64(length),
		writable: writable,
		handle:   handle,
		mapping:  mapping,
	}, nil
}

// mmapFile opens a file and creates a memory mapping.
func mmapFile(path string, writable bool) (*mmap, error) {
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
		return nil, errMmapEmptyFile
	}

	m, err := mmapMap(int(f.Fd()), 0, int(size), writable)
	if err != nil {
		f.Close()
		return nil, err
	}

	return m, nil
}

// sync flushes changes to disk.
func (m *mmap) sync() error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return windows.FlushViewOfFile(uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.size))
}

// syncAsync flushes changes to disk asynchronously (same as sync on Windows).
func (m *mmap) syncAsync() error {
	return m.sync()
}

// syncRange flushes a specific range to disk.
func (m *mmap) syncRange(offset, length int64) error {
	if m.data == nil {
		return errMmapNotMapped
	}
	if offset < 0 || length < 0 || offset+length > m.size {
		return errMmapInvalidRange
	}
	return windows.FlushViewOfFile(uintptr(unsafe.Pointer(&m.data[offset])), uintptr(length))
}

// unmap releases the memory mapping.
func (m *mmap) unmap() error {
	if m.data == nil {
		return nil
	}

	addr := uintptr(unsafe.Pointer(&m.data[0]))

	if err := windows.UnmapViewOfFile(addr); err != nil {
		return &mmapError{"UnmapViewOfFile", err}
	}

	if m.mapping != 0 {
		windows.CloseHandle(m.mapping)
		m.mapping = 0
	}

	m.data = nil
	m.size = 0
	m.capacity = 0
	return nil
}

// remap changes the size of the mapping.
// Windows doesn't support mremap, so we always unmap and remap.
func (m *mmap) remap(newSize int64) error {
	if m.data == nil {
		return errMmapNotMapped
	}

	if newSize <= 0 {
		return errMmapInvalidSize
	}

	if newSize == m.size {
		return nil
	}

	// Unmap current view
	addr := uintptr(unsafe.Pointer(&m.data[0]))
	if err := windows.UnmapViewOfFile(addr); err != nil {
		return &mmapError{"UnmapViewOfFile for remap", err}
	}

	if m.mapping != 0 {
		windows.CloseHandle(m.mapping)
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

	mapping, err := windows.CreateFileMapping(m.handle, nil, prot, maxSizeHigh, maxSizeLow, nil)
	if err != nil {
		m.data = nil
		m.size = 0
		m.mapping = 0
		return &mmapError{"CreateFileMapping for remap", err}
	}

	newAddr, err := windows.MapViewOfFile(mapping, access, 0, 0, uintptr(newSize))
	if err != nil {
		windows.CloseHandle(mapping)
		m.data = nil
		m.size = 0
		m.mapping = 0
		return &mmapError{"MapViewOfFile for remap", err}
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
	m.mapping = mapping
	return nil
}

// lock locks the mapped pages in memory (prevents swapping).
func (m *mmap) lock() error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return windows.VirtualLock(uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.size))
}

// unlock unlocks the mapped pages.
func (m *mmap) unlock() error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return windows.VirtualUnlock(uintptr(unsafe.Pointer(&m.data[0])), uintptr(m.size))
}

// advise provides hints to the kernel about memory usage patterns.
// Windows doesn't have madvise, so these are no-ops.
func (m *mmap) advise(advice int) error {
	if m.data == nil {
		return errMmapNotMapped
	}
	// No-op on Windows
	return nil
}

// adviseSequential hints that pages will be accessed sequentially.
func (m *mmap) adviseSequential() error {
	return m.advise(0)
}

// adviseRandom hints that pages will be accessed randomly.
func (m *mmap) adviseRandom() error {
	return m.advise(0)
}

// adviseWillNeed hints that pages will be needed soon.
func (m *mmap) adviseWillNeed() error {
	return m.advise(0)
}

// adviseDontNeed hints that pages won't be needed soon.
func (m *mmap) adviseDontNeed() error {
	return m.advise(0)
}

// mmap errors
var (
	errMmapInvalidSize  = &mmapError{"invalid size", nil}
	errMmapInvalidRange = &mmapError{"invalid range", nil}
	errMmapNotMapped    = &mmapError{"not mapped", nil}
	errMmapEmptyFile    = &mmapError{"empty file", nil}
)

type mmapError struct {
	op  string
	err error
}

func (e *mmapError) Error() string {
	if e.err != nil {
		return "mmap: " + e.op + ": " + e.err.Error()
	}
	return "mmap: " + e.op
}

func (e *mmapError) Unwrap() error {
	return e.err
}

// Windows protection flags (compatibility with Unix)
const (
	_PROT_READ  = 0x1
	_PROT_WRITE = 0x2
)

// syscallMmap is a compatibility wrapper for code that uses syscall.Mmap directly.
func syscallMmap(fd int, offset int64, length int, prot int, flags int) ([]byte, error) {
	writable := prot&_PROT_WRITE != 0
	m, err := mmapMap(fd, offset, length, writable)
	if err != nil {
		return nil, err
	}
	return m.data, nil
}

// syscallMunmap is a compatibility wrapper for code that uses syscall.Munmap directly.
func syscallMunmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
}
