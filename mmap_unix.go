//go:build unix

package gdbx

import (
	"os"

	"golang.org/x/sys/unix"
)

// mmap represents a memory-mapped file region.
type mmap struct {
	data     []byte // Mapped memory region
	fd       int    // File descriptor
	size     int64  // Current mapped size
	capacity int64  // Reserved address space (can be larger than size)
	writable bool   // True if mapped with write permission
}

// mmapMap creates a new memory mapping for the given file.
// The offset must be page-aligned.
func mmapMap(fd int, offset int64, length int, writable bool) (*mmap, error) {
	if length <= 0 {
		return nil, errMmapInvalidSize
	}

	prot := unix.PROT_READ
	if writable {
		prot |= unix.PROT_WRITE
	}

	flags := unix.MAP_SHARED

	data, err := unix.Mmap(fd, offset, length, prot, flags)
	if err != nil {
		return nil, &mmapError{"mmap", err}
	}

	return &mmap{
		data:     data,
		fd:       fd,
		size:     int64(length),
		capacity: int64(length),
		writable: writable,
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
	return unix.Msync(m.data, unix.MS_SYNC)
}

// syncAsync flushes changes to disk asynchronously.
func (m *mmap) syncAsync() error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return unix.Msync(m.data, unix.MS_ASYNC)
}

// syncRange flushes a specific range to disk.
func (m *mmap) syncRange(offset, length int64) error {
	if m.data == nil {
		return errMmapNotMapped
	}
	if offset < 0 || length < 0 || offset+length > m.size {
		return errMmapInvalidRange
	}
	return unix.Msync(m.data[offset:offset+length], unix.MS_SYNC)
}

// unmap releases the memory mapping.
func (m *mmap) unmap() error {
	if m.data == nil {
		return nil
	}

	err := unix.Munmap(m.data)
	m.data = nil
	m.size = 0
	m.capacity = 0
	return err
}

// remap changes the size of the mapping.
// This may require unmapping and remapping if the size grows beyond capacity.
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

	// Try mremap on Linux
	newData, err := m.tryMremap(int(newSize))
	if err == nil {
		m.data = newData
		m.size = newSize
		if newSize > m.capacity {
			m.capacity = newSize
		}
		return nil
	}

	// Fallback: unmap and remap
	prot := unix.PROT_READ
	if m.writable {
		prot |= unix.PROT_WRITE
	}

	if err := unix.Munmap(m.data); err != nil {
		return &mmapError{"munmap for remap", err}
	}

	newData, err = unix.Mmap(m.fd, 0, int(newSize), prot, unix.MAP_SHARED)
	if err != nil {
		m.data = nil
		m.size = 0
		return &mmapError{"mmap for remap", err}
	}

	m.data = newData
	m.size = newSize
	m.capacity = newSize
	return nil
}

// lock locks the mapped pages in memory (prevents swapping).
func (m *mmap) lock() error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return unix.Mlock(m.data)
}

// unlock unlocks the mapped pages.
func (m *mmap) unlock() error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return unix.Munlock(m.data)
}

// advise provides hints to the kernel about memory usage patterns.
func (m *mmap) advise(advice int) error {
	if m.data == nil {
		return errMmapNotMapped
	}
	return unix.Madvise(m.data, advice)
}

// adviseSequential hints that pages will be accessed sequentially.
func (m *mmap) adviseSequential() error {
	return m.advise(unix.MADV_SEQUENTIAL)
}

// adviseRandom hints that pages will be accessed randomly.
func (m *mmap) adviseRandom() error {
	return m.advise(unix.MADV_RANDOM)
}

// adviseWillNeed hints that pages will be needed soon.
func (m *mmap) adviseWillNeed() error {
	return m.advise(unix.MADV_WILLNEED)
}

// adviseDontNeed hints that pages won't be needed soon.
func (m *mmap) adviseDontNeed() error {
	return m.advise(unix.MADV_DONTNEED)
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
