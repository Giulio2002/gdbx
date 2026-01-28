//go:build unix

package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

// New creates a new memory mapping for the given file descriptor.
// The offset must be page-aligned.
func New(fd int, offset int64, length int, writable bool) (*Map, error) {
	if length <= 0 {
		return nil, ErrInvalidSize
	}

	prot := unix.PROT_READ
	if writable {
		prot |= unix.PROT_WRITE
	}

	flags := unix.MAP_SHARED

	data, err := unix.Mmap(fd, offset, length, prot, flags)
	if err != nil {
		return nil, &Error{Op: "mmap", Err: err}
	}

	return &Map{
		data:     data,
		fd:       fd,
		size:     int64(length),
		capacity: int64(length),
		writable: writable,
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

// Sync flushes changes to disk synchronously.
func (m *Map) Sync() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return unix.Msync(m.data, unix.MS_SYNC)
}

// SyncAsync flushes changes to disk asynchronously.
func (m *Map) SyncAsync() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return unix.Msync(m.data, unix.MS_ASYNC)
}

// SyncRange flushes a specific range to disk.
func (m *Map) SyncRange(offset, length int64) error {
	if m.data == nil {
		return ErrNotMapped
	}
	if offset < 0 || length < 0 || offset+length > m.size {
		return ErrInvalidRange
	}
	return unix.Msync(m.data[offset:offset+length], unix.MS_SYNC)
}

// Close releases the memory mapping.
func (m *Map) Close() error {
	if m.data == nil {
		return nil
	}

	err := unix.Munmap(m.data)
	m.data = nil
	m.size = 0
	m.capacity = 0
	return err
}

// Remap changes the size of the mapping.
// This may require unmapping and remapping if the size grows beyond capacity.
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
		return &Error{Op: "munmap for remap", Err: err}
	}

	newData, err = unix.Mmap(m.fd, 0, int(newSize), prot, unix.MAP_SHARED)
	if err != nil {
		m.data = nil
		m.size = 0
		return &Error{Op: "mmap for remap", Err: err}
	}

	m.data = newData
	m.size = newSize
	m.capacity = newSize
	return nil
}

// Lock locks the mapped pages in memory (prevents swapping).
func (m *Map) Lock() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return unix.Mlock(m.data)
}

// Unlock unlocks the mapped pages.
func (m *Map) Unlock() error {
	if m.data == nil {
		return ErrNotMapped
	}
	return unix.Munlock(m.data)
}

// Advise provides hints to the kernel about memory usage patterns.
func (m *Map) Advise(advice int) error {
	if m.data == nil {
		return ErrNotMapped
	}
	return unix.Madvise(m.data, advice)
}

// AdviseSequential hints that pages will be accessed sequentially.
func (m *Map) AdviseSequential() error {
	return m.Advise(unix.MADV_SEQUENTIAL)
}

// AdviseRandom hints that pages will be accessed randomly.
func (m *Map) AdviseRandom() error {
	return m.Advise(unix.MADV_RANDOM)
}

// AdviseWillNeed hints that pages will be needed soon.
func (m *Map) AdviseWillNeed() error {
	return m.Advise(unix.MADV_WILLNEED)
}

// AdviseDontNeed hints that pages won't be needed soon.
func (m *Map) AdviseDontNeed() error {
	return m.Advise(unix.MADV_DONTNEED)
}
