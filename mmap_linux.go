//go:build linux

package gdbx

import (
	"syscall"
	"unsafe"
)

// tryMremap attempts to use Linux mremap syscall for efficient remapping.
func (m *mmap) tryMremap(newSize int) ([]byte, error) {
	const MREMAP_MAYMOVE = 1

	newAddr, _, errno := syscall.Syscall6(
		syscall.SYS_MREMAP,
		uintptr(unsafe.Pointer(&m.data[0])),
		uintptr(m.size),
		uintptr(newSize),
		MREMAP_MAYMOVE,
		0, 0)

	if errno != 0 {
		return nil, errno
	}

	// Create new slice header pointing to remapped memory
	var newData []byte
	sh := (*struct {
		Data uintptr
		Len  int
		Cap  int
	})(unsafe.Pointer(&newData))
	sh.Data = newAddr
	sh.Len = newSize
	sh.Cap = newSize

	return newData, nil
}
