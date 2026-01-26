//go:build amd64 || 386 || arm64 || arm || riscv64 || mips64le || mipsle || ppc64le || wasm

package gdbx

import "unsafe"

// On little-endian architectures, use direct pointer casts (zero overhead)

//go:nosplit
func putUint64LE(b []byte, v uint64) {
	*(*uint64)(unsafe.Pointer(&b[0])) = v
}

//go:nosplit
func putUint32LE(b []byte, v uint32) {
	*(*uint32)(unsafe.Pointer(&b[0])) = v
}

//go:nosplit
func putUint16LE(b []byte, v uint16) {
	*(*uint16)(unsafe.Pointer(&b[0])) = v
}

//go:nosplit
func getUint64LE(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

//go:nosplit
func getUint32LE(b []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&b[0]))
}

//go:nosplit
func getUint16LE(b []byte) uint16 {
	return *(*uint16)(unsafe.Pointer(&b[0]))
}
