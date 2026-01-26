//go:build !amd64 && !386 && !arm64 && !arm && !riscv64 && !mips64le && !mipsle && !ppc64le && !wasm

package gdbx

import "encoding/binary"

// On big-endian architectures, use encoding/binary for correctness

//go:nosplit
func putUint64LE(b []byte, v uint64) {
	binary.LittleEndian.PutUint64(b, v)
}

//go:nosplit
func putUint32LE(b []byte, v uint32) {
	binary.LittleEndian.PutUint32(b, v)
}

//go:nosplit
func putUint16LE(b []byte, v uint16) {
	binary.LittleEndian.PutUint16(b, v)
}

//go:nosplit
func getUint64LE(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

//go:nosplit
func getUint32LE(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

//go:nosplit
func getUint16LE(b []byte) uint16 {
	return binary.LittleEndian.Uint16(b)
}
