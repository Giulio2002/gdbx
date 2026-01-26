//go:build !amd64

package gdbx

import "bytes"

// prefetchPage is a no-op on non-AMD64 architectures.
func prefetchPage(data []byte) {}

// getKeyAndCompareAsm is the fallback for non-AMD64 architectures.
// It extracts the key at idx from page and compares with searchKey.
func getKeyAndCompareAsm(pageData []byte, idx int, searchKey []byte) int {
	// Get entry offset: stored at pageData[20 + idx*2] as uint16, add 20 for actual offset
	offsetPos := 20 + idx*2
	storedOffset := uint16(pageData[offsetPos]) | uint16(pageData[offsetPos+1])<<8
	offset := int(storedOffset) + 20

	// Get key size from node header at offset+6
	keySize := int(uint16(pageData[offset+6]) | uint16(pageData[offset+7])<<8)

	// Extract key bytes
	keyStart := offset + 8 // nodeSize = 8
	nodeKey := pageData[keyStart : keyStart+keySize]

	// Compare searchKey with nodeKey
	return bytes.Compare(searchKey, nodeKey)
}

// compareKeysAsm is the fallback for non-AMD64 architectures.
func compareKeysAsm(a, b []byte) int {
	return bytes.Compare(a, b)
}

// searchPageAsm is the fallback for non-AMD64 architectures.
func searchPageAsm(pageData []byte, key []byte, isBranch bool) int {
	return -1 // Signal to use Go implementation
}

// binarySearchLeaf8 is the fallback for non-AMD64 architectures.
func binarySearchLeaf8(pageData []byte, key uint64, n int) int {
	return -1 // Signal to use Go implementation
}

// binarySearchBranch8 is the fallback for non-AMD64 architectures.
func binarySearchBranch8(pageData []byte, key uint64, n int) int {
	return -1 // Signal to use Go implementation
}
