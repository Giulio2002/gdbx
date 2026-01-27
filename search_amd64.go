//go:build amd64

package gdbx

// prefetchPage prefetches page data into CPU cache for faster subsequent access.
// Call this when you know a page will be accessed soon (e.g., during tree traversal).
//
//go:noescape
func prefetchPage(data []byte)

// searchPageAsm performs binary search within a page using assembly-optimized comparison.
// Returns the index where key should be inserted or found.
// This is the hot path for all B+tree operations.
//
//go:noescape
func searchPageAsm(pageData []byte, key []byte, isBranch bool) int

// compareKeysAsm compares two keys using SIMD when possible.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
//
//go:noescape
func compareKeysAsm(a, b []byte) int

// getKeyAndCompareAsm extracts key at idx from page and compares with searchKey.
// Combines nodeGetKeyFast + bytes.Compare into single operation to avoid overhead.
// Returns comparison result: -1 if searchKey < nodeKey, 0 if equal, 1 if searchKey > nodeKey.
//
//go:noescape
func getKeyAndCompareAsm(pageData []byte, idx int, searchKey []byte) int

// binarySearchLeaf8 performs binary search on a leaf page for 8-byte keys.
// Does the entire binary search in assembly to avoid Go/asm boundary overhead.
// Returns the index where key should be inserted or found.
// Only call this when key length is 8.
//
//go:noescape
func binarySearchLeaf8(pageData []byte, key uint64, n int) int

// binarySearchBranch8 performs binary search on a branch page for 8-byte keys.
// Does the entire binary search in assembly to avoid Go/asm boundary overhead.
// Returns the index of the child to follow.
// Only call this when key length is 8.
//
//go:noescape
func binarySearchBranch8(pageData []byte, key uint64, n int) int

// binarySearchLeafN performs binary search on a leaf page for N-byte keys.
// Does the entire binary search in assembly with SSE2 comparison.
// Returns the index where key should be inserted or found.
// keyLen must be > 0. Optimized for keys >= 16 bytes.
//
//go:noescape
func binarySearchLeafN(pageData []byte, key []byte, n int) int

// binarySearchBranchN performs binary search on a branch page for N-byte keys.
// Does the entire binary search in assembly with SSE2 comparison.
// Returns the index of the child to follow.
// keyLen must be > 0. Optimized for keys >= 16 bytes.
//
//go:noescape
func binarySearchBranchN(pageData []byte, key []byte, n int) int
