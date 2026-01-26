package gdbx

import (
	"unsafe"
)

// nodeSize is the fixed node header size (8 bytes)
const nodeSize = 8

// nodeFlags define node types
type nodeFlags uint8

const (
	// nodeBig indicates data is on a large/overflow page
	nodeBig nodeFlags = 0x01

	// nodeTree indicates data is a B-tree (sub-database)
	nodeTree nodeFlags = 0x02

	// nodeDup indicates data has duplicates
	nodeDup nodeFlags = 0x04
)

// nodeHeader represents a node header (8 bytes).
// This structure must match the libmdbx node_t layout exactly.
//
// Memory layout (little-endian):
//
//	Offset  Size  Field
//	0       4     dsize/child_pgno (union)
//	4       1     flags
//	5       1     extra (reserved)
//	6       2     ksize
//	8       ...   payload (key followed by data)
type nodeHeader struct {
	// DataSize is the data size for leaf nodes, or child page number for branch nodes
	DataSize uint32
	Flags    nodeFlags
	Extra    uint8
	KeySize  uint16
}

// node provides access to a node within a page.
type node struct {
	data   []byte // Raw node data (header + payload)
	offset uint16 // Offset within page
}

// mdbxExtraNodeBytes is the size of per-entry tracking metadata that some
// MDBX builds add before each node (txnid tracking, alignment padding, etc.)
const mdbxExtraNodeBytes = 20

// nodeFromPage creates a node from a page at the given entry index.
func nodeFromPage(p *page, idx int) *node {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset) >= len(p.Data) {
		return nil
	}

	// Read node directly at the entry offset
	if int(offset)+nodeSize <= len(p.Data) {
		return &node{
			data:   p.Data[offset:],
			offset: offset,
		}
	}

	return nil
}

// nodeFromBytes creates a node from raw bytes.
func nodeFromBytes(data []byte) *node {
	if len(data) < nodeSize {
		return nil
	}
	return &node{data: data}
}

// header returns the node header.
func (n *node) header() *nodeHeader {
	if len(n.data) < nodeSize {
		return nil
	}
	return (*nodeHeader)(unsafe.Pointer(&n.data[0]))
}

// keySize returns the key size.
func (n *node) keySize() uint16 {
	return n.header().KeySize
}

// dataSize returns the data size (for leaf nodes).
func (n *node) dataSize() uint32 {
	return n.header().DataSize
}

// childPgno returns the child page number (for branch nodes).
func (n *node) childPgno() pgno {
	return pgno(n.header().DataSize)
}

// flags returns the node flags.
func (n *node) flags() nodeFlags {
	return n.header().Flags
}

// isBig returns true if data is on a large/overflow page.
func (n *node) isBig() bool {
	return n.header().Flags&nodeBig != 0
}

// isTree returns true if this node contains a sub-database.
func (n *node) isTree() bool {
	return n.header().Flags&nodeTree != 0
}

// isDup returns true if this node has duplicates.
func (n *node) isDup() bool {
	return n.header().Flags&nodeDup != 0
}

// key returns the node's key.
func (n *node) key() []byte {
	h := n.header()
	if h == nil || len(n.data) < nodeSize+int(h.KeySize) {
		return nil
	}
	return n.data[nodeSize : nodeSize+h.KeySize]
}

// nodeData returns the node's data (for leaf nodes).
// For Big nodes, this returns the overflow page number as bytes.
func (n *node) nodeData() []byte {
	h := n.header()
	if h == nil {
		return nil
	}

	dataOffset := nodeSize + int(h.KeySize)
	if h.Flags&nodeBig != 0 {
		// For big nodes, data is a page number (4 bytes)
		if len(n.data) < dataOffset+4 {
			return nil
		}
		return n.data[dataOffset : dataOffset+4]
	}

	dataEnd := dataOffset + int(h.DataSize)
	if len(n.data) < dataEnd {
		return nil
	}
	return n.data[dataOffset:dataEnd]
}

// overflowPgno returns the overflow page number for Big nodes.
func (n *node) overflowPgno() pgno {
	if !n.isBig() {
		return invalidPgno
	}
	h := n.header()
	dataOffset := nodeSize + int(h.KeySize)
	if len(n.data) < dataOffset+4 {
		return invalidPgno
	}
	return pgno(
		uint32(n.data[dataOffset]) |
			uint32(n.data[dataOffset+1])<<8 |
			uint32(n.data[dataOffset+2])<<16 |
			uint32(n.data[dataOffset+3])<<24,
	)
}

// totalSize returns the total size of this node (header + key + data).
func (n *node) totalSize() int {
	h := n.header()
	if h == nil {
		return 0
	}

	size := nodeSize + int(h.KeySize)
	if h.Flags&nodeBig != 0 {
		size += 4 // overflow page number
	} else {
		size += int(h.DataSize)
	}
	return size
}

// nodeCalcSize calculates the size needed for a node with given key and data sizes.
func nodeCalcSize(keySize int, dataSize int, isBig bool) int {
	size := nodeSize + keySize
	if isBig {
		size += 4 // overflow page number
	} else {
		size += dataSize
	}
	return size
}

// nodeMaxKeySize calculates the maximum key size for a given page size.
// This matches libmdbx's calculation: pageSize/2 - nodeSize - sizeof(indx_t)
// which ensures at least 2 entries can fit on a branch page.
func nodeMaxKeySize(pageSize int) int {
	// libmdbx formula: max_key = pagesize / 2 - NODESIZE - sizeof(indx_t)
	// NODESIZE = 8, sizeof(indx_t) = 2
	// This allows keys up to ~2038 bytes on a 4KB page
	return pageSize/2 - nodeSize - 2
}

// nodeMaxDataSize calculates the maximum inline data size for a given page size.
func nodeMaxDataSize(pageSize int) int {
	// Similar to max key, but data can be larger since we only need 2 leaf entries
	// For a leaf page to be useful, it needs at least 2 entries
	// Each entry: 8 byte header + key + data
	// For maximum data with 1-byte key: (pageSize - 20 - 4) / 2 - 9
	return (pageSize-pageHeaderSize-4)/2 - nodeSize - 1
}

// ============== Allocation-free methods for hot path ==============

// nodeGetKeyDirect returns the key at index idx directly from page data without allocating.
// The returned slice has capacity equal to length to prevent callers from accidentally
// modifying page data via append.
func nodeGetKeyDirect(p *page, idx int) []byte {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset)+nodeSize > len(p.Data) {
		return nil
	}
	// Read key size from header (at offset+6, 2 bytes, little endian)
	keySize := uint16(p.Data[offset+6]) | uint16(p.Data[offset+7])<<8
	end := offset + nodeSize + uint16(keySize)
	if int(end) > len(p.Data) {
		return nil
	}
	// Use three-index slice to cap capacity at length
	return p.Data[offset+nodeSize : end : end]
}

// nodeGetDataDirect returns the data at index idx directly from page data without allocating.
// Returns nil for big nodes (overflow pages need different handling).
// The returned slice has capacity equal to length to prevent callers from accidentally
// modifying page data via append.
func nodeGetDataDirect(p *page, idx int) []byte {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset)+nodeSize > len(p.Data) {
		return nil
	}
	// Read header
	dataSize := uint32(p.Data[offset]) | uint32(p.Data[offset+1])<<8 |
		uint32(p.Data[offset+2])<<16 | uint32(p.Data[offset+3])<<24
	flags := nodeFlags(p.Data[offset+4])
	keySize := uint16(p.Data[offset+6]) | uint16(p.Data[offset+7])<<8

	// Check for big node
	if flags&nodeBig != 0 {
		return nil // Caller must handle overflow pages
	}

	dataStart := int(offset) + nodeSize + int(keySize)
	dataEnd := dataStart + int(dataSize)
	if dataEnd > len(p.Data) {
		return nil
	}
	// Use three-index slice to cap capacity at length
	return p.Data[dataStart:dataEnd:dataEnd]
}

// nodeGetChildPgnoDirect returns the child page number for a branch entry.
func nodeGetChildPgnoDirect(p *page, idx int) pgno {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset)+4 > len(p.Data) {
		return invalidPgno
	}
	// Child pgno is in the first 4 bytes (DataSize field)
	return pgno(
		uint32(p.Data[offset]) | uint32(p.Data[offset+1])<<8 |
			uint32(p.Data[offset+2])<<16 | uint32(p.Data[offset+3])<<24,
	)
}

// nodeGetFlagsDirect returns the node flags at index idx.
func nodeGetFlagsDirect(p *page, idx int) nodeFlags {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset)+5 > len(p.Data) {
		return 0
	}
	return nodeFlags(p.Data[offset+4])
}

// nodeGetOverflowPgnoDirect returns the overflow page number for a big node.
func nodeGetOverflowPgnoDirect(p *page, idx int) pgno {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset)+nodeSize > len(p.Data) {
		return invalidPgno
	}
	keySize := uint16(p.Data[offset+6]) | uint16(p.Data[offset+7])<<8
	pgnoOffset := int(offset) + nodeSize + int(keySize)
	if pgnoOffset+4 > len(p.Data) {
		return invalidPgno
	}
	return pgno(
		uint32(p.Data[pgnoOffset]) | uint32(p.Data[pgnoOffset+1])<<8 |
			uint32(p.Data[pgnoOffset+2])<<16 | uint32(p.Data[pgnoOffset+3])<<24,
	)
}

// nodeGetDataSizeDirect returns the data size at index idx.
func nodeGetDataSizeDirect(p *page, idx int) uint32 {
	offset := p.entryOffset(idx)
	if offset == 0 || int(offset)+4 > len(p.Data) {
		return 0
	}
	return uint32(p.Data[offset]) | uint32(p.Data[offset+1])<<8 |
		uint32(p.Data[offset+2])<<16 | uint32(p.Data[offset+3])<<24
}

// ============== Raw byte slice methods (no page struct allocation) ==============
// These methods work directly on byte slices for zero-allocation hot paths.

// nodeGetKeyRaw returns the key at index idx from raw page data.
func nodeGetKeyRaw(data []byte, idx int) []byte {
	offset := pageEntryOffsetDirect(data, idx)
	if offset == 0 || int(offset)+nodeSize > len(data) {
		return nil
	}
	keySize := uint16(data[offset+6]) | uint16(data[offset+7])<<8
	if int(offset)+nodeSize+int(keySize) > len(data) {
		return nil
	}
	return data[offset+nodeSize : int(offset)+nodeSize+int(keySize)]
}

// nodeGetKeyUnchecked returns the key at index idx without bounds checking.
// Caller must ensure idx is valid and page data is well-formed.
// This is for hot paths where bounds have already been verified.
func nodeGetKeyUnchecked(data []byte, idx int) []byte {
	offset := pageEntryOffsetUnchecked(data, idx)
	keySize := uint16(data[offset+6]) | uint16(data[offset+7])<<8
	return data[offset+nodeSize : int(offset)+nodeSize+int(keySize)]
}

// nodeGetDataUnchecked returns the data at index idx without bounds checking.
// Caller must ensure idx is valid, page data is well-formed, and node is not Big.
// This is for hot paths where these conditions have already been verified.
func nodeGetDataUnchecked(data []byte, idx int) []byte {
	offset := pageEntryOffsetUnchecked(data, idx)
	dataSize := uint32(data[offset]) | uint32(data[offset+1])<<8 |
		uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
	keySize := uint16(data[offset+6]) | uint16(data[offset+7])<<8
	dataStart := int(offset) + nodeSize + int(keySize)
	return data[dataStart : dataStart+int(dataSize)]
}

// nodeGetDataRaw returns the data at index idx from raw page data.
// Returns nil for big nodes (overflow pages need different handling).
func nodeGetDataRaw(data []byte, idx int) []byte {
	offset := pageEntryOffsetDirect(data, idx)
	if offset == 0 || int(offset)+nodeSize > len(data) {
		return nil
	}
	dataSize := uint32(data[offset]) | uint32(data[offset+1])<<8 |
		uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
	flags := nodeFlags(data[offset+4])
	keySize := uint16(data[offset+6]) | uint16(data[offset+7])<<8

	if flags&nodeBig != 0 {
		return nil
	}

	dataStart := int(offset) + nodeSize + int(keySize)
	dataEnd := dataStart + int(dataSize)
	if dataEnd > len(data) {
		return nil
	}
	return data[dataStart:dataEnd]
}

// nodeGetChildPgnoRaw returns the child page number for a branch entry from raw data.
func nodeGetChildPgnoRaw(data []byte, idx int) pgno {
	offset := pageEntryOffsetDirect(data, idx)
	if offset == 0 || int(offset)+4 > len(data) {
		return invalidPgno
	}
	return pgno(
		uint32(data[offset]) | uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24,
	)
}

// nodeGetChildPgnoUnchecked returns the child page number without bounds checking.
// Caller must ensure idx is valid and page data is well-formed.
func nodeGetChildPgnoUnchecked(data []byte, idx int) pgno {
	offset := pageEntryOffsetUnchecked(data, idx)
	return pgno(
		uint32(data[offset]) | uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24,
	)
}

// nodeGetFirstChildPgno returns the child page number at index 0 without any calculations.
// This is optimized for the common case of descending to the leftmost child.
// Caller must ensure page data is well-formed and has at least one entry.
func nodeGetFirstChildPgno(data []byte) pgno {
	// Entry 0 offset is stored at pageHeaderSize (20), as a 2-byte little-endian value
	// The stored offset is relative to pageHeaderSize, so add pageHeaderSize to get actual position
	storedOffset := uint16(data[pageHeaderSize]) | uint16(data[pageHeaderSize+1])<<8
	offset := storedOffset + pageHeaderSize
	return pgno(
		uint32(data[offset]) | uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24,
	)
}

// nodeGetFirstKey returns the key at index 0 without any calculations.
// This is optimized for getting the first value from a DUPSORT sub-tree leaf.
// Caller must ensure page data is well-formed and has at least one entry.
func nodeGetFirstKey(data []byte) []byte {
	// Entry 0 offset is stored at pageHeaderSize (20), as a 2-byte little-endian value
	storedOffset := uint16(data[pageHeaderSize]) | uint16(data[pageHeaderSize+1])<<8
	offset := int(storedOffset + pageHeaderSize)
	// Read key size from header (at offset+6, 2 bytes, little endian)
	keySize := int(uint16(data[offset+6]) | uint16(data[offset+7])<<8)
	return data[offset+nodeSize : offset+nodeSize+keySize]
}

// nodeGetLastChildPgno returns the child page number at the last index.
// This is optimized for descending to the rightmost child.
// Caller must ensure page data is well-formed and has at least one entry.
func nodeGetLastChildPgno(data []byte) pgno {
	// Get number of entries from lower field at offset 12
	lower := uint16(data[12]) | uint16(data[13])<<8
	numEntries := int(lower) >> 1
	lastIdx := numEntries - 1

	// Entry offset is stored at pageHeaderSize + idx*2
	storedOffset := uint16(data[pageHeaderSize+lastIdx*2]) | uint16(data[pageHeaderSize+lastIdx*2+1])<<8
	offset := storedOffset + pageHeaderSize
	return pgno(
		uint32(data[offset]) | uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24,
	)
}

// nodeGetLastKey returns the key at the last index.
// This is optimized for getting the last value from a DUPSORT sub-tree leaf.
// Caller must ensure page data is well-formed and has at least one entry.
func nodeGetLastKey(data []byte) []byte {
	// Get number of entries from lower field at offset 12
	lower := uint16(data[12]) | uint16(data[13])<<8
	numEntries := int(lower) >> 1
	lastIdx := numEntries - 1

	// Entry offset is stored at pageHeaderSize + idx*2
	storedOffset := uint16(data[pageHeaderSize+lastIdx*2]) | uint16(data[pageHeaderSize+lastIdx*2+1])<<8
	offset := int(storedOffset + pageHeaderSize)
	// Read key size from header (at offset+6, 2 bytes, little endian)
	keySize := int(uint16(data[offset+6]) | uint16(data[offset+7])<<8)
	return data[offset+nodeSize : offset+nodeSize+keySize]
}

// nodeGetFlagsRaw returns the node flags at index idx from raw data.
func nodeGetFlagsRaw(data []byte, idx int) nodeFlags {
	offset := pageEntryOffsetDirect(data, idx)
	if offset == 0 || int(offset)+5 > len(data) {
		return 0
	}
	return nodeFlags(data[offset+4])
}

// nodeGetFlagsUnchecked returns the node flags without bounds checking.
// Caller must ensure idx is valid and page data is well-formed.
func nodeGetFlagsUnchecked(data []byte, idx int) nodeFlags {
	offset := pageEntryOffsetUnchecked(data, idx)
	return nodeFlags(data[offset+4])
}

// nodeGetNodeDataUnchecked returns key, flags, and data in a single pass without bounds checking.
// For DUPSORT sub-trees (flags & nodeTree != 0), data contains the sub-tree structure.
// For DUPSORT sub-pages (flags & nodeDup != 0), data contains the sub-page.
// Caller must ensure idx is valid and page data is well-formed.
func nodeGetNodeDataUnchecked(data []byte, idx int) (key []byte, flags nodeFlags, nodeData []byte) {
	offset := pageEntryOffsetUnchecked(data, idx)
	// Read header (8 bytes): dataSize(4), flags(1), reserved(1), keySize(2)
	dataSize := uint32(data[offset]) | uint32(data[offset+1])<<8 |
		uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
	flags = nodeFlags(data[offset+4])
	keySize := uint16(data[offset+6]) | uint16(data[offset+7])<<8

	keyStart := int(offset) + nodeSize
	key = data[keyStart : keyStart+int(keySize)]

	// For big nodes, don't return data (caller handles overflow)
	if flags&nodeBig != 0 {
		return key, flags, nil
	}

	dataStart := keyStart + int(keySize)
	nodeData = data[dataStart : dataStart+int(dataSize)]
	return key, flags, nodeData
}

// nodeGetOverflowPgnoRaw returns the overflow page number for a big node from raw data.
func nodeGetOverflowPgnoRaw(data []byte, idx int) pgno {
	offset := pageEntryOffsetDirect(data, idx)
	if offset == 0 || int(offset)+nodeSize > len(data) {
		return invalidPgno
	}
	keySize := uint16(data[offset+6]) | uint16(data[offset+7])<<8
	pgnoOffset := int(offset) + nodeSize + int(keySize)
	if pgnoOffset+4 > len(data) {
		return invalidPgno
	}
	return pgno(
		uint32(data[pgnoOffset]) | uint32(data[pgnoOffset+1])<<8 |
			uint32(data[pgnoOffset+2])<<16 | uint32(data[pgnoOffset+3])<<24,
	)
}

// nodeGetDataSizeRaw returns the data size at index idx from raw data.
func nodeGetDataSizeRaw(data []byte, idx int) uint32 {
	offset := pageEntryOffsetDirect(data, idx)
	if offset == 0 || int(offset)+4 > len(data) {
		return 0
	}
	return uint32(data[offset]) | uint32(data[offset+1])<<8 |
		uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
}

// ============== Page-based fast methods (no bounds checking) ==============
// These methods work with *page and skip all bounds checking for maximum speed.
// Caller MUST ensure idx is valid (0 <= idx < page.numEntries).

// nodeGetKeyFast returns the key at index idx without bounds checking.
// This is the fastest path for binary search in hot loops.
// The returned slice has capacity equal to length to prevent callers from accidentally
// modifying page data via append.
func nodeGetKeyFast(p *page, idx int) []byte {
	offset := p.entryOffsetFast(idx)
	keySize := uint16(p.Data[offset+6]) | uint16(p.Data[offset+7])<<8
	end := offset + nodeSize + uint16(keySize)
	return p.Data[offset+nodeSize : end : end]
}

// nodeGetDataFast returns the data at index idx without bounds checking.
// Returns data bytes for non-big nodes. Caller must check for big flag separately if needed.
// The returned slice has capacity equal to length to prevent callers from accidentally
// modifying page data via append.
func nodeGetDataFast(p *page, idx int) []byte {
	offset := p.entryOffsetFast(idx)
	dataSize := uint32(p.Data[offset]) | uint32(p.Data[offset+1])<<8 |
		uint32(p.Data[offset+2])<<16 | uint32(p.Data[offset+3])<<24
	keySize := uint16(p.Data[offset+6]) | uint16(p.Data[offset+7])<<8
	dataStart := int(offset) + nodeSize + int(keySize)
	dataEnd := dataStart + int(dataSize)
	return p.Data[dataStart:dataEnd:dataEnd]
}

// nodeGetChildPgnoFast returns the child page number without bounds checking.
func nodeGetChildPgnoFast(p *page, idx int) pgno {
	offset := p.entryOffsetFast(idx)
	return pgno(
		uint32(p.Data[offset]) | uint32(p.Data[offset+1])<<8 |
			uint32(p.Data[offset+2])<<16 | uint32(p.Data[offset+3])<<24,
	)
}

// nodeGetFlagsFast returns the node flags without bounds checking.
func nodeGetFlagsFast(p *page, idx int) nodeFlags {
	offset := p.entryOffsetFast(idx)
	return nodeFlags(p.Data[offset+4])
}

// nodeGetKeyFlagsDataFast returns key, flags, and data at index idx without bounds checking.
// Computes offset once to avoid redundant calculations.
// This is the fastest path for operations that need all three values.
func nodeGetKeyFlagsDataFast(p *page, idx int) (key []byte, flags nodeFlags, data []byte) {
	offset := p.entryOffsetFast(idx)
	d := p.Data

	// Read all fields from single offset
	dataSize := uint32(d[offset]) | uint32(d[offset+1])<<8 |
		uint32(d[offset+2])<<16 | uint32(d[offset+3])<<24
	flags = nodeFlags(d[offset+4])
	keySize := uint16(d[offset+6]) | uint16(d[offset+7])<<8

	key = d[offset+nodeSize : offset+nodeSize+keySize]
	dataStart := int(offset) + nodeSize + int(keySize)
	data = d[dataStart : dataStart+int(dataSize)]
	return
}
