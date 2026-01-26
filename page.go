package gdbx

import (
	"encoding/binary"
	"sync"
	"unsafe"
)

// pgno is a page number (32-bit)
type pgno uint32

// txnid is a transaction ID (64-bit)
type txnid uint64

// Constants for page handling
const (
	// pageHeaderSize is the fixed page header size (20 bytes)
	pageHeaderSize = 20

	// invalidPgno represents an invalid/empty page number
	invalidPgno pgno = 0xFFFFFFFF

	// maxPgno is the maximum valid page number
	maxPgno pgno = 0x7FFFffff
)

// pageFlags define page types
type pageFlags uint16

const (
	// pageBranch indicates a branch (internal) page
	pageBranch pageFlags = 0x01

	// pageLeaf indicates a leaf page
	pageLeaf pageFlags = 0x02

	// pageLarge indicates a large/overflow page
	pageLarge pageFlags = 0x04

	// pageMeta indicates a meta page
	pageMeta pageFlags = 0x08

	// pageLegacyDirty is a legacy dirty flag (pre v0.10)
	pageLegacyDirty pageFlags = 0x10

	// pageBad is an explicit flag for invalid pages
	pageBad = pageLegacyDirty

	// pageDupfix indicates a DUPFIXED page
	pageDupfix pageFlags = 0x20

	// pageSubP indicates a sub-page for DUPSORT
	pageSubP pageFlags = 0x40

	// pageSpilled indicates a page spilled in parent txn
	pageSpilled pageFlags = 0x2000

	// pageLoose indicates a freed page available for reuse
	pageLoose pageFlags = 0x4000

	// pageFrozen indicates a retired page with known status
	pageFrozen pageFlags = 0x8000

	// pageTypeMask masks off the type bits
	pageTypeMask = pageBranch | pageLeaf | pageLarge | pageMeta | pageDupfix | pageSubP
)

// pageHeader represents the common page header (20 bytes).
// This structure must match the libmdbx page_t layout exactly.
//
// Memory layout (little-endian):
//
//	Offset  Size  Field
//	0       8     txnid
//	8       2     dupfix_ksize
//	10      2     flags
//	12      2     lower (or pages[0:2] for large pages)
//	14      2     upper (or pages[2:4] for large pages)
//	16      4     pgno
//	20      ...   entries[] (dynamic, indices into node data)
type pageHeader struct {
	Txnid       txnid     // Transaction ID that created this page
	DupfixKsize uint16    // Key size for DUPFIX pages
	Flags       pageFlags // Page type flags
	Lower       uint16    // Lower bound of free space (or overflow page count low)
	Upper       uint16    // Upper bound of free space (or overflow page count high)
	PageNo      pgno      // This page's number
}

// page provides access to a page's data with its header.
type page struct {
	Data []byte // Raw page data (including header)
}

// header returns the page header.
func (p *page) header() *pageHeader {
	if len(p.Data) < pageHeaderSize {
		return nil
	}
	return (*pageHeader)(unsafe.Pointer(&p.Data[0]))
}

// pageNo returns the page number.
func (p *page) pageNo() pgno {
	return p.header().PageNo
}

// pageType returns the page type flags (masked).
func (p *page) pageType() pageFlags {
	return p.header().Flags & pageTypeMask
}

// isBranch returns true if this is a branch page.
func (p *page) isBranch() bool {
	return p.header().Flags&pageBranch != 0
}

// isLeaf returns true if this is a leaf page.
func (p *page) isLeaf() bool {
	return p.header().Flags&pageLeaf != 0
}

// isLarge returns true if this is a large/overflow page.
func (p *page) isLarge() bool {
	return p.header().Flags&pageLarge != 0
}

// isMeta returns true if this is a meta page.
func (p *page) isMeta() bool {
	return p.header().Flags&pageMeta != 0
}

// isDupfix returns true if this is a DUPFIX page.
func (p *page) isDupfix() bool {
	return p.header().Flags&pageDupfix != 0
}

// isSubPage returns true if this is a sub-page.
func (p *page) isSubPage() bool {
	return p.header().Flags&pageSubP != 0
}

// numEntries returns the number of entries on this page.
// In libmdbx: page_numkeys(mp) = mp->lower >> 1
// Entry indices are 2 bytes each, stored starting at offset pageHeaderSize.
func (p *page) numEntries() int {
	h := p.header()
	if h == nil {
		return 0
	}
	return int(h.Lower) >> 1
}

// entryOffset returns the offset of the entry at the given index.
func (p *page) entryOffset(idx int) uint16 {
	if idx < 0 || idx >= p.numEntries() {
		return 0
	}
	// Entry indices start at offset pageHeaderSize (after the page header)
	offset := pageHeaderSize + idx*2
	storedOffset := binary.LittleEndian.Uint16(p.Data[offset:])
	// libmdbx: return ptr_disp(mp, mp->entries[i] + PAGEHDRSZ)
	// Stored offsets are relative to pageHeaderSize, so add it to get actual position
	return storedOffset + uint16(pageHeaderSize)
}

// freeSpace returns the amount of free space on this page.
// In libmdbx format: lower = numEntries*2, upper = pageSize - pageHeaderSize - usedSpace
// FreeSpace = upper - lower
func (p *page) freeSpace() int {
	h := p.header()
	if h == nil {
		return 0
	}
	return int(h.Upper) - int(h.Lower)
}

// overflowPages returns the number of overflow pages (for large pages).
func (p *page) overflowPages() uint32 {
	if !p.isLarge() {
		return 1
	}
	h := p.header()
	// Pages field is stored in lower/upper as a 32-bit value
	return uint32(h.Lower) | (uint32(h.Upper) << 16)
}

// setOverflowPages sets the overflow page count (for large pages).
func (p *page) setOverflowPages(n uint32) {
	h := p.header()
	h.Lower = uint16(n & 0xFFFF)
	h.Upper = uint16(n >> 16)
}

// init initializes a page header.
// libmdbx format:
// - lower = numEntries * 2 (initially 0)
// - upper = pageSize - pageHeaderSize (free space starts after header)
// - entry offsets are relative to pageHeaderSize
func (p *page) init(pno pgno, flags pageFlags, pageSize uint16) {
	// Use 64-bit writes to reduce memory operations
	// Layout: txnid(8) + dupfix_ksize(2) + flags(2) + lower(2) + upper(2) + pgno(4) = 20 bytes
	d := p.Data
	_ = d[19] // bounds check elimination hint

	// Write first 8 bytes: txnid = 0 (fast path on little-endian)
	putUint64LE(d[0:8], 0)

	// Write bytes 8-15: dupfix_ksize(0) + flags + lower(0) + upper
	upper := pageSize - pageHeaderSize
	// Combine: dupfix_ksize(2) + flags(2) + lower(2) + upper(2) = 8 bytes
	val := uint64(flags)<<16 | uint64(upper)<<48
	putUint64LE(d[8:16], val)

	// Write bytes 16-19: pgno (4 bytes)
	putUint32LE(d[16:20], uint32(pno))
}

// validate checks if the page header is valid.
func (p *page) validate(pageSize uint) error {
	if len(p.Data) < pageHeaderSize {
		return errPageTooSmall
	}
	h := p.header()

	// Check flags are valid
	if h.Flags&^(pageTypeMask|pageSpilled|pageLoose|pageFrozen|pageLegacyDirty) != 0 {
		return errPageInvalidFlags
	}

	// For non-large pages, check bounds
	if !p.isLarge() {
		// upper is relative to pageHeaderSize, so upper + pageHeaderSize <= pageSize
		if h.Upper+pageHeaderSize > uint16(pageSize) {
			return errPageInvalidUpper
		}
		// Entry indices must not overlap node data
		if h.Lower > h.Upper {
			return errPageInvalidBounds
		}
	}

	return nil
}

// Errors for page validation
var (
	errPageTooSmall      = &pageError{"page too small"}
	errPageInvalidFlags  = &pageError{"invalid page flags"}
	errPageInvalidLower  = &pageError{"invalid lower bound"}
	errPageInvalidUpper  = &pageError{"invalid upper bound"}
	errPageInvalidBounds = &pageError{"lower > upper"}
)

type pageError struct {
	msg string
}

func (e *pageError) Error() string {
	return "page: " + e.msg
}

// ============== Allocation-free direct access functions ==============
// These functions work directly on byte slices without allocating page structs.

// pageFlagsDirect returns the page flags from raw page data.
// Uses direct byte access for better inlining.
func pageFlagsDirect(data []byte) pageFlags {
	if len(data) < pageHeaderSize {
		return 0
	}
	return pageFlags(uint16(data[10]) | uint16(data[11])<<8)
}

// pageIsLeafDirect returns true if raw page data represents a leaf page.
func pageIsLeafDirect(data []byte) bool {
	return pageFlagsDirect(data)&pageLeaf != 0
}

// pageIsBranchDirect returns true if raw page data represents a branch page.
func pageIsBranchDirect(data []byte) bool {
	return pageFlagsDirect(data)&pageBranch != 0
}

// pageNumEntriesDirect returns the number of entries from raw page data.
// libmdbx: page_numkeys(mp) = mp->lower >> 1
func pageNumEntriesDirect(data []byte) int {
	if len(data) < pageHeaderSize {
		return 0
	}
	lower := uint16(data[12]) | uint16(data[13])<<8
	return int(lower) >> 1
}

// pageEntryOffsetDirect returns the entry offset at index from raw page data.
// libmdbx: return ptr_disp(mp, mp->entries[i] + PAGEHDRSZ)
func pageEntryOffsetDirect(data []byte, idx int) uint16 {
	numEntries := pageNumEntriesDirect(data)
	if idx < 0 || idx >= numEntries {
		return 0
	}
	offset := pageHeaderSize + idx*2
	storedOffset := uint16(data[offset]) | uint16(data[offset+1])<<8
	return storedOffset + uint16(pageHeaderSize)
}

// pageEntryOffsetUnchecked returns the entry offset without bounds checking.
// Caller must ensure idx is valid (0 <= idx < numEntries).
// This is for hot paths where bounds have already been verified.
func pageEntryOffsetUnchecked(data []byte, idx int) uint16 {
	storedOffset := uint16(data[pageHeaderSize+idx*2]) | uint16(data[pageHeaderSize+idx*2+1])<<8
	return storedOffset + pageHeaderSize
}

// entryOffsetFast returns the entry offset for a page without bounds checking.
// Caller must ensure idx is valid (0 <= idx < numEntries).
// This is the fastest path for hot loops where bounds are already verified.
func (p *page) entryOffsetFast(idx int) uint16 {
	storedOffset := uint16(p.Data[pageHeaderSize+idx*2]) | uint16(p.Data[pageHeaderSize+idx*2+1])<<8
	return storedOffset + pageHeaderSize
}

// isBranchFast returns true if this is a branch page without nil checks.
// Caller must ensure p and p.Data are valid.
func (p *page) isBranchFast() bool {
	// Flags at offset 10 (2 bytes)
	flags := pageFlags(uint16(p.Data[10]) | uint16(p.Data[11])<<8)
	return flags&pageBranch != 0
}

// numEntriesFast returns the number of entries without nil checks.
// Caller must ensure p and p.Data are valid.
func (p *page) numEntriesFast() int {
	lower := uint16(p.Data[12]) | uint16(p.Data[13])<<8
	return int(lower) >> 1
}

// isLeafFast returns true if this is a leaf page without nil checks.
// Caller must ensure p and p.Data are valid.
func (p *page) isLeafFast() bool {
	flags := pageFlags(uint16(p.Data[10]) | uint16(p.Data[11])<<8)
	return flags&pageLeaf != 0
}

// ============== Page modification methods ==============

// insertEntry inserts a new entry at the given index.
// The nodeData contains the full node (header + key + data).
// Returns false if there's not enough space.
// Uses alternate format (compatible with mdbx-go) where:
// - upper = actual_position - pageHeaderSize
// - stored offsets = actual_offset - pageHeaderSize
func (p *page) insertEntry(idx int, nodeData []byte) bool {
	return p.insertEntryWithBuf(idx, nodeData, nil)
}

// insertEntryWithBuf is like insertEntry but uses a scratch buffer for compaction.
func (p *page) insertEntryWithBuf(idx int, nodeData []byte, scratchBuf []byte) bool {
	h := p.header()
	numEntries := p.numEntries()

	// Check bounds
	if idx < 0 || idx > numEntries {
		return false
	}

	nodeSize := len(nodeData)
	// Need 2 bytes for entry index + nodeSize for node data
	requiredSpace := 2 + nodeSize
	if p.freeSpace() < requiredSpace {
		// Try compacting to reclaim space from holes
		reclaimed := p.compactWithBuf(scratchBuf)
		if reclaimed == 0 || p.freeSpace() < requiredSpace {
			return false
		}
	}

	// Allocate space for node data at upper end
	// In alternate format: upper is relative to pageHeaderSize
	// actual position = upper + pageHeaderSize
	// new actual position = actual position - nodeSize
	// new upper = new actual position - pageHeaderSize = upper - nodeSize
	newUpper := h.Upper - uint16(nodeSize)
	h.Upper = newUpper

	// Copy node data to the allocated space (actual position = upper + pageHeaderSize)
	actualPosition := newUpper + pageHeaderSize
	copy(p.Data[actualPosition:], nodeData)

	// Shift existing entry indices to make room
	entriesStart := pageHeaderSize
	if idx < numEntries {
		// Move entries from idx onwards by 2 bytes
		src := entriesStart + idx*2
		dst := src + 2
		moveSize := (numEntries - idx) * 2
		copy(p.Data[dst:], p.Data[src:src+moveSize])
	}

	// Write new entry index (alternate format: same as upper value = offset relative to pageHeaderSize)
	entryOffset := entriesStart + idx*2
	putUint16LE(p.Data[entryOffset:], newUpper)

	// Update lower bound (grows by 2 for new entry index)
	h.Lower += 2

	return true
}

// removeEntry removes the entry at the given index.
// Note: This leaves holes in the data area. Call compact() to reclaim space.
func (p *page) removeEntry(idx int) bool {
	h := p.header()
	numEntries := p.numEntries()

	// Check bounds
	if idx < 0 || idx >= numEntries {
		return false
	}

	// Shift entry indices
	entriesStart := pageHeaderSize
	if idx < numEntries-1 {
		src := entriesStart + (idx+1)*2
		dst := entriesStart + idx*2
		moveSize := (numEntries - 1 - idx) * 2
		copy(p.Data[dst:], p.Data[src:src+moveSize])
	}

	// Update lower bound (shrinks by 2)
	h.Lower -= 2

	return true
}

// removeEntriesFrom removes all entries from startIdx to end.
// This is used during page splits for bulk removal.
// Note: This doesn't compact the data area. Call compact() if needed.
func (p *page) removeEntriesFrom(startIdx int) {
	h := p.header()
	numEntries := p.numEntries()
	if startIdx < 0 || startIdx >= numEntries {
		return
	}
	entriesToRemove := numEntries - startIdx
	h.Lower -= uint16(entriesToRemove * 2)
}

// compact eliminates holes in the data area by repacking all node data.
// This reclaims space left by removed entries.
// Returns the amount of space reclaimed.
func (p *page) compact() int {
	return p.compactWithBuf(nil)
}

// compactWithBuf is like compact but uses an external buffer if provided.
// If scratchBuf is nil or too small, falls back to pooled buffer.
func (p *page) compactWithBuf(scratchBuf []byte) int {
	h := p.header()
	numEntries := p.numEntriesFast()
	pageSize := uint16(len(p.Data))

	if numEntries == 0 {
		oldUpper := h.Upper
		h.Upper = pageSize - pageHeaderSize
		return int(h.Upper - oldUpper)
	}

	// Use stack-allocated array for small pages (covers 99% of cases)
	// Max entries per page is typically < 256 for reasonable key/value sizes
	var sizesBuf [256]uint16
	var sizes []uint16
	if numEntries <= 256 {
		sizes = sizesBuf[:numEntries]
	} else {
		sizes = make([]uint16, numEntries)
	}

	// First pass: calculate sizes and total
	totalSize := uint16(0)
	for i := 0; i < numEntries; i++ {
		sizes[i] = uint16(p.calcNodeSizeFast(i))
		totalSize += sizes[i]
	}

	// Early exit if already compact
	expectedUpper := pageSize - pageHeaderSize - totalSize
	if h.Upper == expectedUpper {
		return 0
	}

	// Use the free space at the beginning of the page as temp storage
	// The entry pointers area ends at pageHeaderSize + numEntries*2
	// The upper (data) area starts at upper + pageHeaderSize
	// We can use the gap between them as temp storage
	entryPointersEnd := uint16(pageHeaderSize + numEntries*2)
	dataStart := h.Upper + pageHeaderSize

	// If gap is large enough, use it; otherwise use provided buffer or pool
	var tempBuf []byte
	var needReturn bool
	gapSize := int(dataStart - entryPointersEnd)
	if gapSize >= int(totalSize) {
		// Use gap in page as temp buffer (zero allocation)
		tempBuf = p.Data[entryPointersEnd:dataStart]
	} else if len(scratchBuf) >= int(totalSize) {
		// Use provided scratch buffer (zero allocation)
		tempBuf = scratchBuf[:totalSize]
	} else {
		// Fall back to pooled buffer
		tempBuf = getCompactBuffer(int(totalSize))
		needReturn = true
	}

	// Copy all node data to temp buffer
	tempPos := uint16(0)
	for i := 0; i < numEntries; i++ {
		srcOffset := p.entryOffsetFast(i)
		copy(tempBuf[tempPos:tempPos+sizes[i]], p.Data[srcOffset:srcOffset+sizes[i]])
		tempPos += sizes[i]
	}

	// Write back contiguously from end of page, update pointers
	writePos := pageSize
	tempPos = 0
	for i := 0; i < numEntries; i++ {
		writePos -= sizes[i]
		copy(p.Data[writePos:writePos+sizes[i]], tempBuf[tempPos:tempPos+sizes[i]])
		tempPos += sizes[i]

		// Update entry pointer (stored offset is relative to pageHeaderSize)
		entryPtrOffset := pageHeaderSize + i*2
		putUint16LE(p.Data[entryPtrOffset:], writePos-pageHeaderSize)
	}

	// Return buffer if we used the pool (no defer to avoid allocation)
	if needReturn {
		returnCompactBuffer(tempBuf)
	}

	// Update upper to new position
	oldUpper := h.Upper
	h.Upper = writePos - pageHeaderSize

	return int(h.Upper - oldUpper)
}

// Pool for compact buffers to reduce allocations
var compactBufferPool = sync.Pool{
	New: func() any {
		// Default size for page data (4KB)
		return make([]byte, 4096)
	},
}

func getCompactBuffer(size int) []byte {
	buf := compactBufferPool.Get().([]byte)
	if len(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

func returnCompactBuffer(buf []byte) {
	if cap(buf) >= 4096 {
		compactBufferPool.Put(buf[:cap(buf)])
	}
}

// updateEntry replaces the entry at the given index with new data.
// Returns false if there's not enough space for the new data.
// Uses alternate format (compatible with mdbx-go) where:
// - upper = actual_position - pageHeaderSize
// - stored offsets = actual_offset - pageHeaderSize
func (p *page) updateEntry(idx int, nodeData []byte) bool {
	h := p.header()
	numEntries := p.numEntries()

	if idx < 0 || idx >= numEntries {
		return false
	}

	oldSize := p.calcNodeSize(idx)
	newSize := len(nodeData)

	// If new node fits in old space, write in place
	if newSize <= oldSize {
		offset := p.entryOffset(idx)
		copy(p.Data[offset:], nodeData)
		return true
	}

	// Need more space - allocate at end
	extraSpace := newSize - oldSize
	if p.freeSpace() < extraSpace {
		return false
	}

	// Check if we have enough contiguous space at Upper
	// Must ensure Upper - newSize >= Lower to avoid overwriting entry pointers
	// Use int to avoid underflow
	newUpperInt := int(h.Upper) - newSize
	if newUpperInt < int(h.Lower) {
		// Not enough space - would overwrite entry pointers
		return false
	}
	newUpper := uint16(newUpperInt)

	// Allocate new space (in alternate format, upper is relative to pageHeaderSize)
	h.Upper = newUpper
	actualPosition := newUpper + pageHeaderSize
	copy(p.Data[actualPosition:], nodeData)

	// Update entry index to point to new location (alternate format: same as upper value)
	entryOffset := pageHeaderSize + idx*2
	putUint16LE(p.Data[entryOffset:], newUpper)

	// Old space is now a hole - will be reclaimed later

	return true
}

// calcNodeSize calculates the size of the node at the given index.
func (p *page) calcNodeSize(idx int) int {
	numEntries := p.numEntriesFast()
	if idx < 0 || idx >= numEntries {
		return 0
	}
	return p.calcNodeSizeFast(idx)
}

// calcNodeSizeFast calculates the size of the node at the given index without bounds checking.
// Caller must ensure idx is valid.
func (p *page) calcNodeSizeFast(idx int) int {
	nodeOffset := p.entryOffsetFast(idx)

	// Read node header: dsize(4) + flags(1) + extra(1) + ksize(2)
	dsize := binary.LittleEndian.Uint32(p.Data[nodeOffset:])
	flags := p.Data[nodeOffset+4]
	ksize := binary.LittleEndian.Uint16(p.Data[nodeOffset+6:])

	size := 8 + int(ksize) // Header + key

	// For branch pages, dsize is the child page number, not a data size
	if p.isBranchFast() {
		return size
	}

	// Leaf node: add data size
	if flags&0x01 != 0 {
		// Big node - data is overflow page number (4 bytes)
		size += 4
	} else {
		size += int(dsize)
	}

	return size
}

// splitPoint finds the optimal split point for this page.
// Returns the index at which to split (entries 0..idx-1 go to left, idx.. go to right).
// Takes newNodeSize and insertIdx to ensure both resulting pages will have enough space.
// Uses O(n) single-pass algorithm without heap allocation.
func (p *page) splitPoint(newNodeSize int, insertIdx int) int {
	numEntries := p.numEntriesFast()
	if numEntries == 0 {
		return 0
	}

	// Calculate the total available space per page
	// For N entries: needs N*2 bytes for pointers + sum(nodeSizes) for data
	pageSize := len(p.Data)
	maxSpace := pageSize - pageHeaderSize

	// Calculate total size of all existing entries in one pass
	totalExisting := 0
	for i := 0; i < numEntries; i++ {
		totalExisting += p.calcNodeSizeFast(i)
	}

	// OPTIMIZATION: Append-optimized split
	// If inserting at the end, try to keep all existing entries in the old page
	// and put only the new entry in the new page. This avoids copying entries.
	if insertIdx >= numEntries {
		// Check if old page can hold all existing entries
		leftNeeded := numEntries*2 + totalExisting
		// New page only needs the new entry
		rightNeeded := 2 + newNodeSize
		if leftNeeded <= maxSpace && rightNeeded <= maxSpace {
			return numEntries // All existing go left, new entry goes right
		}
	}

	// Helper to check if a split point is valid (calculates sizes on-the-fly)
	// splitIdx indicates: left page gets entries [0, splitIdx), right gets [splitIdx, numEntries)
	// Valid range is [0, numEntries] inclusive:
	// - splitIdx == 0: left gets only new node, right gets all existing entries
	// - splitIdx == numEntries: left gets all existing entries, right gets only new node
	isValidSplit := func(splitIdx int) bool {
		if splitIdx < 0 || splitIdx > numEntries {
			return false
		}

		// Calculate left side data size: entries [0, splitIdx)
		leftDataSize := 0
		for i := 0; i < splitIdx; i++ {
			leftDataSize += p.calcNodeSizeFast(i)
		}

		// Right side data size is totalExisting - leftDataSize
		rightDataSize := totalExisting - leftDataSize

		// Entry counts
		leftEntries := splitIdx
		rightEntries := numEntries - splitIdx

		// Add new node to appropriate side
		if insertIdx < splitIdx {
			leftEntries++
			leftDataSize += newNodeSize
		} else {
			rightEntries++
			rightDataSize += newNodeSize
		}

		// Both pages must have at least one entry after the split+insert
		if leftEntries == 0 || rightEntries == 0 {
			return false
		}

		// Check if both pages fit
		leftNeeded := leftEntries*2 + leftDataSize
		rightNeeded := rightEntries*2 + rightDataSize

		return leftNeeded <= maxSpace && rightNeeded <= maxSpace
	}

	// Start from midpoint
	mid := numEntries / 2
	if mid == 0 {
		mid = 1
	}

	// First try midpoint
	if isValidSplit(mid) {
		return mid
	}

	// Search outward from midpoint, including edge cases (0 and numEntries)
	for delta := 1; delta <= numEntries; delta++ {
		// Try moving split point toward the new node's insertion point
		if insertIdx < mid {
			// New node goes left, try reducing left side first
			if mid-delta >= 0 && isValidSplit(mid-delta) {
				return mid - delta
			}
			if mid+delta <= numEntries && isValidSplit(mid+delta) {
				return mid + delta
			}
		} else {
			// New node goes right, try increasing split point first
			if mid+delta <= numEntries && isValidSplit(mid+delta) {
				return mid + delta
			}
			if mid-delta >= 0 && isValidSplit(mid-delta) {
				return mid - delta
			}
		}
	}

	// Fallback: no valid split found, return midpoint anyway
	return mid
}

// compactTo compacts this page's data to a destination page.
// This eliminates holes from deleted entries.
// Uses alternate format (compatible with mdbx-go).
func (p *page) compactTo(dst *page, pageSize uint16) {
	h := p.header()
	dstH := dst.header()

	// Initialize destination (alternate format)
	dstH.PageNo = h.PageNo
	dstH.Flags = h.Flags
	dstH.Txnid = h.Txnid
	dstH.DupfixKsize = h.DupfixKsize
	dstH.Lower = 0                         // Alternate format: lower = numEntries * 2
	dstH.Upper = pageSize - pageHeaderSize // Alternate format: upper relative to pageHeaderSize

	// Copy entries in order
	numEntries := p.numEntries()
	for i := 0; i < numEntries; i++ {
		offset := p.entryOffset(i)
		nodeSize := p.calcNodeSize(i)
		if nodeSize > 0 && int(offset)+nodeSize <= len(p.Data) {
			dst.insertEntry(i, p.Data[offset:offset+uint16(nodeSize)])
		}
	}
}
