package gdbx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

// Cursor operation constants (untyped uint for mdbx-go compatibility)
const (
	// First positions at the first key
	First uint = iota
	// FirstDup positions at the first duplicate of current key
	FirstDup
	// GetBoth positions at exact key-value pair
	GetBoth
	// GetBothRange positions at key with value >= specified
	GetBothRange
	// GetCurrent returns current key-value
	GetCurrent
	// GetMultiple returns multiple values (DUPFIXED)
	GetMultiple
	// Last positions at the last key
	Last
	// LastDup positions at the last duplicate of current key
	LastDup
	// Next moves to the next key-value
	Next
	// NextDup moves to the next duplicate of current key
	NextDup
	// NextMultiple returns next multiple values (DUPFIXED)
	NextMultiple
	// NextNoDup moves to the first value of next key
	NextNoDup
	// Prev moves to the previous key-value
	Prev
	// PrevDup moves to the previous duplicate of current key
	PrevDup
	// PrevNoDup moves to the last value of previous key
	PrevNoDup
	// Set positions at specified key
	Set
	// SetKey positions at key, returns key and value
	SetKey
	// SetRange positions at first key >= specified
	SetRange
	// PrevMultiple returns previous multiple values (DUPFIXED)
	PrevMultiple
	// SetLowerbound positions at first key-value >= specified
	SetLowerbound
	// SetUpperbound positions at first key-value > specified
	SetUpperbound
	// LesserThan positions at the key less than specified
	LesserThan
)

// CursorOp is for backward compatibility (deprecated, use uint constants directly)
type CursorOp = uint

// CursorStackSize is the maximum tree depth supported
const CursorStackSize = 32

// cursorState tracks cursor validity
type cursorState uint8

const (
	cursorUninitialized cursorState = iota
	cursorPointing                  // Cursor is at a valid position
	cursorEOF                       // Cursor is past end
	cursorInvalid                   // Cursor is invalidated
)

// dupState tracks position within DUPSORT duplicates
type dupState struct {
	initialized bool                    // Whether dup state is initialized (metadata loaded)
	isSubTree   bool                    // True if duplicates are in a sub-tree (N_TREE)
	atFirst     bool                    // True if positioned at first entry (for O(1) FirstDup)
	atLast      bool                    // True if positioned at last entry (for O(1) LastDup)
	subTree     tree                    // Sub-tree structure (for N_TREE)
	subPages    [CursorStackSize]*page  // Page stack for sub-tree - points to subPagesBuf
	subPagesBuf [CursorStackSize]page   // Embedded page structs to avoid allocation
	subIndices  [CursorStackSize]uint16 // Index stack for sub-tree
	subTop      int8                    // Current position in sub-tree stack

	// For inline sub-pages (N_DUP without N_TREE)
	subPageData      []byte   // The sub-page data
	subPageIdx       int      // Current index in sub-page (0 = first/smallest value)
	subPageNum       int      // Total entries in sub-page
	dupfixSize       int      // Size of each value for DUPFIX
	nodePositions    []int    // Node positions (from entry pointers + PAGEHDRSZ)
	nodePositionsBuf [256]int // Pre-allocated buffer for nodePositions (covers most cases)
}

// Cursor provides navigation through a database.
type Cursor struct {
	signature int32
	state     cursorState
	top       int8 // Current stack position (-1 = empty)
	dbi       DBI
	txn       *Txn
	tree      *tree

	// Cached mmap data for fast page access (avoids txn indirection)
	mmapData    []byte
	mmapVersion uint64 // Version when mmapData was cached (for stale detection after remap)
	pageSize    uint32
	readOnly    bool   // True if transaction is read-only
	isDupSort   bool   // True if this is a DUPSORT database (cached for fast path)
	afterDelete bool   // True after Del() - next move returns current position
	dirtyMask   uint32 // Bitmask of which stack levels have dirty pages

	// Page stack for tree traversal - pages points to pagesBuf to avoid allocation
	pages       [CursorStackSize]*page
	pagesBuf    [CursorStackSize]page   // Embedded page structs
	pgnoCache   [CursorStackSize]pgno   // Cached page numbers (for safe refresh after mmap remap)
	indices     [CursorStackSize]uint16
	stackDirty  [CursorStackSize]*page  // Inline dirty page cache - avoids tracker lookup
	numExpected [CursorStackSize]uint16 // Expected number of entries (for detecting deletions by other cursors)

	// For DUPSORT: nested cursor state
	subcur *Cursor
	dup    dupState // Tracks position within duplicates

	// Linked list in transaction
	next *Cursor

	// Scratch buffers for building nodes (avoids allocation)
	nodeBuf    [512]byte  // For leaf nodes
	branchBuf  [128]byte  // For branch nodes (smaller, just key + 8 byte header)
	subPageBuf [4096]byte // For building sub-pages (DUPSORT)
	valuesBuf  [64][]byte // For parseSubPageValues (avoids allocation for small dup counts)

	// User context
	userCtx any
}

// cursorSignature is the magic number for valid cursors
const cursorSignature int32 = 0x43555253 // "CURS"

// initMmapCache caches mmap data and page size for fast page access.
func (c *Cursor) initMmapCache() {
	if c.txn == nil {
		return
	}
	c.readOnly = c.txn.flags&uint32(TxnReadOnly) != 0
	c.pageSize = c.txn.env.pageSize
	if c.txn.mmapData == nil {
		c.txn.initMmapCache()
	}
	c.mmapData = c.txn.mmapData
	c.mmapVersion = c.txn.env.mmapVersion
}

// refreshStalePages refreshes cursor's cached page references after mmap remap.
// Called when we detect the mmap version has changed.
func (c *Cursor) refreshStalePages() {
	if c.txn == nil || c.txn.env == nil {
		return
	}

	// Update cached mmap data
	if c.txn.env.dataMap != nil {
		c.mmapData = c.txn.env.dataMap.data
		c.txn.mmapData = c.mmapData // Update transaction's cache too
	}
	c.mmapVersion = c.txn.env.mmapVersion

	// Refresh page references in the cursor stack
	// Use pgnoCache since p.pageNo() would access the stale mmap
	for i := int8(0); i <= c.top; i++ {
		if c.pages[i] == nil {
			continue
		}
		// Skip dirty pages (they're heap-allocated or newly written to mmap)
		if c.dirtyMask&(1<<i) != 0 {
			continue
		}
		// Get page number from cache (safe even after remap)
		pn := c.pgnoCache[i]
		newData := c.txn.env.getMmapPageData(pn)
		if newData != nil {
			// Refresh the page struct to point to new mmap location
			c.pagesBuf[i].Data = newData
			c.pages[i] = &c.pagesBuf[i]
		}
	}
}

// valid returns true if the cursor is valid.
func (c *Cursor) valid() bool {
	return c != nil && c.signature == cursorSignature && c.txn != nil
}

// Txn returns the cursor's transaction.
func (c *Cursor) Txn() *Txn {
	return c.txn
}

// DBI returns the cursor's database handle.
func (c *Cursor) DBI() DBI {
	return c.dbi
}

// Close closes the cursor and returns it to the pool.
func (c *Cursor) Close() {
	if c == nil || c.signature != cursorSignature {
		return
	}

	// Remove from transaction's cursor list
	txn := c.txn
	if txn != nil {
		txn.removeCursor(c)
	}

	// Return to pool
	returnCursor(c)
}

// SetUserCtx sets user context data on the cursor.
func (c *Cursor) SetUserCtx(ctx any) {
	c.userCtx = ctx
}

// UserCtx returns the user context data.
func (c *Cursor) UserCtx() any {
	return c.userCtx
}

// Get retrieves key-value at the cursor position based on operation.
func (c *Cursor) Get(key, value []byte, op CursorOp) ([]byte, []byte, error) {
	if !c.valid() {
		return nil, nil, ErrBadCursorError
	}

	switch op {
	case First:
		return c.first()
	case Last:
		return c.last()
	case Next:
		return c.moveNext()
	case Prev:
		return c.movePrev()
	case GetCurrent:
		return c.getCurrent()
	case Set:
		return c.set(key)
	case SetKey:
		return c.setKey(key)
	case SetRange:
		return c.setRange(key)
	case FirstDup:
		return c.firstDup()
	case LastDup:
		return c.lastDup()
	case NextDup:
		return c.nextDup()
	case PrevDup:
		return c.prevDup()
	case NextNoDup:
		return c.nextNoDup()
	case PrevNoDup:
		return c.prevNoDup()
	case GetBoth:
		return c.getBoth(key, value)
	case GetBothRange:
		return c.getBothRange(key, value)
	case SetLowerbound:
		return c.setLowerbound(key, value)
	case SetUpperbound:
		return c.setUpperbound(key, value)
	default:
		return nil, nil, NewError(ErrInvalid)
	}
}

// Put stores a key-value pair at the cursor position.
func (c *Cursor) Put(key, value []byte, flags uint) error {
	if !c.valid() {
		return ErrBadCursorError
	}

	if c.txn.flags&uint32(TxnReadOnly) != 0 {
		return NewError(ErrPermissionDenied)
	}

	return c.put(key, value, flags)
}

// Del deletes the current key-value pair.
func (c *Cursor) Del(flags uint) error {
	if !c.valid() {
		return ErrBadCursorError
	}

	if c.txn.flags&uint32(TxnReadOnly) != 0 {
		return NewError(ErrPermissionDenied)
	}

	if c.state != cursorPointing {
		return ErrNotFoundError
	}

	return c.del(flags)
}

// Count returns the number of values for the current key.
func (c *Cursor) Count() (uint64, error) {
	if !c.valid() {
		return 0, ErrBadCursorError
	}

	if c.state != cursorPointing {
		return 0, ErrNotFoundError
	}

	// For non-DUPSORT databases, always 1
	if c.tree.Flags&uint16(DupSort) == 0 {
		return 1, nil
	}

	return c.countDuplicates()
}

// EOF returns true if the cursor is at end-of-file.
func (c *Cursor) EOF() bool {
	return c.state == cursorEOF
}

// OnFirst returns true if cursor is at the first key.
func (c *Cursor) OnFirst() bool {
	if c.state != cursorPointing {
		return false
	}
	return c.isFirst()
}

// OnLast returns true if cursor is at the last key.
func (c *Cursor) OnLast() bool {
	if c.state != cursorPointing {
		return false
	}
	return c.isLast()
}

// --- Internal cursor operations ---

// currentPage returns the current page.
func (c *Cursor) currentPage() *page {
	if c.top < 0 {
		return nil
	}
	return c.pages[c.top]
}

// currentIndex returns the current index within the page.
func (c *Cursor) currentIndex() uint16 {
	if c.top < 0 {
		return 0
	}
	return c.indices[c.top]
}

// pushPage adds a page to the stack.
func (c *Cursor) pushPage(p *page, idx uint16) error {
	if c.top >= CursorStackSize-1 {
		return ErrCursorFullError
	}
	c.top++
	c.pages[c.top] = p
	c.indices[c.top] = idx
	return nil
}

// pushPageByPgno pushes a page by page number using embedded buffer (no allocation).
func (c *Cursor) pushPageByPgno(pn pgno, idx uint16) error {
	if c.top >= CursorStackSize-1 {
		return ErrCursorFullError
	}
	c.top++

	// Clear dirty state for this level since we're pushing a new page
	// This is critical when navigating to a different page at the same level
	levelBit := uint32(1) << c.top
	c.dirtyMask &^= levelBit
	c.stackDirty[c.top] = nil

	// Fast path for read-only transactions: direct mmap access
	if c.readOnly && c.mmapData != nil {
		offset := uint64(pn) * uint64(c.pageSize)
		c.pagesBuf[c.top].Data = c.mmapData[offset : offset+uint64(c.pageSize)]
		c.pages[c.top] = &c.pagesBuf[c.top]
	} else {
		// Fallback for write transactions - also tracks dirty pages
		buf := &c.pagesBuf[c.top]
		p := c.txn.fillPageHotPath(pn, buf)
		c.pages[c.top] = p
		// If returned page is not our buffer, it's a dirty page from txn
		if p != buf {
			c.dirtyMask |= levelBit
		}
	}
	c.indices[c.top] = idx
	// Record expected number of entries for detecting deletions by other cursors
	c.numExpected[c.top] = uint16(c.pages[c.top].numEntriesFast())
	return nil
}

// popPage removes the top page from the stack.
func (c *Cursor) popPage() (*page, uint16) {
	if c.top < 0 {
		return nil, 0
	}
	p := c.pages[c.top]
	idx := c.indices[c.top]
	// Don't nil pages[c.top] - it's pre-initialized to &pagesBuf[c.top]
	c.top--
	return p, idx
}

// DebugPrune enables verbose debug output for cursor refresh operations
var DebugPrune = false
var DebugCursor = false

// refreshPage ensures the current page reflects any modifications from other cursors.
// For write transactions, if another cursor modified this page through COW, we need
// to use the dirty version. This is called before accessing page data during navigation.
// Returns true if the page was updated (indicating another cursor modified it).
func (c *Cursor) refreshPage() bool {
	if c.top < 0 || c.readOnly {
		return false
	}

	// Check if tree root changed (another cursor did COW and replaced the root)
	// In this case, our entire page stack is stale and needs to be refreshed.
	if c.top >= 0 && c.pages[0].pageNo() != c.tree.Root {
		if DebugPrune {
			fmt.Printf("refreshPage: root changed %d -> %d\n", c.pages[0].pageNo(), c.tree.Root)
		}
		c.refreshStackFromRoot()
		// After refreshing stack, check if we're still at a valid position
		if c.top < 0 {
			return true
		}
	}

	p := c.pages[c.top]
	pn := p.pageNo()

	// Check if there's a dirty version of this page
	if dirty := c.txn.dirtyTracker.get(pn); dirty != nil && dirty != p {
		// Another cursor modified this page - update our reference
		c.pages[c.top] = dirty
		c.dirtyMask |= uint32(1) << c.top
		c.stackDirty[c.top] = dirty
		p = dirty
	}

	// Check if entries were deleted by comparing current count with expected
	newNumEntries := uint16(p.numEntriesFast())
	expectedNum := c.numExpected[c.top]

	if newNumEntries < expectedNum {
		// Entries were deleted - need to adjust cursor behavior
		idx := c.indices[c.top]
		if idx < newNumEntries {
			// Entry was deleted before or at our position, entries shifted down
			// Our index now points to what was the NEXT entry
			// Set afterDelete so Next() returns current instead of advancing
			c.afterDelete = true
		}
		// If idx >= newNumEntries, we're past the end - let moveNext handle it
		// by popping pages and trying to find the next entry
		// Update expected count
		c.numExpected[c.top] = newNumEntries
		return true
	}
	return false
}

// refreshStackFromRoot refreshes the cursor's page stack when the tree root has changed.
// This happens when another cursor did COW and replaced pages from root down.
// We navigate from the new root following the same indices to find our position.
// Since entries may have been deleted, we set afterDelete=true so the next iteration
// returns the current entry instead of advancing.
func (c *Cursor) refreshStackFromRoot() {
	if c.top < 0 || c.tree.isEmpty() {
		c.state = cursorEOF
		return
	}

	// Save current indices and expected counts to detect deletions
	savedTop := c.top
	savedIndices := c.indices
	savedNumExpected := c.numExpected

	if DebugPrune {
		fmt.Printf("refreshStackFromRoot: savedTop=%d, savedIndices[0]=%d, savedNumExpected[0]=%d, tree.Height=%d\n",
			savedTop, savedIndices[0], savedNumExpected[0], c.tree.Height)
	}

	// Clear dirty state
	for i := int8(0); i <= savedTop; i++ {
		c.stackDirty[i] = nil
	}
	c.dirtyMask = 0

	// Start from new root
	c.top = 0
	rootPage := c.txn.fillPageHotPath(c.tree.Root, &c.pagesBuf[0])
	c.pages[0] = rootPage
	newNumEntries := uint16(rootPage.numEntriesFast())

	if DebugPrune {
		fmt.Printf("refreshStackFromRoot: newNumEntries=%d, isLeaf=%v\n", newNumEntries, rootPage.isLeaf())
	}

	// Check if tree height collapsed (was multi-level, now single level)
	// In this case, the cursor's saved position refers to a different tree structure.
	// The old leaf page may have been freed or merged.
	// Position at the start of the new root - this is where iteration should continue.
	if rootPage.isLeaf() && savedTop > 0 {
		if DebugPrune {
			fmt.Printf("refreshStackFromRoot: TREE COLLAPSED! Positioning at start of new root (entries=%d)\n",
				newNumEntries)
		}

		if newNumEntries == 0 {
			c.indices[0] = 0
			c.numExpected[0] = 0
			c.state = cursorEOF
			return
		}

		// Position at first entry - this is the "next" entry after the deletion
		c.indices[0] = 0
		c.numExpected[0] = newNumEntries
		c.afterDelete = true // Current position IS the next entry
		return
	}

	// Normal case: tree didn't collapse, use savedIndices[0] for root
	// Detect if entries were deleted by comparing old vs new entry counts
	// If entries were deleted and our index is still valid, the entry at our index
	// is now the "next" entry, so we should set afterDelete=true.
	if newNumEntries < savedNumExpected[0] && int(savedIndices[0]) < int(newNumEntries) {
		if DebugPrune {
			fmt.Printf("refreshStackFromRoot: setting afterDelete=true (deletion detected)\n")
		}
		c.afterDelete = true
	}
	c.numExpected[0] = newNumEntries

	// Adjust index if it's now out of bounds
	if int(savedIndices[0]) >= int(newNumEntries) {
		if newNumEntries > 0 {
			c.indices[0] = newNumEntries - 1
			if DebugPrune {
				fmt.Printf("refreshStackFromRoot: index adjusted to %d (was out of bounds)\n", c.indices[0])
			}
		} else {
			c.indices[0] = 0
			c.state = cursorEOF
			return
		}
		// Index was adjusted, but this doesn't mean we're at "next" - we're past the end
		// afterDelete should be false here to trigger navigation to parent in moveNext
	} else {
		c.indices[0] = savedIndices[0]
	}

	// Descend following the saved indices
	for level := int8(1); level <= savedTop; level++ {
		parentPage := c.pages[c.top]
		if parentPage.isLeaf() {
			// We've reached a leaf - done descending
			break
		}

		parentIdx := c.indices[c.top]
		if int(parentIdx) >= parentPage.numEntriesFast() {
			// Parent index is out of bounds, we're at the end
			break
		}

		childPgno := c.getChildPgno(parentPage, int(parentIdx))
		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			c.state = cursorEOF
			return
		}

		// Adjust child index and detect deletions
		childPage := c.pages[c.top]
		childNumEntries := uint16(childPage.numEntriesFast())

		// Detect deletions at this level
		if childNumEntries < savedNumExpected[level] && int(savedIndices[level]) < int(childNumEntries) {
			c.afterDelete = true
		}
		c.numExpected[c.top] = childNumEntries

		if int(savedIndices[level]) >= int(childNumEntries) {
			if childNumEntries > 0 {
				c.indices[c.top] = childNumEntries - 1
			} else {
				c.indices[c.top] = 0
			}
		} else {
			c.indices[c.top] = savedIndices[level]
		}
	}
}

// reset clears the cursor stack.
func (c *Cursor) reset() {
	// Clear stackDirty entries that were used (avoid clearing entire array)
	// Only clear if top was valid (>= 0)
	if c.top >= 0 {
		for i := int8(0); i <= c.top; i++ {
			c.stackDirty[i] = nil
		}
	}
	// Don't nil pages[i] - they're pre-initialized to &pagesBuf[i]
	// For write txns with dirty pages, the pointers will be overwritten on next push
	c.top = -1
	c.state = cursorUninitialized
	c.afterDelete = false
	c.dirtyMask = 0
	c.clearDupState()
}

// first positions at the first key.
func (c *Cursor) first() ([]byte, []byte, error) {
	c.reset()

	if c.tree.isEmpty() {
		c.state = cursorEOF
		return nil, nil, ErrNotFoundError
	}

	// Start at root using embedded buffer (no allocation)
	c.top = 0
	rootPage := c.txn.fillPageHotPath(c.tree.Root, &c.pagesBuf[0])
	c.pages[0] = rootPage
	c.indices[0] = 0
	c.numExpected[0] = uint16(rootPage.numEntriesFast())

	// Descend to leftmost leaf using embedded buffers
	for rootPage.isBranchFast() {
		childPgno := c.getChildPgno(rootPage, 0)
		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			return nil, nil, err
		}
		rootPage = c.pages[c.top]
	}

	if rootPage.numEntriesFast() == 0 {
		c.state = cursorEOF
		return nil, nil, ErrNotFoundError
	}

	c.state = cursorPointing
	return c.getCurrent()
}

// last positions at the last key.
func (c *Cursor) last() ([]byte, []byte, error) {
	c.reset()

	if c.tree.isEmpty() {
		c.state = cursorEOF
		return nil, nil, ErrNotFoundError
	}

	// Start at root using embedded buffer (no allocation)
	c.top = 0
	rootPage := c.txn.fillPageHotPath(c.tree.Root, &c.pagesBuf[0])
	c.pages[0] = rootPage
	lastIdx := uint16(rootPage.numEntriesFast() - 1)
	c.indices[0] = lastIdx
	c.numExpected[0] = uint16(rootPage.numEntriesFast())

	// Descend to rightmost leaf using embedded buffers
	for rootPage.isBranchFast() {
		childPgno := c.getChildPgno(rootPage, int(lastIdx))
		c.top++
		rootPage = c.txn.fillPageHotPath(childPgno, &c.pagesBuf[c.top])
		c.pages[c.top] = rootPage
		lastIdx = uint16(rootPage.numEntriesFast() - 1)
		c.indices[c.top] = lastIdx
		c.numExpected[c.top] = uint16(rootPage.numEntriesFast())
	}

	c.state = cursorPointing

	// For DUPSORT tables, position at the last duplicate
	if c.tree.Flags&uint16(DupSort) != 0 {
		return c.lastDup()
	}

	return c.getCurrent()
}

// moveNext moves to the next key-value.
// For DUPSORT tables, this iterates through all duplicates before moving to next key.
func (c *Cursor) moveNext() ([]byte, []byte, error) {
	if c.state == cursorUninitialized {
		return c.first()
	}

	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// Check if another cursor modified our page - this may set afterDelete
	c.refreshPage()

	// After delete, cursor is already at the "next" position - just return current
	if c.afterDelete {
		c.afterDelete = false
		return c.getCurrent()
	}

	// For DUPSORT tables, try to move to next duplicate first
	if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 && c.dup.initialized {
		// Try to move to next duplicate
		if c.dup.isSubTree {
			k, v, err := c.dupSubTreeNext()
			if err == nil {
				return k, v, nil
			}
		} else {
			// Inline sub-page: increment index
			if c.dup.subPageIdx+1 < c.dup.subPageNum {
				c.dup.subPageIdx++
				return c.getCurrent()
			}
		}
		// No more duplicates, fall through to move to next key
	}

	// Clear dup state when moving to a new key
	c.clearDupState()

	// Move to next position in main tree
	// Save current position in case we need to restore it
	savedTop := c.top
	savedIndices := c.indices

	for c.top >= 0 {
		// Refresh page in case another cursor modified it
		c.refreshPage()

		p := c.pages[c.top]
		idx := c.indices[c.top]

		if int(idx)+1 < p.numEntriesFast() {
			// Move to next entry on this page
			c.indices[c.top] = idx + 1

			// If branch, descend to leftmost leaf
			if p.isBranchFast() {
				return c.descendLeft()
			}

			return c.getCurrent()
		}

		// No more entries on this page, go up
		c.popPage()
	}

	// Reached the end - restore cursor to last valid position
	// This matches libmdbx behavior: cursor stays at last entry when Next returns NOTFOUND
	// Keep state as cursorPointing so Prev() works correctly from this position
	c.top = savedTop
	c.indices = savedIndices
	// Don't change state - cursor is still pointing at a valid entry
	return nil, nil, ErrNotFoundError
}

// movePrev moves to the previous key-value.
// For DUPSORT tables, this iterates through all duplicates in reverse before moving to previous key.
func (c *Cursor) movePrev() ([]byte, []byte, error) {
	if c.state == cursorUninitialized {
		return c.last()
	}

	// At EOF, Prev should position at the last entry (matches libmdbx behavior)
	if c.state == cursorEOF {
		return c.last()
	}

	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// After delete, cursor is already at the correct position - just return current
	if c.afterDelete {
		c.afterDelete = false
		return c.getCurrent()
	}

	// For DUPSORT tables, try to move to previous duplicate first
	if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
		// If atLast is set but not initialized (from lastDup), initialize at last position
		if !c.dup.initialized && c.dup.atLast {
			if err := c.initDupStateAtLast(); err == nil {
				// Now we can try prevDup
			}
		}

		if c.dup.initialized {
			// Try to move to previous duplicate
			if c.dup.isSubTree {
				k, v, err := c.dupSubTreePrev()
				if err == nil {
					return k, v, nil
				}
			} else {
				// Inline sub-page: decrement index
				if c.dup.subPageIdx > 0 {
					c.dup.subPageIdx--
					c.dup.atFirst = false
					c.dup.atLast = false
					return c.getCurrent()
				}
			}
			// No more duplicates, fall through to move to previous key
		}
	}

	// Clear dup state when moving to a new key
	c.clearDupState()

	// Move to previous position in main tree
	for c.top >= 0 {
		// Refresh page in case another cursor modified it
		c.refreshPage()

		p := c.pages[c.top]
		idx := c.indices[c.top]

		if idx > 0 {
			// Move to previous entry on this page
			c.indices[c.top] = idx - 1

			// If branch, descend to rightmost leaf
			if p.isBranchFast() {
				k, v, err := c.descendRight()
				if err != nil {
					return k, v, err
				}
				// For DUPSORT databases, position on LAST duplicate of this key
				if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
					return c.lastDup()
				}
				return k, v, nil
			}

			// For DUPSORT databases, position on LAST duplicate of this key
			if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
				return c.lastDup()
			}
			return c.getCurrent()
		}

		// No more entries on this page, go up
		c.popPage()
	}

	// Reached the beginning
	c.state = cursorEOF
	return nil, nil, ErrNotFoundError
}

// getCurrent returns the current key-value.
// Uses allocation-free node access methods for hot path performance.
func (c *Cursor) getCurrent() ([]byte, []byte, error) {
	if c.state != cursorPointing || c.top < 0 {
		return nil, nil, ErrNotFoundError
	}

	// Refresh page in case another cursor modified it
	c.refreshPage()

	p := c.pages[c.top]
	idx := int(c.indices[c.top])

	if idx >= p.numEntriesFast() {
		return nil, nil, ErrNotFoundError
	}

	// Use allocation-free direct access methods
	key := nodeGetKeyDirect(p, idx)
	if key == nil {
		return nil, nil, ErrCorruptedError
	}

	flags := nodeGetFlagsDirect(p, idx)
	data := nodeGetDataDirect(p, idx)

	// Handle large values
	if flags&nodeBig != 0 {
		overflowPgno := nodeGetOverflowPgnoDirect(p, idx)
		dataSize := nodeGetDataSizeDirect(p, idx)
		data, _ = c.txn.getLargeData(overflowPgno, dataSize)
	}

	// Handle DUPSORT: use cached flag for fast path
	if c.isDupSort {
		// If node has N_TREE flag, use sub-tree
		if flags&nodeTree != 0 && len(data) >= 48 {
			// Initialize or use existing dup state
			if !c.dup.initialized {
				if err := c.initDupSubTree(data); err != nil {
					return nil, nil, err
				}
			}
			subValue, err := c.getDupValue()
			if err != nil {
				return nil, nil, err
			}
			return key, subValue, nil
		}

		// If node has N_DUP flag (inline sub-page)
		if flags&nodeDup != 0 && flags&nodeTree == 0 && len(data) >= 16 {
			// Initialize or use existing dup state
			if !c.dup.initialized {
				if err := c.initDupSubPage(data); err != nil {
					return nil, nil, err
				}
			}
			subValue, err := c.getDupValue()
			if err != nil {
				return nil, nil, err
			}
			return key, subValue, nil
		}
	}

	return key, data, nil
}

// clearDupState marks dup state as uninitialized.
// Individual init functions set only what they need - no zeroing required.
func (c *Cursor) clearDupState() {
	c.dup.initialized = false
}

// initDupSubTree initializes dup state for a sub-tree.
// Sets all fields used by sub-tree operations - no pre-clearing needed.
func (c *Cursor) initDupSubTree(treeData []byte) error {
	if len(treeData) < 48 {
		return ErrCorruptedError
	}

	// Parse Tree structure into embedded buffer to avoid allocation
	c.dup.subTree = tree{
		Flags:  uint16(treeData[0]) | uint16(treeData[1])<<8,
		Height: uint16(treeData[2]) | uint16(treeData[3])<<8,
		Root: pgno(
			uint32(treeData[8]) | uint32(treeData[9])<<8 |
				uint32(treeData[10])<<16 | uint32(treeData[11])<<24),
		Items: uint64(treeData[32]) | uint64(treeData[33])<<8 |
			uint64(treeData[34])<<16 | uint64(treeData[35])<<24 |
			uint64(treeData[36])<<32 | uint64(treeData[37])<<40 |
			uint64(treeData[38])<<48 | uint64(treeData[39])<<56,
	}

	if c.dup.subTree.Root == invalidPgno {
		return ErrNotFoundError
	}

	c.dup.isSubTree = true
	c.dup.subTop = 0

	// Navigate to first leaf in sub-tree using embedded page buffers (no allocation)
	subPage := c.txn.fillPageHotPath(c.dup.subTree.Root, &c.dup.subPagesBuf[0])
	c.dup.subPages[0] = subPage
	c.dup.subIndices[0] = 0

	// Descend to leftmost leaf
	for subPage.isBranchFast() {
		childPgno := c.getChildPgno(subPage, 0)
		c.dup.subTop++
		subPage = c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[c.dup.subTop])
		c.dup.subPages[c.dup.subTop] = subPage
		c.dup.subIndices[c.dup.subTop] = 0
	}

	c.dup.initialized = true
	c.dup.atFirst = true
	c.dup.atLast = false
	return nil
}

// initDupSubPage initializes dup state for an inline sub-page.
// libmdbx format: 20-byte page header, entry pointers at offset 20, 8-byte node headers.
// Sets all fields used by sub-page operations - no pre-clearing needed.
func (c *Cursor) initDupSubPage(subPageData []byte) error {
	if len(subPageData) < 20 {
		return ErrCorruptedError
	}

	c.dup.isSubTree = false
	c.dup.subPageData = subPageData
	c.dup.subPageIdx = 0

	// Parse sub-page header using unsafe for speed
	ptr := unsafe.Pointer(&subPageData[0])
	dupfixKsize := int(*(*uint16)(unsafe.Add(ptr, 8)))
	flags := *(*uint16)(unsafe.Add(ptr, 10))
	lower := int(*(*uint16)(unsafe.Add(ptr, 12)))

	// Check for DUPFIX format (P_DUPFIX = 0x20)
	if (flags&uint16(pageDupfix) != 0) && dupfixKsize > 0 && dupfixKsize < 65535 {
		// DUPFIX: fixed-size values after 20-byte header
		c.dup.dupfixSize = dupfixKsize
		c.dup.subPageNum = (len(subPageData) - pageHeaderSize) / dupfixKsize
		c.dup.nodePositions = nil
	} else if lower > 0 && lower < len(subPageData) {
		// Variable-size: lower = numEntries * 2
		numEntries := lower / 2
		c.dup.dupfixSize = 0

		// Use pre-allocated buffer if possible (reuse without zeroing)
		if numEntries <= len(c.dup.nodePositionsBuf) {
			c.dup.nodePositions = c.dup.nodePositionsBuf[:numEntries]
		} else {
			c.dup.nodePositions = make([]int, numEntries)
		}

		// Read entry pointers using unsafe - eliminates bounds checking
		entryPtr := unsafe.Add(ptr, pageHeaderSize)
		for i := 0; i < numEntries; i++ {
			storedOffset := int(*(*uint16)(entryPtr))
			c.dup.nodePositions[i] = storedOffset + pageHeaderSize
			entryPtr = unsafe.Add(entryPtr, 2)
		}
		c.dup.subPageNum = numEntries
	} else {
		c.dup.subPageNum = 0
		c.dup.dupfixSize = 0
		c.dup.nodePositions = nil
	}

	c.dup.initialized = true
	c.dup.atFirst = true
	c.dup.atLast = false
	return nil
}

// initDupSubTreeLast initializes dup state for a sub-tree positioned at the last entry.
func (c *Cursor) initDupSubTreeLast(treeData []byte) error {
	if len(treeData) < 48 {
		return ErrCorruptedError
	}

	// Parse Tree structure into embedded buffer to avoid allocation
	c.dup.subTree = tree{
		Flags:  uint16(treeData[0]) | uint16(treeData[1])<<8,
		Height: uint16(treeData[2]) | uint16(treeData[3])<<8,
		Root: pgno(
			uint32(treeData[8]) | uint32(treeData[9])<<8 |
				uint32(treeData[10])<<16 | uint32(treeData[11])<<24),
		Items: uint64(treeData[32]) | uint64(treeData[33])<<8 |
			uint64(treeData[34])<<16 | uint64(treeData[35])<<24 |
			uint64(treeData[36])<<32 | uint64(treeData[37])<<40 |
			uint64(treeData[38])<<48 | uint64(treeData[39])<<56,
	}

	if c.dup.subTree.Root == invalidPgno {
		return ErrNotFoundError
	}

	c.dup.isSubTree = true
	c.dup.subTop = 0

	// Navigate to last leaf in sub-tree using embedded page buffers (no allocation)
	subPage := c.txn.fillPageHotPath(c.dup.subTree.Root, &c.dup.subPagesBuf[0])
	c.dup.subPages[0] = subPage

	// Descend to rightmost leaf (last entry on each level)
	for subPage.isBranchFast() {
		lastIdx := uint16(subPage.numEntriesFast() - 1)
		c.dup.subIndices[c.dup.subTop] = lastIdx
		childPgno := c.getChildPgno(subPage, int(lastIdx))
		c.dup.subTop++
		subPage = c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[c.dup.subTop])
		c.dup.subPages[c.dup.subTop] = subPage
	}

	// Position at last entry in the leaf
	lastIdx := uint16(subPage.numEntriesFast() - 1)
	c.dup.subIndices[c.dup.subTop] = lastIdx

	c.dup.initialized = true
	c.dup.atFirst = false
	c.dup.atLast = true
	return nil
}

// initDupSubPageLast initializes dup state for an inline sub-page positioned at the last entry.
func (c *Cursor) initDupSubPageLast(subPageData []byte) error {
	if len(subPageData) < 20 {
		return ErrCorruptedError
	}

	c.dup.isSubTree = false
	c.dup.subPageData = subPageData

	// Parse sub-page header using unsafe for speed
	ptr := unsafe.Pointer(&subPageData[0])
	dupfixKsize := int(*(*uint16)(unsafe.Add(ptr, 8)))
	flags := *(*uint16)(unsafe.Add(ptr, 10))
	lower := int(*(*uint16)(unsafe.Add(ptr, 12)))

	// Check for DUPFIX format (P_DUPFIX = 0x20)
	if (flags&uint16(pageDupfix) != 0) && dupfixKsize > 0 && dupfixKsize < 65535 {
		// DUPFIX: fixed-size values after 20-byte header
		c.dup.dupfixSize = dupfixKsize
		c.dup.subPageNum = (len(subPageData) - pageHeaderSize) / dupfixKsize
		c.dup.nodePositions = nil
		// Position at last entry
		c.dup.subPageIdx = c.dup.subPageNum - 1
		if c.dup.subPageIdx < 0 {
			c.dup.subPageIdx = 0
		}
	} else if lower > 0 && lower < len(subPageData) {
		// Variable-size: lower = numEntries * 2
		numEntries := lower / 2
		c.dup.dupfixSize = 0

		// Use pre-allocated buffer if possible (reuse without zeroing)
		if numEntries <= len(c.dup.nodePositionsBuf) {
			c.dup.nodePositions = c.dup.nodePositionsBuf[:numEntries]
		} else {
			c.dup.nodePositions = make([]int, numEntries)
		}

		// Read entry pointers using unsafe - eliminates bounds checking
		entryPtr := unsafe.Add(ptr, pageHeaderSize)
		for i := range numEntries {
			storedOffset := int(*(*uint16)(entryPtr))
			c.dup.nodePositions[i] = storedOffset + pageHeaderSize
			entryPtr = unsafe.Add(entryPtr, 2)
		}
		c.dup.subPageNum = numEntries
		// Position at last entry
		c.dup.subPageIdx = numEntries - 1
		if c.dup.subPageIdx < 0 {
			c.dup.subPageIdx = 0
		}
	} else {
		c.dup.subPageNum = 0
		c.dup.dupfixSize = 0
		c.dup.nodePositions = nil
		c.dup.subPageIdx = 0
	}

	c.dup.initialized = true
	c.dup.atFirst = false
	c.dup.atLast = true
	return nil
}

// getDupValue returns the current duplicate value
// Uses allocation-free node access for performance.
func (c *Cursor) getDupValue() ([]byte, error) {
	if !c.dup.initialized {
		return nil, ErrNotFoundError
	}

	if c.dup.isSubTree {
		// Get value from sub-tree
		if c.dup.subTop < 0 {
			return nil, ErrNotFoundError
		}
		subPage := c.dup.subPages[c.dup.subTop]
		subIdx := int(c.dup.subIndices[c.dup.subTop])

		// Use fast path - bounds already verified during initialization
		// In sub-trees, the key IS the duplicate value
		return nodeGetKeyFast(subPage, subIdx), nil
	}

	// Get value from inline sub-page
	if c.dup.subPageIdx >= c.dup.subPageNum {
		return nil, ErrNotFoundError
	}

	if c.dup.dupfixSize > 0 {
		// DUPFIX: fixed-size values stored contiguously after 20-byte header
		start := 20 + c.dup.subPageIdx*c.dup.dupfixSize
		end := start + c.dup.dupfixSize
		if end > len(c.dup.subPageData) {
			return nil, ErrCorruptedError
		}
		// Use three-index slice to cap capacity at length
		return c.dup.subPageData[start:end:end], nil
	}

	// Variable-size sub-page: use pre-scanned node positions
	// Nodes are stored in nodePositions array in ascending sorted order
	// (index 0 = smallest value, index N-1 = largest value)

	if c.dup.nodePositions == nil || c.dup.subPageIdx >= len(c.dup.nodePositions) {
		return nil, ErrCorruptedError
	}

	nodePos := c.dup.nodePositions[c.dup.subPageIdx]

	// MDBX format: 8-byte node header (dataSize:4 + flags:1 + extra:1 + keySize:2)
	// Entry pointers point to NODE START
	if nodePos+8 > len(c.dup.subPageData) {
		return nil, ErrCorruptedError
	}
	keySize := int(uint16(c.dup.subPageData[nodePos+6]) | uint16(c.dup.subPageData[nodePos+7])<<8)
	valueStart := nodePos + 8
	valueEnd := valueStart + keySize
	// Allow keySize=0 for empty duplicate values
	if keySize >= 0 && keySize < 4096 && valueEnd <= len(c.dup.subPageData) {
		// Use three-index slice to cap capacity at length
		return c.dup.subPageData[valueStart:valueEnd:valueEnd], nil
	}

	return nil, ErrCorruptedError
}

// getFirstSubTreeValue gets the first value from a DUPSORT sub-tree.
// Optimized for read-only transactions with direct mmap access.
func (c *Cursor) getFirstSubTreeValue(treeData []byte) ([]byte, error) {
	// Parse Tree structure - height at offset 2-3, root at offset 8-11
	treePtr := unsafe.Pointer(&treeData[0])
	height := int(*(*uint16)(unsafe.Add(treePtr, 2)))
	subRoot := pgno(*(*uint32)(unsafe.Add(treePtr, 8)))

	if subRoot == invalidPgno {
		return nil, ErrNotFoundError
	}

	// Fast path for read-only transactions: direct mmap access
	if c.readOnly && c.mmapData != nil {
		pageSize := uint64(c.pageSize)
		offset := uint64(subRoot) * pageSize
		pagePtr := unsafe.Pointer(&c.mmapData[offset])

		// Descend height-1 levels using unsafe pointer arithmetic
		for level := 1; level < height; level++ {
			// Entry 0 offset at HeaderSize (20) - read uint16 little-endian
			storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20))
			nodeOffset := uintptr(storedOffset + 20)
			// Child pgno is first 4 bytes of node
			childPgno := uint64(*(*uint32)(unsafe.Add(pagePtr, nodeOffset)))
			offset = childPgno * pageSize
			pagePtr = unsafe.Pointer(&c.mmapData[offset])
		}

		// Get first key from leaf - for DUPSORT sub-trees, the "key" is the value
		storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20))
		nodeOffset := uintptr(storedOffset + 20)
		keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
		keyStart := unsafe.Add(pagePtr, nodeOffset+8)
		return unsafe.Slice((*byte)(keyStart), keySize), nil
	}

	// Fallback for write transactions: use fillPageHotPath to handle dirty pages
	subPage := c.txn.fillPageHotPath(subRoot, &c.dup.subPagesBuf[0])
	pageData := subPage.Data
	pagePtr := unsafe.Pointer(&pageData[0])

	// Descend height-1 levels using unsafe pointer arithmetic
	for level := 1; level < height; level++ {
		// Entry 0 offset at HeaderSize (20) - read uint16 little-endian
		storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20))
		nodeOffset := uintptr(storedOffset + 20)
		// Child pgno is first 4 bytes of node
		childPgno := pgno(*(*uint32)(unsafe.Add(pagePtr, nodeOffset)))
		subPage = c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[0])
		pageData = subPage.Data
		pagePtr = unsafe.Pointer(&pageData[0])
	}

	// Get first key from leaf - for DUPSORT sub-trees, the "key" is the value
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20))
	nodeOffset := uintptr(storedOffset + 20)
	// Key size is at node+6 (2 bytes)
	keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
	// Key data starts at node+8
	keyStart := unsafe.Add(pagePtr, nodeOffset+8)
	return unsafe.Slice((*byte)(keyStart), keySize), nil
}

// getFirstSubPageValue gets the first value from an inline DUPSORT sub-page.
func (c *Cursor) getFirstSubPageValue(subPageData []byte) ([]byte, error) {
	if len(subPageData) < pageHeaderSize {
		return nil, ErrCorruptedError
	}

	// Sub-page header is 20 bytes (same as regular page)
	dupfixKsize := int(uint16(subPageData[8]) | uint16(subPageData[9])<<8)
	flags := uint16(subPageData[10]) | uint16(subPageData[11])<<8
	lower := int(uint16(subPageData[12]) | uint16(subPageData[13])<<8)

	// Check for DUPFIX (fixed-size values)
	if (flags&uint16(pageDupfix) != 0) && dupfixKsize > 0 && dupfixKsize < 65535 {
		// DUPFIX: values are stored directly after 20-byte header
		end := pageHeaderSize + dupfixKsize
		if len(subPageData) >= end {
			// Use three-index slice to cap capacity at length
			return subPageData[pageHeaderSize:end:end], nil
		}
		return nil, ErrCorruptedError
	}

	// Variable-size values: parse sub-page entries
	numEntries := lower / 2
	if numEntries == 0 || lower <= 0 {
		return nil, ErrNotFoundError
	}

	// Entry pointers start at offset 20
	if len(subPageData) < pageHeaderSize+2 {
		return nil, ErrCorruptedError
	}
	// Get first entry's stored offset
	storedOffset := int(uint16(subPageData[pageHeaderSize]) | uint16(subPageData[pageHeaderSize+1])<<8)
	// Actual node position = storedOffset + pageHeaderSize
	nodePos := storedOffset + pageHeaderSize

	// Read the node (nodeSize-byte header: dataSize:4 + flags:1 + extra:1 + keySize:2)
	if nodePos+nodeSize > len(subPageData) {
		return nil, ErrCorruptedError
	}
	keySize := int(uint16(subPageData[nodePos+6]) | uint16(subPageData[nodePos+7])<<8)
	valueStart := nodePos + nodeSize
	valueEnd := valueStart + keySize
	// Allow keySize=0 for empty duplicate values
	if keySize >= 0 && valueEnd <= len(subPageData) {
		// Use three-index slice to cap capacity at length
		return subPageData[valueStart:valueEnd:valueEnd], nil
	}

	return nil, ErrCorruptedError
}

// set positions at the specified key.
func (c *Cursor) set(key []byte) ([]byte, []byte, error) {
	return c.search(key, false)
}

// setKey positions at the key, returning both key and value.
func (c *Cursor) setKey(key []byte) ([]byte, []byte, error) {
	return c.search(key, false)
}

// setRange positions at the first key >= specified.
func (c *Cursor) setRange(key []byte) ([]byte, []byte, error) {
	return c.search(key, true)
}

// search searches for a key in the tree.
func (c *Cursor) search(key []byte, greaterOrEqual bool) ([]byte, []byte, error) {
	c.reset()

	if c.tree.isEmpty() {
		c.state = cursorEOF
		return nil, nil, ErrNotFoundError
	}

	// Start at root using embedded buffer (no allocation)
	c.top = 0
	p := c.txn.fillPageHotPath(c.tree.Root, &c.pagesBuf[0])
	c.pages[0] = p
	// Set numExpected for the root page (not done via pushPageByPgno)
	c.numExpected[0] = uint16(p.numEntriesFast())

	// Search down the tree
	for {
		idx := c.searchPage(p, key)

		if p.isLeafFast() {
			c.indices[c.top] = uint16(idx)

			if idx >= p.numEntriesFast() {
				// Key is larger than all keys on page
				if greaterOrEqual {
					// Try next page via parent
					// Set state to pointing so moveNext can try to advance
					c.state = cursorPointing
					// Position at last valid entry so moveNext works correctly
					if p.numEntriesFast() > 0 {
						c.indices[c.top] = uint16(p.numEntriesFast() - 1)
					}
					return c.moveNext()
				}
				c.state = cursorEOF
				return nil, nil, ErrNotFoundError
			}

			// Check for exact match or greater (use allocation-free method)
			foundKey := nodeGetKeyDirect(p, idx)
			cmp := c.txn.compareKeys(c.dbi, key, foundKey)

			if cmp == 0 || greaterOrEqual {
				c.state = cursorPointing
				return c.getCurrent()
			}

			c.state = cursorEOF
			return nil, nil, ErrNotFoundError
		}

		// Branch page: descend using embedded buffer
		c.indices[c.top] = uint16(idx)
		childPgno := c.getChildPgno(p, idx)

		// Prefetch child page for read-only transactions
		if c.readOnly && c.mmapData != nil {
			offset := uint64(childPgno) * uint64(c.pageSize)
			prefetchPage(c.mmapData[offset : offset+uint64(c.pageSize)])
		}

		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			return nil, nil, err
		}
		p = c.pages[c.top]
	}
}

// searchPage does binary search within a page.
func (c *Cursor) searchPage(p *page, key []byte) int {
	n := p.numEntriesFast()
	if n == 0 {
		return 0
	}

	// Use assembly-optimized path for default comparator (most common case)
	if c.txn.dbiUsesDefaultCmp[c.dbi] {
		return c.searchPageAsm(p, key, n)
	}

	// On branch pages, entry 0 has no key (it's the leftmost child pointer).
	// We search entries 1 to n-1 for the key position.
	// If key < entry[1].key, we return 0 (descend to leftmost child).
	if p.isBranchFast() {
		if n == 1 {
			return 0 // Only leftmost child
		}

		// Fast path for append-only workloads: check last entry first
		lastKey := nodeGetKeyFast(p, n-1)
		cmp := c.txn.compareKeys(c.dbi, key, lastKey)
		if cmp > 0 {
			return n - 1 // Key is greater than all separators, use rightmost child
		}
		if cmp == 0 {
			return n - 1
		}

		// Binary search entries 1 to n-1 using fast path
		low, high := 1, n-2 // Already checked n-1
		for low <= high {
			mid := (low + high) / 2
			nodeKey := nodeGetKeyFast(p, mid)
			cmp = c.txn.compareKeys(c.dbi, key, nodeKey)

			if cmp < 0 {
				high = mid - 1
			} else if cmp > 0 {
				low = mid + 1
			} else {
				return mid
			}
		}

		// low is now the insertion point:
		// - If low == 1, key < all separator keys (we exited because high became 0), use entry 0
		// - Otherwise, use entry low-1 (the last entry with key <= search key)
		return low - 1
	}

	// Leaf page: fast path for append-only workloads
	lastKey := nodeGetKeyFast(p, n-1)
	cmp := c.txn.compareKeys(c.dbi, key, lastKey)
	if cmp > 0 {
		return n // Insert after last
	}
	if cmp == 0 {
		return n - 1 // Found at last position
	}

	// Standard binary search from 0 using fast path
	low, high := 0, n-2 // Already checked n-1
	for low <= high {
		mid := (low + high) / 2
		nodeKey := nodeGetKeyFast(p, mid)
		cmp = c.txn.compareKeys(c.dbi, key, nodeKey)

		if cmp < 0 {
			high = mid - 1
		} else if cmp > 0 {
			low = mid + 1
		} else {
			return mid
		}
	}

	return low
}

// searchPageAsm is the assembly-optimized version of searchPage for default comparator.
// Uses specialized assembly for 8-byte keys (common case) or per-comparison assembly for others.
func (c *Cursor) searchPageAsm(p *page, key []byte, n int) int {
	// Fast path for 8-byte keys: do entire binary search in assembly
	if len(key) == 8 {
		// Convert key to big-endian uint64 for assembly comparison
		key64 := binary.BigEndian.Uint64(key)

		if p.isBranchFast() {
			result := binarySearchBranch8(p.Data, key64, n)
			if result >= 0 {
				return result
			}
			// Fallback if assembly returned -1 (non-8-byte key found in page)
		} else {
			result := binarySearchLeaf8(p.Data, key64, n)
			if result >= 0 {
				return result
			}
		}
	}

	// On branch pages, entry 0 has no key (it's the leftmost child pointer).
	if p.isBranchFast() {
		if n == 1 {
			return 0 // Only leftmost child
		}

		// Fast path for append-only workloads: check last entry first
		cmp := getKeyAndCompareAsm(p.Data, n-1, key)
		if cmp > 0 {
			return n - 1 // Key is greater than all separators, use rightmost child
		}
		if cmp == 0 {
			return n - 1
		}

		// Binary search entries 1 to n-1 using assembly-optimized comparison
		low, high := 1, n-2 // Already checked n-1
		for low <= high {
			mid := (low + high) / 2
			cmp = getKeyAndCompareAsm(p.Data, mid, key)

			if cmp < 0 {
				high = mid - 1
			} else if cmp > 0 {
				low = mid + 1
			} else {
				return mid
			}
		}

		return low - 1
	}

	// Leaf page: fast path for append-only workloads
	cmp := getKeyAndCompareAsm(p.Data, n-1, key)
	if cmp > 0 {
		return n // Insert after last
	}
	if cmp == 0 {
		return n - 1 // Found at last position
	}

	// Standard binary search from 0 using assembly-optimized comparison
	low, high := 0, n-2 // Already checked n-1
	for low <= high {
		mid := (low + high) / 2
		cmp = getKeyAndCompareAsm(p.Data, mid, key)

		if cmp < 0 {
			high = mid - 1
		} else if cmp > 0 {
			low = mid + 1
		} else {
			return mid
		}
	}

	return low
}

// getChildPgno returns the child page number for a branch page entry (allocation-free).
// Uses fast unchecked version since caller has already verified bounds.
func (c *Cursor) getChildPgno(p *page, idx int) pgno {
	return nodeGetChildPgnoFast(p, idx)
}

// descendLeft descends to the leftmost leaf from current position.
// Uses tree height to avoid IsLeafFast check in the loop.
func (c *Cursor) descendLeft() ([]byte, []byte, error) {
	// Calculate levels to descend: tree height - current level - 1
	levelsToDescend := int(c.tree.Height) - int(c.top) - 1

	for i := 0; i < levelsToDescend; i++ {
		p := c.pages[c.top]
		childPgno := nodeGetChildPgnoUnchecked(p.Data, int(c.indices[c.top]))
		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			return nil, nil, err
		}
	}

	return c.getCurrent()
}

// descendLeftFast descends to the leftmost leaf and returns first value without full dup init.
// Uses tree height to avoid IsLeafFast check in the loop.
func (c *Cursor) descendLeftFast() ([]byte, []byte, error) {
	// Calculate levels to descend: tree height - current level - 1
	// c.top is 0-indexed (root is at c.top=0), height is 1-indexed (leaf-only tree has height=1)
	levelsToDescend := int(c.tree.Height) - int(c.top) - 1

	for i := 0; i < levelsToDescend; i++ {
		p := c.pages[c.top]
		childPgno := nodeGetChildPgnoUnchecked(p.Data, int(c.indices[c.top]))
		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			return nil, nil, err
		}
	}

	return c.getFirstValueFast()
}

// descendRight descends to the rightmost leaf from current position.
// Uses tree height to avoid IsLeafFast check in the loop.
func (c *Cursor) descendRight() ([]byte, []byte, error) {
	// Calculate levels to descend: tree height - current level - 1
	levelsToDescend := int(c.tree.Height) - int(c.top) - 1

	// Fast path for read-only transactions: direct mmap access
	if c.readOnly && c.mmapData != nil {
		pageSize := uint64(c.pageSize)
		for i := 0; i < levelsToDescend; i++ {
			p := c.pages[c.top]
			childPgno := nodeGetChildPgnoUnchecked(p.Data, int(c.indices[c.top]))
			c.top++
			offset := uint64(childPgno) * pageSize
			c.pagesBuf[c.top].Data = c.mmapData[offset : offset+pageSize]
			c.pages[c.top] = &c.pagesBuf[c.top]
			lastIdx := uint16(c.pages[c.top].numEntriesFast() - 1)
			c.indices[c.top] = lastIdx
		}
	} else {
		// Fallback for write transactions
		for i := 0; i < levelsToDescend; i++ {
			p := c.pages[c.top]
			childPgno := nodeGetChildPgnoUnchecked(p.Data, int(c.indices[c.top]))
			c.top++
			childPage := c.txn.fillPageHotPath(childPgno, &c.pagesBuf[c.top])
			c.pages[c.top] = childPage
			lastIdx := uint16(childPage.numEntriesFast() - 1)
			c.indices[c.top] = lastIdx
		}
	}

	return c.getCurrent()
}

// firstDup positions at the first duplicate of the current key
// Initializes dup state so subsequent Next() calls iterate through duplicates correctly.
func (c *Cursor) firstDup() ([]byte, []byte, error) {
	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// Fast path: if dup state is initialized and we're already at first,
	// just return the current value without re-descending
	if c.dup.initialized && c.dup.atFirst {
		return c.getCurrent()
	}

	// Use pure unsafe pointer operations to eliminate all bounds checking
	p := c.pages[c.top]
	idx := c.indices[c.top]
	d := p.Data

	// Get entry offset using unsafe
	pagePtr := unsafe.Pointer(&d[0])
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20+int(idx)*2))
	nodePtr := unsafe.Add(pagePtr, uintptr(storedOffset)+20)

	// Read node header
	dataSize := *(*uint32)(nodePtr)
	nodeFlags := nodeFlags(*(*uint8)(unsafe.Add(nodePtr, 4)))
	keySize := int(*(*uint16)(unsafe.Add(nodePtr, 6)))

	// Get key and data using unsafe.Slice
	keyStart := unsafe.Add(nodePtr, 8)
	key := unsafe.Slice((*byte)(keyStart), keySize)
	dataStart := unsafe.Add(keyStart, uintptr(keySize))
	data := unsafe.Slice((*byte)(dataStart), int(dataSize))

	// Handle DUPSORT
	if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
		if nodeFlags&nodeTree != 0 && dataSize >= 48 {
			// Sub-tree: initialize full dup state for subsequent navigation
			if err := c.initDupSubTree(data); err != nil {
				return nil, nil, err
			}
			return c.getCurrent()
		}
		if nodeFlags&nodeDup != 0 && dataSize >= 20 {
			// Sub-page: initialize full dup state for subsequent navigation
			if err := c.initDupSubPage(data); err != nil {
				return nil, nil, err
			}
			return c.getCurrent()
		}
	}

	// Not a DUPSORT node or single value - clear dup state
	c.clearDupState()
	return key, data, nil
}

// lastDup positions at the last duplicate of the current key
// Uses fast direct value access. PrevDup will lazily initialize dup state if needed.
func (c *Cursor) lastDup() ([]byte, []byte, error) {
	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// Fast path: if dup state is initialized and we're already at last,
	// just return the current value without re-descending
	if c.dup.initialized && c.dup.atLast {
		return c.getCurrent()
	}

	// Use pure unsafe pointer operations to eliminate all bounds checking
	p := c.pages[c.top]
	idx := c.indices[c.top]
	d := p.Data

	// Get entry offset using unsafe
	pagePtr := unsafe.Pointer(&d[0])
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20+int(idx)*2))
	nodePtr := unsafe.Add(pagePtr, uintptr(storedOffset)+20)

	// Read node header
	dataSize := *(*uint32)(nodePtr)
	nodeFlags := nodeFlags(*(*uint8)(unsafe.Add(nodePtr, 4)))
	keySize := int(*(*uint16)(unsafe.Add(nodePtr, 6)))

	// Get key and data using unsafe.Slice
	keyStart := unsafe.Add(nodePtr, 8)
	key := unsafe.Slice((*byte)(keyStart), keySize)
	dataStart := unsafe.Add(keyStart, uintptr(keySize))
	data := unsafe.Slice((*byte)(dataStart), int(dataSize))

	// Handle DUPSORT
	if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
		if nodeFlags&nodeTree != 0 && dataSize >= 48 {
			// Sub-tree: initialize full dup state at last position
			if err := c.initDupSubTreeLast(data); err != nil {
				return nil, nil, err
			}
			return c.getCurrent()
		}
		if nodeFlags&nodeDup != 0 && dataSize >= 20 {
			// Sub-page: initialize full dup state at last position
			if err := c.initDupSubPageLast(data); err != nil {
				return nil, nil, err
			}
			return c.getCurrent()
		}
	}

	// Not a DUPSORT node or single value - clear dup state
	c.clearDupState()
	return key, data, nil
}

// nextDup moves to the next duplicate of the current key
func (c *Cursor) nextDup() ([]byte, []byte, error) {
	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// After delete, cursor is already at the "next" position - just return current
	if c.afterDelete {
		c.afterDelete = false
		return c.getCurrent()
	}

	// If positioned at last dup (from lastDup), there's no next
	if c.dup.atLast && !c.dup.initialized {
		return nil, nil, ErrNotFoundError
	}

	// Ensure dup state is initialized
	if !c.dup.initialized {
		_, _, err := c.getCurrent()
		if err != nil {
			return nil, nil, err
		}
	}

	if !c.dup.initialized {
		// Not a DUPSORT node, no more duplicates
		return nil, nil, ErrNotFoundError
	}

	// Move to next duplicate
	if c.dup.isSubTree {
		return c.dupSubTreeNext()
	}

	// Inline sub-page - clear position flags
	c.dup.atFirst = false
	c.dup.atLast = false
	c.dup.subPageIdx++
	if c.dup.subPageIdx >= c.dup.subPageNum {
		c.dup.subPageIdx = c.dup.subPageNum - 1
		return nil, nil, ErrNotFoundError
	}

	return c.getCurrentWithDup()
}

// prevDup moves to the previous duplicate of the current key
func (c *Cursor) prevDup() ([]byte, []byte, error) {
	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// After delete, cursor is already at the correct position - just return current
	if c.afterDelete {
		c.afterDelete = false
		return c.getCurrent()
	}

	// Ensure dup state is initialized
	if !c.dup.initialized {
		// If atLast flag is set (from lastDup), initialize at last position
		if c.dup.atLast {
			if err := c.initDupStateAtLast(); err != nil {
				return nil, nil, err
			}
		} else {
			// Initialize at first position (default)
			_, _, err := c.getCurrent()
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if !c.dup.initialized {
		// Not a DUPSORT node, no previous duplicates
		return nil, nil, ErrNotFoundError
	}

	// Move to previous duplicate
	if c.dup.isSubTree {
		return c.dupSubTreePrev()
	}

	// Inline sub-page - check bounds first
	if c.dup.subPageIdx <= 0 {
		return nil, nil, ErrNotFoundError
	}
	// Clear position flags since we're navigating
	c.dup.atFirst = false
	c.dup.atLast = false
	c.dup.subPageIdx--

	return c.getCurrentWithDup()
}

// dupSubTreeLast positions at the last entry in the sub-tree
func (c *Cursor) dupSubTreeLast() error {
	if c.dup.subTree.Root == invalidPgno {
		return ErrNotFoundError
	}

	// Re-navigate to last leaf in sub-tree using embedded page buffers
	c.dup.subTop = -1
	c.dup.subTop++
	subPage := c.txn.fillPageHotPath(c.dup.subTree.Root, &c.dup.subPagesBuf[c.dup.subTop])
	c.dup.subPages[c.dup.subTop] = subPage
	lastIdx := uint16(subPage.numEntriesFast() - 1)
	c.dup.subIndices[c.dup.subTop] = lastIdx

	for subPage.isBranchFast() {
		childPgno := c.getChildPgno(subPage, int(lastIdx))
		c.dup.subTop++
		subPage = c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[c.dup.subTop])
		c.dup.subPages[c.dup.subTop] = subPage
		lastIdx = uint16(subPage.numEntriesFast() - 1)
		c.dup.subIndices[c.dup.subTop] = lastIdx
	}

	c.dup.atFirst = false
	c.dup.atLast = true
	return nil
}

// initDupStateAtLast initializes dup state and positions at the last entry.
// Used by prevDup when lastDup was called without initializing dup state.
func (c *Cursor) initDupStateAtLast() error {
	p := c.pages[c.top]
	idx := int(c.indices[c.top])

	flags := nodeGetFlagsFast(p, idx)
	data := nodeGetDataFast(p, idx)

	// Handle N_TREE (sub-tree)
	if flags&nodeTree != 0 && len(data) >= 48 {
		if err := c.initDupSubTree(data); err != nil {
			return err
		}
		return c.dupSubTreeLast()
	}

	// Handle N_DUP (inline sub-page)
	if flags&nodeDup != 0 && len(data) >= 16 {
		if err := c.initDupSubPage(data); err != nil {
			return err
		}
		c.dup.subPageIdx = c.dup.subPageNum - 1
		c.dup.atFirst = false
		c.dup.atLast = true
		return nil
	}

	// Single value - already at last (and first)
	return nil
}

// dupSubTreeNext moves to the next entry in the sub-tree
func (c *Cursor) dupSubTreeNext() ([]byte, []byte, error) {
	if c.dup.subTop < 0 {
		return nil, nil, ErrNotFoundError
	}

	// Clear position flags since we're navigating
	c.dup.atFirst = false
	c.dup.atLast = false

	// Try to move within current page
	for c.dup.subTop >= 0 {
		p := c.dup.subPages[c.dup.subTop]
		idx := c.dup.subIndices[c.dup.subTop]

		if int(idx)+1 < p.numEntriesFast() {
			c.dup.subIndices[c.dup.subTop] = idx + 1

			// If branch, descend to leftmost leaf
			if p.isBranchFast() {
				return c.dupSubTreeDescendLeft()
			}

			return c.getCurrentWithDup()
		}

		// Go up
		c.dup.subPages[c.dup.subTop] = nil
		c.dup.subTop--
	}

	return nil, nil, ErrNotFoundError
}

// dupSubTreePrev moves to the previous entry in the sub-tree
func (c *Cursor) dupSubTreePrev() ([]byte, []byte, error) {
	if c.dup.subTop < 0 {
		return nil, nil, ErrNotFoundError
	}

	// Clear position flags since we're navigating
	c.dup.atFirst = false
	c.dup.atLast = false

	// Try to move within current page
	for c.dup.subTop >= 0 {
		idx := c.dup.subIndices[c.dup.subTop]

		if idx > 0 {
			c.dup.subIndices[c.dup.subTop] = idx - 1
			p := c.dup.subPages[c.dup.subTop]

			// If branch, descend to rightmost leaf
			if p.isBranchFast() {
				return c.dupSubTreeDescendRight()
			}

			return c.getCurrentWithDup()
		}

		// Go up
		c.dup.subPages[c.dup.subTop] = nil
		c.dup.subTop--
	}

	return nil, nil, ErrNotFoundError
}

// dupSubTreeDescendLeft descends to the leftmost leaf in sub-tree
func (c *Cursor) dupSubTreeDescendLeft() ([]byte, []byte, error) {
	for {
		p := c.dup.subPages[c.dup.subTop]
		if !p.isBranchFast() {
			return c.getCurrentWithDup()
		}

		childPgno := c.getChildPgno(p, int(c.dup.subIndices[c.dup.subTop]))
		c.dup.subTop++
		childPage := c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[c.dup.subTop])
		c.dup.subPages[c.dup.subTop] = childPage
		c.dup.subIndices[c.dup.subTop] = 0
	}
}

// dupSubTreeDescendRight descends to the rightmost leaf in sub-tree
func (c *Cursor) dupSubTreeDescendRight() ([]byte, []byte, error) {
	for {
		p := c.dup.subPages[c.dup.subTop]
		if !p.isBranchFast() {
			return c.getCurrentWithDup()
		}

		childPgno := c.getChildPgno(p, int(c.dup.subIndices[c.dup.subTop]))
		c.dup.subTop++
		childPage := c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[c.dup.subTop])
		c.dup.subPages[c.dup.subTop] = childPage
		lastIdx := uint16(childPage.numEntriesFast() - 1)
		c.dup.subIndices[c.dup.subTop] = lastIdx
	}
}

// getCurrentWithDup returns the current key and duplicate value
// Uses allocation-free node access for performance.
func (c *Cursor) getCurrentWithDup() ([]byte, []byte, error) {
	if c.state != cursorPointing || c.top < 0 {
		return nil, nil, ErrNotFoundError
	}

	// Get the main key using allocation-free method
	p := c.pages[c.top]
	idx := int(c.indices[c.top])
	key := nodeGetKeyDirect(p, idx)
	if key == nil {
		return nil, nil, ErrCorruptedError
	}

	// Get the duplicate value
	value, err := c.getDupValue()
	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

// nextNoDup moves to the first value of the next key (skipping remaining duplicates).
func (c *Cursor) nextNoDup() ([]byte, []byte, error) {
	if c.state == cursorUninitialized {
		return c.first()
	}

	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// Clear dup state - we're moving to a new key
	c.clearDupState()

	// Refresh page first to detect if another cursor modified it.
	// If entries were deleted before our position, afterDelete will be set
	// and our index now points to what should be returned next.
	c.refreshPage()
	if c.afterDelete {
		c.afterDelete = false
		return c.getCurrent()
	}

	// Move to next position in main tree (skip duplicates)
	for c.top >= 0 {
		// Refresh page at start of each iteration in case of modifications
		c.refreshPage()

		// Check if refreshPage detected a deletion - if so, current position is the "next" entry
		if c.afterDelete {
			c.afterDelete = false
			// If we're at a branch level after deletion, we need to descend to get a leaf entry
			p := c.pages[c.top]
			if p.isBranchFast() {
				return c.descendLeftFast()
			}
			return c.getCurrent()
		}

		p := c.pages[c.top]
		idx := c.indices[c.top]

		// Inline NumEntriesFast
		lower := uint16(p.Data[12]) | uint16(p.Data[13])<<8
		numEntries := int(lower) >> 1

		if int(idx)+1 < numEntries {
			// Move to next entry on this page
			c.indices[c.top] = idx + 1

			// Inline IsBranchFast check
			flags := uint16(p.Data[10]) | uint16(p.Data[11])<<8
			if flags&uint16(pageBranch) != 0 {
				return c.descendLeftFast()
			}

			// Use getCurrent() to properly initialize dup state for subsequent Next() calls
			return c.getCurrent()
		}

		// No more entries on this page, go up
		c.popPage()
	}

	// Reached the end
	c.state = cursorEOF
	return nil, nil, ErrNotFoundError
}

// getFirstValueFast gets the first duplicate value without setting up full dup cursor state.
// This is an optimization for nextNoDup/prevNoDup which just need the first/last value.
// Uses unsafe pointers throughout for maximum performance.
func (c *Cursor) getFirstValueFast() ([]byte, []byte, error) {
	if c.state != cursorPointing || c.top < 0 {
		return nil, nil, ErrNotFoundError
	}

	p := c.pages[c.top]
	idx := int(c.indices[c.top])
	pageData := p.Data

	// Get entry offset using unsafe pointer arithmetic
	pagePtr := unsafe.Pointer(&pageData[0])
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20+idx*2))
	nodeOffset := uintptr(storedOffset + 20)

	// Read node header using unsafe
	nodePtr := unsafe.Add(pagePtr, nodeOffset)
	dataSize := *(*uint32)(nodePtr)
	nodeFlags := nodeFlags(*(*uint8)(unsafe.Add(nodePtr, 4)))
	keySize := int(*(*uint16)(unsafe.Add(nodePtr, 6)))

	// Get key - node header is 8 bytes
	keyStart := unsafe.Add(nodePtr, 8)
	key := unsafe.Slice((*byte)(keyStart), keySize)

	// Handle large values
	if nodeFlags&nodeBig != 0 {
		overflowPgno := nodeGetOverflowPgnoDirect(p, idx)
		data, _ := c.txn.getLargeData(overflowPgno, dataSize)
		return key, data, nil
	}

	// Get data pointer
	dataStart := unsafe.Add(keyStart, uintptr(keySize))
	data := unsafe.Slice((*byte)(dataStart), int(dataSize))

	// Handle DUPSORT - get first value directly without cursor state
	if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
		if nodeFlags&nodeTree != 0 && dataSize >= 48 {
			// Sub-tree: get first value directly
			val, err := c.getFirstSubTreeValue(data)
			if err != nil {
				return nil, nil, err
			}
			return key, val, nil
		}
		if nodeFlags&nodeDup != 0 && nodeFlags&nodeTree == 0 && dataSize >= 16 {
			// Sub-page: get first value directly
			val, err := c.getFirstSubPageValue(data)
			if err != nil {
				return nil, nil, err
			}
			return key, val, nil
		}
	}

	return key, data, nil
}

// getLastValueFast gets the last duplicate value without setting up full dup cursor state.
// This is an optimization for prevNoDup which needs the last value of the previous key.
// Uses unsafe pointers throughout for maximum performance.
func (c *Cursor) getLastValueFast() ([]byte, []byte, error) {
	if c.state != cursorPointing || c.top < 0 {
		return nil, nil, ErrNotFoundError
	}

	p := c.pages[c.top]
	idx := int(c.indices[c.top])
	pageData := p.Data

	// Get entry offset using unsafe pointer arithmetic
	pagePtr := unsafe.Pointer(&pageData[0])
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20+idx*2))
	nodeOffset := uintptr(storedOffset + 20)

	// Read node header using unsafe
	nodePtr := unsafe.Add(pagePtr, nodeOffset)
	dataSize := *(*uint32)(nodePtr)
	nodeFlags := nodeFlags(*(*uint8)(unsafe.Add(nodePtr, 4)))
	keySize := int(*(*uint16)(unsafe.Add(nodePtr, 6)))

	// Get key - node header is 8 bytes
	keyStart := unsafe.Add(nodePtr, 8)
	key := unsafe.Slice((*byte)(keyStart), keySize)

	// Handle large values
	if nodeFlags&nodeBig != 0 {
		overflowPgno := nodeGetOverflowPgnoDirect(p, idx)
		data, _ := c.txn.getLargeData(overflowPgno, dataSize)
		return key, data, nil
	}

	// Get data pointer
	dataStart := unsafe.Add(keyStart, uintptr(keySize))
	data := unsafe.Slice((*byte)(dataStart), int(dataSize))

	// Handle DUPSORT - get last value directly without cursor state
	if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
		if nodeFlags&nodeTree != 0 && dataSize >= 48 {
			// Sub-tree: get last value directly
			val, err := c.getLastSubTreeValue(data)
			if err != nil {
				return nil, nil, err
			}
			return key, val, nil
		}
		if nodeFlags&nodeDup != 0 && nodeFlags&nodeTree == 0 && dataSize >= 20 {
			// Sub-page: get last value directly
			val, err := c.getLastSubPageValue(data)
			if err != nil {
				return nil, nil, err
			}
			return key, val, nil
		}
	}

	return key, data, nil
}

// getLastSubTreeValue gets the last value from a DUPSORT sub-tree.
// Uses unsafe pointers for maximum performance - no bounds checking.
func (c *Cursor) getLastSubTreeValue(treeData []byte) ([]byte, error) {
	// Parse Tree structure using unsafe
	treePtr := unsafe.Pointer(&treeData[0])
	height := int(*(*uint16)(unsafe.Add(treePtr, 2)))
	subRoot := pgno(*(*uint32)(unsafe.Add(treePtr, 8)))

	if subRoot == invalidPgno {
		return nil, ErrNotFoundError
	}

	// Fast path for read-only transactions: direct mmap access
	if c.readOnly && c.mmapData != nil {
		pageSize := uint64(c.pageSize)
		offset := uint64(subRoot) * pageSize
		pagePtr := unsafe.Pointer(&c.mmapData[offset])

		// Descend height-1 levels to rightmost child using unsafe
		for level := 1; level < height; level++ {
			// Get numEntries from lower field (offset 12)
			lower := *(*uint16)(unsafe.Add(pagePtr, 12))
			numEntries := int(lower) >> 1
			lastIdx := numEntries - 1
			// Get last entry offset
			storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+lastIdx*2)))
			nodeOffset := uintptr(storedOffset + 20)
			// Child pgno is first 4 bytes of node
			childPgno := uint64(*(*uint32)(unsafe.Add(pagePtr, nodeOffset)))
			offset = childPgno * pageSize
			pagePtr = unsafe.Pointer(&c.mmapData[offset])
		}

		// Get last key from leaf
		lower := *(*uint16)(unsafe.Add(pagePtr, 12))
		numEntries := int(lower) >> 1
		lastIdx := numEntries - 1
		storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+lastIdx*2)))
		nodeOffset := uintptr(storedOffset + 20)
		keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
		keyStart := unsafe.Add(pagePtr, nodeOffset+8)
		return unsafe.Slice((*byte)(keyStart), keySize), nil
	}

	// Fallback for write transactions: use fillPageHotPath to handle dirty pages
	subPage := c.txn.fillPageHotPath(subRoot, &c.dup.subPagesBuf[0])
	pageData := subPage.Data
	pagePtr := unsafe.Pointer(&pageData[0])

	// Descend height-1 levels to rightmost child using unsafe
	for level := 1; level < height; level++ {
		// Get numEntries from lower field (offset 12)
		lower := *(*uint16)(unsafe.Add(pagePtr, 12))
		numEntries := int(lower) >> 1
		lastIdx := numEntries - 1
		// Get last entry offset
		storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+lastIdx*2)))
		nodeOffset := uintptr(storedOffset + 20)
		// Child pgno is first 4 bytes of node
		childPgno := pgno(*(*uint32)(unsafe.Add(pagePtr, nodeOffset)))
		subPage = c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[0])
		pageData = subPage.Data
		pagePtr = unsafe.Pointer(&pageData[0])
	}

	// Get last key from leaf
	lower := *(*uint16)(unsafe.Add(pagePtr, 12))
	numEntries := int(lower) >> 1
	lastIdx := numEntries - 1
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+lastIdx*2)))
	nodeOffset := uintptr(storedOffset + 20)
	// Key size is at node+6 (2 bytes)
	keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
	// Key data starts at node+8
	keyStart := unsafe.Add(pagePtr, nodeOffset+8)
	return unsafe.Slice((*byte)(keyStart), keySize), nil
}

// getLastSubPageValue gets the last value from an inline DUPSORT sub-page.
func (c *Cursor) getLastSubPageValue(subPageData []byte) ([]byte, error) {
	if len(subPageData) < pageHeaderSize {
		return nil, ErrCorruptedError
	}

	dupfixKsize := int(uint16(subPageData[8]) | uint16(subPageData[9])<<8)
	flags := uint16(subPageData[10]) | uint16(subPageData[11])<<8
	lower := int(uint16(subPageData[12]) | uint16(subPageData[13])<<8)

	// Check for DUPFIX (fixed-size values)
	if (flags&uint16(pageDupfix) != 0) && dupfixKsize > 0 && dupfixKsize < 65535 {
		numEntries := (len(subPageData) - pageHeaderSize) / dupfixKsize
		if numEntries == 0 {
			return nil, ErrNotFoundError
		}
		// Get last value
		start := pageHeaderSize + (numEntries-1)*dupfixKsize
		end := start + dupfixKsize
		if end <= len(subPageData) {
			// Use three-index slice to cap capacity at length
			return subPageData[start:end:end], nil
		}
		return nil, ErrCorruptedError
	}

	// Variable-size values
	numEntries := lower / 2
	if numEntries == 0 || lower <= 0 {
		return nil, ErrNotFoundError
	}

	// Get last entry's stored offset
	lastEntryPtrPos := pageHeaderSize + (numEntries-1)*2
	if lastEntryPtrPos+2 > len(subPageData) {
		return nil, ErrCorruptedError
	}
	storedOffset := int(uint16(subPageData[lastEntryPtrPos]) | uint16(subPageData[lastEntryPtrPos+1])<<8)
	nodePos := storedOffset + pageHeaderSize

	// Read the node
	if nodePos+nodeSize > len(subPageData) {
		return nil, ErrCorruptedError
	}
	keySize := int(uint16(subPageData[nodePos+6]) | uint16(subPageData[nodePos+7])<<8)
	valueStart := nodePos + nodeSize
	valueEnd := valueStart + keySize
	// Allow keySize=0 for empty duplicate values
	if keySize >= 0 && valueEnd <= len(subPageData) {
		// Use three-index slice to cap capacity at length
		return subPageData[valueStart:valueEnd:valueEnd], nil
	}

	return nil, ErrCorruptedError
}

// descendRightFast descends to the rightmost leaf and returns last value without full dup init.
func (c *Cursor) descendRightFast() ([]byte, []byte, error) {
	// Fast path for read-only transactions: direct mmap access
	if c.readOnly && c.mmapData != nil {
		pageSize := uint64(c.pageSize)
		for {
			p := c.pages[c.top]
			if p.isLeafFast() {
				return c.getLastValueFast()
			}

			childPgno := c.getChildPgno(p, int(c.indices[c.top]))
			c.top++
			offset := uint64(childPgno) * pageSize
			c.pagesBuf[c.top].Data = c.mmapData[offset : offset+pageSize]
			c.pages[c.top] = &c.pagesBuf[c.top]
			lastIdx := uint16(c.pages[c.top].numEntriesFast() - 1)
			c.indices[c.top] = lastIdx
		}
	}

	// Fallback for write transactions
	for {
		p := c.pages[c.top]
		if p.isLeafFast() {
			return c.getLastValueFast()
		}

		childPgno := c.getChildPgno(p, int(c.indices[c.top]))
		c.top++
		childPage := c.txn.fillPageHotPath(childPgno, &c.pagesBuf[c.top])
		c.pages[c.top] = childPage
		lastIdx := uint16(childPage.numEntriesFast() - 1)
		c.indices[c.top] = lastIdx
	}
}

// prevNoDup moves to the last value of the previous key (skipping remaining duplicates).
func (c *Cursor) prevNoDup() ([]byte, []byte, error) {
	if c.state == cursorUninitialized {
		return c.last()
	}

	if c.state != cursorPointing {
		return nil, nil, ErrNotFoundError
	}

	// Clear dup state - we're moving to a new key
	c.clearDupState()

	// Refresh page first to detect if another cursor modified it.
	// For Prev, if entries were deleted, we handle it differently than Next.
	c.refreshPage()

	// Move to previous position in main tree (skip duplicates)
	for c.top >= 0 {
		// Refresh page at start of each iteration in case of modifications
		c.refreshPage()
		p := c.pages[c.top]
		idx := c.indices[c.top]

		if idx > 0 {
			// Move to previous entry on this page
			c.indices[c.top] = idx - 1

			// If branch, descend to rightmost leaf (fast path)
			if p.isBranchFast() {
				k, v, err := c.descendRightFast()
				if err != nil {
					return k, v, err
				}
				// For DUPSORT, position at last duplicate
				if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
					return c.lastDup()
				}
				return k, v, nil
			}

			// For DUPSORT, position at last duplicate
			if c.tree != nil && c.tree.Flags&uint16(DupSort) != 0 {
				return c.lastDup()
			}
			return c.getCurrent()
		}

		// No more entries on this page, go up
		c.popPage()
	}

	// Reached the beginning
	c.state = cursorEOF
	return nil, nil, ErrNotFoundError
}

// getBoth positions at the exact key-value pair (DUPSORT databases).
// Returns ErrNotFound if the exact pair doesn't exist.
func (c *Cursor) getBoth(key, value []byte) ([]byte, []byte, error) {
	// First, find the key using optimized search that skips dup init
	foundKey, err := c.setNoGetCurrent(key)
	if err != nil {
		return nil, nil, err
	}

	// For non-DUPSORT, get the value and compare
	if c.tree.Flags&uint16(DupSort) == 0 {
		p := c.pages[c.top]
		idx := int(c.indices[c.top])
		foundVal := nodeGetDataFast(p, idx)
		if c.txn.compareDupValues(c.dbi, foundVal, value) == 0 {
			return foundKey, foundVal, nil
		}
		return nil, nil, ErrNotFoundError
	}

	// For DUPSORT, search directly without full dup init
	return c.searchDupValueDirect(foundKey, value, true)
}

// getBothRange positions at the key with the first value >= specified (DUPSORT databases).
func (c *Cursor) getBothRange(key, value []byte) ([]byte, []byte, error) {
	// First, find the key using optimized search that skips dup init
	foundKey, err := c.setNoGetCurrent(key)
	if err != nil {
		return nil, nil, err
	}

	// For non-DUPSORT, get the value and return
	if c.tree.Flags&uint16(DupSort) == 0 {
		p := c.pages[c.top]
		idx := int(c.indices[c.top])
		foundVal := nodeGetDataFast(p, idx)
		return foundKey, foundVal, nil
	}

	// For DUPSORT, search directly without full dup init
	return c.searchDupValueDirect(foundKey, value, false)
}

// setNoGetCurrent positions at the exact key without calling getCurrent.
// Returns the key found. Used by getBoth to avoid redundant dup initialization.
func (c *Cursor) setNoGetCurrent(key []byte) ([]byte, error) {
	if c.tree == nil || c.tree.Root == invalidPgno {
		c.state = cursorEOF
		return nil, ErrNotFoundError
	}

	c.clearDupState()

	// Use embedded page buffer for root (no allocation)
	c.top = 0
	rootPage := c.txn.fillPageHotPath(c.tree.Root, &c.pagesBuf[0])
	c.pages[0] = rootPage
	c.state = cursorPointing

	// Navigate through tree using tree height (avoid IsBranchFast check in loop)
	height := int(c.tree.Height)
	for level := 1; level < height; level++ {
		p := c.pages[c.top]
		idx := c.searchPage(p, key)
		c.indices[c.top] = uint16(idx)

		childPgno := nodeGetChildPgnoFast(p, idx)
		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			return nil, err
		}
	}

	// Binary search in leaf
	p := c.pages[c.top]
	idx := c.searchPage(p, key)
	n := p.numEntriesFast()

	if idx >= n {
		c.state = cursorEOF
		return nil, ErrNotFoundError
	}

	c.indices[c.top] = uint16(idx)
	foundKey := nodeGetKeyFast(p, idx)
	if c.txn.compareKeys(c.dbi, foundKey, key) != 0 {
		c.state = cursorEOF
		return nil, ErrNotFoundError
	}

	return foundKey, nil
}

// searchDupValueDirect searches for a value within duplicates without full dup init.
// This is optimized for GetBoth/GetBothRange where we don't need to navigate to first leaf.
func (c *Cursor) searchDupValueDirect(key []byte, value []byte, exact bool) ([]byte, []byte, error) {
	p := c.pages[c.top]
	idx := int(c.indices[c.top])

	flags := nodeGetFlagsFast(p, idx)
	data := nodeGetDataFast(p, idx)

	// Handle N_TREE (sub-tree)
	if flags&nodeTree != 0 && len(data) >= 48 {
		return c.searchDupSubTreeDirect(key, data, value, exact)
	}

	// Handle N_DUP (inline sub-page)
	if flags&nodeDup != 0 && len(data) >= 16 {
		return c.searchDupSubPageDirect(key, data, value, exact)
	}

	// Single value - compare directly
	cmp := c.txn.compareDupValues(c.dbi, data, value)
	if exact {
		if cmp == 0 {
			return key, data, nil
		}
		return nil, nil, ErrNotFoundError
	}
	// For GetBothRange, return only if data >= value
	if cmp >= 0 {
		return key, data, nil
	}
	return nil, nil, ErrNotFoundError
}

// searchDupSubTreeDirect does binary search in sub-tree without initializing full dup state.
// Uses unsafe pointers for hot path to minimize overhead.
func (c *Cursor) searchDupSubTreeDirect(key []byte, treeData []byte, value []byte, exact bool) ([]byte, []byte, error) {
	// Parse tree structure using unsafe
	treePtr := unsafe.Pointer(&treeData[0])
	root := *(*uint32)(unsafe.Add(treePtr, 8))

	if root == 0xFFFFFFFF {
		return nil, nil, ErrNotFoundError
	}

	height := int(*(*uint16)(unsafe.Add(treePtr, 2)))

	// Clear and init minimal dup state - use embedded buffer to avoid allocation
	c.clearDupState()
	c.dup.isSubTree = true
	c.dup.subTree = tree{
		Flags:  *(*uint16)(treePtr),
		Height: uint16(height),
		Root:   pgno(root),
		Items:  *(*uint64)(unsafe.Add(treePtr, 32)),
	}
	c.dup.subTop = -1

	// Navigate directly to target value using binary search
	c.dup.subTop++
	// Use fillPageHotPath to handle dirty pages in write transactions
	subPage := c.txn.fillPageHotPath(pgno(root), &c.dup.subPagesBuf[c.dup.subTop])
	c.dup.subPages[c.dup.subTop] = subPage

	// Navigate to leaf using value as search key
	// Use tree height to avoid IsBranchFast check in loop
	for level := 1; level < height; level++ {
		pageData := subPage.Data
		pagePtr := unsafe.Pointer(&pageData[0])
		// Get numEntries from lower field
		lower := *(*uint16)(unsafe.Add(pagePtr, 12))
		n := int(lower) >> 1

		// On branch pages, entry 0 has no key (it's the leftmost child pointer).
		// We search entries 1 to n-1 for the key position.
		var idx int
		if n <= 1 {
			idx = 0 // Only leftmost child
		} else {
			// Binary search entries 1 to n-1
			low, high := 1, n-1
			for low <= high {
				mid := (low + high) / 2
				// Inline GetKeyFast
				storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+mid*2)))
				nodeOffset := uintptr(storedOffset + 20)
				keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
				nodeKey := unsafe.Slice((*byte)(unsafe.Add(pagePtr, nodeOffset+8)), keySize)

				cmp := c.txn.compareDupValues(c.dbi, value, nodeKey)
				if cmp < 0 {
					// For range search: if search value is a prefix of separator,
					// the separator IS >= search value, so descend to this child.
					if len(value) < keySize && bytes.Equal(value, nodeKey[:len(value)]) {
						low = mid + 1 // Treat as match, descend to child mid
						break
					}
					high = mid - 1
				} else if cmp > 0 {
					low = mid + 1
				} else {
					low = mid + 1 // Found exact match, use this child
					break
				}
			}
			// low is now the insertion point, use entry low-1
			idx = low - 1
		}

		c.dup.subIndices[c.dup.subTop] = uint16(idx)
		// Inline GetChildPgnoFast
		storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+idx*2)))
		nodeOffset := uintptr(storedOffset + 20)
		childPgno := pgno(*(*uint32)(unsafe.Add(pagePtr, nodeOffset)))

		c.dup.subTop++
		// Use fillPageHotPath to handle dirty pages in write transactions
		subPage = c.txn.fillPageHotPath(childPgno, &c.dup.subPagesBuf[c.dup.subTop])
		c.dup.subPages[c.dup.subTop] = subPage
	}

	// Binary search in leaf using unsafe
	pageData := subPage.Data
	pagePtr := unsafe.Pointer(&pageData[0])
	lower := *(*uint16)(unsafe.Add(pagePtr, 12))
	n := int(lower) >> 1

	low, high := 0, n-1
	foundIdx := n
	for low <= high {
		mid := (low + high) / 2
		// Inline GetKeyFast
		storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+mid*2)))
		nodeOffset := uintptr(storedOffset + 20)
		keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
		nodeKey := unsafe.Slice((*byte)(unsafe.Add(pagePtr, nodeOffset+8)), keySize)

		cmp := c.txn.compareDupValues(c.dbi, value, nodeKey)
		if cmp < 0 {
			high = mid - 1
			foundIdx = mid
		} else if cmp > 0 {
			low = mid + 1
		} else {
			// Exact match
			c.dup.subIndices[c.dup.subTop] = uint16(mid)
			c.dup.initialized = true
			return key, nodeKey, nil
		}
	}

	if exact {
		return nil, nil, ErrNotFoundError
	}

	// For GetBothRange, return first value >= target
	if foundIdx >= n {
		return nil, nil, ErrNotFoundError
	}
	c.dup.subIndices[c.dup.subTop] = uint16(foundIdx)
	c.dup.initialized = true
	// Get found value
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, uintptr(20+foundIdx*2)))
	nodeOffset := uintptr(storedOffset + 20)
	keySize := int(*(*uint16)(unsafe.Add(pagePtr, nodeOffset+6)))
	foundVal := unsafe.Slice((*byte)(unsafe.Add(pagePtr, nodeOffset+8)), keySize)
	return key, foundVal, nil
}

// searchDupSubPageDirect does binary search in inline sub-page without full init.
func (c *Cursor) searchDupSubPageDirect(key []byte, subPageData []byte, value []byte, exact bool) ([]byte, []byte, error) {
	// Parse sub-page header
	if len(subPageData) < pageHeaderSize {
		return nil, nil, ErrCorruptedError
	}

	flags := uint16(subPageData[10]) | uint16(subPageData[11])<<8
	lower := uint16(subPageData[12]) | uint16(subPageData[13])<<8
	numEntries := int(lower) >> 1

	if numEntries == 0 {
		return nil, nil, ErrNotFoundError
	}

	isDupfix := flags&uint16(pageDupfix) != 0

	// Binary search
	low, high := 0, numEntries-1
	foundIdx := numEntries

	for low <= high {
		mid := (low + high) / 2
		var nodeValue []byte

		if isDupfix {
			dupfixSize := int(uint16(subPageData[8]) | uint16(subPageData[9])<<8)
			if dupfixSize > 0 {
				start := pageHeaderSize + mid*dupfixSize
				end := start + dupfixSize
				if end <= len(subPageData) {
					nodeValue = subPageData[start:end]
				}
			}
		} else {
			// Variable-size: read entry offset and parse node
			entryOffset := pageHeaderSize + mid*2
			if entryOffset+2 > len(subPageData) {
				return nil, nil, ErrCorruptedError
			}
			storedOffset := uint16(subPageData[entryOffset]) | uint16(subPageData[entryOffset+1])<<8
			nodePos := int(storedOffset) + pageHeaderSize

			if nodePos+nodeSize > len(subPageData) {
				return nil, nil, ErrCorruptedError
			}
			keySize := int(uint16(subPageData[nodePos+6]) | uint16(subPageData[nodePos+7])<<8)
			valueStart := nodePos + nodeSize
			valueEnd := valueStart + keySize
			// Allow keySize=0 for empty duplicate values
			if keySize >= 0 && valueEnd <= len(subPageData) {
				nodeValue = subPageData[valueStart:valueEnd]
			}
		}

		if nodeValue == nil {
			return nil, nil, ErrCorruptedError
		}

		cmp := c.txn.compareDupValues(c.dbi, value, nodeValue)
		if cmp < 0 {
			high = mid - 1
			foundIdx = mid
		} else if cmp > 0 {
			low = mid + 1
		} else {
			// Exact match - set full dup state for subsequent navigation
			c.initDupSubPageState(subPageData, numEntries, isDupfix, mid)
			return key, nodeValue, nil
		}
	}

	if exact {
		return nil, nil, ErrNotFoundError
	}

	// For GetBothRange, return first value >= target
	if foundIdx >= numEntries {
		return nil, nil, ErrNotFoundError
	}

	// Get the value at foundIdx
	var foundVal []byte
	if isDupfix {
		dupfixSize := int(uint16(subPageData[8]) | uint16(subPageData[9])<<8)
		start := pageHeaderSize + foundIdx*dupfixSize
		end := start + dupfixSize
		if end <= len(subPageData) {
			foundVal = subPageData[start:end]
		}
	} else {
		entryOffset := pageHeaderSize + foundIdx*2
		storedOffset := uint16(subPageData[entryOffset]) | uint16(subPageData[entryOffset+1])<<8
		nodePos := int(storedOffset) + pageHeaderSize
		keySize := int(uint16(subPageData[nodePos+6]) | uint16(subPageData[nodePos+7])<<8)
		valueStart := nodePos + nodeSize
		valueEnd := valueStart + keySize
		// Allow keySize=0 for empty duplicate values
		if keySize >= 0 && valueEnd <= len(subPageData) {
			foundVal = subPageData[valueStart:valueEnd]
		}
	}

	if foundVal == nil {
		return nil, nil, ErrCorruptedError
	}

	// Set full dup state for subsequent navigation
	c.initDupSubPageState(subPageData, numEntries, isDupfix, foundIdx)
	return key, foundVal, nil
}

// initDupSubPageState initializes dup state for inline sub-page navigation.
// Called by searchDupSubPageDirect after finding a value to enable Next/Prev navigation.
func (c *Cursor) initDupSubPageState(subPageData []byte, numEntries int, isDupfix bool, idx int) {
	c.dup.initialized = true
	c.dup.isSubTree = false
	c.dup.subPageData = subPageData
	c.dup.subPageIdx = idx
	c.dup.subPageNum = numEntries

	if isDupfix {
		// DUPFIX: fixed-size values
		dupfixSize := int(uint16(subPageData[8]) | uint16(subPageData[9])<<8)
		c.dup.dupfixSize = dupfixSize
		c.dup.nodePositions = nil
	} else {
		// Variable-size: need to populate nodePositions for getDupValue
		c.dup.dupfixSize = 0

		// Use pre-allocated buffer if possible
		if numEntries <= len(c.dup.nodePositionsBuf) {
			c.dup.nodePositions = c.dup.nodePositionsBuf[:numEntries]
		} else {
			c.dup.nodePositions = make([]int, numEntries)
		}

		// Read entry pointers
		for i := range numEntries {
			entryOffset := pageHeaderSize + i*2
			storedOffset := int(uint16(subPageData[entryOffset]) | uint16(subPageData[entryOffset+1])<<8)
			c.dup.nodePositions[i] = storedOffset + pageHeaderSize
		}
	}
}

// setLowerbound positions at first key-value pair >= specified.
// For non-DUPSORT databases, this is equivalent to SetRange.
// For DUPSORT databases, positions at the exact (key,value) or the next greater pair.
func (c *Cursor) setLowerbound(key, value []byte) ([]byte, []byte, error) {
	// First, position at the key using SetRange
	foundKey, foundVal, err := c.setRange(key)
	if err != nil {
		return nil, nil, err
	}

	// For non-DUPSORT, we're done
	if c.tree.Flags&uint16(DupSort) == 0 {
		return foundKey, foundVal, nil
	}

	// Compare keys
	cmp := c.txn.compareKeys(c.dbi, foundKey, key)
	if cmp > 0 {
		// Found key is greater than requested, return it
		return foundKey, foundVal, nil
	}

	// Keys are equal - need to find value >= specified value
	if value == nil {
		return foundKey, foundVal, nil
	}

	// Search within duplicates for value >= specified
	for {
		valCmp := c.txn.compareDupValues(c.dbi, foundVal, value)
		if valCmp >= 0 {
			return foundKey, foundVal, nil
		}

		// Move to next duplicate
		foundKey, foundVal, err = c.nextDup()
		if err != nil {
			if IsNotFound(err) {
				// No more duplicates, move to next key
				return c.moveNext()
			}
			return nil, nil, err
		}
	}
}

// setUpperbound positions at first key-value pair > specified.
// For non-DUPSORT databases, positions at first key > specified.
// For DUPSORT databases, positions at the first pair strictly greater than (key,value).
func (c *Cursor) setUpperbound(key, value []byte) ([]byte, []byte, error) {
	// First, position at the key using SetRange
	foundKey, foundVal, err := c.setRange(key)
	if err != nil {
		return nil, nil, err
	}

	// For non-DUPSORT, find first key > specified
	if c.tree.Flags&uint16(DupSort) == 0 {
		cmp := c.txn.compareKeys(c.dbi, foundKey, key)
		if cmp > 0 {
			return foundKey, foundVal, nil
		}
		// Keys are equal, move to next
		return c.moveNext()
	}

	// Compare keys
	cmp := c.txn.compareKeys(c.dbi, foundKey, key)
	if cmp > 0 {
		// Found key is greater than requested, return it
		return foundKey, foundVal, nil
	}

	// Keys are equal - need to find value > specified value
	if value == nil {
		// No value specified, move to next key
		return c.nextNoDup()
	}

	// Search within duplicates for value > specified
	for {
		valCmp := c.txn.compareDupValues(c.dbi, foundVal, value)
		if valCmp > 0 {
			return foundKey, foundVal, nil
		}

		// Move to next duplicate
		foundKey, foundVal, err = c.nextDup()
		if err != nil {
			if IsNotFound(err) {
				// No more duplicates, move to next key
				return c.moveNext()
			}
			return nil, nil, err
		}
	}
}

// countDuplicates returns the number of duplicates for the current key
// Uses unsafe pointers for maximum performance
func (c *Cursor) countDuplicates() (uint64, error) {
	if c.state != cursorPointing {
		return 0, ErrNotFoundError
	}

	// Fast path: if dup state is already initialized, use it
	if c.dup.initialized {
		if c.dup.isSubTree {
			return c.dup.subTree.Items, nil
		}
		return uint64(c.dup.subPageNum), nil
	}

	// Fast path: extract count directly using unsafe
	p := c.pages[c.top]
	idx := int(c.indices[c.top])
	pageData := p.Data

	// Get entry offset and node using unsafe
	pagePtr := unsafe.Pointer(&pageData[0])
	storedOffset := *(*uint16)(unsafe.Add(pagePtr, 20+idx*2))
	nodeOffset := uintptr(storedOffset + 20)
	nodePtr := unsafe.Add(pagePtr, nodeOffset)

	// Read dataSize and flags
	dataSize := *(*uint32)(nodePtr)
	nodeFlags := nodeFlags(*(*uint8)(unsafe.Add(nodePtr, 4)))
	keySize := int(*(*uint16)(unsafe.Add(nodePtr, 6)))

	if nodeFlags&nodeTree != 0 {
		// N_TREE: parse Tree.Items directly
		if dataSize < 48 {
			return 1, nil
		}
		// Data starts at node+8+keySize, Items at offset 32
		dataPtr := unsafe.Add(nodePtr, uintptr(8+keySize))
		items := *(*uint64)(unsafe.Add(dataPtr, 32))
		return items, nil
	}

	if nodeFlags&nodeDup != 0 {
		// N_DUP: count from sub-page header
		if dataSize < 20 {
			return 1, nil
		}
		// Sub-page lower at offset 12
		dataPtr := unsafe.Add(nodePtr, uintptr(8+keySize))
		lower := *(*uint16)(unsafe.Add(dataPtr, 12))
		return uint64(lower >> 1), nil
	}

	// Not a DUPSORT node
	return 1, nil
}
func (c *Cursor) isFirst() bool { return c.indices[c.top] == 0 }
func (c *Cursor) isLast() bool {
	p := c.pages[c.top]
	return int(c.indices[c.top]) == p.numEntriesFast()-1
}

// ErrBadCursorError indicates an invalid cursor
var ErrBadCursorError = NewError(ErrBadTxn)

// ErrPermissionDenied indicates a write operation on read-only transaction
const ErrPermissionDenied ErrorCode = -1 // Will use syscall.EACCES
