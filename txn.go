package gdbx

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"sync"
	"time"
	"unsafe"
)

// Global cursor cache - avoids sync.Pool.Put allocation overhead
var (
	globalCursorCache   []*Cursor
	globalCursorCacheMu sync.Mutex
	cursorCacheCap      = 256 // Cache up to 256 cursors
)

// newCursorFromCache creates a new cursor, either from cache or freshly allocated.
func newCursorFromCache() *Cursor {
	globalCursorCacheMu.Lock()
	n := len(globalCursorCache)
	if n > 0 {
		c := globalCursorCache[n-1]
		globalCursorCache = globalCursorCache[:n-1]
		globalCursorCacheMu.Unlock()
		return c
	}
	globalCursorCacheMu.Unlock()

	// Allocate new cursor
	c := &Cursor{
		signature: cursorSignature,
		state:     cursorUninitialized,
		top:       -1,
	}
	// Pre-initialize page pointers to embedded buffers
	for i := 0; i < CursorStackSize; i++ {
		c.pages[i] = &c.pagesBuf[i]
		c.dup.subPages[i] = &c.dup.subPagesBuf[i]
	}
	c.dup.subTop = -1
	return c
}

// returnCursorToCache returns a cursor to the global cache.
func returnCursorToCache(c *Cursor) {
	if c == nil {
		return
	}
	// Reset cursor state
	c.signature = 0
	c.state = cursorUninitialized
	c.top = -1
	c.txn = nil
	c.tree = nil
	c.mmapData = nil
	c.next = nil
	c.subcur = nil
	c.userCtx = nil

	globalCursorCacheMu.Lock()
	if len(globalCursorCache) < cursorCacheCap {
		globalCursorCache = append(globalCursorCache, c)
	}
	globalCursorCacheMu.Unlock()
}

// Global transaction caches - avoid sync.Pool.Put allocation overhead
var (
	globalWriteTxnCache   []*Txn
	globalWriteTxnCacheMu sync.Mutex
	writeTxnCacheCap      = 64

	globalReadTxnCache   []*Txn
	globalReadTxnCacheMu sync.Mutex
	readTxnCacheCap      = 256
)

// getWriteTxnFromCache returns a write transaction from cache or allocates new.
func getWriteTxnFromCache() *Txn {
	globalWriteTxnCacheMu.Lock()
	n := len(globalWriteTxnCache)
	if n > 0 {
		txn := globalWriteTxnCache[n-1]
		globalWriteTxnCache = globalWriteTxnCache[:n-1]
		globalWriteTxnCacheMu.Unlock()
		return txn
	}
	globalWriteTxnCacheMu.Unlock()
	return &Txn{}
}

// returnWriteTxnToCache returns a write transaction to the cache.
func returnWriteTxnToCache(txn *Txn) {
	globalWriteTxnCacheMu.Lock()
	if len(globalWriteTxnCache) < writeTxnCacheCap {
		globalWriteTxnCache = append(globalWriteTxnCache, txn)
	}
	globalWriteTxnCacheMu.Unlock()
}

// getReadTxnFromCache returns a read transaction from cache or allocates new.
func getReadTxnFromCache() *Txn {
	globalReadTxnCacheMu.Lock()
	n := len(globalReadTxnCache)
	if n > 0 {
		txn := globalReadTxnCache[n-1]
		globalReadTxnCache = globalReadTxnCache[:n-1]
		globalReadTxnCacheMu.Unlock()
		return txn
	}
	globalReadTxnCacheMu.Unlock()
	return &Txn{}
}

// returnReadTxnToCache returns a read transaction to the cache.
func returnReadTxnToCache(txn *Txn) {
	globalReadTxnCacheMu.Lock()
	if len(globalReadTxnCache) < readTxnCacheCap {
		globalReadTxnCache = append(globalReadTxnCache, txn)
	}
	globalReadTxnCacheMu.Unlock()
}

// metaPagePool reduces allocations for meta page updates
var metaPagePool = sync.Pool{
	New: func() any {
		// Default page size, will be resized if needed
		return make([]byte, 4096)
	},
}

// getPooledPageStruct gets a page struct from the global cache
func getPooledPageStruct(data []byte) *page {
	return getPageStructFromCache(data)
}

// dirtyPageTracker provides O(1) dirty page lookups using a flat array.
// This replaces the map[pgno]*page for better performance on write-heavy workloads.
type dirtyPageTracker struct {
	// Flat array for pages 0-65535 (256MB at 4KB page size)
	// Uses 512KB memory but zero allocations during operation
	pages [65536]*page

	// Fallback map for page numbers >= 64K (rare but must be supported)
	overflow map[pgno]*page

	// Bitmap tracking which pages are set (for fast iteration)
	bitmap [1024]uint64 // 65536 bits

	count int
}

// get returns the dirty page for the given page number, or nil if not dirty.
func (d *dirtyPageTracker) get(pn pgno) *page {
	if pn < 65536 {
		return d.pages[pn]
	}
	// Fallback to overflow map for large page numbers (rare)
	if d.overflow == nil {
		return nil
	}
	return d.overflow[pn]
}

// set stores a dirty page.
func (d *dirtyPageTracker) set(pn pgno, p *page) {
	if pn < 65536 {
		if d.pages[pn] == nil {
			d.count++
			// Set bitmap for iteration
			d.bitmap[pn>>6] |= 1 << (pn & 63)
		}
		d.pages[pn] = p
		return
	}
	// Fallback to overflow map for large page numbers
	if d.overflow == nil {
		d.overflow = make(map[pgno]*page)
	}
	if d.overflow[pn] == nil {
		d.count++
	}
	d.overflow[pn] = p
}

// forEach iterates over all dirty pages.
func (d *dirtyPageTracker) forEach(fn func(pgno, *page)) {
	// Iterate using bitmap for efficiency
	for i := 0; i < 1024; i++ {
		b := d.bitmap[i]
		if b == 0 {
			continue
		}
		base := pgno(i * 64)
		for b != 0 {
			// Find lowest set bit using CPU instruction
			pos := bits.TrailingZeros64(b)
			pn := base + pgno(pos)
			if d.pages[pn] != nil {
				fn(pn, d.pages[pn])
			}
			b &= b - 1 // clear lowest bit
		}
	}

	// Iterate overflow map
	for pn, p := range d.overflow {
		fn(pn, p)
	}
}

// clear resets the tracker to empty state.
func (d *dirtyPageTracker) clear() {
	// Fast path: nothing to clear
	if d.count == 0 {
		return
	}

	// Use Go's built-in clear for the arrays - this is very fast
	clear(d.pages[:])
	clear(d.bitmap[:])
	clear(d.overflow)

	d.count = 0
}

// len returns the number of dirty pages.
func (d *dirtyPageTracker) len() int {
	return d.count
}

// txnSignature is the magic number for valid transactions
const txnSignature int32 = 0x54584E58 // "TXNX"

// Txn represents a database transaction.
type Txn struct {
	signature int32
	flags     uint32
	env       *Env
	txnID     txnid
	parent    *Txn
	mu        sync.RWMutex

	// Tree state (copy-on-write from meta)
	trees []tree

	// Read transaction state
	readerSlot *readerSlot
	slotIdx    int

	// Write transaction state
	dirtyTracker dirtyPageTracker
	freePages    []pgno
	allocatedPg  pgno // Next page to allocate

	// Cursor tracking
	cursors []*Cursor

	// Cached cursors for Txn.Put/Get/Del (one per DBI, avoids open/close overhead)
	cachedCursors []*Cursor

	// DBI state
	dbiDirty []bool

	// Cached per-DBI state for hot path (avoids mutex lookups)
	dbiComparators       []func(a, b []byte) int // Cached key comparators per DBI
	dbiDupComparators    []func(a, b []byte) int // Cached dup value comparators per DBI
	dbiUsesDefaultCmp    []bool                  // True if DBI uses bytes.Compare for keys
	dbiUsesDefaultDupCmp []bool                  // True if DBI uses bytes.Compare for dup values

	// Pooled page data to return after commit/abort
	pooledPageData    [][]byte
	pooledPageStructs []*page

	// Scratch buffer for page compaction (avoids sync.Pool overhead)
	compactBuf [4096]byte

	// Page cache for read-only transactions (reduces allocations during iteration)
	pageCache map[pgno]*page

	// Direct mmap access for read-only hot paths (set on first use)
	mmapData []byte // Cached mmap data slice
	pageSize uint32 // Cached page size

	// User context
	userCtx any
}

// valid returns true if the transaction is valid.
func (txn *Txn) valid() bool {
	return txn != nil && txn.signature == txnSignature
}

// Env returns the transaction's environment.
func (txn *Txn) Env() *Env {
	return txn.env
}

// ID returns the transaction ID.
func (txn *Txn) ID() uint64 {
	return uint64(txn.txnID)
}

// IsReadOnly returns true if this is a read-only transaction.
func (txn *Txn) IsReadOnly() bool {
	return txn.flags&uint32(TxnReadOnly) != 0
}

// persistNamedDBTrees writes modified named database trees back to MainDBI.
// NOTE: This only persists the tree data to MainDBI. The cached trees in
// env.dbis are updated later in updateCachedDBITrees() AFTER the commit
// is complete. This ordering is critical to prevent read transactions from
// seeing new tree roots before the mmap has been extended to include those pages.
func (txn *Txn) persistNamedDBTrees() error {
	if txn.dbiDirty == nil {
		return nil
	}

	// Check if any named databases are dirty
	hasDirtyNamedDB := false
	for i := CoreDBs; i < len(txn.dbiDirty); i++ {
		if txn.dbiDirty[i] {
			hasDirtyNamedDB = true
			break
		}
	}

	if !hasDirtyNamedDB {
		return nil
	}

	// Open cursor on MainDBI to update named DB entries
	cursor, err := txn.OpenCursor(MainDBI)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// Write each dirty named database tree to MainDBI
	// Note: We do NOT update info.tree here - that happens in updateCachedDBITrees()
	// after the commit is complete to avoid race with read transactions.
	txn.env.dbisMu.RLock()
	defer txn.env.dbisMu.RUnlock()

	for i := CoreDBs; i < len(txn.dbiDirty); i++ {
		if !txn.dbiDirty[i] {
			continue
		}

		info := txn.env.dbis[i]
		if info == nil || info.name == "" {
			continue
		}

		// Serialize the updated tree
		tree := &txn.trees[i]
		treeData := serializeTreeToBytes(tree)

		// Update in MainDBI (use PutTree to preserve N_TREE flag)
		if err := cursor.PutTree([]byte(info.name), treeData, 0); err != nil {
			return err
		}
	}

	return nil
}

// updateCachedDBITrees updates the cached trees in env.dbis after commit completes.
// This must be called AFTER updateMeta() to ensure read transactions don't see
// new tree roots before the mmap has been extended to include those pages.
func (txn *Txn) updateCachedDBITrees() {
	if txn.dbiDirty == nil {
		return
	}

	txn.env.dbisMu.Lock()
	defer txn.env.dbisMu.Unlock()

	for i := CoreDBs; i < len(txn.dbiDirty); i++ {
		if !txn.dbiDirty[i] {
			continue
		}

		info := txn.env.dbis[i]
		if info == nil || info.name == "" {
			continue
		}

		// Now it's safe to update the cached tree
		tree := &txn.trees[i]
		info.tree = tree.clone()
	}
}

// CommitLatency contains timing information about a commit operation.
// For mdbx-go API compatibility.
type CommitLatency struct {
	Preparation time.Duration
	GCWallClock time.Duration
	GCCpuTime   time.Duration
	Audit       time.Duration
	Write       time.Duration
	Sync        time.Duration
	Ending      time.Duration
	Whole       time.Duration
}

// Commit commits the transaction and returns latency information.
// Returns (CommitLatency, error) for mdbx-go API compatibility.
func (txn *Txn) Commit() (CommitLatency, error) {
	var latency CommitLatency
	if !txn.valid() {
		return latency, NewError(ErrBadTxn)
	}

	if txn.IsReadOnly() {
		txn.Abort()
		return latency, nil
	}

	// Persist named database trees back to MainDBI (before acquiring lock)
	if err := txn.persistNamedDBTrees(); err != nil {
		txn.Abort()
		return latency, err
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	// Close all cursors
	txn.closeAllCursors()

	// Write dirty pages
	if err := txn.writeDirtyPages(); err != nil {
		txn.abortInternal()
		return latency, err
	}

	// Update meta page
	if err := txn.updateMeta(); err != nil {
		txn.abortInternal()
		return latency, err
	}

	// Update cached DBI trees AFTER meta is committed and mmap is extended.
	// This ensures read transactions don't see new tree roots before the
	// mmap has the pages they reference.
	txn.updateCachedDBITrees()

	// Note: sync happens in updateMeta, not here (avoid double sync)

	// Release write lock
	txn.env.lockFile.unlockWriter()
	txn.env.txnMu.Lock()
	txn.env.writeTxn = nil
	txn.env.txnCond.Broadcast()
	txn.env.txnMu.Unlock()

	// Return page data to env cache (avoids sync.Pool overhead)
	txn.env.returnPageDataToCache(txn.pooledPageData)
	txn.pooledPageData = txn.pooledPageData[:0]
	returnPageStructsToCache(txn.pooledPageStructs)
	txn.pooledPageStructs = txn.pooledPageStructs[:0]

	// Clear page cache (may have entries from reading before modification)
	for k := range txn.pageCache {
		delete(txn.pageCache, k)
	}

	// Signal transaction done - allows Close() to proceed with unmapping
	txn.env.txnWg.Done()

	// Return to cache
	txn.signature = 0
	txn.env = nil
	txn.parent = nil
	txn.mmapData = nil // Clear cached mmap - may have changed size
	returnWriteTxnToCache(txn)
	return latency, nil
}

// Abort aborts the transaction.
func (txn *Txn) Abort() {
	if !txn.valid() {
		return
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.abortInternal()
}

// abortInternal performs the actual abort (must hold lock).
func (txn *Txn) abortInternal() error {
	// Close all cursors
	txn.closeAllCursors()

	isReadOnly := txn.IsReadOnly()

	if isReadOnly {
		// Release reader slot
		if txn.readerSlot != nil {
			txn.env.lockFile.releaseReaderSlot(txn.readerSlot, txn.slotIdx)
			txn.readerSlot = nil
		}

		// Try to clean up old mmaps if no readers need them
		txn.env.tryCleanupOldMmaps()

		// Return pooled page structs from page cache
		returnPageStructsToCache(txn.pooledPageStructs)
		// Clear but keep the backing allocation for reuse
		txn.pooledPageStructs = txn.pooledPageStructs[:0]
		// Clear page cache map but keep the map for reuse
		clear(txn.pageCache)
	} else {
		// Release write lock
		txn.env.lockFile.unlockWriter()
		txn.env.txnMu.Lock()
		txn.env.writeTxn = nil
		txn.env.txnCond.Broadcast()
		txn.env.txnMu.Unlock()

		// Clear dirty page tracker for reuse
		txn.dirtyTracker.clear()
		txn.freePages = txn.freePages[:0]

		// Return page data to env cache (avoids sync.Pool overhead)
		txn.env.returnPageDataToCache(txn.pooledPageData)
		txn.pooledPageData = txn.pooledPageData[:0]
		returnPageStructsToCache(txn.pooledPageStructs)
		txn.pooledPageStructs = txn.pooledPageStructs[:0]
	}

	txn.signature = 0

	// Return transactions to appropriate cache
	if isReadOnly {
		// Signal reader done - allows Close() to proceed with unmapping
		txn.env.txnWg.Done()
		// Clear references before returning to cache
		txn.env = nil
		txn.userCtx = nil
		txn.mmapData = nil // Clear cached mmap - may have changed size
		returnReadTxnToCache(txn)
	} else {
		// Signal transaction done - allows Close() to proceed with unmapping
		txn.env.txnWg.Done()
		txn.env = nil
		txn.parent = nil
		txn.mmapData = nil // Clear cached mmap - may have changed size
		returnWriteTxnToCache(txn)
	}

	return nil
}

// Reset resets a read-only transaction for reuse.
func (txn *Txn) Reset() {
	if !txn.valid() || !txn.IsReadOnly() {
		return
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	// Close all cursors
	txn.closeAllCursors()

	// Release reader slot but keep transaction object
	if txn.readerSlot != nil {
		txn.env.lockFile.releaseReaderSlot(txn.readerSlot, txn.slotIdx)
		txn.readerSlot = nil
	}
}

// Renew renews a reset read-only transaction.
func (txn *Txn) Renew() error {
	if !txn.valid() || !txn.IsReadOnly() {
		return NewError(ErrBadTxn)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.readerSlot != nil {
		return NewError(ErrBadTxn) // Not reset
	}

	// Re-acquire reader slot
	pid := uint32(0) // TODO: os.Getpid()
	tid := uint64(0) // TODO: goroutine ID
	slot, slotIdx, err := txn.env.lockFile.acquireReaderSlot(pid, tid)
	if err != nil {
		return WrapError(ErrReadersFull, err)
	}

	// Get current meta (atomic load for concurrent access)
	txn.env.mu.RLock()
	meta := txn.env.meta.Load().recentMeta()
	// Update mmap cache while holding the lock
	txn.mmapData = txn.env.dataMap.data
	txn.pageSize = txn.env.pageSize
	txn.env.mu.RUnlock()

	if meta == nil {
		txn.env.lockFile.releaseReaderSlot(slot, slotIdx)
		return NewError(ErrCorrupted)
	}

	txn.readerSlot = slot
	txn.slotIdx = slotIdx
	txn.txnID = meta.txnID()

	// Set reader's txnid
	txn.env.lockFile.setReaderTxnid(slot, uint64(meta.txnID()))

	// Refresh tree state
	txn.trees[FreeDBI] = meta.GCTree
	txn.trees[MainDBI] = meta.MainTree

	// Refresh trees for all named DBIs that exist in env.dbis
	txn.env.dbisMu.RLock()
	for i := CoreDBs; i < len(txn.env.dbis) && i < len(txn.trees); i++ {
		info := txn.env.dbis[i]
		if info != nil && info.tree != nil {
			txn.trees[i] = *info.tree
		}
	}
	txn.env.dbisMu.RUnlock()

	return nil
}

// OpenDBISimple opens a database/table within the transaction using default comparators.
// This is the simple form without custom comparators (mdbx-go compatibility).
func (txn *Txn) OpenDBISimple(name string, flags uint) (DBI, error) {
	return txn.OpenDBI(name, flags, nil, nil)
}

// OpenDBI opens a database/table within the transaction.
// The cmp parameter is the key comparison function (nil for default).
// The dcmp parameter is the data/value comparison function for DUPSORT (nil for default).
func (txn *Txn) OpenDBI(name string, flags uint, cmp, dcmp CmpFunc) (DBI, error) {
	if !txn.valid() {
		return 0, NewError(ErrBadTxn)
	}

	// Empty name means the main database
	if name == "" {
		return MainDBI, nil
	}

	return txn.openNamedDBI(name, flags, cmp, dcmp)
}

// openNamedDBI opens a named database.
func (txn *Txn) openNamedDBI(name string, flags uint, cmp, dcmp CmpFunc) (DBI, error) {
	// Search for existing DBI slot first
	txn.env.dbisMu.RLock()
	existingSlot := -1
	for i, info := range txn.env.dbis {
		if info != nil && info.name == name {
			existingSlot = i
			break
		}
	}
	txn.env.dbisMu.RUnlock()

	// For read-only transactions with existing slot, we still need to read the tree
	// from MainDBI to ensure MVCC consistency (the cached tree might be newer than
	// our transaction's snapshot). For write transactions, use the cached tree.
	if existingSlot >= 0 && !txn.IsReadOnly() {
		txn.env.dbisMu.RLock()
		info := txn.env.dbis[existingSlot]
		if info != nil && info.tree != nil && existingSlot < len(txn.trees) {
			txn.trees[existingSlot] = *info.tree
		}
		txn.env.dbisMu.RUnlock()
		return DBI(existingSlot), nil
	}

	// Search main database for the named db's Tree metadata
	// Note: This must happen without holding txn.mu to avoid deadlock
	cursor, err := txn.OpenCursor(MainDBI)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	// Look up the database name in the main tree
	_, treeData, err := cursor.Get([]byte(name), nil, Set)
	if err != nil {
		if IsNotFound(err) {
			// Database doesn't exist
			if flags&Create == 0 {
				return 0, NewError(ErrNotFound)
			}
			// Need to create - requires write transaction
			if txn.IsReadOnly() {
				return 0, NewError(ErrPermissionDenied)
			}
			// Create the new database
			return txn.createNamedDBI(name, flags, cmp, dcmp, cursor)
		}
		return 0, err
	}

	// Parse the Tree structure from the value (48 bytes)
	if len(treeData) < 48 {
		return 0, NewError(ErrCorrupted)
	}

	tree := parseTreeFromBytes(treeData)

	// Allocate a slot for this DBI (need lock for modification)
	txn.env.dbisMu.Lock()
	defer txn.env.dbisMu.Unlock()

	// Check again in case another goroutine added it
	for i, info := range txn.env.dbis {
		if info != nil && info.name == name {
			return DBI(i), nil
		}
	}

	for i := CoreDBs; i < int(txn.env.maxDBs); i++ {
		if txn.env.dbis[i] == nil {
			txn.env.dbis[i] = &dbiInfo{
				name:  name,
				flags: flags,
				tree:  tree,
				cmp:   cmp,
				dcmp:  dcmp,
			}
			// Also copy tree to txn.trees for cursor access
			if i < len(txn.trees) {
				txn.trees[i] = *tree
			}
			return DBI(i), nil
		}
	}

	return 0, NewError(ErrDBsFull)
}

// createNamedDBI creates a new named database.
func (txn *Txn) createNamedDBI(name string, flags uint, cmp, dcmp CmpFunc, cursor *Cursor) (DBI, error) {
	// Create an empty tree with InvalidPgno as root
	tree := &tree{
		Flags:       uint16(flags & 0xFFFF), // DBFlags map directly to Tree flags
		Height:      0,
		DupfixSize:  0,
		Root:        invalidPgno,
		BranchPages: 0,
		LeafPages:   0,
		LargePages:  0,
		Sequence:    0,
		Items:       0,
		ModTxnid:    txnid(txn.txnID),
	}

	// Serialize tree to 48 bytes
	treeData := serializeTreeToBytes(tree)

	// Store the tree in the main database with the name as key
	// Use PutTree to set the N_TREE flag on the node (required for libmdbx compatibility)
	if err := cursor.PutTree([]byte(name), treeData, 0); err != nil {
		return 0, err
	}

	// Allocate a DBI slot
	txn.env.dbisMu.Lock()
	defer txn.env.dbisMu.Unlock()

	// Find an empty slot
	for i := CoreDBs; i < int(txn.env.maxDBs); i++ {
		if txn.env.dbis[i] == nil {
			txn.env.dbis[i] = &dbiInfo{
				name:  name,
				flags: flags,
				tree:  tree,
				cmp:   cmp,
				dcmp:  dcmp,
			}
			// Also copy tree to txn.trees for cursor access
			if i < len(txn.trees) {
				txn.trees[i] = *tree
			}
			return DBI(i), nil
		}
	}

	return 0, NewError(ErrDBsFull)
}

// serializeTreeToBytes serializes a Tree structure to 48 bytes (allocates).
func serializeTreeToBytes(tree *tree) []byte {
	data := make([]byte, treeSize)
	serializeTreeToBuf(tree, data)
	return data
}

// serializeTreeToBuf serializes a Tree structure to a provided buffer (no allocation).
// The buffer must be at least 48 bytes.
func serializeTreeToBuf(tree *tree, data []byte) {
	binary.LittleEndian.PutUint16(data[0:2], tree.Flags)
	binary.LittleEndian.PutUint16(data[2:4], tree.Height)
	binary.LittleEndian.PutUint32(data[4:8], tree.DupfixSize)
	binary.LittleEndian.PutUint32(data[8:12], uint32(tree.Root))
	binary.LittleEndian.PutUint32(data[12:16], uint32(tree.BranchPages))
	binary.LittleEndian.PutUint32(data[16:20], uint32(tree.LeafPages))
	binary.LittleEndian.PutUint32(data[20:24], uint32(tree.LargePages))
	binary.LittleEndian.PutUint64(data[24:32], tree.Sequence)
	binary.LittleEndian.PutUint64(data[32:40], tree.Items)
	binary.LittleEndian.PutUint64(data[40:48], uint64(tree.ModTxnid))
}

// parseTreeFromBytes parses a Tree structure from raw bytes.
func parseTreeFromBytes(data []byte) *tree {
	if len(data) < 48 {
		return nil
	}

	tree := &tree{
		Flags:       uint16(data[0]) | uint16(data[1])<<8,
		Height:      uint16(data[2]) | uint16(data[3])<<8,
		DupfixSize:  uint32(data[4]) | uint32(data[5])<<8 | uint32(data[6])<<16 | uint32(data[7])<<24,
		Root:        pgno(uint32(data[8]) | uint32(data[9])<<8 | uint32(data[10])<<16 | uint32(data[11])<<24),
		BranchPages: pgno(uint32(data[12]) | uint32(data[13])<<8 | uint32(data[14])<<16 | uint32(data[15])<<24),
		LeafPages:   pgno(uint32(data[16]) | uint32(data[17])<<8 | uint32(data[18])<<16 | uint32(data[19])<<24),
		LargePages:  pgno(uint32(data[20]) | uint32(data[21])<<8 | uint32(data[22])<<16 | uint32(data[23])<<24),
		Sequence: uint64(data[24]) | uint64(data[25])<<8 | uint64(data[26])<<16 | uint64(data[27])<<24 |
			uint64(data[28])<<32 | uint64(data[29])<<40 | uint64(data[30])<<48 | uint64(data[31])<<56,
		Items: uint64(data[32]) | uint64(data[33])<<8 | uint64(data[34])<<16 | uint64(data[35])<<24 |
			uint64(data[36])<<32 | uint64(data[37])<<40 | uint64(data[38])<<48 | uint64(data[39])<<56,
		ModTxnid: txnid(uint64(data[40]) | uint64(data[41])<<8 | uint64(data[42])<<16 | uint64(data[43])<<24 |
			uint64(data[44])<<32 | uint64(data[45])<<40 | uint64(data[46])<<48 | uint64(data[47])<<56),
	}

	return tree
}

// GetTree returns the tree info for a DBI (for debugging).
func (txn *Txn) GetTree(dbi DBI) *tree {
	if int(dbi) < len(txn.trees) {
		return &txn.trees[dbi]
	}
	return nil
}

// DebugGetPage returns a page by number (for debugging).
func (txn *Txn) DebugGetPage(pageNum uint32) ([]byte, error) {
	p, err := txn.getPage(pgno(pageNum))
	if err != nil {
		return nil, err
	}
	return p.Data, nil
}

// CloseDBI closes a database handle.
func (txn *Txn) CloseDBI(dbi DBI) error {
	// DBIs are actually closed at environment level
	// This is a no-op for compatibility
	return nil
}

// Get retrieves a value by key.
// This is optimized to avoid cursor allocation for simple lookups.
func (txn *Txn) Get(dbi DBI, key []byte) ([]byte, error) {
	if !txn.valid() {
		return nil, NewError(ErrBadTxn)
	}

	if int(dbi) >= len(txn.trees) {
		return nil, NewError(ErrBadDBI)
	}

	// FreeDBI (0) is the GC database, not accessible for normal operations
	if dbi == FreeDBI {
		return nil, NewError(ErrBadDBI)
	}

	tree := &txn.trees[dbi]
	if tree.isEmpty() {
		return nil, ErrNotFoundError
	}

	// Fast path: direct tree search without cursor allocation
	return txn.directGet(tree, dbi, key)
}

// directGet performs a direct tree search without cursor overhead.
// Uses allocation-free methods for maximum performance.
func (txn *Txn) directGet(tree *tree, dbi DBI, key []byte) ([]byte, error) {
	// Ensure comparator is cached for this DBI
	if int(dbi) < len(txn.dbiComparators) && txn.dbiComparators[dbi] == nil {
		txn.cacheComparator(dbi)
	}

	// Fast path for read-only transactions: direct mmap access
	if txn.flags&uint32(TxnReadOnly) != 0 {
		// Use hyper-optimized path for default comparator
		if txn.dbiUsesDefaultCmp[dbi] {
			return txn.directGetReadOnlyFast(tree.Root, key)
		}
		return txn.directGetReadOnly(tree.Root, key, txn.dbiComparators[dbi])
	}

	// Get comparator for write path
	cmp := txn.dbiComparators[dbi]

	// Write transaction path: need to check dirty pages
	data, err := txn.getPageData(tree.Root)
	if err != nil {
		return nil, err
	}

	for {
		idx, exact := txn.searchPageRawExact(data, key, cmp)

		if pageIsLeafDirect(data) {
			if !exact {
				return nil, ErrNotFoundError
			}

			flags := nodeGetFlagsUnchecked(data, idx)
			if flags&nodeBig != 0 {
				dataSize := nodeGetDataSizeRaw(data, idx)
				overflowPgno := nodeGetOverflowPgnoRaw(data, idx)
				return txn.getLargeData(overflowPgno, dataSize)
			}

			nodeData := nodeGetDataUnchecked(data, idx)

			// Handle DupSort: check for sub-tree (nodeTree) or sub-page (nodeDup)
			if flags&nodeTree != 0 && len(nodeData) >= 48 {
				// Sub-tree: return first value
				return txn.getFirstSubTreeValueReadOnly(nodeData)
			}
			if flags&nodeDup != 0 && len(nodeData) >= 20 {
				// Sub-page: return first value
				return getFirstSubPageValue(nodeData)
			}

			return nodeData, nil
		}

		pgno := nodeGetChildPgnoUnchecked(data, idx)
		data, err = txn.getPageData(pgno)
		if err != nil {
			return nil, err
		}
	}
}

// directGetReadOnly is the fast path for read-only transactions with custom comparators.
// Accesses mmap directly without dirty page checks.
func (txn *Txn) directGetReadOnly(rootPgno pgno, key []byte, cmp func(a, b []byte) int) ([]byte, error) {
	// Use cached mmap data (set during txn creation) to avoid race with write txn remap
	mmapData := txn.mmapData
	pageSize := uint64(txn.pageSize)

	pgno := rootPgno
	for {
		offset := uint64(pgno) * pageSize
		data := mmapData[offset : offset+pageSize]

		idx, exact := txn.searchPageRawExact(data, key, cmp)

		if pageIsLeafDirect(data) {
			if !exact {
				return nil, ErrNotFoundError
			}

			flags := nodeGetFlagsUnchecked(data, idx)
			if flags&nodeBig != 0 {
				dataSize := nodeGetDataSizeRaw(data, idx)
				overflowPgno := nodeGetOverflowPgnoRaw(data, idx)
				return txn.getLargeData(overflowPgno, dataSize)
			}

			nodeData := nodeGetDataUnchecked(data, idx)

			// Handle DupSort: check for sub-tree (nodeTree) or sub-page (nodeDup)
			if flags&nodeTree != 0 && len(nodeData) >= 48 {
				// Sub-tree: return first value
				return txn.getFirstSubTreeValueReadOnly(nodeData)
			}
			if flags&nodeDup != 0 && len(nodeData) >= 20 {
				// Sub-page: return first value
				return getFirstSubPageValue(nodeData)
			}

			return nodeData, nil
		}

		pgno = nodeGetChildPgnoUnchecked(data, idx)
	}
}

// directGetReadOnlyFast is the hyper-optimized Get path for read-only transactions
// using the default bytes.Compare comparator. It inlines all operations and uses
// unsafe pointer arithmetic to minimize overhead.
func (txn *Txn) directGetReadOnlyFast(rootPgno pgno, key []byte) ([]byte, error) {
	// Use cached mmap data (set during txn creation) to avoid race with write txn remap
	mmapBase := uintptr(unsafe.Pointer(&txn.mmapData[0]))
	pageSize := uintptr(txn.pageSize)

	currentPgno := rootPgno
	for {
		// Direct pointer to page start
		base := mmapBase + uintptr(currentPgno)*pageSize

		// Read header: flags at offset 10, lower at offset 12
		// flags tells us if leaf/branch, lower >> 1 = numEntries
		flags := *(*uint16)(unsafe.Pointer(base + 10))
		lower := *(*uint16)(unsafe.Pointer(base + 12))
		n := int(lower >> 1)

		isLeaf := flags&0x02 != 0 // page.Leaf = 0x02

		var idx int
		var exact bool

		if !isLeaf {
			// Branch page: binary search entries 1 to n-1
			if n <= 1 {
				idx = 0
			} else {
				low, high := 1, n-1
				for low <= high {
					mid := int(uint(low+high) >> 1)
					// Get entry offset: entries start at offset 20
					storedOffset := *(*uint16)(unsafe.Pointer(base + 20 + uintptr(mid*2)))
					nodeBase := base + uintptr(storedOffset) + 20

					// Read key: keySize at node+6, key data at node+8
					keySize := int(*(*uint16)(unsafe.Pointer(nodeBase + 6)))
					nodeKey := unsafe.Slice((*byte)(unsafe.Pointer(nodeBase+8)), keySize)

					c := bytes.Compare(key, nodeKey)
					if c < 0 {
						high = mid - 1
					} else if c > 0 {
						low = mid + 1
					} else {
						idx = mid
						exact = true
						goto done
					}
				}

				// Check if key < first entry's key
				if low == 1 {
					storedOffset := *(*uint16)(unsafe.Pointer(base + 20 + 2))
					nodeBase := base + uintptr(storedOffset) + 20
					keySize := int(*(*uint16)(unsafe.Pointer(nodeBase + 6)))
					nodeKey := unsafe.Slice((*byte)(unsafe.Pointer(nodeBase+8)), keySize)
					if bytes.Compare(key, nodeKey) < 0 {
						idx = 0
						goto done
					}
				}
				idx = low - 1
			}
		} else {
			// Leaf page: binary search from 0
			if n == 0 {
				return nil, ErrNotFoundError
			}
			low, high := 0, n-1
			for low <= high {
				mid := int(uint(low+high) >> 1)
				storedOffset := *(*uint16)(unsafe.Pointer(base + 20 + uintptr(mid*2)))
				nodeBase := base + uintptr(storedOffset) + 20

				keySize := int(*(*uint16)(unsafe.Pointer(nodeBase + 6)))
				nodeKey := unsafe.Slice((*byte)(unsafe.Pointer(nodeBase+8)), keySize)

				c := bytes.Compare(key, nodeKey)
				if c < 0 {
					high = mid - 1
				} else if c > 0 {
					low = mid + 1
				} else {
					idx = mid
					exact = true
					goto done
				}
			}
			idx = low
		}

	done:
		if isLeaf {
			if !exact {
				return nil, ErrNotFoundError
			}

			// Get node at idx
			storedOffset := *(*uint16)(unsafe.Pointer(base + 20 + uintptr(idx*2)))
			nodeBase := base + uintptr(storedOffset) + 20

			nodeFlags := *(*uint8)(unsafe.Pointer(nodeBase + 4))
			if nodeFlags&0x01 != 0 { // nodeBig
				// Handle overflow pages
				dataSize := *(*uint32)(unsafe.Pointer(nodeBase))
				keySize := *(*uint16)(unsafe.Pointer(nodeBase + 6))
				pgnoOffset := nodeBase + 8 + uintptr(keySize)
				overflowPgno := pgno(*(*uint32)(unsafe.Pointer(pgnoOffset)))
				return txn.getLargeData(overflowPgno, dataSize)
			}

			// Get inline data
			dataSize := int(*(*uint32)(unsafe.Pointer(nodeBase)))
			keySize := int(*(*uint16)(unsafe.Pointer(nodeBase + 6)))
			dataStart := nodeBase + 8 + uintptr(keySize)
			nodeData := unsafe.Slice((*byte)(unsafe.Pointer(dataStart)), dataSize)

			// Handle DupSort: check for sub-tree (nodeTree=0x02) or sub-page (nodeDup=0x04)
			if nodeFlags&0x02 != 0 && dataSize >= 48 {
				// Sub-tree: return first value
				return txn.getFirstSubTreeValueReadOnly(nodeData)
			}
			if nodeFlags&0x04 != 0 && dataSize >= 20 {
				// Sub-page: return first value
				return getFirstSubPageValue(nodeData)
			}

			return nodeData, nil
		}

		// Branch: descend to child
		storedOffset := *(*uint16)(unsafe.Pointer(base + 20 + uintptr(idx*2)))
		nodeBase := base + uintptr(storedOffset) + 20
		currentPgno = pgno(*(*uint32)(unsafe.Pointer(nodeBase)))
	}
}

// getFirstSubPageValue extracts the first value from an inline DUPSORT sub-page.
func getFirstSubPageValue(subPageData []byte) ([]byte, error) {
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

// getFirstSubTreeValueReadOnly extracts the first value from a DUPSORT sub-tree.
// Uses direct mmap access for read-only transactions.
func (txn *Txn) getFirstSubTreeValueReadOnly(treeData []byte) ([]byte, error) {
	if len(treeData) < 48 {
		return nil, ErrCorruptedError
	}

	// Parse Tree structure - height at offset 2-3, root at offset 8-11
	height := int(uint16(treeData[2]) | uint16(treeData[3])<<8)
	subRoot := pgno(uint32(treeData[8]) | uint32(treeData[9])<<8 | uint32(treeData[10])<<16 | uint32(treeData[11])<<24)

	if subRoot == invalidPgno || height == 0 {
		return nil, ErrNotFoundError
	}

	// Use cached mmap data (set during txn creation) to avoid race with write txn remap
	mmapData := txn.mmapData
	pageSz := uint64(txn.pageSize)

	// Navigate to the leftmost leaf
	currentPgno := subRoot
	for level := 1; level < height; level++ {
		offset := uint64(currentPgno) * pageSz
		pageData := mmapData[offset : offset+pageSz]

		// Get first entry's stored offset
		storedOffset := int(uint16(pageData[pageHeaderSize]) | uint16(pageData[pageHeaderSize+1])<<8)
		nodeOffset := storedOffset + pageHeaderSize

		// Child pgno is first 4 bytes of node
		currentPgno = pgno(uint32(pageData[nodeOffset]) | uint32(pageData[nodeOffset+1])<<8 |
			uint32(pageData[nodeOffset+2])<<16 | uint32(pageData[nodeOffset+3])<<24)
	}

	// Now at leaf page - get first entry
	offset := uint64(currentPgno) * pageSz
	pageData := mmapData[offset : offset+pageSz]

	storedOffset := int(uint16(pageData[pageHeaderSize]) | uint16(pageData[pageHeaderSize+1])<<8)
	nodeOffset := storedOffset + pageHeaderSize

	// Key size is at node+6 (2 bytes) - in sub-trees, the "key" is actually the duplicate value
	keySize := int(uint16(pageData[nodeOffset+6]) | uint16(pageData[nodeOffset+7])<<8)
	valueStart := nodeOffset + nodeSize
	valueEnd := valueStart + keySize

	// Allow keySize=0 for empty duplicate values
	if keySize >= 0 && valueEnd <= len(pageData) {
		// Use three-index slice to cap capacity at length
		return pageData[valueStart:valueEnd:valueEnd], nil
	}

	return nil, ErrCorruptedError
}

// searchPageRawExact does binary search and returns whether an exact match was found.
// Takes comparator directly to avoid repeated dbiComparators lookup.
// Returns (index, exactMatch).
func (txn *Txn) searchPageRawExact(data []byte, key []byte, cmp func(a, b []byte) int) (int, bool) {
	n := pageNumEntriesDirect(data)
	if n == 0 {
		return 0, false
	}

	if pageIsBranchDirect(data) {
		if n == 1 {
			return 0, false
		}

		// Binary search entries 1 to n-1
		low, high := 1, n-1
		for low <= high {
			mid := (low + high) / 2
			nodeKey := nodeGetKeyUnchecked(data, mid)
			c := cmp(key, nodeKey)

			if c < 0 {
				high = mid - 1
			} else if c > 0 {
				low = mid + 1
			} else {
				return mid, true
			}
		}

		// low is now the insertion point: return low - 1
		return low - 1, false
	}

	// Leaf page: standard binary search from 0
	low, high := 0, n-1
	for low <= high {
		mid := (low + high) / 2
		nodeKey := nodeGetKeyUnchecked(data, mid)
		c := cmp(key, nodeKey)

		if c < 0 {
			high = mid - 1
		} else if c > 0 {
			low = mid + 1
		} else {
			return mid, true
		}
	}

	return low, false
}

// getCachedCursor returns a cached cursor for the DBI, creating one if needed.
// This avoids the overhead of opening/closing cursors for each Put/Get/Del.
func (txn *Txn) getCachedCursor(dbi DBI) (*Cursor, error) {
	idx := int(dbi)
	// Grow slice if needed
	if idx >= len(txn.cachedCursors) {
		newCursors := make([]*Cursor, idx+1)
		copy(newCursors, txn.cachedCursors)
		txn.cachedCursors = newCursors
	}

	if txn.cachedCursors[idx] != nil {
		return txn.cachedCursors[idx], nil
	}

	// Create new cursor
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		return nil, err
	}
	txn.cachedCursors[idx] = cursor
	return cursor, nil
}

// Put stores a key-value pair.
func (txn *Txn) Put(dbi DBI, key, value []byte, flags uint) error {
	if !txn.valid() {
		return NewError(ErrBadTxn)
	}

	if txn.IsReadOnly() {
		return NewError(ErrPermissionDenied)
	}

	cursor, err := txn.getCachedCursor(dbi)
	if err != nil {
		return err
	}

	return cursor.Put(key, value, flags)
}

// Del deletes a key (and optionally a specific value for DUPSORT).
func (txn *Txn) Del(dbi DBI, key, value []byte) error {
	if !txn.valid() {
		return NewError(ErrBadTxn)
	}

	if txn.IsReadOnly() {
		return NewError(ErrPermissionDenied)
	}

	cursor, err := txn.getCachedCursor(dbi)
	if err != nil {
		return err
	}

	var op CursorOp = Set
	var delFlags uint = 0

	if value != nil {
		op = GetBoth
	} else {
		// When value is nil, delete all values for the key (NoDupData)
		delFlags = NoDupData
	}

	_, _, err = cursor.Get(key, value, op)
	if err != nil {
		return err
	}

	return cursor.Del(delFlags)
}

// OpenCursor opens a cursor on a database.
func (txn *Txn) OpenCursor(dbi DBI) (*Cursor, error) {
	if !txn.valid() {
		return nil, NewError(ErrBadTxn)
	}

	if int(dbi) >= len(txn.trees) {
		return nil, NewError(ErrBadDBI)
	}

	// Ensure comparator is cached for key comparisons
	if int(dbi) < len(txn.dbiComparators) && txn.dbiComparators[dbi] == nil {
		txn.cacheComparator(dbi)
	}

	// Get cursor from cache
	cursor := newCursorFromCache()
	cursor.signature = cursorSignature
	cursor.state = cursorUninitialized
	cursor.top = -1
	cursor.txn = txn
	cursor.dbi = dbi
	cursor.tree = &txn.trees[dbi]
	cursor.isDupSort = cursor.tree.Flags&uint16(DupSort) != 0 // Cache for fast path
	cursor.afterDelete = false                                // Reset delete state
	cursor.subcur = nil
	cursor.next = nil
	cursor.userCtx = nil
	cursor.dirtyMask = 0
	// Reset ALL dup state to prevent corruption from cached cursors
	cursor.dup.initialized = false
	cursor.dup.isSubTree = false
	cursor.dup.atFirst = false
	cursor.dup.atLast = false
	cursor.dup.subTop = -1
	cursor.dup.subPageIdx = 0
	cursor.dup.subPageNum = 0
	cursor.dup.dupfixSize = 0
	cursor.dup.subPageData = nil
	cursor.dup.nodePositions = nil
	cursor.initMmapCache() // Initialize mmap cache for fast page access

	// Add to transaction's cursor list
	txn.mu.Lock()
	txn.cursors = append(txn.cursors, cursor)
	txn.mu.Unlock()

	return cursor, nil
}

// returnCursor returns a cursor to the cache.
func returnCursor(c *Cursor) {
	// Reset ALL pages to embedded buffers and clear dirty cache
	// This ensures no stale pointers remain when cursor is reused
	for i := 0; i < CursorStackSize; i++ {
		c.pages[i] = &c.pagesBuf[i]
		c.stackDirty[i] = nil
		c.indices[i] = 0
		c.numExpected[i] = 0
		c.dup.subPages[i] = &c.dup.subPagesBuf[i]
		c.dup.subIndices[i] = 0
	}
	// Reset all dup state to prevent corruption when cursor is reused
	c.dup.initialized = false
	c.dup.isSubTree = false
	c.dup.atFirst = false
	c.dup.atLast = false
	c.dup.subTop = -1
	c.dup.subPageIdx = 0
	c.dup.subPageNum = 0
	c.dup.dupfixSize = 0
	c.dup.subPageData = nil
	c.dup.nodePositions = nil
	c.dirtyMask = 0

	// Return to global cache
	returnCursorToCache(c)
}

// removeCursor removes a cursor from the transaction's list.
// Uses swap-with-last for O(1) removal instead of O(n) slice shift.
func (txn *Txn) removeCursor(c *Cursor) {
	txn.mu.Lock()
	n := len(txn.cursors)
	for i := 0; i < n; i++ {
		if txn.cursors[i] == c {
			// Swap with last element and truncate
			txn.cursors[i] = txn.cursors[n-1]
			txn.cursors[n-1] = nil // Allow GC
			txn.cursors = txn.cursors[:n-1]
			break
		}
	}
	txn.mu.Unlock()
}

// closeAllCursors closes all open cursors.
func (txn *Txn) closeAllCursors() {
	for _, c := range txn.cursors {
		if c != nil {
			c.signature = 0
			c.txn = nil
		}
	}
	txn.cursors = nil
	// Clear cached cursors (they're already in cursors slice)
	txn.cachedCursors = nil
}

// getPage returns a page, checking dirty pages first for write transactions.
// For read-only transactions, pages are cached to reduce allocations during iteration.
func (txn *Txn) getPage(pg pgno) (*page, error) {
	// Fast path for read-only transactions - check cache directly
	if txn.flags&uint32(TxnReadOnly) != 0 {
		if txn.pageCache != nil {
			if p, ok := txn.pageCache[pg]; ok {
				return p, nil
			}
		}
	} else {
		// Write transactions: check dirty pages only (no map lookup for clean pages)
		if p := txn.dirtyTracker.get(pg); p != nil {
			return p, nil
		}
		// Skip pageCache for write transactions - read directly from mmap
	}

	// Check parent transaction
	if txn.parent != nil {
		return txn.parent.getPage(pg)
	}

	// Get raw page data from environment (allocation-free)
	data, err := txn.env.getPageData(pg)
	if err != nil {
		return nil, err
	}

	// Create Page struct using pool
	p := getPooledPageStruct(data)

	// Cache the page only for read-only transactions (lazy initialization)
	if txn.flags&uint32(TxnReadOnly) != 0 {
		if txn.pageCache == nil {
			txn.pageCache = make(map[pgno]*page, 64)
		}
		txn.pageCache[pg] = p
		txn.pooledPageStructs = append(txn.pooledPageStructs, p)
	}

	return p, nil
}

// getPageData returns raw page data without allocating a Page struct.
// This is for allocation-free hot paths in read operations.
func (txn *Txn) getPageData(pg pgno) ([]byte, error) {
	// Check dirty pages first (write transactions)
	if p := txn.dirtyTracker.get(pg); p != nil {
		return p.Data, nil
	}

	// Check parent transaction
	if txn.parent != nil {
		return txn.parent.getPageData(pg)
	}

	// Get from environment mmap directly
	return txn.env.getPageData(pg)
}

// initMmapCache initializes cached mmap data for fast page access.
// Called once on first fast access.
func (txn *Txn) initMmapCache() {
	if txn.mmapData == nil && txn.env.dataMap != nil {
		txn.mmapData = txn.env.dataMap.data
		txn.pageSize = txn.env.pageSize
	}
}

// getPageDataFast returns raw page data with minimal overhead.
// For read-only transactions in hot loops. No map lookup, no error checks.
// Caller must ensure pgno is valid.
func (txn *Txn) getPageDataFast(pgno pgno) []byte {
	if txn.mmapData == nil {
		txn.initMmapCache()
	}
	offset := uint64(pgno) * uint64(txn.pageSize)
	end := offset + uint64(txn.pageSize)

	// Check bounds - if page is beyond our cached mmap, refresh from env.
	// This can happen if a concurrent write transaction extended the database
	// and we're accessing tree data that references new pages.
	// Old mmaps are kept alive in env.oldMmaps so this is safe.
	if end > uint64(len(txn.mmapData)) {
		txn.mmapData = txn.env.dataMap.data
		// If still out of bounds after refresh, this is a bug
		if end > uint64(len(txn.mmapData)) {
			// Return empty slice to avoid panic - caller will handle error
			return nil
		}
	}
	return txn.mmapData[offset:end]
}

// fillPageHotPath fills a Page struct with page data, optimized for hot path.
// Uses the provided Page buffer to avoid allocation.
// For read-only transactions: fills from direct mmap.
// For write transactions: returns dirty page or fills from mmap.
// Returns the page to use (either buf filled, or a dirty page from cache).
func (txn *Txn) fillPageHotPath(pgno pgno, buf *page) *page {
	// Fast path for read-only transactions
	if txn.flags&uint32(TxnReadOnly) != 0 {
		buf.Data = txn.getPageDataFast(pgno)
		return buf
	}
	// Write transactions need to check dirty pages
	if p := txn.dirtyTracker.get(pgno); p != nil {
		return p
	}
	// Not dirty, fill from mmap
	buf.Data = txn.getPageDataFast(pgno)
	return buf
}

// getLargeData retrieves data from overflow pages.
// MDBX format: first page has header, subsequent pages are raw data with no header.
// Since overflow pages are contiguous, we can return a direct slice for read-only txns.
func (txn *Txn) getLargeData(overflowPgno pgno, size uint32) ([]byte, error) {
	// Fast path for read-only transactions: direct mmap slice (zero-copy)
	if txn.flags&uint32(TxnReadOnly) != 0 && txn.mmapData != nil {
		pageSize := uint64(txn.pageSize)
		// Data starts after header on first overflow page
		start := uint64(overflowPgno)*pageSize + pageHeaderSize
		end := start + uint64(size)
		if end <= uint64(len(txn.mmapData)) {
			return txn.mmapData[start:end], nil
		}
	}

	// Slow path for write transactions: must copy since pages may be in dirty list
	pageSize := txn.env.pageSize
	firstPageData := pageSize - pageHeaderSize // First page has header

	// Calculate number of pages needed
	remaining := int(size) - int(firstPageData)
	numPages := uint32(1)
	if remaining > 0 {
		numPages += uint32((remaining + int(pageSize) - 1) / int(pageSize))
	}

	data := make([]byte, 0, size)
	for i := uint32(0); i < numPages; i++ {
		p, err := txn.getPage(overflowPgno + pgno(i))
		if err != nil {
			return nil, err
		}

		if i == 0 {
			// First page has header - skip it
			data = append(data, p.Data[pageHeaderSize:]...)
		} else {
			// Subsequent pages are raw data with no header
			data = append(data, p.Data...)
		}
	}

	return data[:size], nil
}

// compareKeys compares two keys using the database's comparator.
func (txn *Txn) compareKeys(dbi DBI, a, b []byte) int {
	// Fast path: use cached comparator (caller should ensure it's cached via cacheComparator)
	return txn.dbiComparators[dbi](a, b)
}

// compareKeysCached is the same as compareKeys but caches the comparator on first use.
// This is used internally to ensure the cache is populated.
func (txn *Txn) cacheComparator(dbi DBI) {
	if int(dbi) >= len(txn.dbiComparators) {
		return
	}

	// Check if already cached
	if txn.dbiComparators[dbi] != nil {
		return
	}

	// Look up the comparator (only once per DBI per transaction)
	txn.env.dbisMu.RLock()
	if int(dbi) < len(txn.env.dbis) && txn.env.dbis[dbi] != nil {
		cmp := txn.env.dbis[dbi].cmp
		if cmp != nil {
			txn.dbiComparators[dbi] = cmp
			txn.dbiUsesDefaultCmp[dbi] = false
		} else {
			// Cache the default comparator
			txn.dbiComparators[dbi] = bytes.Compare
			txn.dbiUsesDefaultCmp[dbi] = true
		}
	} else {
		txn.dbiComparators[dbi] = bytes.Compare
		txn.dbiUsesDefaultCmp[dbi] = true
	}
	txn.env.dbisMu.RUnlock()
}

// compareDupValues compares two values using the database's dup comparator.
// For DUPSORT databases, this uses the custom dup comparator if set, otherwise bytes.Compare.
func (txn *Txn) compareDupValues(dbi DBI, a, b []byte) int {
	// Fast path: use cached comparator (avoids mutex lock on every comparison)
	if int(dbi) < len(txn.dbiDupComparators) {
		if txn.dbiUsesDefaultDupCmp[dbi] {
			// Ultra-fast path: inline bytes.Compare for default case
			return bytes.Compare(a, b)
		}
		if txn.dbiDupComparators[dbi] != nil {
			return txn.dbiDupComparators[dbi](a, b)
		}
	}
	// Initialize cached comparator
	txn.initDupComparator(dbi)
	if txn.dbiUsesDefaultDupCmp[dbi] {
		return bytes.Compare(a, b)
	}
	return txn.dbiDupComparators[dbi](a, b)
}

// initDupComparator initializes the cached dup comparator for a DBI.
func (txn *Txn) initDupComparator(dbi DBI) {
	if int(dbi) >= len(txn.dbiDupComparators) {
		// Extend slices
		newSize := max(int(dbi)+1, 8)
		newDupCmps := make([]func(a, b []byte) int, newSize)
		newUsesDefault := make([]bool, newSize)
		copy(newDupCmps, txn.dbiDupComparators)
		copy(newUsesDefault, txn.dbiUsesDefaultDupCmp)
		txn.dbiDupComparators = newDupCmps
		txn.dbiUsesDefaultDupCmp = newUsesDefault
	}

	if txn.dbiDupComparators[dbi] != nil {
		return // Already initialized
	}

	// Get from env with lock (only once)
	txn.env.dbisMu.RLock()
	var dcmp func(a, b []byte) int
	if int(dbi) < len(txn.env.dbis) && txn.env.dbis[dbi] != nil {
		dcmp = txn.env.dbis[dbi].dcmp
	}
	txn.env.dbisMu.RUnlock()

	if dcmp != nil {
		txn.dbiDupComparators[dbi] = dcmp
		txn.dbiUsesDefaultDupCmp[dbi] = false
	} else {
		txn.dbiDupComparators[dbi] = bytes.Compare
		txn.dbiUsesDefaultDupCmp[dbi] = true
	}
}

// writeDirtyPages writes all dirty pages to the data file.
func (txn *Txn) writeDirtyPages() error {
	if txn.dirtyTracker.len() == 0 {
		return nil
	}

	pageSize := int64(txn.env.pageSize)

	// Calculate required file size
	requiredSize := int64(txn.allocatedPg) * pageSize

	// Get current mmap size (avoids syscall)
	currentSize := txn.env.dataMap.size

	// Extend file if needed
	if requiredSize > currentSize {
		if err := txn.env.dataFile.Truncate(requiredSize); err != nil {
			return WrapError(ErrProblem, err)
		}

		// Keep old mmap alive for readers (COW safety)
		// Don't unmap - readers may still have pointers into it
		oldMap := txn.env.dataMap

		writable := txn.env.flags&ReadOnly == 0 && txn.env.flags&WriteMap != 0
		dm, err := mmapMap(int(txn.env.dataFile.Fd()), 0, int(requiredSize), writable)
		if err != nil {
			return WrapError(ErrProblem, err)
		}

		// Hold write lock while updating dataMap to prevent readers from racing
		txn.env.mu.Lock()
		txn.env.dataMap = dm

		// Store old mmap for later cleanup
		if oldMap != nil {
			txn.env.oldMmapsMu.Lock()
			txn.env.oldMmaps = append(txn.env.oldMmaps, oldMap)
			txn.env.oldMmapsMu.Unlock()
		}

		// Re-read meta to update pointers after remap
		err = txn.env.readMeta()
		txn.env.mu.Unlock()
		if err != nil {
			return WrapError(ErrProblem, err)
		}
	}

	// Write all dirty pages
	useWriteMap := txn.env.flags&WriteMap != 0 && txn.env.dataMap != nil
	var writeErr error

	txn.dirtyTracker.forEach(func(pn pgno, p *page) {
		if writeErr != nil {
			return
		}
		offset := int64(pn) * pageSize

		if useWriteMap {
			// WriteMap mode: check if page is in mmap
			// If p.Data points into mmap, it's already written
			// If it points to allocated memory, we need to copy to mmap or write to file
			mmapData := txn.env.dataMap.data
			end := int(offset) + len(p.Data)
			if end <= len(mmapData) {
				// Check if p.Data is already the mmap slice (same backing array)
				mmapSlice := mmapData[offset:end]
				if &p.Data[0] == &mmapSlice[0] {
					// Already in mmap, nothing to do
					return
				}
				// Copy to mmap
				copy(mmapSlice, p.Data)
				return
			}
		}
		// Write to file
		if _, err := txn.env.dataFile.WriteAt(p.Data, offset); err != nil {
			writeErr = WrapError(ErrProblem, err)
		}
	})

	return writeErr
}

// updateMeta writes a new meta page.
func (txn *Txn) updateMeta() error {
	// Get next meta page index
	metaIdx := txn.env.meta.Load().nextMetaIndex()
	pageSize := txn.env.pageSize

	// Get meta page buffer from pool
	metaPageIface := metaPagePool.Get()
	metaPage := metaPageIface.([]byte)
	if len(metaPage) < int(pageSize) {
		metaPage = make([]byte, pageSize)
	} else {
		metaPage = metaPage[:pageSize]
		// Clear the buffer (clear uses optimized memclr)
		clear(metaPage)
	}
	defer metaPagePool.Put(metaPage)

	// Write page header at offset 0
	// Meta page headers have txnid=0 (the actual txnid is in the meta body)
	pageHdr := (*pageHeader)(unsafe.Pointer(&metaPage[0]))
	pageHdr.PageNo = pgno(metaIdx)
	pageHdr.Flags = pageMeta
	pageHdr.Txnid = 0 // Meta pages use txnid from meta body, not page header

	// Meta content starts after page header (offset 20)
	meta := (*meta)(unsafe.Pointer(&metaPage[pageHeaderSize]))

	// Copy from recent meta
	recentMeta := txn.env.meta.Load().recentMeta()
	if recentMeta == nil {
		return NewError(ErrCorrupted)
	}
	*meta = *recentMeta

	// Update with transaction changes
	meta.setTxnid(txn.txnID)
	meta.GCTree = txn.trees[FreeDBI]
	meta.MainTree = txn.trees[MainDBI]
	meta.Geometry.Now = txn.allocatedPg
	meta.Geometry.Next = txn.allocatedPg // Next page to allocate

	// Two-phase update: set txnid_b to 0 first
	meta.beginMetaUpdate(txn.txnID)

	// Write meta page
	offset := int64(metaIdx) * int64(pageSize)
	if _, err := txn.env.dataFile.WriteAt(metaPage, offset); err != nil {
		return WrapError(ErrProblem, err)
	}

	// Complete two-phase update
	meta.endMetaUpdate(txn.txnID)

	// Write meta page again with updated txnid_b
	if _, err := txn.env.dataFile.WriteAt(metaPage, offset); err != nil {
		return WrapError(ErrProblem, err)
	}

	// Single sync after all writes complete (unless NoSync mode)
	noSync := txn.flags&uint32(TxnNoSync) != 0
	noMetaSync := txn.env.flags&NoMetaSync != 0
	if !noSync && !noMetaSync {
		if err := txn.env.dataFile.Sync(); err != nil {
			return WrapError(ErrProblem, err)
		}
	}

	// Update environment's meta from the current mmap
	// (writeDirtyPages already handles remapping when file grows)
	// Hold write lock to prevent readers from seeing intermediate state
	// where mt.recent is -1 during updateFromPages
	txn.env.mu.Lock()
	err := txn.env.readMeta()
	txn.env.mu.Unlock()
	if err != nil {
		return err
	}

	return nil
}

// SetUserCtx sets user context on the transaction.
func (txn *Txn) SetUserCtx(ctx any) {
	txn.userCtx = ctx
}

// UserCtx returns the user context.
func (txn *Txn) UserCtx() any {
	return txn.userCtx
}

// Stat returns statistics for a database.
func (txn *Txn) Stat(dbi DBI) (*Stat, error) {
	if !txn.valid() {
		return nil, NewError(ErrBadTxn)
	}

	if int(dbi) >= len(txn.trees) {
		return nil, NewError(ErrBadDBI)
	}

	t := &txn.trees[dbi]
	return &Stat{
		PageSize:      txn.env.pageSize,
		Depth:         uint32(t.Height),
		BranchPages:   uint64(t.BranchPages),
		LeafPages:     uint64(t.LeafPages),
		LargePages:    uint64(t.LargePages),
		OverflowPages: uint64(t.LargePages),
		Entries:       t.Items,
		Root:          uint32(t.Root),
		ModTxnID:      uint64(t.ModTxnid),
	}, nil
}

// Stat holds database statistics.
type Stat struct {
	PageSize      uint32 // Page size in bytes
	Depth         uint32 // Tree depth
	BranchPages   uint64 // Number of branch pages
	LeafPages     uint64 // Number of leaf pages
	LargePages    uint64 // Number of overflow pages
	OverflowPages uint64 // Alias for LargePages (mdbx-go compat)
	Entries       uint64 // Number of entries
	Root          uint32 // Root page number (for debugging)
	ModTxnID      uint64 // Last modification transaction ID
}

// Cmp compares two keys using the database's comparator.
func (txn *Txn) Cmp(dbi DBI, a, b []byte) int {
	txn.cacheComparator(dbi)
	if int(dbi) >= len(txn.dbiComparators) || txn.dbiComparators[dbi] == nil {
		return bytes.Compare(a, b)
	}
	return txn.compareKeys(dbi, a, b)
}

// DCmp compares two values using the database's dup comparator.
func (txn *Txn) DCmp(dbi DBI, a, b []byte) int {
	return txn.compareDupValues(dbi, a, b)
}

// ListDBI lists all named databases.
func (txn *Txn) ListDBI() ([]string, error) {
	if !txn.valid() {
		return nil, NewError(ErrBadTxn)
	}

	cursor, err := txn.OpenCursor(MainDBI)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var names []string
	for {
		key, _, err := cursor.Get(nil, nil, Next)
		if IsNotFound(err) {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, string(key))
	}

	return names, nil
}

// CreateDBI creates a new named database.
// This is a convenience wrapper around OpenDBI with Create flag.
func (txn *Txn) CreateDBI(name string) (DBI, error) {
	return txn.OpenDBISimple(name, Create)
}

// OpenRoot opens the root/main database.
func (txn *Txn) OpenRoot(flags uint) (DBI, error) {
	return MainDBI, nil
}

// StatDBI returns statistics for a database.
// This is an alias for Stat (mdbx-go compatibility).
func (txn *Txn) StatDBI(dbi DBI) (*Stat, error) {
	return txn.Stat(dbi)
}

// Flags returns the flags for a database.
// This is an alias for DBIFlags (mdbx-go compatibility).
func (txn *Txn) Flags(dbi DBI) (uint, error) {
	return txn.DBIFlags(dbi)
}

// Sub runs a nested transaction.
func (txn *Txn) Sub(fn TxnOp) error {
	// Pure Go implementation doesn't support true nested transactions
	// Just run the function in the current transaction
	return fn(txn)
}

// RunOp runs a function in the transaction.
// If terminate is true, the transaction is committed/aborted based on error.
func (txn *Txn) RunOp(fn TxnOp, terminate bool) error {
	err := fn(txn)
	if terminate {
		if err != nil {
			txn.Abort()
		} else {
			_, err = txn.Commit()
		}
	}
	return err
}

// PutReserve reserves space for a value and returns a slice to write into.
func (txn *Txn) PutReserve(dbi DBI, key []byte, n int, flags uint) ([]byte, error) {
	// For now, just do a regular put with a zero-filled slice
	value := make([]byte, n)
	err := txn.Put(dbi, key, value, flags)
	if err != nil {
		return nil, err
	}
	// Return a slice that points to the stored value
	// Note: In a real implementation, this would return the actual mmap'd location
	return value, nil
}

// ReleaseAllCursors closes all cursors in the transaction.
func (txn *Txn) ReleaseAllCursors(unbind bool) error {
	for _, c := range txn.cursors {
		if unbind {
			c.Unbind()
		} else {
			c.Close()
		}
	}
	txn.cursors = txn.cursors[:0]
	return nil
}

// Park parks a read-only transaction, releasing its reader slot.
func (txn *Txn) Park(autounpark bool) error {
	if !txn.valid() || !txn.IsReadOnly() {
		return NewError(ErrBadTxn)
	}
	// Release reader slot but keep transaction object
	if txn.readerSlot != nil {
		txn.env.lockFile.releaseReaderSlot(txn.readerSlot, txn.slotIdx)
		txn.readerSlot = nil
	}
	return nil
}

// Unpark resumes a parked transaction.
func (txn *Txn) Unpark(restartIfOusted bool) error {
	if !txn.valid() || !txn.IsReadOnly() {
		return NewError(ErrBadTxn)
	}
	// Re-acquire reader slot
	if txn.readerSlot == nil {
		slot, idx, err := txn.env.lockFile.acquireReaderSlot(cachedPID, uint64(uintptr(unsafe.Pointer(txn))))
		if err != nil {
			return WrapError(ErrReadersFull, err)
		}
		txn.readerSlot = slot
		txn.slotIdx = idx
		txn.env.lockFile.setReaderTxnid(slot, uint64(txn.txnID))
	}
	return nil
}

// EnvWarmup warms up the environment by reading pages.
func (txn *Txn) EnvWarmup(flags uint, timeout time.Duration) error {
	// No-op in pure Go implementation
	return nil
}

// TxInfo contains transaction information.
type TxInfo struct {
	ID             uint64
	ReaderLag      uint64
	SpaceUsed      uint64
	SpaceLimitSoft uint64
	SpaceLimitHard uint64
	SpaceRetired   uint64
	SpaceLeftover  uint64
	SpaceDirty     uint64
	Spill          uint64 // Pages spilled to disk
	Unspill        uint64 // Pages unspilled from disk
}

// Info returns information about the transaction.
func (txn *Txn) Info(scanRlt bool) (*TxInfo, error) {
	if !txn.valid() {
		return nil, NewError(ErrBadTxn)
	}
	return &TxInfo{
		ID: uint64(txn.txnID),
	}, nil
}
