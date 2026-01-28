package gdbx

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	mmappkg "github.com/Giulio2002/gdbx/mmap"
	"github.com/Giulio2002/gdbx/spill"
)

// sysPageSize is the system's memory page size, cached at init time.
// This is used to align database file sizes for mdbx read-only compatibility.
var sysPageSize = int64(syscall.Getpagesize())

// alignToSysPageSize rounds up a size to be aligned to the system page size.
// This is required for mdbx compatibility - mdbx rejects databases with file
// sizes not aligned to the system page size when opening in read-only mode.
func alignToSysPageSize(size int64) int64 {
	if size%sysPageSize == 0 {
		return size
	}
	return ((size / sysPageSize) + 1) * sysPageSize
}

// envSignature is the magic number for valid environments
const envSignature uint32 = 0x454E5658 // "ENVX"

// Env represents a database environment.
type Env struct {
	signature uint32
	flags     uint
	path      string
	label     Label // Environment label (mdbx-go compatibility)
	mu        sync.RWMutex

	// File handles
	dataFile *os.File
	dataMap  *mmappkg.Map
	lockFile *lockFile

	// Old mmaps waiting to be cleaned up (for COW safety)
	// These are kept alive until no readers need them
	oldMmaps   []*mmappkg.Map
	oldMmapsMu sync.Mutex

	// Transaction tracking for safe Close()
	// Close() waits for all transactions to finish before unmapping
	txnWg sync.WaitGroup

	// Configuration
	pageSize   uint32
	maxReaders uint32
	maxDBs     uint32

	// Geometry
	geoLower  uint64 // Minimum size in bytes
	geoUpper  uint64 // Maximum size in bytes
	geoNow    uint64 // Current size in bytes
	geoGrow   uint64 // Growth step in bytes
	geoShrink uint64 // Shrink threshold in bytes

	// Meta page tracking (atomic for concurrent read/write txn access)
	meta atomic.Pointer[metaTriple]

	// Transaction state
	writeTxn *Txn       // Current write transaction (if any)
	txnMu    sync.Mutex // Protects write transaction
	txnCond  *sync.Cond // Condition variable for waiting on write txn

	// Database handles
	dbis    []*dbiInfo
	dbisMu  sync.RWMutex
	mainDBI DBI
	freeDBI DBI

	// User context
	userCtx any

	// mmap version counter - incremented on each remap
	// Used by cursors to detect stale page references
	mmapVersion uint64

	// Spill buffer for dirty pages (reduces heap pressure)
	spillBuf *spill.Buffer
}

// dbiInfo holds information about an open database.
type dbiInfo struct {
	name  string
	flags uint
	tree  *tree
	cmp   func(a, b []byte) int // Key comparator
	dcmp  func(a, b []byte) int // Data comparator (for DUPSORT)
}

// NewEnv creates a new environment handle.
// The environment must be opened with Open before use.
// The label parameter is for identification purposes (mdbx-go compatibility).
func NewEnv(label Label) (*Env, error) {
	e := &Env{
		signature:  envSignature,
		label:      label,
		maxReaders: 126,
		maxDBs:     16,
		pageSize:   DefaultPageSize,
		dbis:       make([]*dbiInfo, MaxDBI),
	}
	e.txnCond = sync.NewCond(&e.txnMu)
	return e, nil
}

// valid returns true if the environment is valid.
func (e *Env) valid() bool {
	return e != nil && e.signature == envSignature
}

// Open opens the environment at the given path.
func (e *Env) Open(path string, flags uint, mode os.FileMode) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dataFile != nil {
		return NewError(ErrInvalid) // Already open
	}

	e.flags = flags
	e.path = path

	// Determine file paths
	var dataPath, lockPath string
	if flags&NoSubdir != 0 {
		dataPath = path
		lockPath = path + LockSuffix
	} else {
		// Create directory if needed
		if err := os.MkdirAll(path, mode|0700); err != nil {
			return WrapError(ErrInvalid, err)
		}
		dataPath = filepath.Join(path, DataFileName)
		lockPath = filepath.Join(path, LockFileName)
	}

	// Open lock file first
	create := flags&ReadOnly == 0
	lf, err := openLockFile(lockPath, int(e.maxReaders), create)
	if err != nil {
		return WrapError(ErrInvalid, err)
	}
	e.lockFile = lf

	// Open data file
	fileFlags := os.O_RDWR
	if flags&ReadOnly != 0 {
		fileFlags = os.O_RDONLY
	} else {
		fileFlags |= os.O_CREATE
	}

	dataFile, err := os.OpenFile(dataPath, fileFlags, mode)
	if err != nil {
		e.lockFile.close()
		return WrapError(ErrInvalid, err)
	}
	e.dataFile = dataFile

	// Get file info
	fi, err := dataFile.Stat()
	if err != nil {
		e.closeFiles()
		return WrapError(ErrInvalid, err)
	}

	fileSize := fi.Size()

	// Initialize new database if empty
	if fileSize == 0 {
		if flags&ReadOnly != 0 {
			e.closeFiles()
			return NewError(ErrInvalid)
		}
		if err := e.initNewDB(); err != nil {
			e.closeFiles()
			return err
		}
		fi, _ = dataFile.Stat()
		fileSize = fi.Size()
	}

	// Memory-map the data file
	writable := flags&ReadOnly == 0 && flags&WriteMap != 0
	dm, err := mmappkg.New(int(dataFile.Fd()), 0, int(fileSize), writable)
	if err != nil {
		e.closeFiles()
		return WrapError(ErrInvalid, err)
	}
	e.dataMap = dm

	// Read and validate meta pages
	if err := e.readMeta(); err != nil {
		e.closeFiles()
		return err
	}

	// Update geometry from meta
	m := e.meta.Load().recentMeta()
	if m == nil {
		e.closeFiles()
		return NewError(ErrCorrupted)
	}
	e.pageSize = m.pageSize()
	if e.pageSize == 0 {
		e.pageSize = DefaultPageSize
	}

	e.geoLower = uint64(m.Geometry.Lower) * uint64(e.pageSize)
	e.geoUpper = uint64(m.Geometry.DBPgsize) * uint64(e.pageSize) // DBPgsize holds upper limit, not page size
	e.geoNow = uint64(m.Geometry.Now) * uint64(e.pageSize)

	// Initialize core DBIs
	e.freeDBI = FreeDBI
	e.mainDBI = MainDBI

	// Initialize spill buffer for dirty pages (reduces heap pressure)
	// Only for writable environments
	if flags&ReadOnly == 0 {
		spillPath := path + ".spill"
		if flags&NoSubdir != 0 {
			spillPath = path + "-spill"
		}
		buf, err := spill.New(spillPath, e.pageSize, spill.DefaultInitialCap)
		if err != nil {
			e.closeFiles()
			return WrapError(ErrProblem, err)
		}
		e.spillBuf = buf
	}

	return nil
}

// initNewDB initializes a new database file.
func (e *Env) initNewDB() error {
	// Use geometry for initial size, with minimum of 3 meta pages
	initialSize := int64(e.geoNow)
	minSize := int64(NumMetas) * int64(e.pageSize)
	if initialSize < minSize {
		initialSize = minSize
	}

	// Align to system page size for mdbx read-only compatibility
	initialSize = alignToSysPageSize(initialSize)

	// Extend file to initial geometry size
	if err := e.dataFile.Truncate(initialSize); err != nil {
		return WrapError(ErrInvalid, err)
	}

	// Write meta pages
	for i := 0; i < NumMetas; i++ {
		metaPage := make([]byte, e.pageSize)
		txnID := txnid(InitialTxnID - uint64(NumMetas-1-i))

		// Write page header at offset 0
		// Meta page headers have txnid=0 (the actual txnid is in the meta body)
		pageHdr := (*pageHeader)(unsafe.Pointer(&metaPage[0]))
		pageHdr.PageNo = pgno(i)
		pageHdr.Flags = pageMeta
		pageHdr.Txnid = 0 // Meta pages use txnid from meta body, not page header

		// Write meta content starting after page header (offset 20)
		m := (*meta)(unsafe.Pointer(&metaPage[pageHeaderSize]))
		initMeta(m, e.pageSize, txnID)

		offset := int64(i) * int64(e.pageSize)
		if _, err := e.dataFile.WriteAt(metaPage, offset); err != nil {
			return WrapError(ErrInvalid, err)
		}
	}

	return e.dataFile.Sync()
}

// readMeta reads and validates all meta pages.
// Always creates a new metaTriple and atomically swaps to avoid races
// between concurrent read and write transactions.
func (e *Env) readMeta() error {
	data := e.dataMap.Data()
	if len(data) < int(e.pageSize)*NumMetas {
		return NewError(ErrCorrupted)
	}

	var pages [NumMetas][]byte
	for i := 0; i < NumMetas; i++ {
		// Meta content starts after page header (20 bytes)
		start := i*int(e.pageSize) + pageHeaderSize
		end := (i + 1) * int(e.pageSize)
		pages[i] = data[start:end]
	}

	// Always create a new metaTriple and atomically swap to avoid race
	// with concurrent readers calling recentMeta()
	mt, err := newMetaTriple(pages)
	if err != nil {
		return WrapError(ErrCorrupted, err)
	}

	e.meta.Store(mt)
	return nil
}

// closeFiles closes all open files.
func (e *Env) closeFiles() {
	if e.spillBuf != nil {
		e.spillBuf.Close(true) // Delete spill file on env close
		e.spillBuf = nil
	}
	if e.dataMap != nil {
		e.dataMap.Close()
		e.dataMap = nil
	}
	// Clean up any old mmaps kept for COW safety
	e.oldMmapsMu.Lock()
	for _, m := range e.oldMmaps {
		if m != nil {
			m.Close()
		}
	}
	e.oldMmaps = nil
	e.oldMmapsMu.Unlock()

	if e.dataFile != nil {
		e.dataFile.Close()
		e.dataFile = nil
	}
	if e.lockFile != nil {
		e.lockFile.close()
		e.lockFile = nil
	}
}

// tryCleanupOldMmaps attempts to clean up old mmaps if no readers need them.
// Called periodically (e.g., when a read transaction ends).
//
// NOTE: This function is currently disabled because we can't reliably determine
// when it's safe to unmap old mmaps. A read transaction caches mmapData at
// start time, but hasActiveReaders() doesn't track which mmap each reader is using.
// Unmapping an old mmap while a reader still has a pointer to it causes SIGSEGV.
// Old mmaps are cleaned up in closeFiles() on environment close instead.
func (e *Env) tryCleanupOldMmaps() {
	// Disabled for safety - old mmaps are cleaned up in closeFiles() instead
	// TODO: Implement proper tracking of which transactions use which mmap
	return

	/*
		e.oldMmapsMu.Lock()
		if len(e.oldMmaps) == 0 {
			e.oldMmapsMu.Unlock()
			return
		}

		// Check if any readers are active
		if e.lockFile != nil && e.lockFile.hasActiveReaders() {
			// Readers are active, can't clean up yet
			e.oldMmapsMu.Unlock()
			return
		}

		// No active readers, safe to clean up
		for _, m := range e.oldMmaps {
			if m != nil {
				m.unmap()
			}
		}
		e.oldMmaps = nil
		e.oldMmapsMu.Unlock()
	*/
}

// Close closes the environment and releases resources.
// Waits for all active read transactions to finish before unmapping.
func (e *Env) Close() {
	if !e.valid() {
		return
	}

	// Mark as closing first (under lock) to prevent new readers
	e.mu.Lock()
	e.signature = 0
	e.mu.Unlock()

	// Wait for all active readers to finish
	// This prevents SIGSEGV from readers accessing unmapped memory
	e.txnWg.Wait()

	// Now safe to close files and unmap
	e.mu.Lock()
	defer e.mu.Unlock()

	e.closeFiles()
}

// CloseEx closes the environment with optional sync control.
func (e *Env) CloseEx(dontSync bool) {
	if !dontSync && e.dataMap != nil && e.dataMap.Writable() {
		e.dataMap.Sync()
	}
	e.Close()
}

// Sync flushes the environment to disk.
// If force is true, a synchronous flush is performed.
// If nonblock is true, the function returns immediately if a sync is already in progress.
func (e *Env) Sync(force bool, nonblock bool) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.dataMap == nil {
		return NewError(ErrInvalid)
	}

	if force {
		return e.dataMap.Sync()
	}
	return e.dataMap.SyncAsync()
}

// SetMaxDBs sets the maximum number of named databases.
// Must be called before Open.
func (e *Env) SetMaxDBs(dbs uint32) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dataFile != nil {
		return NewError(ErrInvalid) // Already open
	}

	if dbs > MaxDBI {
		dbs = MaxDBI
	}
	e.maxDBs = dbs

	return nil
}

// SetMaxReaders sets the maximum number of reader slots.
// Must be called before Open.
func (e *Env) SetMaxReaders(readers uint32) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dataFile != nil {
		return NewError(ErrInvalid) // Already open
	}

	e.maxReaders = readers

	return nil
}

// SetPageSize sets the page size for a new database.
// Must be called before Open. Must be a power of 2 between 256 and 65536.
func (e *Env) SetPageSize(size uint32) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}
	if size < MinPageSize || size > MaxPageSize {
		return NewError(ErrInvalid)
	}
	// Must be power of 2
	if size&(size-1) != 0 {
		return NewError(ErrInvalid)
	}
	e.pageSize = size
	return nil
}

// SetGeometry sets the database size parameters.
func (e *Env) SetGeometry(sizeLower, sizeNow, sizeUpper, growthStep, shrinkThreshold int64, pageSize int) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if pageSize > 0 {
		if pageSize < MinPageSize || pageSize > MaxPageSize {
			return NewError(ErrInvalid)
		}
		// Must be power of 2
		if pageSize&(pageSize-1) != 0 {
			return NewError(ErrInvalid)
		}
		e.pageSize = uint32(pageSize)
	}

	if sizeLower > 0 {
		e.geoLower = uint64(sizeLower)
	}
	if sizeUpper > 0 {
		e.geoUpper = uint64(sizeUpper)
	}
	if sizeNow > 0 {
		e.geoNow = uint64(sizeNow)
	}
	if growthStep > 0 {
		e.geoGrow = uint64(growthStep)
	}
	if shrinkThreshold > 0 {
		e.geoShrink = uint64(shrinkThreshold)
	}

	return nil
}

// Path returns the environment path.
func (e *Env) Path() string {
	return e.path
}

// Flags returns the environment flags.
func (e *Env) Flags() (uint, error) {
	if !e.valid() {
		return 0, NewError(ErrInvalid)
	}
	return e.flags, nil
}

// Label returns the environment label.
func (e *Env) Label() Label {
	return e.label
}

// MaxKeySize returns the maximum key size for the environment.
// This matches libmdbx's branch page constraint:
// BRANCH_NODE_MAX = EVEN_FLOOR((PAGEROOM - indx - node) / 2 - indx)
// MaxKeySize = BRANCH_NODE_MAX - NODESIZE
func (e *Env) MaxKeySize() int {
	ps := int(e.pageSize)
	if ps == 0 {
		ps = DefaultPageSize
	}
	pageRoom := ps - 20                          // pageSize - header
	branchNodeMax := ((pageRoom-2-8)/2 - 2) &^ 1 // EVEN_FLOOR formula
	return branchNodeMax - 8
}

// MaxValSize returns the maximum inline value size.
// Values larger than this are stored on overflow pages automatically.
func (e *Env) MaxValSize() int {
	ps := int(e.pageSize)
	if ps == 0 {
		ps = DefaultPageSize
	}
	// Need at least 2 entries per leaf page
	// Each entry: nodeSize(8) + key(1 min) + value
	// maxVal = pageSize/2 - nodeSize - minKey - indxSize
	return ps/2 - 8 - 1 - 2
}

// LeafNodeMax returns the maximum size of a leaf node.
// This matches libmdbx's leaf_nodemax calculation.
func (e *Env) LeafNodeMax() int {
	ps := int(e.pageSize)
	if ps == 0 {
		ps = DefaultPageSize
	}
	// leaf_nodemax = pageSize - HeaderSize - 2*sizeof(indx_t)
	// We need room for at least 2 entries, so (pageSize - HeaderSize) / 2
	return (ps - 20) / 2
}

// SubPageLimit returns the maximum inline sub-page size.
// When a sub-page exceeds this, it's converted to a sub-tree.
// This matches libmdbx's subpage_limit calculation.
func (e *Env) SubPageLimit() int {
	// subpage_limit = leaf_nodemax - NODESIZE
	return e.LeafNodeMax() - 8
}

// SetUserCtx sets user context on the environment.
func (e *Env) SetUserCtx(ctx any) {
	e.userCtx = ctx
}

// UserCtx returns the user context.
func (e *Env) UserCtx() any {
	return e.userCtx
}

// BeginTxn starts a new transaction.
func (e *Env) BeginTxn(parent *Txn, flags uint) (*Txn, error) {
	if !e.valid() {
		return nil, NewError(ErrInvalid)
	}

	if flags&TxnReadOnly != 0 {
		return e.beginReadTxn()
	}
	return e.beginWriteTxn(parent, flags)
}

// beginReadTxn starts a read-only transaction.
func (e *Env) beginReadTxn() (*Txn, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.dataMap == nil {
		return nil, NewError(ErrInvalid)
	}

	// Acquire reader slot (use cached PID to avoid syscall)
	slot, slotIdx, err := e.lockFile.acquireReaderSlot(cachedPID, 0)
	if err != nil {
		return nil, WrapError(ErrReadersFull, err)
	}

	// Get current meta (atomic load for concurrent access with write txn)
	meta := e.meta.Load().recentMeta()
	if meta == nil {
		e.lockFile.releaseReaderSlot(slot, slotIdx)
		return nil, NewError(ErrCorrupted)
	}

	// Get transaction from cache
	txn := getReadTxnFromCache()

	// Initialize/reset the transaction
	txn.signature = txnSignature
	txn.flags = uint32(TxnReadOnly)
	txn.env = e
	txn.txnID = meta.txnID()
	txn.parent = nil
	txn.readerSlot = slot
	txn.slotIdx = slotIdx
	// Keep pageCache and pooledPageStructs backing allocation if they exist
	// They were cleared during previous abort, just ensure they're ready for reuse
	if txn.pooledPageStructs != nil {
		txn.pooledPageStructs = txn.pooledPageStructs[:0]
	}
	// pageCache was cleared during abort, no need to do anything
	txn.userCtx = nil

	// Reuse or allocate slices - avoid clearing loops by using clear() builtin
	maxDBs := int(e.maxDBs)
	if cap(txn.dbiComparators) >= maxDBs {
		txn.dbiComparators = txn.dbiComparators[:maxDBs]
		clear(txn.dbiComparators)
	} else {
		txn.dbiComparators = make([]func(a, b []byte) int, maxDBs)
	}

	if cap(txn.dbiUsesDefaultCmp) >= maxDBs {
		txn.dbiUsesDefaultCmp = txn.dbiUsesDefaultCmp[:maxDBs]
		clear(txn.dbiUsesDefaultCmp)
	} else {
		txn.dbiUsesDefaultCmp = make([]bool, maxDBs)
	}

	if cap(txn.trees) >= maxDBs {
		txn.trees = txn.trees[:maxDBs]
	} else {
		txn.trees = make([]tree, maxDBs)
	}

	// Set reader's txnid
	e.lockFile.setReaderTxnid(slot, uint64(meta.txnID()))

	// Initialize mmap cache while holding the lock to avoid race with write txn remap
	txn.mmapData = e.dataMap.Data()
	txn.pageSize = e.pageSize

	// Track reader for safe Close() - Close() will wait for all readers to finish
	e.txnWg.Add(1)

	// Copy tree state for core DBIs
	txn.trees[FreeDBI] = meta.GCTree
	txn.trees[MainDBI] = meta.MainTree

	// Copy tree state for named DBIs that are already opened
	// Use maxDBs (user-configured limit) instead of len(e.dbis) which is MaxDBI (32765)
	e.dbisMu.RLock()
	for i := CoreDBs; i < maxDBs; i++ {
		if e.dbis[i] != nil && e.dbis[i].tree != nil {
			txn.trees[i] = *e.dbis[i].tree
		}
	}
	e.dbisMu.RUnlock()

	return txn, nil
}

// beginWriteTxn starts a write transaction.
func (e *Env) beginWriteTxn(parent *Txn, flags uint) (*Txn, error) {
	e.txnMu.Lock()

	// Wait for any existing write transaction to finish
	for e.writeTxn != nil {
		e.txnCond.Wait()
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.dataMap == nil {
		e.txnMu.Unlock()
		return nil, NewError(ErrInvalid)
	}

	if e.flags&ReadOnly != 0 {
		e.txnMu.Unlock()
		return nil, NewError(ErrPermissionDenied)
	}

	// Acquire writer lock
	if err := e.lockFile.lockWriter(); err != nil {
		e.txnMu.Unlock()
		return nil, WrapError(ErrBusy, err)
	}

	// Re-read meta in case it changed
	if err := e.readMeta(); err != nil {
		e.lockFile.unlockWriter()
		e.txnMu.Unlock()
		return nil, err
	}

	meta := e.meta.Load().recentMeta()
	if meta == nil {
		e.lockFile.unlockWriter()
		e.txnMu.Unlock()
		return nil, NewError(ErrCorrupted)
	}

	// Get transaction from cache
	txn := getWriteTxnFromCache()
	txn.signature = txnSignature
	txn.flags = uint32(flags)
	txn.env = e
	txn.txnID = meta.txnID() + 1
	txn.parent = parent
	txn.allocatedPg = meta.Geometry.Now
	txn.cursors = nil
	txn.dbiDirty = nil
	txn.userCtx = nil

	// Clear dirty page tracker for reuse
	txn.dirtyTracker.clear()
	txn.hasNonMmapPages = false

	// Reuse or create free pages slice
	if txn.freePages == nil {
		txn.freePages = make([]pgno, 0, 16)
	} else {
		txn.freePages = txn.freePages[:0]
	}

	// Reuse or create caches
	if txn.dbiComparators == nil || len(txn.dbiComparators) < int(e.maxDBs) {
		txn.dbiComparators = make([]func(a, b []byte) int, e.maxDBs)
	} else {
		clear(txn.dbiComparators[:e.maxDBs])
	}

	if txn.dbiUsesDefaultCmp == nil || len(txn.dbiUsesDefaultCmp) < int(e.maxDBs) {
		txn.dbiUsesDefaultCmp = make([]bool, e.maxDBs)
	} else {
		clear(txn.dbiUsesDefaultCmp[:e.maxDBs])
	}

	// Reuse or create trees slice
	if txn.trees == nil || len(txn.trees) < int(e.maxDBs) {
		txn.trees = make([]tree, e.maxDBs)
	}

	// Initialize mmap cache while holding the lock
	txn.mmapData = e.dataMap.Data()
	txn.pageSize = e.pageSize

	// Track transaction for safe Close() - Close() will wait for all transactions to finish
	e.txnWg.Add(1)

	// Copy tree state for core DBIs
	txn.trees[FreeDBI] = meta.GCTree
	txn.trees[MainDBI] = meta.MainTree

	// Copy tree state for named DBIs that are already opened
	// Use e.maxDBs (user-configured limit) instead of len(e.dbis) which is MaxDBI (32765)
	e.dbisMu.RLock()
	for i := CoreDBs; i < int(e.maxDBs); i++ {
		if e.dbis[i] != nil && e.dbis[i].tree != nil {
			txn.trees[i] = *e.dbis[i].tree
		}
	}
	e.dbisMu.RUnlock()

	e.writeTxn = txn
	e.txnMu.Unlock()

	return txn, nil
}

// getPageData returns raw page data without allocating a page struct.
// This is for allocation-free hot paths in read operations.
func (e *Env) getPageData(pg pgno) ([]byte, error) {
	if e.dataMap == nil {
		return nil, NewError(ErrInvalid)
	}

	data := e.dataMap.Data()
	offset := uint64(pg) * uint64(e.pageSize)
	end := offset + uint64(e.pageSize)

	if end > uint64(len(data)) {
		return nil, NewError(ErrPageNotFound)
	}

	return data[offset:end], nil
}

// EnvInfoGeo contains geometry information for an environment.
type EnvInfoGeo struct {
	Lower   uint64 // Lower limit for datafile size
	Upper   uint64 // Upper limit for datafile size
	Current uint64 // Current datafile size
	Shrink  uint64 // Shrink threshold in bytes
	Grow    uint64 // Growth step in bytes
}

// EnvInfoPageOps contains page operation statistics.
type EnvInfoPageOps struct {
	Newly    uint64 // Newly allocated pages
	Cow      uint64 // Copy-on-write pages
	Clone    uint64 // Cloned pages
	Split    uint64 // Page splits
	Merge    uint64 // Page merges
	Spill    uint64 // Spilled dirty pages
	Unspill  uint64 // Unspilled pages
	Wops     uint64 // Write operations
	Minicore uint64 // Minicore pages
	Prefault uint64 // Prefaulted pages
	Msync    uint64 // Memory sync operations
	Fsync    uint64 // File sync operations
}

// EnvInfo holds environment information.
type EnvInfo struct {
	Geo               EnvInfoGeo
	PageOps           EnvInfoPageOps
	MapSize           int64
	LastPNO           int64 // Alias for LastPgNo (mdbx-go compatibility)
	LastPgNo          int64
	LastTxnID         uint64
	RecentTxnID       uint64
	LatterReaderTxnID uint64
	MaxReaders        uint32
	NumReaders        uint32
	PageSize          uint32
	SystemPageSize    uint32
	MiLastPgNo        uint64
	AutoSyncThreshold uint64
	UnsyncedBytes     uint64
	SinceSync         Duration16dot16
	AutosyncPeriod    Duration16dot16
	SinceReaderCheck  Duration16dot16
	Flags             uint32
	// Legacy fields for backward compatibility
	GeoLower   uint64
	GeoUpper   uint64
	GeoCurrent uint64
	GeoShrink  uint64
	GeoGrow    uint64
}

// Stat returns statistics about the environment.
func (e *Env) Stat() (*Stat, error) {
	mt := e.meta.Load()
	if mt == nil {
		return nil, NewError(ErrInvalid)
	}
	m := mt.recentMeta()
	if m == nil {
		return nil, NewError(ErrCorrupted)
	}
	return &Stat{
		PageSize:      e.pageSize,
		Depth:         uint32(m.MainTree.Height),
		BranchPages:   uint64(m.MainTree.BranchPages),
		LeafPages:     uint64(m.MainTree.LeafPages),
		LargePages:    uint64(m.MainTree.LargePages),
		OverflowPages: uint64(m.MainTree.LargePages),
		Entries:       m.MainTree.Items,
		ModTxnID:      uint64(m.MainTree.ModTxnid),
	}, nil
}

// Info returns information about the environment.
// The txn parameter is optional and can be nil.
func (e *Env) Info(txn *Txn) (*EnvInfo, error) {
	mt := e.meta.Load()
	if mt == nil {
		return nil, NewError(ErrInvalid)
	}
	m := mt.recentMeta()
	if m == nil {
		return nil, NewError(ErrCorrupted)
	}
	g := m.Geometry

	geoLower := uint64(g.Lower) * uint64(e.pageSize)
	geoUpper := uint64(g.DBPgsize) * uint64(e.pageSize)
	geoCurrent := uint64(g.Now) * uint64(e.pageSize)
	geoShrink := uint64(g.ShrinkPV)
	geoGrow := uint64(g.GrowPV)
	lastPgNo := int64(g.Now)
	mapSize := int64(g.Now) * int64(e.pageSize)
	lastTxnID := uint64(m.txnID())

	// Use txn info if provided
	if txn != nil && txn.valid() {
		lastTxnID = uint64(txn.txnID)
	}

	return &EnvInfo{
		Geo: EnvInfoGeo{
			Lower:   geoLower,
			Upper:   geoUpper,
			Current: geoCurrent,
			Shrink:  geoShrink,
			Grow:    geoGrow,
		},
		PageOps:           EnvInfoPageOps{}, // Page op stats not tracked yet
		MapSize:           mapSize,
		LastPNO:           lastPgNo,
		LastPgNo:          lastPgNo,
		LastTxnID:         lastTxnID,
		RecentTxnID:       uint64(m.txnID()),
		LatterReaderTxnID: 0, // Would need to scan reader table
		MaxReaders:        e.maxReaders,
		NumReaders:        0, // Would need to scan reader table
		PageSize:          e.pageSize,
		SystemPageSize:    4096, // OS page size, typically 4KB
		MiLastPgNo:        uint64(lastPgNo),
		AutoSyncThreshold: 0,
		UnsyncedBytes:     0,
		SinceSync:         0,
		AutosyncPeriod:    0,
		SinceReaderCheck:  0,
		Flags:             uint32(e.flags),
		GeoLower:          geoLower,
		GeoUpper:          geoUpper,
		GeoCurrent:        geoCurrent,
		GeoShrink:         geoShrink,
		GeoGrow:           geoGrow,
	}, nil
}

// SetEnvFlags sets or clears environment flags.
func (e *Env) SetEnvFlags(flags uint, enable bool) error {
	if enable {
		e.flags |= flags
	} else {
		e.flags &^= flags
	}
	return nil
}

// MaxDBs returns the maximum number of named databases.
func (e *Env) MaxDBs() uint32 {
	return e.maxDBs
}

// MaxReaders returns the maximum number of readers.
func (e *Env) MaxReaders() uint32 {
	return e.maxReaders
}

// SetFlags sets flags in the environment.
func (e *Env) SetFlags(flags uint) error {
	e.flags |= flags
	return nil
}

// UnsetFlags clears flags in the environment.
func (e *Env) UnsetFlags(flags uint) error {
	e.flags &^= flags
	return nil
}

// CloseDBI closes a database handle.
// This is normally unnecessary as handles are closed when the environment closes.
func (e *Env) CloseDBI(db DBI) {
	e.dbisMu.Lock()
	defer e.dbisMu.Unlock()
	if int(db) < len(e.dbis) {
		e.dbis[db] = nil
	}
}

// FD returns the file descriptor of the data file.
func (e *Env) FD() (uintptr, error) {
	if e.dataFile == nil {
		return 0, NewError(ErrInvalid)
	}
	return e.dataFile.Fd(), nil
}

// ReaderCheck clears stale entries from the reader lock table.
// Returns the number of stale readers cleared.
func (e *Env) ReaderCheck() (int, error) {
	if e.lockFile == nil {
		return 0, NewError(ErrInvalid)
	}
	return e.lockFile.cleanupStaleReaders(), nil
}

// ReaderInfo contains information about a reader slot (mdbx-go compatibility).
type ReaderInfo struct {
	Slot   int
	TxnID  uint64
	PID    int
	Thread uint64
	Bytes  uint64
	RetxL  uint64
}

// ReaderList returns information about all active readers.
func (e *Env) ReaderList(fn func(info ReaderInfo) error) error {
	if e.lockFile == nil {
		return NewError(ErrInvalid)
	}
	// Iterate through reader slots
	slots := e.lockFile.slots
	if e.lockFile.lockless {
		slots = e.lockFile.memSlots
	}
	for i := 0; i < len(slots); i++ {
		slot := &slots[i]
		if slot.txnid == 0 {
			continue
		}
		info := ReaderInfo{
			Slot:   i,
			TxnID:  slot.txnid,
			PID:    int(slot.pid),
			Thread: slot.tid,
			Bytes:  uint64(slot.snapshotPagesUsed) * uint64(e.pageSize),
			RetxL:  slot.snapshotPagesRetired,
		}
		if err := fn(info); err != nil {
			return err
		}
	}
	return nil
}

// Copy copies the environment to a path.
func (e *Env) Copy(path string, flags uint) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	// Open destination file
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return e.CopyFD(f.Fd(), flags)
}

// CopyFD copies the environment to a file descriptor.
func (e *Env) CopyFD(fd uintptr, flags uint) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	// Start a read transaction to get a consistent snapshot
	txn, err := e.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		return err
	}
	defer txn.Abort()

	// Get the source file
	srcFD, err := e.FD()
	if err != nil {
		return err
	}

	// Get file size from meta
	m := e.meta.Load().recentMeta()
	if m == nil {
		return NewError(ErrCorrupted)
	}
	fileSize := int64(m.Geometry.Now) * int64(e.pageSize)

	// Copy the data
	srcFile := os.NewFile(srcFD, "")
	dstFile := os.NewFile(fd, "")

	// Reset to beginning
	if _, err := srcFile.Seek(0, 0); err != nil {
		return err
	}

	buf := make([]byte, 64*1024) // 64KB buffer
	var written int64
	for written < fileSize {
		toRead := fileSize - written
		if toRead > int64(len(buf)) {
			toRead = int64(len(buf))
		}
		n, err := srcFile.Read(buf[:toRead])
		if err != nil {
			return err
		}
		if _, err := dstFile.Write(buf[:n]); err != nil {
			return err
		}
		written += int64(n)
	}

	// Sync destination
	return dstFile.Sync()
}

// UpdateLocked behaves like Update but does not lock the calling goroutine.
// Use this if the calling goroutine is already locked to its thread.
func (e *Env) UpdateLocked(fn TxnOp) error {
	txn, err := e.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		return err
	}
	err = fn(txn)
	if err != nil {
		txn.Abort()
		return err
	}
	_, err = txn.Commit()
	return err
}

// SetDebug sets debug flags for this environment (mdbx-go compatibility).
func (e *Env) SetDebug(flags uint) error {
	// Debug flags are global in gdbx, delegate to global function
	SetDebug(flags)
	return nil
}

// Option constants for GetOption/SetOption (mdbx-go compatibility)
const (
	OptMaxDB                        uint = 0
	OptMaxReaders                   uint = 1
	OptSyncBytes                    uint = 2
	OptSyncPeriod                   uint = 3
	OptRpAugmentLimit               uint = 4
	OptLooseLimit                   uint = 5
	OptDpReserveLimit               uint = 6
	OptDpReverseLimit               uint = 6 // Alias for OptDpReserveLimit (mdbx-go typo compatibility)
	OptTxnDpLimit                   uint = 7
	OptTxnDpInitial                 uint = 8
	OptSpillMinDenominator          uint = 9
	OptSpillMaxDenominator          uint = 10
	OptSpillParent4ChildDenominator uint = 11
	OptMergeThreshold16dot16Percent uint = 12
	OptWriteThroughThreshold        uint = 13
	OptPreFaultWriteEnable          uint = 14
	OptPreferWafInsteadofBalance    uint = 15
	OptGCTimeLimit                  uint = 16
)

// GetOption returns an environment option value.
func (e *Env) GetOption(option uint) (uint64, error) {
	if !e.valid() {
		return 0, NewError(ErrInvalid)
	}
	switch option {
	case OptMaxDB:
		return uint64(e.maxDBs), nil
	case OptMaxReaders:
		return uint64(e.maxReaders), nil
	case OptSyncBytes:
		return 0, nil // Not implemented
	case OptSyncPeriod:
		return 0, nil // Not implemented
	default:
		return 0, nil
	}
}

// SetOption sets an environment option value.
func (e *Env) SetOption(option uint, value uint64) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}
	switch option {
	case OptMaxDB:
		e.maxDBs = uint32(value)
	case OptMaxReaders:
		e.maxReaders = uint32(value)
	default:
		// Ignore unknown options
	}
	return nil
}

// GetSyncBytes returns the threshold for auto-sync.
func (e *Env) GetSyncBytes() (uint, error) {
	return 0, nil // Not implemented - returns 0 (disabled)
}

// SetSyncBytes sets the threshold for auto-sync.
func (e *Env) SetSyncBytes(threshold uint) error {
	// Not implemented - silently ignore
	return nil
}

// GetSyncPeriod returns the period for auto-sync.
func (e *Env) GetSyncPeriod() (time.Duration, error) {
	return 0, nil // Not implemented - returns 0 (disabled)
}

// SetSyncPeriod sets the period for auto-sync.
func (e *Env) SetSyncPeriod(period time.Duration) error {
	// Not implemented - silently ignore
	return nil
}

// SetStrictThreadMode sets strict thread mode.
// In gdbx (pure Go), this is a no-op since Go handles threading differently.
func (e *Env) SetStrictThreadMode(mode bool) {
	// No-op in pure Go implementation
}

// Global page data cache - shared across all environments to maximize reuse
// This avoids sync.Pool's internal allocation overhead while maintaining
// page reuse across transaction boundaries.
var (
	globalPageCache     = make(map[uint32]*pageDataCache) // keyed by page size
	globalPageCacheMu   sync.Mutex
	defaultPageCacheCap = 8192 // Cache up to 8K pages per size (~32MB at 4KB pages)
)

type pageDataCache struct {
	pages [][]byte
	size  uint32
}

// getGlobalPageCache returns the global page cache for a given page size
func getGlobalPageCache(pageSize uint32) *pageDataCache {
	globalPageCacheMu.Lock()
	defer globalPageCacheMu.Unlock()

	cache := globalPageCache[pageSize]
	if cache == nil {
		cache = &pageDataCache{
			pages: make([][]byte, 0, defaultPageCacheCap),
			size:  pageSize,
		}
		globalPageCache[pageSize] = cache
	}
	return cache
}

// getPageDataFromCache returns a page data buffer from the global cache.
func (e *Env) getPageDataFromCache() []byte {
	cache := getGlobalPageCache(e.pageSize)

	globalPageCacheMu.Lock()
	n := len(cache.pages)
	if n == 0 {
		globalPageCacheMu.Unlock()
		return make([]byte, e.pageSize)
	}
	// Pop from end (LIFO for better cache locality)
	data := cache.pages[n-1]
	cache.pages = cache.pages[:n-1]
	globalPageCacheMu.Unlock()
	return data
}

// returnPageDataToCache returns page data buffers to the global cache.
func (e *Env) returnPageDataToCache(pages [][]byte) {
	if len(pages) == 0 {
		return
	}

	cache := getGlobalPageCache(e.pageSize)

	globalPageCacheMu.Lock()
	// Calculate how many we can add
	available := defaultPageCacheCap - len(cache.pages)
	if available <= 0 {
		globalPageCacheMu.Unlock()
		return // Cache full, let GC handle excess
	}

	toAdd := len(pages)
	if toAdd > available {
		toAdd = available
	}

	// Append to cache
	cache.pages = append(cache.pages, pages[:toAdd]...)
	globalPageCacheMu.Unlock()
}

// Global page struct cache - persistent across GC cycles
var (
	pageStructCache   []*page
	pageStructCacheMu sync.Mutex
)

// getPageStructFromCache returns a page struct from the cache or allocates new.
func getPageStructFromCache(data []byte) *page {
	pageStructCacheMu.Lock()
	if n := len(pageStructCache); n > 0 {
		p := pageStructCache[n-1]
		pageStructCache = pageStructCache[:n-1]
		pageStructCacheMu.Unlock()
		p.Data = data
		return p
	}
	pageStructCacheMu.Unlock()
	return &page{Data: data}
}

// returnPageStructsToCache returns page structs to the cache.
func returnPageStructsToCache(pages []*page) {
	if len(pages) == 0 {
		return
	}
	pageStructCacheMu.Lock()
	for _, p := range pages {
		p.Data = nil
	}
	pageStructCache = append(pageStructCache, pages...)
	pageStructCacheMu.Unlock()
}

// getMmapPageData returns page data directly from mmap for WriteMap mode.
// This returns a slice pointing directly into the mmap - zero allocation.
// Returns nil if the page is beyond mmap bounds.
func (e *Env) getMmapPageData(pn pgno) []byte {
	if e.dataMap == nil {
		return nil
	}
	data := e.dataMap.Data()
	offset := uint64(pn) * uint64(e.pageSize)
	end := offset + uint64(e.pageSize)
	if end > uint64(len(data)) {
		return nil
	}
	return data[offset:end]
}

// isWriteMap returns true if WriteMap mode is enabled.
func (e *Env) isWriteMap() bool {
	return e.flags&WriteMap != 0
}

// extendMmap extends the mmap to accommodate the given page number.
// This is called when WriteMap mode needs pages beyond current mmap bounds.
// Returns true if extension was successful.
func (e *Env) extendMmap(needPgno pgno) bool {
	if e.dataMap == nil {
		return false
	}

	// Calculate needed size (page after needPgno must fit)
	neededSize := int64(needPgno+1) * int64(e.pageSize)

	// Check if already large enough
	currentSize := int64(len(e.dataMap.Data()))
	if neededSize <= currentSize {
		return true
	}

	// Calculate new size with growth step
	growStep := int64(e.geoGrow)
	if growStep <= 0 {
		growStep = 64 * 1024 * 1024 // Default 64MB growth
	}
	newSize := ((neededSize + growStep - 1) / growStep) * growStep

	// Align to system page size for mdbx read-only compatibility
	newSize = alignToSysPageSize(newSize)

	// Cap at upper limit
	if e.geoUpper > 0 && uint64(newSize) > e.geoUpper {
		newSize = int64(e.geoUpper)
		if neededSize > newSize {
			return false // Can't grow enough
		}
	}

	// Extend the file first
	if err := e.dataFile.Truncate(newSize); err != nil {
		return false
	}

	// Remap
	if err := e.dataMap.Remap(newSize); err != nil {
		return false
	}

	// Increment mmap version so cursors know to refresh their cached page references
	e.mmapVersion++

	// Reload meta pointers to point to new mmap
	if err := e.readMeta(); err != nil {
		return false
	}

	return true
}

// PreExtendMmap extends the mmap to the specified size.
// Call this after Open() when using WriteMap mode to avoid heap allocations
// during write transactions. The size should be large enough to accommodate
// all pages that will be written during the transaction.
// NOTE: This should be called before any transactions start, as remapping
// while transactions are active can cause issues.
// Returns nil on success, error on failure.
func (e *Env) PreExtendMmap(size int64) error {
	if e.dataMap == nil {
		return NewError(ErrInvalid)
	}

	currentSize := int64(len(e.dataMap.Data()))
	if size <= currentSize {
		return nil // Already large enough
	}

	// Align to system page size for mdbx read-only compatibility
	size = alignToSysPageSize(size)

	// Cap at upper limit
	if e.geoUpper > 0 && uint64(size) > e.geoUpper {
		size = int64(e.geoUpper)
	}

	// Extend the file
	if err := e.dataFile.Truncate(size); err != nil {
		return WrapError(ErrProblem, err)
	}

	// Keep old mmap alive for safety (in case any reader cached the pointer)
	oldMap := e.dataMap

	// Create new mmap
	writable := e.flags&ReadOnly == 0 && e.flags&WriteMap != 0
	newMap, err := mmappkg.New(int(e.dataFile.Fd()), 0, int(size), writable)
	if err != nil {
		return WrapError(ErrProblem, err)
	}

	// Update dataMap atomically
	e.mu.Lock()
	e.dataMap = newMap
	// Store old mmap for later cleanup
	if oldMap != nil {
		e.oldMmapsMu.Lock()
		e.oldMmaps = append(e.oldMmaps, oldMap)
		e.oldMmapsMu.Unlock()
	}
	e.mu.Unlock()

	// Increment mmap version
	e.mmapVersion++

	// Reload meta pointers
	if err := e.readMeta(); err != nil {
		return WrapError(ErrProblem, err)
	}

	return nil
}

// EnableSpillBuffer creates a spill buffer for dirty pages.
// This reduces heap pressure by storing dirty pages in a memory-mapped file
// instead of Go-allocated memory. Call this after Open() but before
// starting write transactions.
// The initialCap parameter specifies the initial capacity in pages.
// If initialCap is 0, a default value is used.
func (e *Env) EnableSpillBuffer(initialCap uint32) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.spillBuf != nil {
		return nil // Already enabled
	}

	// Create spill file path
	spillPath := e.path + ".spill"
	if e.flags&NoSubdir != 0 {
		spillPath = e.path + "-spill"
	}

	buf, err := spill.New(spillPath, e.pageSize, initialCap)
	if err != nil {
		return WrapError(ErrProblem, err)
	}

	e.spillBuf = buf
	return nil
}

// SpillBuffer returns the spill buffer, or nil if not enabled.
func (e *Env) SpillBuffer() *spill.Buffer {
	return e.spillBuf
}

var debugEnabled = false

// SetDebugLog enables or disables debug logging (for debugging only).
func SetDebugLog(enabled bool) {
	debugEnabled = enabled
}
