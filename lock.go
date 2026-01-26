//go:build unix

package gdbx

import (
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// cachedPID is the process ID, cached at init to avoid syscall overhead
var cachedPID = uint32(os.Getpid())

// Constants for lock file
const (
	// lockMagic for lock file validation
	lockMagic uint64 = (0x59659DBDEF4C11 << 8) + 6 // MDBX_MAGIC + LOCK_VERSION

	// defaultMaxReaders is the default number of reader slots
	defaultMaxReaders = 126

	// readerSlotSize is the size of each reader slot
	readerSlotSize = 32

	// lockHeaderSize is the size of the lock file header
	lockHeaderSize = 256
)

// readerSlot represents a reader in the lock file.
// This structure must match libmdbx reader_slot_t exactly.
//
// Memory layout:
//
//	Offset  Size  Field
//	0       8     txnid (atomic)
//	8       8     tid (thread ID, atomic)
//	16      4     pid (process ID, atomic)
//	20      4     snapshot_pages_used (atomic)
//	24      8     snapshot_pages_retired (atomic)
type readerSlot struct {
	txnid                uint64 // Transaction ID when started (atomic)
	tid                  uint64 // Thread/Goroutine ID (atomic)
	pid                  uint32 // Process ID (atomic)
	snapshotPagesUsed    uint32 // Pages in snapshot (atomic)
	snapshotPagesRetired uint64 // Retired pages at snapshot (atomic)
}

// Special TID values
const (
	tidTxnOusted uint64 = 0xFFFFFFFFFFFFFFFF - 1 // Evicted transaction
	tidTxnParked uint64 = 0xFFFFFFFFFFFFFFFF     // Parked transaction
)

// lockHeader is the lock file header.
// This structure must match libmdbx shared_lck header.
type lockHeader struct {
	magicAndVersion    uint64    // Magic + version
	osFormat           uint32    // OS and format info
	envMode            uint32    // Environment open flags
	autosyncThreshold  uint32    // Pages before auto-sync
	metaSyncTxnID      uint32    // Meta sync checkpoint
	autosyncPeriod     uint64    // Auto-sync period
	baitUniqueness     uint64    // Uniqueness marker
	mlockCount         [2]uint32 // Mlock page counter
	_                  [64]byte  // Padding for cache alignment
	cachedOldest       uint64    // Cached oldest active txnid
	eoosTimestamp      uint64    // Out-of-sync enter time
	unsyncVolume       uint64    // Unsynced bytes
	_                  [32]byte  // More padding
	numReaders         uint32    // Number of active readers
	readersRefreshFlag uint32    // Readers refresh indicator
}

// lockFile manages the lock file and reader slots.
type lockFile struct {
	file       *os.File
	data       []byte // Memory-mapped lock file
	header     *lockHeader
	slots      []readerSlot
	maxReaders int
	writerLock bool
	lockless   bool         // Lockless mode for read-only access
	memSlots   []readerSlot // In-memory slots for lockless mode
	memHeader  *lockHeader  // In-memory header for lockless mode

	// Slot freelist for fast acquisition (LIFO stack)
	freeSlots []int32    // Stack of free slot indices (-1 terminated)
	freeMu    sync.Mutex // Protects freeSlots
}

// openLockFile opens or creates a lock file.
func openLockFile(path string, maxReaders int, create bool) (*lockFile, error) {
	if maxReaders <= 0 {
		maxReaders = defaultMaxReaders
	}

	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}

	f, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		// For read-only mode, return a lockless file handle
		if !create {
			return openLockFileReadOnly(path, maxReaders)
		}
		return nil, err
	}

	lf := &lockFile{
		file:       f,
		maxReaders: maxReaders,
	}

	// Get file size
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := fi.Size()
	expectedSize := int64(lockHeaderSize + maxReaders*readerSlotSize)

	if size == 0 && create {
		// Initialize new lock file
		if err := lf.initialize(expectedSize); err != nil {
			f.Close()
			return nil, err
		}
	} else if size < expectedSize {
		// For small/empty lock files, use lockless mode
		f.Close()
		return openLockFileReadOnly(path, maxReaders)
	}

	// Memory-map the lock file
	if err := lf.mmap(); err != nil {
		f.Close()
		return nil, err
	}

	// Validate magic
	if lf.header.magicAndVersion != lockMagic {
		lf.close()
		return nil, errLockInvalidFile
	}

	return lf, nil
}

// openLockFileReadOnly opens a lock file for read-only access without requiring
// a valid lock file. This is used for read-only database access when the lock
// file is missing, empty, or invalid.
func openLockFileReadOnly(path string, maxReaders int) (*lockFile, error) {
	if maxReaders <= 0 {
		maxReaders = defaultMaxReaders
	}

	// Try to open read-only first
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		// Lock file doesn't exist or can't be opened - that's OK for read-only
		f = nil
	}

	lf := &lockFile{
		file:       f,
		maxReaders: maxReaders,
		lockless:   true, // Mark as lockless mode
	}

	// Create in-memory slots for reader tracking
	lf.memSlots = make([]readerSlot, maxReaders)
	lf.slots = lf.memSlots

	// Create in-memory header
	lf.memHeader = &lockHeader{
		magicAndVersion: lockMagic,
		numReaders:      0,
	}
	lf.header = lf.memHeader

	return lf, nil
}

// initialize creates a new lock file.
func (lf *lockFile) initialize(size int64) error {
	// Extend file
	if err := lf.file.Truncate(size); err != nil {
		return err
	}

	// Write header
	header := lockHeader{
		magicAndVersion: lockMagic,
		numReaders:      0,
	}

	headerBytes := (*[lockHeaderSize]byte)(unsafe.Pointer(&header))[:]
	if _, err := lf.file.WriteAt(headerBytes, 0); err != nil {
		return err
	}

	return lf.file.Sync()
}

// mmap memory-maps the lock file.
func (lf *lockFile) mmap() error {
	fi, err := lf.file.Stat()
	if err != nil {
		return err
	}

	size := int(fi.Size())
	data, err := syscall.Mmap(int(lf.file.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	lf.data = data
	lf.header = (*lockHeader)(unsafe.Pointer(&data[0]))

	// Create slice of reader slots
	slotData := data[lockHeaderSize:]
	numSlots := min((len(slotData))/readerSlotSize, lf.maxReaders)

	lf.slots = unsafe.Slice((*readerSlot)(unsafe.Pointer(&slotData[0])), numSlots)

	return nil
}

// close closes the lock file.
func (lf *lockFile) close() error {
	if lf.data != nil {
		if err := syscall.Munmap(lf.data); err != nil {
			return err
		}
		lf.data = nil
	}

	if lf.writerLock {
		lf.unlockWriter()
	}

	if lf.file != nil {
		return lf.file.Close()
	}

	return nil
}

// lockWriter acquires the exclusive writer lock.
func (lf *lockFile) lockWriter() error {
	err := syscall.Flock(int(lf.file.Fd()), syscall.LOCK_EX)
	if err != nil {
		return &lockError{"acquire writer lock", err}
	}
	lf.writerLock = true
	return nil
}

// tryLockWriter attempts to acquire the writer lock without blocking.
func (lf *lockFile) tryLockWriter() (bool, error) {
	err := syscall.Flock(int(lf.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if err == syscall.EWOULDBLOCK {
			return false, nil
		}
		return false, &lockError{"try writer lock", err}
	}
	lf.writerLock = true
	return true, nil
}

// unlockWriter releases the writer lock.
func (lf *lockFile) unlockWriter() error {
	if !lf.writerLock {
		return nil
	}
	err := syscall.Flock(int(lf.file.Fd()), syscall.LOCK_UN)
	if err != nil {
		return &lockError{"release writer lock", err}
	}
	lf.writerLock = false
	return nil
}

// hasActiveReaders returns true if any reader slots are in use.
// Used to determine if old mmaps can be safely cleaned up.
func (lf *lockFile) hasActiveReaders() bool {
	if lf.lockless {
		// In lockless mode, check in-memory slots
		for i := range lf.memSlots {
			if lf.memSlots[i].txnid != 0 {
				return true
			}
		}
		return false
	}

	// Check actual slots
	for i := range lf.slots {
		if lf.slots[i].txnid != 0 {
			return true
		}
	}
	return false
}

// acquireReaderSlot finds and acquires a free reader slot.
// Uses a LIFO freelist for O(1) acquisition in common case.
func (lf *lockFile) acquireReaderSlot(pid uint32, tid uint64) (*readerSlot, int, error) {
	// Fast path: try to pop from freelist
	lf.freeMu.Lock()
	if len(lf.freeSlots) > 0 {
		idx := lf.freeSlots[len(lf.freeSlots)-1]
		lf.freeSlots = lf.freeSlots[:len(lf.freeSlots)-1]
		lf.freeMu.Unlock()

		slot := &lf.slots[idx]
		// Slot should be free, but verify and claim it
		if atomic.CompareAndSwapUint64(&slot.txnid, 0, ^uint64(0)) {
			atomic.StoreUint32(&slot.pid, pid)
			atomic.StoreUint64(&slot.tid, tid)
			return slot, int(idx), nil
		}
		// Slot was taken (race), fall through to slow path
	} else {
		lf.freeMu.Unlock()
	}

	// Slow path: scan for free slot
	for i := range lf.slots {
		slot := &lf.slots[i]

		// Check if slot is free (txnid == 0)
		if atomic.LoadUint64(&slot.txnid) == 0 {
			// Try to claim it
			if atomic.CompareAndSwapUint64(&slot.txnid, 0, ^uint64(0)) {
				// Got it, now set our info
				atomic.StoreUint32(&slot.pid, pid)
				atomic.StoreUint64(&slot.tid, tid)
				return slot, i, nil
			}
		}
	}

	return nil, -1, errLockReadersFull
}

// releaseReaderSlot releases a reader slot and adds it to freelist.
func (lf *lockFile) releaseReaderSlot(slot *readerSlot, slotIdx int) {
	atomic.StoreUint64(&slot.txnid, 0)
	atomic.StoreUint64(&slot.tid, 0)
	atomic.StoreUint32(&slot.pid, 0)

	// Add to freelist for fast reuse
	lf.freeMu.Lock()
	lf.freeSlots = append(lf.freeSlots, int32(slotIdx))
	lf.freeMu.Unlock()
}

// setReaderTxnid sets the transaction ID for a reader slot.
func (lf *lockFile) setReaderTxnid(slot *readerSlot, txnid uint64) {
	atomic.StoreUint64(&slot.txnid, txnid)
}

// oldestReader returns the oldest active reader's transaction ID.
func (lf *lockFile) oldestReader() uint64 {
	oldest := ^uint64(0) // Max value

	for i := range lf.slots {
		txnid := atomic.LoadUint64(&lf.slots[i].txnid)
		if txnid > 0 && txnid < oldest && txnid != ^uint64(0) {
			oldest = txnid
		}
	}

	// Update cached value
	atomic.StoreUint64(&lf.header.cachedOldest, oldest)

	return oldest
}

// cachedOldestReader returns the cached oldest reader value.
func (lf *lockFile) cachedOldestReader() uint64 {
	return atomic.LoadUint64(&lf.header.cachedOldest)
}

// numActiveReaders returns the count of active readers.
func (lf *lockFile) numActiveReaders() int {
	count := 0
	for i := range lf.slots {
		txnid := atomic.LoadUint64(&lf.slots[i].txnid)
		if txnid > 0 && txnid != ^uint64(0) {
			count++
		}
	}
	return count
}

// cleanupStaleReaders removes readers from dead processes.
func (lf *lockFile) cleanupStaleReaders() int {
	cleaned := 0
	myPID := uint32(os.Getpid())

	for i := range lf.slots {
		slot := &lf.slots[i]
		txnid := atomic.LoadUint64(&slot.txnid)
		if txnid == 0 || txnid == ^uint64(0) {
			continue
		}

		pid := atomic.LoadUint32(&slot.pid)
		if pid == 0 || pid == myPID {
			continue
		}

		// Check if process is alive
		if !processExists(int(pid)) {
			// Process is dead, free the slot
			atomic.StoreUint64(&slot.txnid, 0)
			cleaned++
		}
	}

	return cleaned
}

// processExists checks if a process exists.
func processExists(pid int) bool {
	// Send signal 0 to check if process exists
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// Lock file errors
var (
	errLockFileTooSmall = &lockError{"lock file too small", nil}
	errLockInvalidFile  = &lockError{"invalid lock file", nil}
	errLockReadersFull  = &lockError{"reader slots full", nil}
)

type lockError struct {
	op  string
	err error
}

func (e *lockError) Error() string {
	if e.err != nil {
		return "lock: " + e.op + ": " + e.err.Error()
	}
	return "lock: " + e.op
}

func (e *lockError) Unwrap() error {
	return e.err
}
