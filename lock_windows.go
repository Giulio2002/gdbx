//go:build windows

package gdbx

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/windows"
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
type readerSlot struct {
	txnid                uint64 // Transaction ID when started (atomic)
	tid                  uint64 // Thread/Goroutine ID (atomic)
	pid                  uint32 // Process ID (atomic)
	snapshotPagesUsed    uint32 // Pages in snapshot (atomic)
	snapshotPagesRetired uint64 // Retired pages at snapshot (atomic)
}

// Special TID values
const (
	tidTxnOusted uint64 = 0xFFFFFFFFFFFFFFFF - 1
	tidTxnParked uint64 = 0xFFFFFFFFFFFFFFFF
)

// lockHeader is the lock file header.
type lockHeader struct {
	magicAndVersion    uint64
	osFormat           uint32
	envMode            uint32
	autosyncThreshold  uint32
	metaSyncTxnID      uint32
	autosyncPeriod     uint64
	baitUniqueness     uint64
	mlockCount         [2]uint32
	_                  [64]byte
	cachedOldest       uint64
	eoosTimestamp      uint64
	unsyncVolume       uint64
	_                  [32]byte
	numReaders         uint32
	readersRefreshFlag uint32
}

// lockFile manages the lock file and reader slots.
type lockFile struct {
	file       *os.File
	data       []byte
	header     *lockHeader
	slots      []readerSlot
	maxReaders int
	writerLock bool
	lockless   bool
	memSlots   []readerSlot
	memHeader  *lockHeader
	mapping    windows.Handle // Windows file mapping handle

	// Slot freelist
	freeSlots []int32
	freeMu    sync.Mutex
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
		if !create {
			return openLockFileReadOnly(path, maxReaders)
		}
		return nil, err
	}

	lf := &lockFile{
		file:       f,
		maxReaders: maxReaders,
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := fi.Size()
	expectedSize := int64(lockHeaderSize + maxReaders*readerSlotSize)

	if size == 0 && create {
		if err := lf.initialize(expectedSize); err != nil {
			f.Close()
			return nil, err
		}
	} else if size < expectedSize {
		f.Close()
		return openLockFileReadOnly(path, maxReaders)
	}

	if err := lf.mmap(); err != nil {
		f.Close()
		return nil, err
	}

	if lf.header.magicAndVersion != lockMagic {
		lf.close()
		return nil, errLockInvalidFile
	}

	return lf, nil
}

// openLockFileReadOnly opens a lock file for read-only access.
func openLockFileReadOnly(path string, maxReaders int) (*lockFile, error) {
	if maxReaders <= 0 {
		maxReaders = defaultMaxReaders
	}

	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		f = nil
	}

	lf := &lockFile{
		file:       f,
		maxReaders: maxReaders,
		lockless:   true,
	}

	lf.memSlots = make([]readerSlot, maxReaders)
	lf.slots = lf.memSlots

	lf.memHeader = &lockHeader{
		magicAndVersion: lockMagic,
		numReaders:      0,
	}
	lf.header = lf.memHeader

	return lf, nil
}

// initialize creates a new lock file.
func (lf *lockFile) initialize(size int64) error {
	if err := lf.file.Truncate(size); err != nil {
		return err
	}

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

	size := fi.Size()
	handle := windows.Handle(lf.file.Fd())

	// Create file mapping
	maxSizeHigh := uint32(uint64(size) >> 32)
	maxSizeLow := uint32(size)

	mapping, err := windows.CreateFileMapping(handle, nil, windows.PAGE_READWRITE, maxSizeHigh, maxSizeLow, nil)
	if err != nil {
		return err
	}

	addr, err := windows.MapViewOfFile(mapping, windows.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		windows.CloseHandle(mapping)
		return err
	}

	// Create slice from mapped memory
	var data []byte
	sh := (*struct {
		Data uintptr
		Len  int
		Cap  int
	})(unsafe.Pointer(&data))
	sh.Data = addr
	sh.Len = int(size)
	sh.Cap = int(size)

	lf.data = data
	lf.mapping = mapping
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
		addr := uintptr(unsafe.Pointer(&lf.data[0]))
		windows.UnmapViewOfFile(addr)
		lf.data = nil
	}

	if lf.mapping != 0 {
		windows.CloseHandle(lf.mapping)
		lf.mapping = 0
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
	handle := windows.Handle(lf.file.Fd())

	// Lock the entire file exclusively
	var overlapped windows.Overlapped
	err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &overlapped)
	if err != nil {
		return &lockError{"acquire writer lock", err}
	}
	lf.writerLock = true
	return nil
}

// tryLockWriter attempts to acquire the writer lock without blocking.
func (lf *lockFile) tryLockWriter() (bool, error) {
	handle := windows.Handle(lf.file.Fd())

	var overlapped windows.Overlapped
	err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if err != nil {
		// ERROR_LOCK_VIOLATION means lock is held by another process
		if err == windows.ERROR_LOCK_VIOLATION {
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

	handle := windows.Handle(lf.file.Fd())
	var overlapped windows.Overlapped
	err := windows.UnlockFileEx(handle, 0, 1, 0, &overlapped)
	if err != nil {
		return &lockError{"release writer lock", err}
	}
	lf.writerLock = false
	return nil
}

// hasActiveReaders returns true if any reader slots are in use.
func (lf *lockFile) hasActiveReaders() bool {
	if lf.lockless {
		for i := range lf.memSlots {
			if lf.memSlots[i].txnid != 0 {
				return true
			}
		}
		return false
	}

	for i := range lf.slots {
		if lf.slots[i].txnid != 0 {
			return true
		}
	}
	return false
}

// acquireReaderSlot finds and acquires a free reader slot.
func (lf *lockFile) acquireReaderSlot(pid uint32, tid uint64) (*readerSlot, int, error) {
	lf.freeMu.Lock()
	if len(lf.freeSlots) > 0 {
		idx := lf.freeSlots[len(lf.freeSlots)-1]
		lf.freeSlots = lf.freeSlots[:len(lf.freeSlots)-1]
		lf.freeMu.Unlock()

		slot := &lf.slots[idx]
		if atomic.CompareAndSwapUint64(&slot.txnid, 0, ^uint64(0)) {
			atomic.StoreUint32(&slot.pid, pid)
			atomic.StoreUint64(&slot.tid, tid)
			return slot, int(idx), nil
		}
	} else {
		lf.freeMu.Unlock()
	}

	for i := range lf.slots {
		slot := &lf.slots[i]

		if atomic.LoadUint64(&slot.txnid) == 0 {
			if atomic.CompareAndSwapUint64(&slot.txnid, 0, ^uint64(0)) {
				atomic.StoreUint32(&slot.pid, pid)
				atomic.StoreUint64(&slot.tid, tid)
				return slot, i, nil
			}
		}
	}

	return nil, -1, errLockReadersFull
}

// releaseReaderSlot releases a reader slot.
func (lf *lockFile) releaseReaderSlot(slot *readerSlot, slotIdx int) {
	atomic.StoreUint64(&slot.txnid, 0)
	atomic.StoreUint64(&slot.tid, 0)
	atomic.StoreUint32(&slot.pid, 0)

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
	oldest := ^uint64(0)

	for i := range lf.slots {
		txnid := atomic.LoadUint64(&lf.slots[i].txnid)
		if txnid > 0 && txnid < oldest && txnid != ^uint64(0) {
			oldest = txnid
		}
	}

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

		if !processExists(int(pid)) {
			atomic.StoreUint64(&slot.txnid, 0)
			cleaned++
		}
	}

	return cleaned
}

// processExists checks if a process exists on Windows.
func processExists(pid int) bool {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	windows.CloseHandle(handle)
	return true
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
