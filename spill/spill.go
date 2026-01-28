package spill

import (
	"os"
	"sync"

	"github.com/Giulio2002/gdbx/mmap"
)

// DefaultInitialCap is the default initial capacity (number of pages) per segment.
const DefaultInitialCap = 1024

// DefaultMaxSegments is the maximum number of segments (limits total capacity).
const DefaultMaxSegments = 256

// segment represents a single mmap'd region of the spill buffer.
type segment struct {
	file   *os.File
	mmap   *mmap.Map
	path   string
	bitmap *Bitmap
	cap    uint32
}

// Buffer is a memory-mapped file used to spill dirty pages.
// This reduces heap pressure by storing dirty pages in mmap'd memory
// rather than Go-allocated heap memory.
// Uses multiple segments to allow growth without invalidating existing slices.
type Buffer struct {
	mu          sync.Mutex
	basePath    string
	pageSize    uint32
	segmentCap  uint32 // Capacity per segment
	segments    []*segment
	curSegment  int // Current segment for allocations
	totalAlloc  uint32
}

// Slot represents an allocated slot in the spill buffer.
type Slot struct {
	Pgno       uint32 // Original page number (set by caller)
	SegmentIdx uint16 // Which segment
	SlotIdx    uint16 // Index within segment
}

// New creates or reopens a spill buffer at the given path.
// The pageSize determines the size of each slot.
// initialCap is the initial capacity in number of pages per segment.
func New(path string, pageSize, initialCap uint32) (*Buffer, error) {
	if initialCap == 0 {
		initialCap = DefaultInitialCap
	}

	b := &Buffer{
		basePath:   path,
		pageSize:   pageSize,
		segmentCap: initialCap,
		segments:   make([]*segment, 0, 4),
	}

	// Create initial segment
	if err := b.addSegment(); err != nil {
		return nil, err
	}

	return b, nil
}

// addSegment creates a new segment.
func (b *Buffer) addSegment() error {
	if len(b.segments) >= DefaultMaxSegments {
		return ErrBufferFull
	}

	segIdx := len(b.segments)
	segPath := b.basePath
	if segIdx > 0 {
		segPath = b.basePath + "." + itoa(segIdx)
	}

	// Create the file (always truncate for simplicity)
	file, err := os.OpenFile(segPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	// Set size
	fileSize := int64(b.segmentCap) * int64(b.pageSize)
	if err := file.Truncate(fileSize); err != nil {
		file.Close()
		os.Remove(segPath)
		return err
	}

	// Create mmap
	m, err := mmap.New(int(file.Fd()), 0, int(fileSize), true)
	if err != nil {
		file.Close()
		os.Remove(segPath)
		return err
	}

	seg := &segment{
		file:   file,
		mmap:   m,
		path:   segPath,
		bitmap: NewBitmap(b.segmentCap),
		cap:    b.segmentCap,
	}

	b.segments = append(b.segments, seg)
	return nil
}

// itoa converts int to string without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// Close closes the spill buffer.
// If deleteFile is true, the underlying files are also deleted.
func (b *Buffer) Close(deleteFile bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for _, seg := range b.segments {
		if seg.mmap != nil {
			if err := seg.mmap.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if seg.file != nil {
			if err := seg.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if deleteFile && seg.path != "" {
			if err := os.Remove(seg.path); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	b.segments = nil
	return firstErr
}

// Allocate allocates a slot and returns the page data slice and slot info.
// Automatically extends by adding new segments if needed.
func (b *Buffer) Allocate() ([]byte, *Slot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Try current segment first
	for b.curSegment < len(b.segments) {
		seg := b.segments[b.curSegment]
		slotIdx, ok := seg.bitmap.Allocate()
		if ok {
			b.totalAlloc++
			offset := int64(slotIdx) * int64(b.pageSize)
			data := seg.mmap.Data()[offset : offset+int64(b.pageSize)]
			return data, &Slot{SegmentIdx: uint16(b.curSegment), SlotIdx: uint16(slotIdx)}, nil
		}
		// Current segment full, try next
		b.curSegment++
	}

	// All segments full, add new one
	if err := b.addSegment(); err != nil {
		return nil, nil, err
	}

	seg := b.segments[b.curSegment]
	slotIdx, ok := seg.bitmap.Allocate()
	if !ok {
		return nil, nil, ErrBufferFull // Shouldn't happen with fresh segment
	}

	b.totalAlloc++
	offset := int64(slotIdx) * int64(b.pageSize)
	data := seg.mmap.Data()[offset : offset+int64(b.pageSize)]
	return data, &Slot{SegmentIdx: uint16(b.curSegment), SlotIdx: uint16(slotIdx)}, nil
}

// Get returns the page data for a given slot.
func (b *Buffer) Get(slot *Slot) []byte {
	if slot == nil {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if int(slot.SegmentIdx) >= len(b.segments) {
		return nil
	}
	seg := b.segments[slot.SegmentIdx]
	if slot.SlotIdx >= uint16(seg.cap) {
		return nil
	}

	offset := int64(slot.SlotIdx) * int64(b.pageSize)
	end := offset + int64(b.pageSize)
	if end > int64(len(seg.mmap.Data())) {
		return nil
	}
	return seg.mmap.Data()[offset:end]
}

// Release returns a slot to the pool.
func (b *Buffer) Release(slot *Slot) {
	if slot == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if int(slot.SegmentIdx) >= len(b.segments) {
		return
	}
	seg := b.segments[slot.SegmentIdx]
	seg.bitmap.Free(uint32(slot.SlotIdx))
	b.totalAlloc--

	// If releasing from a segment before curSegment, update curSegment
	// so future allocations check earlier segments first
	if int(slot.SegmentIdx) < b.curSegment {
		b.curSegment = int(slot.SegmentIdx)
	}
}

// ReleaseBulk returns multiple slots to the pool.
func (b *Buffer) ReleaseBulk(slots []*Slot) {
	if len(slots) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	minSeg := b.curSegment
	for _, slot := range slots {
		if slot == nil {
			continue
		}
		if int(slot.SegmentIdx) >= len(b.segments) {
			continue
		}
		seg := b.segments[slot.SegmentIdx]
		seg.bitmap.Free(uint32(slot.SlotIdx))
		b.totalAlloc--
		if int(slot.SegmentIdx) < minSeg {
			minSeg = int(slot.SegmentIdx)
		}
	}
	b.curSegment = minSeg
}

// Clear releases all slots without closing the buffer.
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, seg := range b.segments {
		seg.bitmap.Clear()
	}
	b.curSegment = 0
	b.totalAlloc = 0
}

// Capacity returns the total capacity in number of pages.
func (b *Buffer) Capacity() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return uint32(len(b.segments)) * b.segmentCap
}

// AllocatedCount returns the number of allocated slots.
func (b *Buffer) AllocatedCount() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.totalAlloc
}

// PageSize returns the page size of this buffer.
func (b *Buffer) PageSize() uint32 {
	return b.pageSize
}

// Error types
var ErrBufferFull = &spillError{"buffer full (max segments reached)"}

type spillError struct {
	msg string
}

func (e *spillError) Error() string {
	return "spill: " + e.msg
}
