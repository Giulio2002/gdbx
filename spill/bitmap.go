// Package spill provides a memory-mapped spill buffer for dirty pages.
package spill

import "math/bits"

// Bitmap tracks slot allocation using a bitset.
// Uses uint64 words for efficient 64-bit operations.
type Bitmap struct {
	words    []uint64
	numSlots uint32
	freeHint uint32 // Hint for where to start searching for free slots
}

// NewBitmap creates a bitmap capable of tracking the given number of slots.
func NewBitmap(numSlots uint32) *Bitmap {
	numWords := (numSlots + 63) / 64
	return &Bitmap{
		words:    make([]uint64, numWords),
		numSlots: numSlots,
		freeHint: 0,
	}
}

// Allocate finds and marks a free slot.
// Returns the slot index or (MaxUint32, false) if no free slot is available.
func (b *Bitmap) Allocate() (uint32, bool) {
	numWords := uint32(len(b.words))
	if numWords == 0 {
		return 0, false
	}

	// Start searching from freeHint
	startWord := b.freeHint / 64
	for i := uint32(0); i < numWords; i++ {
		wordIdx := (startWord + i) % numWords
		word := b.words[wordIdx]

		// Find first zero bit (inverted trailing zeros = first zero position)
		if word != ^uint64(0) {
			// Has at least one free bit
			bitPos := bits.TrailingZeros64(^word)
			slot := wordIdx*64 + uint32(bitPos)
			if slot >= b.numSlots {
				// Beyond valid range, continue searching
				continue
			}

			// Mark as allocated
			b.words[wordIdx] |= 1 << bitPos
			b.freeHint = slot + 1
			return slot, true
		}
	}

	return 0, false
}

// Free marks a slot as available.
func (b *Bitmap) Free(slot uint32) {
	if slot >= b.numSlots {
		return
	}
	wordIdx := slot / 64
	bitPos := slot % 64
	b.words[wordIdx] &^= 1 << bitPos

	// Update hint if this is earlier
	if slot < b.freeHint {
		b.freeHint = slot
	}
}

// Clear resets all slots to free.
func (b *Bitmap) Clear() {
	for i := range b.words {
		b.words[i] = 0
	}
	b.freeHint = 0
}

// Extend increases the bitmap capacity to accommodate more slots.
func (b *Bitmap) Extend(newCap uint32) {
	if newCap <= b.numSlots {
		return
	}

	newNumWords := (newCap + 63) / 64
	if newNumWords > uint32(len(b.words)) {
		newWords := make([]uint64, newNumWords)
		copy(newWords, b.words)
		b.words = newWords
	}
	b.numSlots = newCap
}

// IsAllocated returns true if the slot is marked as allocated.
func (b *Bitmap) IsAllocated(slot uint32) bool {
	if slot >= b.numSlots {
		return false
	}
	wordIdx := slot / 64
	bitPos := slot % 64
	return b.words[wordIdx]&(1<<bitPos) != 0
}

// Count returns the number of allocated slots.
func (b *Bitmap) Count() uint32 {
	var count uint32
	for _, word := range b.words {
		count += uint32(bits.OnesCount64(word))
	}
	return count
}

// Capacity returns the total number of slots.
func (b *Bitmap) Capacity() uint32 {
	return b.numSlots
}
