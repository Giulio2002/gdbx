package spill

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestBitmapAllocate(t *testing.T) {
	b := NewBitmap(64)

	// Allocate all slots
	allocated := make(map[uint32]bool)
	for i := 0; i < 64; i++ {
		slot, ok := b.Allocate()
		if !ok {
			t.Fatalf("failed to allocate slot %d", i)
		}
		if allocated[slot] {
			t.Fatalf("duplicate slot %d", slot)
		}
		allocated[slot] = true
	}

	// Should fail now
	_, ok := b.Allocate()
	if ok {
		t.Error("should not allocate when full")
	}
}

func TestBitmapFree(t *testing.T) {
	b := NewBitmap(10)

	// Allocate some
	slots := make([]uint32, 5)
	for i := range slots {
		slot, ok := b.Allocate()
		if !ok {
			t.Fatal("failed to allocate")
		}
		slots[i] = slot
	}

	// Free all
	for _, slot := range slots {
		b.Free(slot)
	}

	// Should be able to allocate again
	for i := 0; i < 5; i++ {
		_, ok := b.Allocate()
		if !ok {
			t.Fatal("failed to reallocate after free")
		}
	}
}

func TestBitmapClear(t *testing.T) {
	b := NewBitmap(32)

	// Allocate all
	for i := 0; i < 32; i++ {
		b.Allocate()
	}

	if b.Count() != 32 {
		t.Errorf("count should be 32, got %d", b.Count())
	}

	// Clear
	b.Clear()

	if b.Count() != 0 {
		t.Errorf("count should be 0 after clear, got %d", b.Count())
	}

	// Should allocate from beginning
	slot, ok := b.Allocate()
	if !ok || slot != 0 {
		t.Errorf("expected slot 0, got %d, ok=%v", slot, ok)
	}
}

func TestBitmapExtend(t *testing.T) {
	b := NewBitmap(10)

	// Allocate all
	for i := 0; i < 10; i++ {
		_, ok := b.Allocate()
		if !ok {
			t.Fatal("failed to allocate")
		}
	}

	// Extend
	b.Extend(20)

	if b.Capacity() != 20 {
		t.Errorf("capacity should be 20, got %d", b.Capacity())
	}

	// Should be able to allocate more
	for i := 0; i < 10; i++ {
		slot, ok := b.Allocate()
		if !ok {
			t.Fatal("failed to allocate after extend")
		}
		if slot < 10 {
			t.Errorf("expected slot >= 10, got %d", slot)
		}
	}
}

func TestBitmapIsAllocated(t *testing.T) {
	b := NewBitmap(10)

	slot, _ := b.Allocate()

	if !b.IsAllocated(slot) {
		t.Error("slot should be allocated")
	}

	if b.IsAllocated(9) {
		t.Error("slot 9 should not be allocated")
	}

	b.Free(slot)

	if b.IsAllocated(slot) {
		t.Error("slot should be free after Free()")
	}
}

func TestBufferNew(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	if buf.Capacity() != 100 {
		t.Errorf("capacity should be 100, got %d", buf.Capacity())
	}

	if buf.PageSize() != 4096 {
		t.Errorf("page size should be 4096, got %d", buf.PageSize())
	}
}

func TestBufferAllocate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate a slot
	data, slot, err := buf.Allocate()
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != 4096 {
		t.Errorf("data length should be 4096, got %d", len(data))
	}

	if slot == nil {
		t.Fatal("slot should not be nil")
	}

	// Write to data
	testData := []byte("hello spill buffer")
	copy(data, testData)

	// Read back through Get
	readData := buf.Get(slot)
	if !bytes.HasPrefix(readData, testData) {
		t.Errorf("data mismatch: got %q", readData[:len(testData)])
	}
}

func TestBufferRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate
	_, slot, err := buf.Allocate()
	if err != nil {
		t.Fatal(err)
	}

	if buf.AllocatedCount() != 1 {
		t.Errorf("allocated count should be 1, got %d", buf.AllocatedCount())
	}

	// Release
	buf.Release(slot)

	if buf.AllocatedCount() != 0 {
		t.Errorf("allocated count should be 0 after release, got %d", buf.AllocatedCount())
	}
}

func TestBufferReleaseBulk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate multiple
	slots := make([]*Slot, 5)
	for i := range slots {
		_, slot, err := buf.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		slots[i] = slot
	}

	if buf.AllocatedCount() != 5 {
		t.Errorf("allocated count should be 5, got %d", buf.AllocatedCount())
	}

	// Bulk release
	buf.ReleaseBulk(slots)

	if buf.AllocatedCount() != 0 {
		t.Errorf("allocated count should be 0 after bulk release, got %d", buf.AllocatedCount())
	}
}

func TestBufferSegmentGrowth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate all slots in first segment
	slots := make([]*Slot, 5)
	for i := range slots {
		_, slot, err := buf.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		slots[i] = slot
	}

	// Capacity should still be 5 (one segment)
	if buf.Capacity() != 5 {
		t.Errorf("capacity should be 5, got %d", buf.Capacity())
	}

	// Allocate one more - should trigger new segment
	_, slot6, err := buf.Allocate()
	if err != nil {
		t.Fatal(err)
	}

	// Capacity should now be 10 (two segments)
	if buf.Capacity() != 10 {
		t.Errorf("capacity should be 10, got %d", buf.Capacity())
	}

	// Verify slot6 is in segment 1
	if slot6.SegmentIdx != 1 {
		t.Errorf("slot6 should be in segment 1, got %d", slot6.SegmentIdx)
	}

	// Should allocate more from segment 1
	for i := 0; i < 4; i++ {
		_, _, err := buf.Allocate()
		if err != nil {
			t.Fatalf("failed to allocate in segment 1: %v", err)
		}
	}
}

func TestBufferAutoExtend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate more than initial capacity
	for i := 0; i < 10; i++ {
		_, _, err := buf.Allocate()
		if err != nil {
			t.Fatalf("failed to allocate slot %d: %v", i, err)
		}
	}

	// Capacity should have grown
	if buf.Capacity() < 10 {
		t.Errorf("capacity should be at least 10, got %d", buf.Capacity())
	}
}

func TestBufferClear(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate some
	for i := 0; i < 5; i++ {
		buf.Allocate()
	}

	if buf.AllocatedCount() != 5 {
		t.Errorf("allocated count should be 5, got %d", buf.AllocatedCount())
	}

	// Clear
	buf.Clear()

	if buf.AllocatedCount() != 0 {
		t.Errorf("allocated count should be 0 after clear, got %d", buf.AllocatedCount())
	}

	// Capacity unchanged
	if buf.Capacity() != 10 {
		t.Errorf("capacity should still be 10, got %d", buf.Capacity())
	}
}

func TestBufferClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Close without delete
	if err := buf.Close(false); err != nil {
		t.Fatal(err)
	}

	// File should still exist
	if _, err := New(path, 4096, 10); err != nil {
		t.Errorf("file should exist: %v", err)
	}

	// Create and close with delete
	buf2, _ := New(path, 4096, 10)
	buf2.Close(true)

	// File should be deleted
	buf3, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err) // We're creating fresh, so error would be unexpected
	}
	buf3.Close(true)
}

func TestBufferDataPersistence(t *testing.T) {
	// Test that data written to a slot persists within the same session
	dir := t.TempDir()
	path := filepath.Join(dir, "spill.dat")

	buf, err := New(path, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Close(true)

	// Allocate and write
	data1, slot1, _ := buf.Allocate()
	testData := []byte("persistent data test")
	copy(data1, testData)

	// Read back same session
	readData := buf.Get(slot1)
	if !bytes.HasPrefix(readData, testData) {
		t.Errorf("data mismatch within session: got %q", readData[:len(testData)])
	}

	// Release and reallocate same slot
	buf.Release(slot1)

	data2, slot2, _ := buf.Allocate()
	// The data should still be there since spill buffer doesn't zero on release
	// (but this is implementation detail, new data written should work)
	copy(data2, []byte("new data"))

	readData2 := buf.Get(slot2)
	if !bytes.HasPrefix(readData2, []byte("new data")) {
		t.Errorf("new data mismatch: got %q", readData2[:8])
	}
}
