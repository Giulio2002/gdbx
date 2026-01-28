package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestDebugSplit tries to trigger the splitIdx=0 case more aggressively
func TestDebugSplit(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-debugsplit-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	maxVal := env.MaxValSize()
	t.Logf("MaxValSize: %d", maxVal)
	t.Logf("Page capacity: %d", 4096-20)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Strategy: Fill page so that entries total is close to page capacity,
	// then try to insert a huge node in the middle.
	//
	// Page capacity = 4076 bytes
	// Each entry needs: 2 (pointer) + 8 (header) + keyLen + valLen
	//
	// If we have 2 entries of ~2000 bytes each = 4000 bytes total
	// Plus pointers: 4 bytes
	// Total: 4004 bytes, leaving ~70 bytes free
	//
	// Now insert a 2073 byte node at position 1 (middle)
	// splitIdx=0 would mean: new node alone (2073 + 2 = 2075) fits
	// All existing (4004) does NOT fit (> 4076)!
	// So splitIdx=0 is invalid!
	//
	// splitIdx=1 would mean: entry[0] + new node on left, entry[1] on right
	// Left: 2 entries * 2 = 4 + 2000 + 2073 = 4077 > 4076 - invalid!
	// Right: 1 entry * 2 = 2 + 2000 = 2002 - valid
	//
	// splitIdx=2 would mean: all existing on left, new node on right
	// Left: 2 entries * 2 = 4 + 4000 = 4004 - valid
	// Right: 1 entry * 2 = 2 + 2073 = 2075 - valid
	// This should be chosen!

	// Let's try filling with large entries
	entrySize := 1800 // Each entry: 2 + 8 + 20 + 1800 = 1830 bytes
	keySize := 20

	// Entry 0: key = 00...
	k0 := make([]byte, keySize)
	v0 := make([]byte, entrySize)
	if err := txn.Put(dbi, k0, v0, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	t.Logf("Inserted entry 0: key=%x, nodeSize=%d", k0[0], 8+keySize+entrySize)

	// Entry 1: key = 20...
	k1 := make([]byte, keySize)
	k1[0] = 0x20
	v1 := make([]byte, entrySize)
	if err := txn.Put(dbi, k1, v1, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	t.Logf("Inserted entry 1: key=%x, nodeSize=%d", k1[0], 8+keySize+entrySize)

	// Total so far: 2 * (1830 + 2) = 3664 bytes
	t.Logf("Total page usage: ~%d bytes", 2*(8+keySize+entrySize+2))
	t.Logf("Free space: ~%d bytes", 4076-2*(8+keySize+entrySize+2))

	// Now try to insert a large node at position 1 (between 00 and 20)
	// Key = 10...
	k := make([]byte, keySize)
	k[0] = 0x10
	v := make([]byte, maxVal) // 2037 bytes
	nodeSize := 8 + keySize + len(v)
	t.Logf("Inserting middle node: key=%x, nodeSize=%d", k[0], nodeSize)

	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Insert failed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Log("Test passed!")
}

// TestExtremeCase tries an extreme case to trigger splitIdx=0
func TestExtremeCase(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-extreme-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	maxVal := env.MaxValSize()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Fill page with one huge entry
	// If there's 1 entry of ~3500 bytes, we have:
	// Used: 2 + 3500 = 3502
	// Free: 4076 - 3502 = 574
	//
	// Now insert another huge entry (2073 bytes) - doesn't fit
	// Split options:
	// - splitIdx=0: new (2075) on left, existing (3502) on right - right too big!
	// - splitIdx=1: existing (3502) on left, new (2075) on right - both fit!

	// So splitIdx=1 should be chosen. Let's try with even bigger entries.

	// Entry 0: key = 00, value = pageCapacity - overhead
	// overhead = header(20) + 1 pointer(2) + node header(8) + key(20) = 50
	// So value = 4076 - 50 = 4026? No that's too big for one entry.
	// Actually max entry size is about pageCapacity/2 to allow 2 entries

	// Let me try a different approach: fill page with many small entries
	// until free space is minimal, then try inserting a huge one

	keySize := 4
	valSize := 36 // Node = 8 + 4 + 36 = 48 bytes, with pointer = 50
	numEntries := 80 // 80 * 50 = 4000 bytes

	for i := 0; i < numEntries; i++ {
		k := make([]byte, keySize)
		k[0] = byte(i * 2) // Even numbers: 0, 2, 4, ...
		v := make([]byte, valSize)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	t.Logf("Inserted %d entries, each ~50 bytes, total ~%d bytes", numEntries, numEntries*50)

	// Now insert a huge node at position 40 (middle)
	// Key = 79 (between 78 and 80)
	k := make([]byte, keySize)
	k[0] = 79
	v := make([]byte, maxVal)
	t.Logf("Inserting huge node at middle, nodeSize=%d", 8+keySize+maxVal)

	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Huge insert failed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Log("Extreme test passed!")
}
