package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestSplitIdxZeroWithMiddleInsert specifically tests the bug where
// splitIdx=0 with idx > 0 would cause ErrPageFull.
//
// The bug: When splitIdx=0, all existing entries move to the new page,
// leaving the old page empty (numEntries=0). The code then tries to
// insert at the original idx position, but when idx > 0, the bounds
// check (idx > numEntries) fails and returns false -> ErrPageFull.
//
// The fix: When splitIdx=0, always insert at index 0 since the page is empty.
func TestSplitIdxZeroWithMiddleInsert(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-splitidxzero-fix-*")
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
	t.Logf("MaxValSize: %d, PageCapacity: %d", maxVal, 4096-20)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Strategy to trigger splitIdx=0:
	// 1. Fill page with small entries so total size is close to maxSpace
	// 2. New huge node can only fit ALONE on a page (splitIdx=0 or splitIdx=numEntries)
	// 3. Insert the huge node in the MIDDLE (idx > 0 and idx < numEntries)
	// 4. Split algorithm should choose splitIdx=0 (new node alone on left)
	// 5. Bug: code would try to insert at idx > 0 in empty page -> fail
	// 6. Fix: code inserts at index 0 when splitIdx=0

	// Fill page with small entries (each ~35 bytes)
	// Node = 8 (header) + 4 (key) + 20 (val) = 32, with 2-byte pointer = 34
	// To fill 4076 bytes: need ~120 entries
	numSmallEntries := 100

	for i := 0; i < numSmallEntries; i++ {
		k := make([]byte, 4)
		// Keys: 0x0000, 0x0002, 0x0004, ... (even numbers, leaving gaps for insertion)
		k[0] = byte((i * 2) >> 8)
		k[1] = byte((i * 2) & 0xFF)
		v := make([]byte, 20)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Calculate approximate used space
	// 100 entries * (2 + 8 + 4 + 20) = 100 * 34 = 3400 bytes
	t.Logf("Filled page with %d entries, approx %d bytes used", numSmallEntries, numSmallEntries*34)
	t.Logf("Approx free space: %d bytes", 4076-numSmallEntries*34)

	// Now insert a HUGE node in the middle
	// Key: 0x0063 (99 in decimal) - sorts between 0x0062 (entry 49) and 0x0064 (entry 50)
	// So idx should be ~50 (in the middle)
	k := make([]byte, 4)
	k[0] = 0x00
	k[1] = 0x63 // 99 - sorts between 98 (0x62) and 100 (0x64)

	// Value close to maxVal so the node is huge (~2045 bytes total)
	v := make([]byte, maxVal)

	nodeSize := 8 + len(k) + len(v)
	t.Logf("Inserting huge node in middle: nodeSize=%d, key=%x", nodeSize, k)

	// This should trigger a split. Before the fix, splitIdx=0 with idx>0 would fail.
	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Insert huge node failed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify the data
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	got, err := txn.Get(dbi, k)
	if err != nil {
		txn.Abort()
		t.Fatalf("Get failed: %v", err)
	}
	if len(got) != len(v) {
		t.Errorf("Got value length %d, want %d", len(got), len(v))
	}
	txn.Abort()

	t.Log("splitIdx=0 with middle insert: PASSED (bug was fixed)")
}

// TestSplitIdxZeroVariousPositions tests insertions at various positions
// to ensure the fix handles all cases correctly.
func TestSplitIdxZeroVariousPositions(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		insertPos int // Relative position: 0=beginning, 50=middle, 99=end
	}{
		{"beginning", 0},
		{"quarter", 25},
		{"middle", 50},
		{"three_quarter", 75},
		{"end", 99},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "gdbx-splitpos-*")
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

			// Fill with 100 small entries
			for i := 0; i < 100; i++ {
				k := make([]byte, 4)
				k[0] = byte((i * 2) >> 8)
				k[1] = byte((i * 2) & 0xFF)
				v := make([]byte, 20)
				if err := txn.Put(dbi, k, v, 0); err != nil {
					txn.Abort()
					t.Fatal(err)
				}
			}

			// Insert at specified position
			k := make([]byte, 4)
			insertKey := testCase.insertPos*2 + 1 // Odd number to insert between evens
			k[0] = byte(insertKey >> 8)
			k[1] = byte(insertKey & 0xFF)
			v := make([]byte, maxVal)

			err = txn.Put(dbi, k, v, 0)
			if err != nil {
				txn.Abort()
				t.Fatalf("Insert at position %d failed: %v", testCase.insertPos, err)
			}

			if _, err := txn.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}
		})
	}
}
