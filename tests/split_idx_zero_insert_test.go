package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestSplitIdxZeroInsertBug tests the bug where splitIdx=0 with idx>0 causes ErrPageFull.
// When splitIdx=0, ALL existing entries go to the new page, leaving the old page empty.
// The new node should be inserted at index 0 in the empty old page, but the code
// was incorrectly using the original idx.
func TestSplitIdxZeroInsertBug(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-splitidxzero-*")
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

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// To trigger splitIdx=0, we need:
	// 1. A page full of small entries
	// 2. Insert a HUGE new node that doesn't fit alongside any existing entries
	// 3. The new node must be inserted somewhere in the MIDDLE (idx > 0)

	// Fill the page with many small entries with keys that are sorted
	// such that our "large" key will be in the middle
	for i := 0; i < 30; i++ {
		k := make([]byte, 20)
		// Keys: 00, 02, 04, 06, 08, ...
		k[0] = byte(i * 2)
		v := make([]byte, 50) // Small values
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Now insert a key that sorts in the MIDDLE with a value that's
	// just under the overflow threshold (so it's inline but HUGE)
	// Key 15 should sort between 14 (entry 7) and 16 (entry 8)
	k := make([]byte, 20)
	k[0] = 15 // Sorts between key[7] (14) and key[8] (16)

	// Value just under overflow threshold - this creates a huge node
	// that likely won't fit with ANY existing entries on the same page
	v := make([]byte, maxVal)

	t.Logf("Inserting large node at middle position, nodeSize=%d", 8+len(k)+len(v))

	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Insert failed with: %v", err)
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

	t.Log("Test passed!")
}

// TestSplitIdxZeroStress is a more aggressive version to trigger the bug
func TestSplitIdxZeroStress(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-splitidxzero-stress-*")
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

	// Fill with many tiny entries first
	for i := 0; i < 100; i++ {
		k := make([]byte, 4)
		k[0] = byte(i)
		k[1] = byte(i >> 8)
		v := make([]byte, 10)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Now insert large values at various positions
	for insertion := 0; insertion < 20; insertion++ {
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		// Key that sorts at different positions
		k := make([]byte, 4)
		k[0] = byte(insertion*5 + 2) // 2, 7, 12, 17, ... inserts between existing keys

		// Large value close to threshold
		v := make([]byte, maxVal-50)

		err = txn.Put(dbi, k, v, 0)
		if err != nil {
			txn.Abort()
			t.Fatalf("Insert %d failed: %v", insertion, err)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Commit %d failed: %v", insertion, err)
		}
	}

	t.Log("Stress test passed!")
}
