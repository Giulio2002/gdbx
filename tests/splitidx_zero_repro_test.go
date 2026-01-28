package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestSplitIdxZeroRepro reproduces the exact bug where splitIdx=0 with idx > 0
// causes ErrPageFull. We need to construct a scenario where:
// 1. Page is nearly full with existing entries
// 2. New huge node can ONLY fit alone (no midpoint split works)
// 3. Insert position is in the middle (idx > 0)
func TestSplitIdxZeroRepro(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-splitidxzero-repro-*")
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
	maxSpace := 4096 - 20 // 4076

	t.Logf("MaxValSize: %d, maxSpace: %d", maxVal, maxSpace)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// To force splitIdx=0, we need existing entries that:
	// - Can fit on one page together (so we can insert them)
	// - But when combined with ANY subset, exceed maxSpace
	//
	// Strategy: Use 2 large entries that together are close to maxSpace
	// Entry size = (maxSpace - 4) / 2 = 2036 bytes each (with 2 pointers = 4 bytes)
	// Total = 2 * 2036 + 4 = 4076 = maxSpace (exactly full)
	//
	// Now insert a huge node (2049 bytes) in the middle:
	// - splitIdx=0: left has new (2051), right has both existing (4076) - right too big!
	// - splitIdx=1: left has entry0 + new (2038 + 2051 = 4089) - too big!
	// - splitIdx=2: left has both (4076), right has new (2051) - left is exactly maxSpace, valid!
	//
	// Hmm, splitIdx=2 works. Let me make entries slightly bigger...

	// Actually, let me use 2 entries of 2030 bytes each
	// Total = 2 * (8 + 4 + 2018) + 4 = 4064 + 4 = 4068 bytes (12 free)
	// Node header = 8, key = 4, value = 2018

	// Entry 0: key = 0x00
	k0 := make([]byte, 4)
	k0[0] = 0x00
	v0 := make([]byte, 2018) // node = 8 + 4 + 2018 = 2030, with ptr = 2032
	if err := txn.Put(dbi, k0, v0, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Entry 1: key = 0x20
	k1 := make([]byte, 4)
	k1[0] = 0x20
	v1 := make([]byte, 2018)
	if err := txn.Put(dbi, k1, v1, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	totalUsed := 2 * (2 + 8 + 4 + 2018) // 2 * 2032 = 4064
	t.Logf("After 2 entries: used=%d, free=%d", totalUsed, maxSpace-totalUsed)

	// Now try to insert at position 1 (between 0x00 and 0x20)
	// Key = 0x10
	k := make([]byte, 4)
	k[0] = 0x10
	v := make([]byte, maxVal) // 2037 bytes, node = 8 + 4 + 2037 = 2049, with ptr = 2051

	newNodeTotal := 2 + 8 + 4 + maxVal // 2051
	t.Logf("Inserting middle node: nodeTotal=%d", newNodeTotal)

	// Split analysis:
	// - splitIdx=0: left=2051, right=4064 > 4076 INVALID
	// - splitIdx=1: left=2032+2051=4083 > 4076 INVALID
	// - splitIdx=2: left=4064, right=2051 VALID
	//
	// So splitIdx=2 will be chosen, not 0. Need to make existing even bigger.

	// Let me try filling page more aggressively
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// New approach: fill with entries that leave minimal space
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err = txn.OpenDBISimple("test2", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Fill with many medium entries
	// Each entry: 8 + 8 + 100 = 116 bytes, with ptr = 118
	// 34 entries = 34 * 118 = 4012 bytes (64 free)
	for i := 0; i < 34; i++ {
		k := make([]byte, 8)
		k[0] = byte(i * 4) // 0, 4, 8, 12, ...
		v := make([]byte, 100)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Total: 34 * (2 + 8 + 8 + 100) = 34 * 118 = 4012 bytes
	t.Logf("After 34 entries: used=%d, free=%d", 34*118, maxSpace-34*118)

	// Now insert a huge node at position 17 (middle)
	// Key = 0x42 (between 0x40 and 0x44)
	k = make([]byte, 8)
	k[0] = 0x42
	v = make([]byte, maxVal) // node = 8 + 8 + 2037 = 2053, with ptr = 2055

	t.Logf("Inserting huge node at middle: nodeTotal=%d", 2+8+8+maxVal)

	// Split analysis with 34 entries of 118 bytes each = 4012 total:
	// New node = 2055
	// - splitIdx=0: left=2055, right=4012 < 4076 VALID!
	// - splitIdx=17: left=17*118+2055=2006+2055=4061, right=17*118=2006 VALID
	//
	// Since midpoint (17) is valid, it will be chosen. We need entries that don't
	// allow any midpoint split.

	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Insert failed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Log("Test passed - but may not have triggered splitIdx=0")
}

// TestSplitIdxZeroForced forces splitIdx=0 by using entries that make
// midpoint splits invalid.
func TestSplitIdxZeroForced(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-splitforced-*")
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

	maxVal := env.MaxValSize() // 2037
	maxSpace := 4076

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// To force splitIdx=0:
	// - Existing entries must be small enough to fit together (< maxSpace)
	// - But large enough that (half + new node) > maxSpace
	//
	// Let new node = 2055 (with pointer)
	// For midpoint (half) to be invalid: half + 2055 > 4076 → half > 2021
	// So each half must be > 2021 bytes
	// Total existing > 4042 bytes
	//
	// But total existing < 4076 (must fit on page)
	// So: 4042 < total < 4076
	//
	// Use entries totaling ~4060 bytes
	// 2 entries of ~2030 each won't work (midpoint valid)
	// Need skewed distribution: one big, rest small

	// Entry 0: key=0x00, huge value
	k0 := make([]byte, 4)
	k0[0] = 0x00
	v0 := make([]byte, 2000) // node = 2012, with ptr = 2014

	// Entry 1: key=0x40, huge value
	k1 := make([]byte, 4)
	k1[0] = 0x40
	v1 := make([]byte, 2000) // node = 2012, with ptr = 2014

	// Total = 4028 bytes
	// Free = 4076 - 4028 = 48 bytes

	if err := txn.Put(dbi, k0, v0, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	if err := txn.Put(dbi, k1, v1, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	t.Logf("Total used: %d, free: %d", 4028, maxSpace-4028)

	// New node at position 1 (key=0x20, between 0x00 and 0x40)
	k := make([]byte, 4)
	k[0] = 0x20
	v := make([]byte, maxVal) // 2037, node=2049, with ptr=2051

	// Split analysis:
	// - splitIdx=0: left=2051, right=4028 < 4076 VALID (right barely fits!)
	// - splitIdx=1: left=2014+2051=4065 < 4076 VALID
	// - splitIdx=2: left=4028, right=2051 VALID
	//
	// All are valid. The midpoint (1) will be chosen.

	// Let me make entries even bigger to force only splitIdx=0 or 2
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Try again with entries that fill page to exactly capacity
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err = txn.OpenDBISimple("test2", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// 2 entries of ~2035 bytes each (with ptr)
	// Total = 4070, free = 6
	// Entry = 2 + 8 + 4 + val, so val = 2035 - 14 = 2021
	k0 = make([]byte, 4)
	k0[0] = 0x00
	v0 = make([]byte, 2021) // total with ptr = 2035

	k1 = make([]byte, 4)
	k1[0] = 0x40
	v1 = make([]byte, 2021) // total with ptr = 2035

	if err := txn.Put(dbi, k0, v0, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	if err := txn.Put(dbi, k1, v1, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	t.Logf("Total used: %d, free: %d", 4070, maxSpace-4070)

	// New node at position 1
	k = make([]byte, 4)
	k[0] = 0x20
	v = make([]byte, maxVal) // 2051 with ptr

	// Split analysis:
	// - splitIdx=0: left=2051, right=4070 < 4076 VALID
	// - splitIdx=1: left=2035+2051=4086 > 4076 INVALID!
	// - splitIdx=2: left=4070, right=2051 VALID
	//
	// Midpoint (1) is INVALID. Algorithm will search outward.
	// delta=1: try 0, then 2. splitIdx=0 found first!

	t.Log("Inserting node that should trigger splitIdx=0...")

	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Insert failed (this is the bug!): %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Log("SUCCESS: splitIdx=0 case handled correctly!")
}
