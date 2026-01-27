// Package tests contains compatibility tests for large key/value combinations.
// Tests that gdbx and libmdbx handle large keys and values identically,
// including automatic overflow page handling.
package tests

import (
	"bytes"
	"crypto/rand"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestLargeKeyValueCompat_MdbxToGdbx creates large key/value pairs with mdbx
// and verifies gdbx can read them correctly.
func TestLargeKeyValueCompat_MdbxToGdbx(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Setup mdbx environment
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	menv.SetOption(mdbx.OptMaxDB, 10)

	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := menv.MaxKeySize()
	t.Logf("mdbx MaxKeySize: %d", maxKey)

	// Test cases with various key/value size combinations
	testCases := []struct {
		name    string
		keySize int
		valSize int
	}{
		{"small_key_small_val", 10, 100},
		{"small_key_medium_val", 10, 1000},
		{"small_key_large_val", 10, 2000},
		{"small_key_overflow_val", 10, 5000},  // Forces overflow
		{"small_key_big_overflow", 10, 10000}, // Definitely overflow
		{"medium_key_small_val", 500, 100},
		{"medium_key_medium_val", 500, 1000},
		{"medium_key_large_val", 500, 2000},
		{"large_key_small_val", maxKey, 100},
		{"large_key_medium_val", maxKey, 1000},
		{"large_key_large_val", maxKey, 2000},       // Near page capacity
		{"large_key_overflow_val", maxKey, 3000},   // Forces overflow
		{"maxkey_maxval_combined", maxKey, 2044},   // Exact page capacity
		{"maxkey_overflow", maxKey, 2045},          // Just over - overflow
	}

	// Create entries with mdbx
	entries := make(map[string][]byte)

	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBI("test", mdbx.Create, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for _, tc := range testCases {
		key := make([]byte, tc.keySize)
		rand.Read(key)
		// Make key unique by prefixing with test name
		copy(key, tc.name)

		value := make([]byte, tc.valSize)
		rand.Read(value)

		err := txn.Put(dbi, key, value, 0)
		if err != nil {
			t.Errorf("mdbx Put %s (key=%d, val=%d) failed: %v", tc.name, tc.keySize, tc.valSize, err)
			continue
		}

		entries[string(key)] = value
		t.Logf("mdbx Put %s (key=%d, val=%d): OK", tc.name, tc.keySize, tc.valSize)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	menv.Close()

	// Read with gdbx and verify
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	genv.SetMaxDBs(10)
	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gtxn.Abort()

	gdbi, err := gtxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	for keyStr, expectedVal := range entries {
		key := []byte(keyStr)
		gotVal, err := gtxn.Get(gdbi, key)
		if err != nil {
			t.Errorf("gdbx Get (key len=%d) failed: %v", len(key), err)
			continue
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("gdbx Get (key len=%d): value mismatch, got %d bytes, want %d bytes",
				len(key), len(gotVal), len(expectedVal))
		}
	}

	t.Logf("All %d entries verified successfully", len(entries))
}

// TestLargeKeyValueCompat_GdbxToMdbx creates large key/value pairs with gdbx
// and verifies mdbx can read them correctly.
func TestLargeKeyValueCompat_GdbxToMdbx(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Setup gdbx environment
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	genv.SetMaxDBs(10)
	if err := genv.Open(db.path, 0, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := genv.MaxKeySize()
	t.Logf("gdbx MaxKeySize: %d", maxKey)

	// Test cases
	testCases := []struct {
		name    string
		keySize int
		valSize int
	}{
		{"small_key_small_val", 10, 100},
		{"small_key_overflow_val", 10, 5000},
		{"medium_key_medium_val", 500, 1000},
		{"large_key_small_val", maxKey, 100},
		{"large_key_large_val", maxKey, 2000},
		{"maxkey_near_capacity", maxKey, 2044},
		{"maxkey_overflow", maxKey, 2045},
		{"maxkey_big_overflow", maxKey, 5000},
	}

	// Create entries with gdbx
	entries := make(map[string][]byte)

	gtxn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	gdbi, err := gtxn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		gtxn.Abort()
		t.Fatal(err)
	}

	for _, tc := range testCases {
		key := make([]byte, tc.keySize)
		rand.Read(key)
		copy(key, tc.name)

		value := make([]byte, tc.valSize)
		rand.Read(value)

		err := gtxn.Put(gdbi, key, value, 0)
		if err != nil {
			t.Errorf("gdbx Put %s (key=%d, val=%d) failed: %v", tc.name, tc.keySize, tc.valSize, err)
			continue
		}

		entries[string(key)] = value
		t.Logf("gdbx Put %s (key=%d, val=%d): OK", tc.name, tc.keySize, tc.valSize)
	}

	if _, err := gtxn.Commit(); err != nil {
		t.Fatal(err)
	}
	genv.Close()

	// Read with mdbx and verify
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer menv.Close()

	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	menv.SetOption(mdbx.OptMaxDB, 10)

	if err := menv.Open(db.path, mdbx.Readonly, 0644); err != nil {
		t.Fatal(err)
	}

	mtxn, err := menv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mtxn.Abort()

	mdbi, err := mtxn.OpenDBI("test", 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	for keyStr, expectedVal := range entries {
		key := []byte(keyStr)
		gotVal, err := mtxn.Get(mdbi, key)
		if err != nil {
			t.Errorf("mdbx Get (key len=%d) failed: %v", len(key), err)
			continue
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("mdbx Get (key len=%d): value mismatch, got %d bytes, want %d bytes",
				len(key), len(gotVal), len(expectedVal))
		}
	}

	t.Logf("All %d entries verified successfully", len(entries))
}

// TestMaxKeySizeMatch verifies gdbx and mdbx return the same MaxKeySize
func TestMaxKeySizeMatch(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Get mdbx MaxKeySize
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}
	mdbxMaxKey := menv.MaxKeySize()
	menv.Close()

	// Get gdbx MaxKeySize
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}
	gdbxMaxKey := genv.MaxKeySize()
	genv.Close()

	t.Logf("mdbx MaxKeySize: %d", mdbxMaxKey)
	t.Logf("gdbx MaxKeySize: %d", gdbxMaxKey)

	if mdbxMaxKey != gdbxMaxKey {
		t.Errorf("MaxKeySize mismatch: mdbx=%d, gdbx=%d", mdbxMaxKey, gdbxMaxKey)
	}
}

// TestOverflowPageCompat tests that overflow pages are compatible
func TestOverflowPageCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Test various overflow sizes
	overflowSizes := []int{
		5000,   // 2 pages
		10000,  // 3 pages
		20000,  // 5 pages
		50000,  // 13 pages
		100000, // 25 pages
	}

	for _, size := range overflowSizes {
		t.Run("mdbx_to_gdbx_"+string(rune(size)), func(t *testing.T) {
			testOverflowMdbxToGdbx(t, size)
		})
		t.Run("gdbx_to_mdbx_"+string(rune(size)), func(t *testing.T) {
			testOverflowGdbxToMdbx(t, size)
		})
	}
}

func testOverflowMdbxToGdbx(t *testing.T, valSize int) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Create with mdbx
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	key := []byte("overflow_test_key")
	value := make([]byte, valSize)
	rand.Read(value)

	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, _ := txn.OpenRoot(0)
	if err := txn.Put(dbi, key, value, 0); err != nil {
		txn.Abort()
		t.Fatalf("mdbx Put (val=%d) failed: %v", valSize, err)
	}
	txn.Commit()
	menv.Close()

	// Read with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gtxn.Abort()

	gotVal, err := gtxn.Get(gdbx.MainDBI, key)
	if err != nil {
		t.Fatalf("gdbx Get failed: %v", err)
	}
	if !bytes.Equal(gotVal, value) {
		t.Errorf("Value mismatch: got %d bytes, want %d bytes", len(gotVal), len(value))
	}
}

func testOverflowGdbxToMdbx(t *testing.T, valSize int) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Create with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	if err := genv.Open(db.path, 0, 0644); err != nil {
		t.Fatal(err)
	}

	key := []byte("overflow_test_key")
	value := make([]byte, valSize)
	rand.Read(value)

	gtxn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := gtxn.Put(gdbx.MainDBI, key, value, 0); err != nil {
		gtxn.Abort()
		t.Fatalf("gdbx Put (val=%d) failed: %v", valSize, err)
	}
	gtxn.Commit()
	genv.Close()

	// Read with mdbx
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer menv.Close()

	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := menv.Open(db.path, mdbx.Readonly, 0644); err != nil {
		t.Fatal(err)
	}

	mtxn, err := menv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mtxn.Abort()

	mdbi, _ := mtxn.OpenRoot(0)
	gotVal, err := mtxn.Get(mdbi, key)
	if err != nil {
		t.Fatalf("mdbx Get failed: %v", err)
	}
	if !bytes.Equal(gotVal, value) {
		t.Errorf("Value mismatch: got %d bytes, want %d bytes", len(gotVal), len(value))
	}
}

// TestBoundaryValuesCompat tests values right at the overflow boundary
func TestBoundaryValuesCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Setup environments
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := menv.MaxKeySize()
	// Calculate boundary: pageCapacity - nodeHeader - keyLen
	// pageCapacity = 4096 - 20 - 2 = 4074
	// For maxKey=2022: boundary = 4074 - 8 - 2022 = 2044
	boundary := 4074 - 8 - maxKey

	t.Logf("MaxKeySize: %d, Overflow boundary with maxKey: %d", maxKey, boundary)

	// Test values around the boundary
	testVals := []int{
		boundary - 10, // Definitely inline
		boundary - 1,  // Just under
		boundary,      // Exact boundary
		boundary + 1,  // Just over - should overflow
		boundary + 10, // Definitely overflow
	}

	// Create with mdbx
	entries := make(map[string][]byte)
	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, _ := txn.OpenRoot(0)

	for i, valSize := range testVals {
		key := make([]byte, maxKey)
		copy(key, []byte("boundary_test_"))
		key[14] = byte(i)

		value := make([]byte, valSize)
		rand.Read(value)

		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Errorf("mdbx Put (val=%d) failed: %v", valSize, err)
			continue
		}
		entries[string(key)] = value
		t.Logf("mdbx Put (key=%d, val=%d): OK", len(key), valSize)
	}

	txn.Commit()
	menv.Close()

	// Read with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gtxn.Abort()

	for keyStr, expectedVal := range entries {
		key := []byte(keyStr)
		gotVal, err := gtxn.Get(gdbx.MainDBI, key)
		if err != nil {
			t.Errorf("gdbx Get (key=%d, val=%d) failed: %v", len(key), len(expectedVal), err)
			continue
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("gdbx Get (val=%d): value mismatch", len(expectedVal))
		} else {
			t.Logf("gdbx Get (val=%d): OK", len(expectedVal))
		}
	}
}

// TestMultipleLargeEntriesCompat tests many large entries together
func TestMultipleLargeEntriesCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Create with gdbx - multiple large entries
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	if err := genv.Open(db.path, 0, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := genv.MaxKeySize()
	entries := make(map[string][]byte)

	gtxn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 50 entries with varying sizes
	for i := 0; i < 50; i++ {
		keySize := 10 + (i % (maxKey - 10))
		valSize := 100 + (i * 200) // 100 to 10000 bytes

		key := make([]byte, keySize)
		rand.Read(key)
		key[0] = byte(i)

		value := make([]byte, valSize)
		rand.Read(value)

		if err := gtxn.Put(gdbx.MainDBI, key, value, 0); err != nil {
			t.Errorf("gdbx Put %d (key=%d, val=%d) failed: %v", i, keySize, valSize, err)
			continue
		}
		entries[string(key)] = value
	}

	gtxn.Commit()
	genv.Close()

	// Read with mdbx
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer menv.Close()

	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := menv.Open(db.path, mdbx.Readonly, 0644); err != nil {
		t.Fatal(err)
	}

	mtxn, err := menv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mtxn.Abort()

	mdbi, _ := mtxn.OpenRoot(0)

	verified := 0
	for keyStr, expectedVal := range entries {
		key := []byte(keyStr)
		gotVal, err := mtxn.Get(mdbi, key)
		if err != nil {
			t.Errorf("mdbx Get failed: %v", err)
			continue
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("mdbx Get: value mismatch for key len=%d", len(key))
			continue
		}
		verified++
	}

	t.Logf("Verified %d/%d entries", verified, len(entries))
	if verified != len(entries) {
		t.Errorf("Not all entries verified: %d/%d", verified, len(entries))
	}
}
