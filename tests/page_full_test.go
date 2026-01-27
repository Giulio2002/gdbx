package tests

import (
	"os"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestMaxKeyMaxValueCombination tests that using both maxKey and maxValue
// together triggers ErrPageFull (bug: they should fit or value should go to overflow)
func TestMaxKeyMaxValueCombination(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-pagefull-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	// Get the limits
	maxKey := env.MaxKeySize()
	maxVal := env.MaxValSize()
	pageSize := 4096 // Default page size

	t.Logf("PageSize: %d, MaxKeySize: %d, MaxValSize: %d", pageSize, maxKey, maxVal)
	t.Logf("Node size with max key+val: %d (header=8 + key=%d + val=%d)", 8+maxKey+maxVal, maxKey, maxVal)
	t.Logf("Available space on empty page: %d (pageSize=%d - header=20 - entryPtr=2)", pageSize-20-2, pageSize)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	db, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		t.Fatal(err)
	}

	// Create max-size key and value
	key := make([]byte, maxKey)
	for i := range key {
		key[i] = byte(i % 256)
	}

	value := make([]byte, maxVal)
	for i := range value {
		value[i] = byte((i + 128) % 256)
	}

	// This should work but currently triggers ErrPageFull
	err = txn.Put(db, key, value, 0)
	if err != nil {
		t.Errorf("Put with maxKey(%d) + maxVal(%d) failed: %v", maxKey, maxVal, err)
		t.Logf("This is a bug - the combination should either fit or value should go to overflow")
	} else {
		t.Logf("Put succeeded")

		// Verify we can read it back
		got, err := txn.Get(db, key)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		} else if len(got) != len(value) {
			t.Errorf("Got value length %d, want %d", len(got), len(value))
		}
	}
}

// TestLargeKeyModerateValue tests various key/value size combinations
func TestLargeKeyModerateValue(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-largekey-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(20)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	maxKey := env.MaxKeySize()
	maxVal := env.MaxValSize()

	testCases := []struct {
		name    string
		keySize int
		valSize int
	}{
		{"maxKey_smallVal", maxKey, 100},
		{"maxKey_halfMaxVal", maxKey, maxVal / 2},
		{"maxKey_maxVal-100", maxKey, maxVal - 100},
		{"maxKey_maxVal-10", maxKey, maxVal - 10},
		{"maxKey_maxVal-1", maxKey, maxVal - 1},
		{"maxKey_maxVal", maxKey, maxVal},
		{"halfMaxKey_maxVal", maxKey / 2, maxVal},
		{"smallKey_maxVal", 10, maxVal},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer txn.Abort()

			db, err := txn.OpenDBISimple(tc.name, gdbx.Create)
			if err != nil {
				t.Fatal(err)
			}

			key := make([]byte, tc.keySize)
			value := make([]byte, tc.valSize)

			err = txn.Put(db, key, value, 0)
			if err != nil {
				t.Errorf("Put(keySize=%d, valSize=%d) failed: %v", tc.keySize, tc.valSize, err)
			}
		})
	}
}

// TestSplitWithLargeNode tests that page splits work correctly with large nodes
func TestSplitWithLargeNode(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-split-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	maxKey := env.MaxKeySize()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	db, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Fill page with large keys to force splits
	// Use keys that are close to maxKey but leave room for the node header
	keySize := maxKey - 100 // Leave some room
	valSize := 100

	for i := 0; i < 20; i++ {
		key := make([]byte, keySize)
		key[0] = byte(i) // Make keys unique

		value := make([]byte, valSize)

		err = txn.Put(db, key, value, 0)
		if err != nil {
			t.Errorf("Put %d failed: %v", i, err)
			break
		}
	}

	_, err = txn.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestDupSortWithLargeValues tests DupSort tables with values near the limit
func TestDupSortWithLargeValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-dupsort-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	maxVal := env.MaxValSize()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	db, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := []byte("testkey")

	// Add multiple large values for the same key
	for i := 0; i < 5; i++ {
		// Use value size close to max but fitting in a sub-page initially
		valSize := 500 // Start with values that fit in sub-page
		value := make([]byte, valSize)
		value[0] = byte(i)

		err = txn.Put(db, key, value, 0)
		if err != nil {
			t.Errorf("Put dup %d failed: %v", i, err)
		}
	}

	// Now try adding a very large value that forces sub-tree conversion
	largeVal := make([]byte, maxVal/2)
	largeVal[0] = 0xFF

	err = txn.Put(db, key, largeVal, 0)
	if err != nil {
		t.Errorf("Put large dup value failed: %v", err)
	}

	_, err = txn.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestNodeSizeAtBoundary tests node sizes right at the boundary
func TestNodeSizeAtBoundary(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-boundary-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	pageSize := 4096 // Default page size
	maxKey := env.MaxKeySize()
	maxVal := env.MaxValSize()

	// Calculate the exact boundary
	// Node = 8 (header) + keyLen + valLen
	// Page can hold: pageSize - 20 (header) - 2 (entry ptr) = pageSize - 22
	maxNodeSize := pageSize - 22
	nodeHeaderSize := 8

	t.Logf("PageSize: %d", pageSize)
	t.Logf("MaxNodeSize that fits: %d", maxNodeSize)
	t.Logf("MaxKey: %d, MaxVal: %d", maxKey, maxVal)
	t.Logf("MaxKey + MaxVal + header: %d", maxKey+maxVal+nodeHeaderSize)

	// Test cases at the boundary
	testCases := []struct {
		name    string
		keySize int
		valSize int
	}{
		// These should all work
		{"fits_exactly", 100, maxNodeSize - nodeHeaderSize - 100},
		{"fits_with_1_byte_room", 100, maxNodeSize - nodeHeaderSize - 100 - 1},

		// This might fail if maxKey + maxVal > maxNodeSize - nodeHeaderSize
		{"maxKey_adjusted_val", maxKey, maxNodeSize - nodeHeaderSize - maxKey},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.valSize < 0 {
				t.Skipf("Skipping - calculated valSize is negative: %d", tc.valSize)
			}
			if tc.valSize > maxVal {
				t.Logf("Note: valSize %d > maxVal %d, will use overflow", tc.valSize, maxVal)
			}

			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer txn.Abort()

			db, err := txn.OpenDBISimple(tc.name, gdbx.Create)
			if err != nil {
				t.Fatal(err)
			}

			key := make([]byte, tc.keySize)
			value := make([]byte, tc.valSize)

			err = txn.Put(db, key, value, 0)
			if err != nil {
				t.Errorf("Put(keySize=%d, valSize=%d, nodeSize=%d) failed: %v",
					tc.keySize, tc.valSize, tc.keySize+tc.valSize+nodeHeaderSize, err)
			}
		})
	}
}
