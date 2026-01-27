package tests

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestSplitIdxZero reproduces the bug where splitIdx=0 caused "page has no space" error.
// When a large node needs to be inserted and the only valid split is to put it alone
// on the left page (splitIdx=0), the old code incorrectly put it on the right page.
func TestSplitIdxZero(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-split-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Use small page size (4KB) to trigger the issue more easily
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	if err := env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096); err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Create a DBI
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// For a 4KB page:
	// - maxSpace = 4096 - 20 = 4076 bytes
	// - LeafNodeMax = 4076 / 2 = 2038 bytes
	//
	// We want to create a scenario where:
	// 1. Page has a few entries that together use most of the space
	// 2. We insert a large node (~2000 bytes) that requires splitIdx=0
	//    (the large node alone on left, all existing entries on right)

	// First, insert entries that will fill most of a page
	// Each entry: 8 bytes header + key + value
	// Let's create 3 entries of ~1300 bytes each = ~3900 bytes total
	// This leaves ~176 bytes free, not enough for a large node

	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")

	// Values sized to nearly fill the page
	// Node size = 8 (header) + 4 (key) + len(value)
	// 3 entries need 3*2 = 6 bytes for pointers
	// So we have 4076 - 6 = 4070 bytes for node data
	// Each node: 8 + 4 + value = 12 + value
	// 3 nodes: 36 + 3*value = 4070 => value = 1344 bytes each
	value1 := bytes.Repeat([]byte("a"), 1344)
	value2 := bytes.Repeat([]byte("b"), 1344)
	value3 := bytes.Repeat([]byte("c"), 1344)

	// Insert the three entries
	if err := txn.Put(dbi, key1, value1, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	if err := txn.Put(dbi, key2, value2, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}
	if err := txn.Put(dbi, key3, value3, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Now insert a large node at the beginning (key "aaa" < "key1")
	// This should trigger splitIdx=0: new node alone on left, existing entries on right
	// Node size = 8 + 3 + 2000 = 2011 bytes (close to LeafNodeMax of 2038)
	largeKey := []byte("aaa")
	largeValue := bytes.Repeat([]byte("x"), 2000)

	// This was failing with "page has no space" before the fix
	if err := txn.Put(dbi, largeKey, largeValue, 0); err != nil {
		txn.Abort()
		t.Fatalf("Failed to insert large node (splitIdx=0 bug): %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify all entries are readable
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	// Check all 4 entries exist
	expectedKeys := [][]byte{largeKey, key1, key2, key3}
	expectedValues := [][]byte{largeValue, value1, value2, value3}

	for i, expectedKey := range expectedKeys {
		v, err := txn.Get(dbi, expectedKey)
		if err != nil {
			t.Fatalf("Failed to get key %q: %v", expectedKey, err)
		}
		if !bytes.Equal(v, expectedValues[i]) {
			t.Fatalf("Value mismatch for key %q: got %d bytes, want %d bytes", expectedKey, len(v), len(expectedValues[i]))
		}
	}

	t.Log("splitIdx=0 test passed: large node correctly placed alone on left page")
}

// TestSplitIdxZeroDupSort tests the splitIdx=0 case with DupSort tables,
// which was the actual scenario causing issues in Erigon.
func TestSplitIdxZeroDupSort(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-split-dup-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	// Use 4KB page size
	if err := env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096); err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Create DupSort table
	dbi, err := txn.OpenDBISimple("duptest", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Create entries with inline sub-pages that will grow large
	// When a sub-page grows too large, it converts to a sub-tree node
	// which is 8 + keyLen + 48 bytes

	// First, create several keys with many duplicate values to fill the page
	// Each DupSort node with sub-page: 8 + keyLen + subPageSize

	key1 := bytes.Repeat([]byte("k"), 100) // 100-byte key
	key2 := bytes.Repeat([]byte("l"), 100)
	key3 := bytes.Repeat([]byte("m"), 100)

	// Add many small duplicates to each key to create large sub-pages
	for i := 0; i < 50; i++ {
		val := []byte{byte(i)}
		if err := txn.Put(dbi, key1, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if err := txn.Put(dbi, key2, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if err := txn.Put(dbi, key3, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	// Now insert a new key at the beginning with many duplicates
	// This creates a large node that may require splitIdx=0
	key0 := bytes.Repeat([]byte("a"), 100) // Sorts before key1
	for i := 0; i < 50; i++ {
		val := []byte{byte(i)}
		if err := txn.Put(dbi, key0, val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Failed to insert duplicate %d for key0: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify all entries
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Count all entries
	count := 0
	_, _, err = cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		count++
		_, _, err = cursor.Get(nil, nil, gdbx.Next)
	}
	if !gdbx.IsNotFound(err) {
		t.Fatalf("Unexpected error during iteration: %v", err)
	}

	expectedCount := 50 * 4 // 4 keys, 50 dups each
	if count != expectedCount {
		t.Fatalf("Entry count mismatch: got %d, want %d", count, expectedCount)
	}

	t.Logf("DupSort splitIdx=0 test passed: %d entries verified", count)
}
