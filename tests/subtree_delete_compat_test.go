// Package tests contains compatibility tests for DUPSORT sub-tree deletion.
// These tests verify that gdbx can delete values from sub-trees and produce
// databases that libmdbx can read correctly.
package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestSubTreeDeleteCompat tests deleting values from a sub-tree and verifying
// the result with libmdbx.
func TestSubTreeDeleteCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "gdbx-subtree-delete-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Create database with gdbx and add many values to trigger sub-tree
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Create DUPSORT database and insert many values
	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("dupsort", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Insert 200 values to trigger sub-tree conversion
	key := []byte("testkey")
	numValues := 200
	for i := 0; i < numValues; i++ {
		value := []byte(fmt.Sprintf("value-%04d", i))
		if err := txn.Put(dbi, key, value, 0); err != nil {
			txn.Abort()
			env.Close()
			t.Fatalf("Put failed for value %d: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}

	// Now delete every other value
	txn, err = env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	deletedCount := 0
	for i := 0; i < numValues; i += 2 {
		value := []byte(fmt.Sprintf("value-%04d", i))
		_, _, err := cur.Get(key, value, gdbx.GetBoth)
		if err != nil {
			cur.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("GetBoth failed for value %d: %v", i, err)
		}

		if err := cur.Del(0); err != nil {
			cur.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("Del failed for value %d: %v", i, err)
		}
		deletedCount++
	}

	cur.Close()
	t.Logf("Deleted %d values with gdbx", deletedCount)

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit after delete failed: %v", err)
	}

	env.Close()

	// Now verify with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)

	if err := mdbxEnv.Open(dbPath, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	mdbxDbi, err := mdbxTxn.OpenDBI("dupsort", 0, nil, nil)
	if err != nil {
		t.Fatalf("libmdbx OpenDBI failed: %v", err)
	}

	// Count remaining values with libmdbx
	mdbxCur, err := mdbxTxn.OpenCursor(mdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxCur.Close()

	// Position at the key
	_, _, err = mdbxCur.Get(key, nil, mdbx.Set)
	if err != nil {
		t.Fatalf("libmdbx Set failed: %v", err)
	}

	// Count duplicates
	count, err := mdbxCur.Count()
	if err != nil {
		t.Fatalf("libmdbx Count failed: %v", err)
	}

	expectedCount := uint64(numValues - deletedCount)
	if count != expectedCount {
		t.Errorf("libmdbx Count: got %d, want %d", count, expectedCount)
	}
	t.Logf("libmdbx reports %d remaining values (expected %d)", count, expectedCount)

	// Verify all remaining values are correct (odd-indexed)
	k, v, err := mdbxCur.Get(key, nil, mdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	readCount := 0
	expectedIdx := 1 // First remaining value should be value-0001
	for err == nil {
		if string(k) != string(key) {
			t.Errorf("Unexpected key: %q", k)
		}
		expected := fmt.Sprintf("value-%04d", expectedIdx)
		if string(v) != expected {
			t.Errorf("Value %d: got %q, want %q", readCount, v, expected)
		}
		readCount++
		expectedIdx += 2
		k, v, err = mdbxCur.Get(nil, nil, mdbx.NextDup)
	}

	if readCount != int(expectedCount) {
		t.Errorf("Read %d values, expected %d", readCount, expectedCount)
	}

	t.Logf("libmdbx verified %d values correctly", readCount)
}

// TestSubTreeDeleteAllCompat tests deleting ALL values from a sub-tree.
func TestSubTreeDeleteAllCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "gdbx-subtree-delete-all-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Create database with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Create DUPSORT database and insert values
	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("dupsort", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Insert values for two keys - one with sub-tree, one with sub-page
	key1 := []byte("key1")
	key2 := []byte("key2")

	// Key1: 200 values (sub-tree)
	for i := 0; i < 200; i++ {
		value := []byte(fmt.Sprintf("val1-%04d", i))
		if err := txn.Put(dbi, key1, value, 0); err != nil {
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}
	}

	// Key2: 5 values (sub-page)
	for i := 0; i < 5; i++ {
		value := []byte(fmt.Sprintf("val2-%04d", i))
		if err := txn.Put(dbi, key2, value, 0); err != nil {
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Delete ALL values from key1 (sub-tree)
	txn, err = env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Position at key1 and delete all values
	_, _, err = cur.Get(key1, nil, gdbx.Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Delete using NoDupData flag to delete all duplicates at once
	if err := cur.Del(gdbx.NoDupData); err != nil {
		cur.Close()
		txn.Abort()
		env.Close()
		t.Fatalf("Del with NoDupData failed: %v", err)
	}

	cur.Close()
	t.Log("Deleted all values for key1")

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}

	env.Close()

	// Verify with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)

	if err := mdbxEnv.Open(dbPath, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	mdbxDbi, err := mdbxTxn.OpenDBI("dupsort", 0, nil, nil)
	if err != nil {
		t.Fatalf("libmdbx OpenDBI failed: %v", err)
	}

	// key1 should not exist
	_, err = mdbxTxn.Get(mdbxDbi, key1)
	if !mdbx.IsNotFound(err) {
		t.Errorf("key1 should not exist, got: %v", err)
	} else {
		t.Log("key1 correctly not found")
	}

	// key2 should still exist with 5 values
	mdbxCur, err := mdbxTxn.OpenCursor(mdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxCur.Close()

	_, _, err = mdbxCur.Get(key2, nil, mdbx.Set)
	if err != nil {
		t.Fatalf("libmdbx Set for key2 failed: %v", err)
	}

	count, err := mdbxCur.Count()
	if err != nil {
		t.Fatalf("libmdbx Count for key2 failed: %v", err)
	}

	if count != 5 {
		t.Errorf("key2 count: got %d, want 5", count)
	}

	t.Logf("key2 has %d values as expected", count)
}

// TestRoundTripSubTreeDelete tests writing with libmdbx, deleting with gdbx,
// and verifying with libmdbx.
func TestRoundTripSubTreeDelete(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "gdbx-roundtrip-delete-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Create database with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)

	if err := mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Create, 0644); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	mdbxDbi, err := mdbxTxn.OpenDBI("dupsort", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		mdbxTxn.Abort()
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Insert 200 values with libmdbx to create a sub-tree
	key := []byte("testkey")
	for i := 0; i < 200; i++ {
		value := []byte(fmt.Sprintf("value-%04d", i))
		if err := mdbxTxn.Put(mdbxDbi, key, value, 0); err != nil {
			mdbxTxn.Abort()
			mdbxEnv.Close()
			t.Fatalf("libmdbx Put failed: %v", err)
		}
	}

	if _, err := mdbxTxn.Commit(); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	mdbxEnv.Close()
	t.Log("Created database with libmdbx (200 values)")

	// Now delete values with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("dupsort", 0)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Delete first 50 values
	for i := 0; i < 50; i++ {
		value := []byte(fmt.Sprintf("value-%04d", i))
		_, _, err := cur.Get(key, value, gdbx.GetBoth)
		if err != nil {
			cur.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("gdbx GetBoth failed for %d: %v", i, err)
		}

		if err := cur.Del(0); err != nil {
			cur.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("gdbx Del failed for %d: %v", i, err)
		}
	}

	cur.Close()

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	env.Close()
	t.Log("Deleted 50 values with gdbx")

	// Verify with libmdbx
	mdbxEnv, err = mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)

	if err := mdbxEnv.Open(dbPath, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err = mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	mdbxDbi, err = mdbxTxn.OpenDBI("dupsort", 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mdbxCur, err := mdbxTxn.OpenCursor(mdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxCur.Close()

	_, _, err = mdbxCur.Get(key, nil, mdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	count, err := mdbxCur.Count()
	if err != nil {
		t.Fatal(err)
	}

	if count != 150 {
		t.Errorf("libmdbx Count: got %d, want 150", count)
	}

	// Verify first remaining value is value-0050
	k, v, err := mdbxCur.Get(key, nil, mdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	if string(k) != string(key) {
		t.Errorf("Unexpected key: %q", k)
	}

	if string(v) != "value-0050" {
		t.Errorf("First value: got %q, want %q", v, "value-0050")
	}

	t.Logf("Round-trip test passed: libmdbx verified %d remaining values", count)
}
