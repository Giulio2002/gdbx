package tests

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestCursorVisibilityAfterWrite tests what a cursor sees when the same RW transaction
// writes data after the cursor was opened.
func TestCursorVisibilityAfterWrite(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "cursor-visibility-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mdbxPath := filepath.Join(dir, "mdbx.db")
	gdbxPath := filepath.Join(dir, "gdbx.db")

	// Test with mdbx
	t.Log("=== Testing with mdbx ===")
	mdbxResults := testCursorVisibilityMdbx(t, mdbxPath)

	// Test with gdbx
	t.Log("=== Testing with gdbx ===")
	gdbxResults := testCursorVisibilityGdbx(t, gdbxPath)

	// Compare results
	t.Log("=== Comparing results ===")
	if len(mdbxResults) != len(gdbxResults) {
		t.Fatalf("Result count mismatch: mdbx=%d, gdbx=%d", len(mdbxResults), len(gdbxResults))
	}

	for i := range mdbxResults {
		if mdbxResults[i] != gdbxResults[i] {
			t.Errorf("Result %d mismatch: mdbx=%q, gdbx=%q", i, mdbxResults[i], gdbxResults[i])
		}
	}

	t.Log("Cursor visibility behavior matches between mdbx and gdbx")
}

func testCursorVisibilityMdbx(t *testing.T, path string) []string {
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	// Start RW transaction
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBI("test", mdbx.Create, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert initial data
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("initial%d", i))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("mdbx: Inserted initial 5 keys")

	// Open cursor BEFORE writing more data
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Read first entry with cursor
	k, v, err := cursor.Get(nil, nil, mdbx.First)
	if err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatal(err)
	}
	t.Logf("mdbx: Cursor at first: k=%q, v=%q", k, v)

	// Now write MORE data using the same transaction (not cursor)
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("added%d", i))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("mdbx: Added 5 more keys via txn.Put")

	// Also update an existing key
	if err := txn.Put(dbi, []byte("key2"), []byte("UPDATED2"), 0); err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatal(err)
	}
	t.Log("mdbx: Updated key2 via txn.Put")

	// Now iterate with the cursor that was opened BEFORE the writes
	var results []string
	t.Log("mdbx: Iterating with cursor opened before writes:")
	k, v, err = cursor.Get(nil, nil, mdbx.First)
	for err == nil {
		result := fmt.Sprintf("%s=%s", k, v)
		results = append(results, result)
		t.Logf("  %s", result)
		k, v, err = cursor.Get(nil, nil, mdbx.Next)
	}
	if !mdbx.IsNotFound(err) {
		cursor.Close()
		txn.Abort()
		t.Fatalf("Unexpected error: %v", err)
	}

	cursor.Close()
	txn.Abort()

	return results
}

func testCursorVisibilityGdbx(t *testing.T, path string) []string {
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetMaxDBs(10)

	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Start RW transaction
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert initial data
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("initial%d", i))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("gdbx: Inserted initial 5 keys")

	// Open cursor BEFORE writing more data
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Read first entry with cursor
	k, v, err := cursor.Get(nil, nil, gdbx.First)
	if err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatal(err)
	}
	t.Logf("gdbx: Cursor at first: k=%q, v=%q", k, v)

	// Now write MORE data using the same transaction (not cursor)
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("added%d", i))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("gdbx: Added 5 more keys via txn.Put")

	// Also update an existing key
	if err := txn.Put(dbi, []byte("key2"), []byte("UPDATED2"), 0); err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatal(err)
	}
	t.Log("gdbx: Updated key2 via txn.Put")

	// Now iterate with the cursor that was opened BEFORE the writes
	var results []string
	t.Log("gdbx: Iterating with cursor opened before writes:")
	k, v, err = cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		result := fmt.Sprintf("%s=%s", k, v)
		results = append(results, result)
		t.Logf("  %s", result)
		k, v, err = cursor.Get(nil, nil, gdbx.Next)
	}
	if !gdbx.IsNotFound(err) {
		cursor.Close()
		txn.Abort()
		t.Fatalf("Unexpected error: %v", err)
	}

	cursor.Close()
	txn.Abort()

	return results
}

// TestCursorVisibilityDupSort tests cursor visibility with DupSort tables
func TestCursorVisibilityDupSort(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "cursor-visibility-dup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mdbxPath := filepath.Join(dir, "mdbx.db")
	gdbxPath := filepath.Join(dir, "gdbx.db")

	// Test with mdbx
	t.Log("=== Testing DupSort with mdbx ===")
	mdbxResults := testCursorVisibilityDupMdbx(t, mdbxPath)

	// Test with gdbx
	t.Log("=== Testing DupSort with gdbx ===")
	gdbxResults := testCursorVisibilityDupGdbx(t, gdbxPath)

	// Compare results
	t.Log("=== Comparing DupSort results ===")
	if len(mdbxResults) != len(gdbxResults) {
		t.Fatalf("Result count mismatch: mdbx=%d, gdbx=%d", len(mdbxResults), len(gdbxResults))
	}

	for i := range mdbxResults {
		if mdbxResults[i] != gdbxResults[i] {
			t.Errorf("Result %d mismatch: mdbx=%q, gdbx=%q", i, mdbxResults[i], gdbxResults[i])
		}
	}

	t.Log("DupSort cursor visibility behavior matches between mdbx and gdbx")
}

func testCursorVisibilityDupMdbx(t *testing.T, path string) []string {
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBI("duptest", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert initial duplicates for key1
	key1 := []byte("key1")
	for i := 0; i < 3; i++ {
		val := []byte(fmt.Sprintf("val%d", i))
		if err := txn.Put(dbi, key1, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("mdbx: Inserted 3 values for key1")

	// Open cursor and position at key1
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	k, v, err := cursor.Get(key1, nil, mdbx.Set)
	if err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatal(err)
	}
	t.Logf("mdbx: Cursor positioned at: k=%q, v=%q", k, v)

	// Add more duplicates to key1 via txn.Put
	for i := 3; i < 6; i++ {
		val := []byte(fmt.Sprintf("val%d", i))
		if err := txn.Put(dbi, key1, val, 0); err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("mdbx: Added 3 more values for key1 via txn.Put")

	// Iterate all duplicates with the cursor
	var results []string
	t.Log("mdbx: Iterating duplicates:")
	k, v, err = cursor.Get(key1, nil, mdbx.Set)
	for err == nil && bytes.Equal(k, key1) {
		result := fmt.Sprintf("%s=%s", k, v)
		results = append(results, result)
		t.Logf("  %s", result)
		k, v, err = cursor.Get(nil, nil, mdbx.Next)
	}

	cursor.Close()
	txn.Abort()

	return results
}

func testCursorVisibilityDupGdbx(t *testing.T, path string) []string {
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetMaxDBs(10)

	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("duptest", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert initial duplicates for key1
	key1 := []byte("key1")
	for i := 0; i < 3; i++ {
		val := []byte(fmt.Sprintf("val%d", i))
		if err := txn.Put(dbi, key1, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("gdbx: Inserted 3 values for key1")

	// Open cursor and position at key1
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	k, v, err := cursor.Get(key1, nil, gdbx.Set)
	if err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatal(err)
	}
	t.Logf("gdbx: Cursor positioned at: k=%q, v=%q", k, v)

	// Add more duplicates to key1 via txn.Put
	for i := 3; i < 6; i++ {
		val := []byte(fmt.Sprintf("val%d", i))
		if err := txn.Put(dbi, key1, val, 0); err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatal(err)
		}
	}
	t.Log("gdbx: Added 3 more values for key1 via txn.Put")

	// Iterate all duplicates with the cursor
	var results []string
	t.Log("gdbx: Iterating duplicates:")
	k, v, err = cursor.Get(key1, nil, gdbx.Set)
	for err == nil && bytes.Equal(k, key1) {
		result := fmt.Sprintf("%s=%s", k, v)
		results = append(results, result)
		t.Logf("  %s", result)
		k, v, err = cursor.Get(nil, nil, gdbx.Next)
	}

	cursor.Close()
	txn.Abort()

	return results
}
