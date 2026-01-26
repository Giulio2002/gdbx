// Package tests contains reverse compatibility tests - writing with gdbx
// and reading with libmdbx to verify gdbx produces valid MDBX databases.
package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// newReverseTestDB creates a temporary file path for reverse compat tests.
// gdbx creates the database file, and libmdbx reads it with NoSubdir flag.
func newReverseTestDB(t *testing.T) *testDB {
	t.Helper()
	dir, err := os.MkdirTemp("", "gdbx-reverse-*")
	if err != nil {
		t.Fatal(err)
	}
	return &testDB{
		path: filepath.Join(dir, "test.db"),
		cleanup: func() {
			os.RemoveAll(dir)
		},
	}
}

// TestReverseBasicReadWrite creates a database with gdbx and reads it with libmdbx.
func TestReverseBasicReadWrite(t *testing.T) {
	// Pin to OS thread for libmdbx
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Put some key-value pairs
	testData := map[string]string{
		"hello":                      "world",
		"foo":                        "bar",
		"github.com/Giulio2002/gdbx": "works",
		"key123":                     "value456",
	}

	for k, v := range testData {
		if err := txn.Put(gdbx.MainDBI, []byte(k), []byte(v), gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatalf("Put %q failed: %v", k, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}
	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all values with libmdbx
	for k, want := range testData {
		got, err := mdbxTxn.Get(dbi, []byte(k))
		if err != nil {
			t.Errorf("libmdbx Get %q failed: %v", k, err)
			continue
		}
		if string(got) != want {
			t.Errorf("libmdbx Get %q: got %q, want %q", k, got, want)
		}
	}

	t.Logf("Successfully wrote %d entries with gdbx and read them with libmdbx", len(testData))
}

// TestReverseManyEntries creates many entries with gdbx and verifies with libmdbx.
func TestReverseManyEntries(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	numEntries := 1000

	// Write with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	keys := make([]string, numEntries)
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%08d", i)
		value := fmt.Sprintf("value-%08d", i)
		keys[i] = key
		if err := txn.Put(gdbx.MainDBI, []byte(key), []byte(value), gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatalf("Put %q failed: %v", key, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}
	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify random lookups
	for i := 0; i < 100; i++ {
		idx := i * 10
		key := fmt.Sprintf("key-%08d", idx)
		want := fmt.Sprintf("value-%08d", idx)

		got, err := mdbxTxn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("libmdbx Get %q failed: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("libmdbx Get %q: got %q, want %q", key, got, want)
		}
	}

	// Verify count by cursor iteration
	cursor, err := mdbxTxn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	for {
		_, _, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("cursor iteration error: %v", err)
		}
		count++
	}

	if count != numEntries {
		t.Errorf("libmdbx count mismatch: got %d, want %d", count, numEntries)
	}

	t.Logf("Successfully wrote %d entries with gdbx and verified with libmdbx", numEntries)
}

// TestReverseCursorIteration verifies cursor operations on gdbx-written data.
func TestReverseCursorIteration(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Insert in non-alphabetical order
	entries := []struct{ k, v string }{
		{"zebra", "z"},
		{"apple", "a"},
		{"mango", "m"},
		{"banana", "b"},
		{"orange", "o"},
	}

	for _, e := range entries {
		if err := txn.Put(gdbx.MainDBI, []byte(e.k), []byte(e.v), gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatalf("Put %q failed: %v", e.k, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}
	env.Close()

	// Expected sorted order
	sortedKeys := []string{"apple", "banana", "mango", "orange", "zebra"}

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := mdbxTxn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Iterate forward and verify order
	var gotKeys []string
	for {
		k, _, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("cursor error: %v", err)
		}
		gotKeys = append(gotKeys, string(k))
	}

	if len(gotKeys) != len(sortedKeys) {
		t.Errorf("key count mismatch: got %d, want %d", len(gotKeys), len(sortedKeys))
	}

	for i, want := range sortedKeys {
		if i >= len(gotKeys) {
			break
		}
		if gotKeys[i] != want {
			t.Errorf("key[%d] mismatch: got %q, want %q", i, gotKeys[i], want)
		}
	}

	// Test SetRange (seek)
	k, v, err := cursor.Get([]byte("ma"), nil, mdbx.SetRange)
	if err != nil {
		t.Fatalf("SetRange failed: %v", err)
	}
	if string(k) != "mango" {
		t.Errorf("SetRange key mismatch: got %q, want %q", k, "mango")
	}
	if string(v) != "m" {
		t.Errorf("SetRange value mismatch: got %q, want %q", v, "m")
	}

	t.Log("Cursor iteration verified successfully")
}

// TestReverseBinaryKeys tests binary (non-string) keys.
func TestReverseBinaryKeys(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Insert binary keys
	type entry struct {
		key   []byte
		value []byte
	}
	entries := []entry{
		{[]byte{0x00, 0x01, 0x02}, []byte("zero-one-two")},
		{[]byte{0xFF, 0xFE, 0xFD}, []byte("high-bytes")},
		{[]byte{0x80}, []byte("mid-byte")},
	}

	for _, e := range entries {
		if err := txn.Put(gdbx.MainDBI, e.key, e.value, gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}
	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		got, err := mdbxTxn.Get(dbi, e.key)
		if err != nil {
			t.Errorf("libmdbx Get %x failed: %v", e.key, err)
			continue
		}
		if !bytes.Equal(got, e.value) {
			t.Errorf("libmdbx Get %x: got %q, want %q", e.key, got, e.value)
		}
	}

	t.Log("Binary keys verified successfully")
}

// TestReverseUpdates tests that updates work correctly.
func TestReverseUpdates(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write initial data with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// First transaction: insert
	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	if err := txn.Put(gdbx.MainDBI, []byte("key1"), []byte("original"), gdbx.Upsert); err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}
	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Second transaction: update
	txn2, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	if err := txn2.Put(gdbx.MainDBI, []byte("key1"), []byte("updated"), gdbx.Upsert); err != nil {
		txn2.Abort()
		env.Close()
		t.Fatal(err)
	}
	if _, err := txn2.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	got, err := mdbxTxn.Get(dbi, []byte("key1"))
	if err != nil {
		t.Fatalf("libmdbx Get failed: %v", err)
	}
	if string(got) != "updated" {
		t.Errorf("libmdbx Get: got %q, want %q", got, "updated")
	}

	t.Log("Update verified successfully")
}

// TestReverseDelete tests that deletes work correctly.
func TestReverseDelete(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Insert multiple keys
	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := txn.Put(gdbx.MainDBI, []byte(k), []byte("value-"+k), gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Delete some keys
	txn2, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	if err := txn2.Del(gdbx.MainDBI, []byte("b"), nil); err != nil {
		txn2.Abort()
		env.Close()
		t.Fatal(err)
	}
	if err := txn2.Del(gdbx.MainDBI, []byte("d"), nil); err != nil {
		txn2.Abort()
		env.Close()
		t.Fatal(err)
	}
	if _, err := txn2.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify remaining keys
	remaining := []string{"a", "c", "e"}
	deleted := []string{"b", "d"}

	for _, k := range remaining {
		got, err := mdbxTxn.Get(dbi, []byte(k))
		if err != nil {
			t.Errorf("libmdbx Get %q (should exist) failed: %v", k, err)
			continue
		}
		want := "value-" + k
		if string(got) != want {
			t.Errorf("libmdbx Get %q: got %q, want %q", k, got, want)
		}
	}

	for _, k := range deleted {
		_, err := mdbxTxn.Get(dbi, []byte(k))
		if !mdbx.IsNotFound(err) {
			t.Errorf("libmdbx Get %q (should be deleted): expected NotFound, got %v", k, err)
		}
	}

	t.Log("Delete verified successfully")
}

// TestReverseLargeValues tests overflow pages with large values.
func TestReverseLargeValues(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Create a large value that requires overflow pages
	// Default page size is 4096, max inline value is about 2000 bytes
	largeValue := make([]byte, 8000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	if err := txn.Put(gdbx.MainDBI, []byte("large-key"), largeValue, gdbx.Upsert); err != nil {
		txn.Abort()
		env.Close()
		t.Fatalf("Put large value failed: %v", err)
	}

	// Also add a small value for comparison
	if err := txn.Put(gdbx.MainDBI, []byte("small-key"), []byte("small"), gdbx.Upsert); err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}
	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify large value
	got, err := mdbxTxn.Get(dbi, []byte("large-key"))
	if err != nil {
		t.Fatalf("libmdbx Get large-key failed: %v", err)
	}
	if !bytes.Equal(got, largeValue) {
		t.Errorf("libmdbx large value mismatch: got %d bytes, want %d bytes", len(got), len(largeValue))
		if len(got) > 0 && len(largeValue) > 0 {
			// Check first few bytes
			checkLen := 100
			if len(got) < checkLen {
				checkLen = len(got)
			}
			if !bytes.Equal(got[:checkLen], largeValue[:checkLen]) {
				t.Errorf("First %d bytes differ: got %x, want %x", checkLen, got[:checkLen], largeValue[:checkLen])
			}
		}
	}

	// Verify small value still works
	small, err := mdbxTxn.Get(dbi, []byte("small-key"))
	if err != nil {
		t.Fatalf("libmdbx Get small-key failed: %v", err)
	}
	if string(small) != "small" {
		t.Errorf("libmdbx small value: got %q, want %q", small, "small")
	}

	t.Logf("Large value (%d bytes) verified successfully", len(largeValue))
}

// TestReversePageSplit tests that page splits work correctly.
func TestReversePageSplit(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx - enough data to cause page splits
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Insert 500 entries with medium-sized values to force page splits
	numEntries := 500
	valueSize := 100
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < numEntries; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Put(gdbx.MainDBI, key, value, gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit failed: %v", err)
	}
	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		got, err := mdbxTxn.Get(dbi, key)
		if err != nil {
			t.Errorf("libmdbx Get %d failed: %v", i, err)
			continue
		}
		if !bytes.Equal(got, value) {
			t.Errorf("libmdbx Get %d: value mismatch", i)
		}
	}

	// Verify sorted order via cursor
	cursor, err := mdbxTxn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	var prevKey []byte
	for {
		k, _, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("cursor error: %v", err)
		}

		if prevKey != nil && bytes.Compare(prevKey, k) >= 0 {
			t.Errorf("keys not sorted: %x >= %x", prevKey, k)
		}
		prevKey = append([]byte{}, k...)
		count++
	}

	if count != numEntries {
		t.Errorf("count mismatch: got %d, want %d", count, numEntries)
	}

	t.Logf("Page split test: %d entries verified in sorted order", numEntries)
}

// TestReverseMultipleTransactions tests multiple sequential transactions.
func TestReverseMultipleTransactions(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx in multiple transactions
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Transaction 1
	txn1, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("txn1-key-%03d", i)
		val := fmt.Sprintf("txn1-val-%03d", i)
		if err := txn1.Put(gdbx.MainDBI, []byte(key), []byte(val), gdbx.Upsert); err != nil {
			txn1.Abort()
			env.Close()
			t.Fatal(err)
		}
	}
	if _, err := txn1.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Transaction 2
	txn2, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("txn2-key-%03d", i)
		val := fmt.Sprintf("txn2-val-%03d", i)
		if err := txn2.Put(gdbx.MainDBI, []byte(key), []byte(val), gdbx.Upsert); err != nil {
			txn2.Abort()
			env.Close()
			t.Fatal(err)
		}
	}
	if _, err := txn2.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Transaction 3: some updates
	txn3, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}
	if err := txn3.Put(gdbx.MainDBI, []byte("txn1-key-000"), []byte("updated-by-txn3"), gdbx.Upsert); err != nil {
		txn3.Abort()
		env.Close()
		t.Fatal(err)
	}
	if _, err := txn3.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}

	env.Close()

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify txn1 keys (except updated one)
	for i := 1; i < 100; i++ {
		key := fmt.Sprintf("txn1-key-%03d", i)
		want := fmt.Sprintf("txn1-val-%03d", i)
		got, err := mdbxTxn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get %q failed: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Get %q: got %q, want %q", key, got, want)
		}
	}

	// Verify updated key
	got, err := mdbxTxn.Get(dbi, []byte("txn1-key-000"))
	if err != nil {
		t.Fatalf("Get updated key failed: %v", err)
	}
	if string(got) != "updated-by-txn3" {
		t.Errorf("Updated key: got %q, want %q", got, "updated-by-txn3")
	}

	// Verify txn2 keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("txn2-key-%03d", i)
		want := fmt.Sprintf("txn2-val-%03d", i)
		got, err := mdbxTxn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get %q failed: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Get %q: got %q, want %q", key, got, want)
		}
	}

	// Count total entries
	cursor, err := mdbxTxn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	for {
		_, _, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count != 200 {
		t.Errorf("Total count: got %d, want %d", count, 200)
	}

	t.Logf("Multiple transactions verified: %d total entries", count)
}

// TestReverseKeyOrder verifies that gdbx maintains correct key ordering.
func TestReverseKeyOrder(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newReverseTestDB(t)
	defer db.cleanup()

	// Write with gdbx in random order
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Open(db.path, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Insert in intentionally scrambled order
	scrambled := []string{
		"zebra", "apple", "mango", "banana", "kiwi",
		"zzz", "aaa", "mmm", "bbb", "kkk",
	}

	for _, k := range scrambled {
		if err := txn.Put(gdbx.MainDBI, []byte(k), []byte("val-"+k), gdbx.Upsert); err != nil {
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}
	env.Close()

	// Get expected sorted order
	sorted := make([]string, len(scrambled))
	copy(sorted, scrambled)
	sort.Strings(sorted)

	// Read with libmdbx
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	if err := mdbxEnv.Open(db.path, mdbx.Readonly|mdbx.NoSubdir, 0644); err != nil {
		t.Fatalf("libmdbx Open failed: %v", err)
	}

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	dbi, err := mdbxTxn.OpenRoot(0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := mdbxTxn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Verify order matches expected sorted order
	idx := 0
	for {
		k, v, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		if idx >= len(sorted) {
			t.Errorf("More entries than expected")
			break
		}

		if string(k) != sorted[idx] {
			t.Errorf("Key[%d] mismatch: got %q, want %q", idx, k, sorted[idx])
		}

		want := "val-" + sorted[idx]
		if string(v) != want {
			t.Errorf("Value[%d] mismatch: got %q, want %q", idx, v, want)
		}

		idx++
	}

	if idx != len(sorted) {
		t.Errorf("Entry count mismatch: got %d, want %d", idx, len(sorted))
	}

	t.Logf("Key order verified: %d entries in correct sorted order", len(sorted))
}
