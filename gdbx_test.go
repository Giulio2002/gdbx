package gdbx

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNewEnv(t *testing.T) {
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	if env == nil {
		t.Fatal("NewEnv returned nil")
	}
	if !env.valid() {
		t.Fatal("environment is not valid")
	}
}

func TestOpenClose(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and open environment
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Verify path
	if env.Path() != dbPath {
		t.Errorf("Path mismatch: got %q, want %q", env.Path(), dbPath)
	}

	// Close
	env.Close()
}

func TestBeginAbortTransaction(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and open environment
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Begin a read-only transaction
	txn, err := env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	if !txn.IsReadOnly() {
		t.Error("transaction should be read-only")
	}

	// Abort transaction
	txn.Abort()
}

func TestWriteTransaction(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and open environment
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Begin a write transaction
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	if txn.IsReadOnly() {
		t.Error("transaction should not be read-only")
	}

	// Abort (don't commit for now since Put isn't fully implemented)
	txn.Abort()
}

func TestMaxKeySize(t *testing.T) {
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}

	maxKey := env.MaxKeySize()
	if maxKey <= 0 {
		t.Errorf("MaxKeySize should be > 0, got %d", maxKey)
	}

	// Default page size is 4096, max key = pageSize/2 - 8 - 2 = 2038
	expected := DefaultPageSize/2 - 8 - 2
	if maxKey != expected {
		t.Errorf("MaxKeySize mismatch: got %d, want %d", maxKey, expected)
	}
}

func TestErrorCodes(t *testing.T) {
	// Verify error creation
	err := NewError(ErrNotFound)
	if err == nil {
		t.Fatal("NewError returned nil")
	}

	if err.Code != ErrNotFound {
		t.Errorf("error code mismatch: got %d, want %d", err.Code, ErrNotFound)
	}

	// Test IsNotFound
	if !IsNotFound(err) {
		t.Error("IsNotFound should return true")
	}

	// Test Code function
	if Code(err) != ErrNotFound {
		t.Errorf("Code mismatch: got %d, want %d", Code(err), ErrNotFound)
	}

	// Test nil error
	if Code(nil) != Success {
		t.Errorf("Code(nil) should return Success, got %d", Code(nil))
	}
}

func TestConstants(t *testing.T) {
	// Verify constants match MDBX
	if Magic != 0x59659DBDEF4C11 {
		t.Errorf("Magic mismatch: got %x, want %x", Magic, uint64(0x59659DBDEF4C11))
	}

	if DataVersion != 3 {
		t.Errorf("DataVersion mismatch: got %d, want %d", DataVersion, 3)
	}

	if NumMetas != 3 {
		t.Errorf("NumMetas mismatch: got %d, want %d", NumMetas, 3)
	}

	if MaxDBI != 32765 {
		t.Errorf("MaxDBI mismatch: got %d, want %d", MaxDBI, 32765)
	}
}

func TestPutGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and open environment
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Begin a write transaction
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	// Put a key-value pair
	key := []byte("hello")
	value := []byte("world")
	err = txn.Put(MainDBI, key, value, Upsert)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the value back
	got, err := txn.Get(MainDBI, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Get mismatch: got %q, want %q", got, value)
	}

	// Commit
	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Read it back in a new transaction
	txn2, err := env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn2.Abort()

	got2, err := txn2.Get(MainDBI, key)
	if err != nil {
		t.Fatalf("Get (read txn) failed: %v", err)
	}

	if string(got2) != string(value) {
		t.Errorf("Get (read txn) mismatch: got %q, want %q", got2, value)
	}
}

func TestMultiplePuts(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Begin a write transaction
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	// Put multiple key-value pairs
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"aaa":  "first",
		"zzz":  "last",
	}

	for k, v := range testData {
		err = txn.Put(MainDBI, []byte(k), []byte(v), Upsert)
		if err != nil {
			t.Fatalf("Put %q failed: %v", k, err)
		}
	}

	// Verify all values
	for k, want := range testData {
		got, err := txn.Get(MainDBI, []byte(k))
		if err != nil {
			t.Fatalf("Get %q failed: %v", k, err)
		}
		if string(got) != want {
			t.Errorf("Get %q: got %q, want %q", k, got, want)
		}
	}

	// Commit and reopen
	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify in read transaction
	txn2, err := env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn2.Abort()

	for k, want := range testData {
		got, err := txn2.Get(MainDBI, []byte(k))
		if err != nil {
			t.Fatalf("Get %q (read txn) failed: %v", k, err)
		}
		if string(got) != want {
			t.Errorf("Get %q (read txn): got %q, want %q", k, got, want)
		}
	}
}

func TestUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Insert initial value
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	key := []byte("mykey")
	err = txn.Put(MainDBI, key, []byte("value1"), Upsert)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Update the value
	txn2, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn (update) failed: %v", err)
	}

	err = txn2.Put(MainDBI, key, []byte("value2"), Upsert)
	if err != nil {
		t.Fatalf("Put (update) failed: %v", err)
	}

	// Check updated value
	got, err := txn2.Get(MainDBI, key)
	if err != nil {
		t.Fatalf("Get (after update) failed: %v", err)
	}
	if string(got) != "value2" {
		t.Errorf("Get (after update): got %q, want %q", got, "value2")
	}

	_, err = txn2.Commit()
	if err != nil {
		t.Fatalf("Commit (update) failed: %v", err)
	}

	// Verify persisted
	txn3, err := env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (verify) failed: %v", err)
	}
	defer txn3.Abort()

	got, err = txn3.Get(MainDBI, key)
	if err != nil {
		t.Fatalf("Get (verify) failed: %v", err)
	}
	if string(got) != "value2" {
		t.Errorf("Get (verify): got %q, want %q", got, "value2")
	}
}

func TestNoOverwrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	key := []byte("mykey")

	// First put should succeed
	err = txn.Put(MainDBI, key, []byte("value1"), NoOverwrite)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Second put with NoOverwrite should fail
	err = txn.Put(MainDBI, key, []byte("value2"), NoOverwrite)
	if err == nil {
		t.Fatal("Second Put with NoOverwrite should have failed")
	}
	if !IsKeyExist(err) {
		t.Errorf("Expected ErrKeyExist, got: %v", err)
	}

	// Value should still be original
	got, err := txn.Get(MainDBI, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "value1" {
		t.Errorf("Value was overwritten: got %q, want %q", got, "value1")
	}
}

func TestDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Insert some data
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	err = txn.Put(MainDBI, []byte("key1"), []byte("value1"), Upsert)
	if err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	err = txn.Put(MainDBI, []byte("key2"), []byte("value2"), Upsert)
	if err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}

	err = txn.Put(MainDBI, []byte("key3"), []byte("value3"), Upsert)
	if err != nil {
		t.Fatalf("Put key3 failed: %v", err)
	}

	// Delete key2
	err = txn.Del(MainDBI, []byte("key2"), nil)
	if err != nil {
		t.Fatalf("Del failed: %v", err)
	}

	// Verify key2 is gone
	_, err = txn.Get(MainDBI, []byte("key2"))
	if !IsNotFound(err) {
		t.Errorf("Expected NotFound after delete, got: %v", err)
	}

	// Verify key1 and key3 still exist
	v1, err := txn.Get(MainDBI, []byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if string(v1) != "value1" {
		t.Errorf("key1 value mismatch: got %q, want %q", v1, "value1")
	}

	v3, err := txn.Get(MainDBI, []byte("key3"))
	if err != nil {
		t.Fatalf("Get key3 failed: %v", err)
	}
	if string(v3) != "value3" {
		t.Errorf("key3 value mismatch: got %q, want %q", v3, "value3")
	}

	// Commit and verify deletion persists
	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Read transaction to verify
	txn2, err := env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn2.Abort()

	_, err = txn2.Get(MainDBI, []byte("key2"))
	if !IsNotFound(err) {
		t.Errorf("Expected NotFound after commit, got: %v", err)
	}

	v1, err = txn2.Get(MainDBI, []byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 (read txn) failed: %v", err)
	}
	if string(v1) != "value1" {
		t.Errorf("key1 value mismatch (read txn): got %q, want %q", v1, "value1")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	// Try to delete non-existent key
	err = txn.Del(MainDBI, []byte("nonexistent"), nil)
	if !IsNotFound(err) {
		t.Errorf("Expected NotFound for non-existent key, got: %v", err)
	}
}

func TestCursorIteration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Insert data
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		err = txn.Put(MainDBI, []byte(k), []byte("value-"+k), Upsert)
		if err != nil {
			t.Fatalf("Put %q failed: %v", k, err)
		}
	}

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Read with cursor
	txn2, err := env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn2.Abort()

	cursor, err := txn2.OpenCursor(MainDBI)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	// Iterate forward and collect keys
	var foundKeys []string
	k, _, err := cursor.Get(nil, nil, First)
	for err == nil {
		foundKeys = append(foundKeys, string(k))
		k, _, err = cursor.Get(nil, nil, Next)
	}

	if !IsNotFound(err) {
		t.Fatalf("Unexpected error during iteration: %v", err)
	}

	// Keys should be in sorted order
	if len(foundKeys) != len(keys) {
		t.Errorf("Found %d keys, expected %d", len(foundKeys), len(keys))
	}

	// Verify all keys are present
	for _, want := range keys {
		found := false
		for _, got := range foundKeys {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Key %q not found during iteration", want)
		}
	}
}

func TestDupSortDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create DUPSORT database and insert values
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("testdb", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert multiple values for k1
	err = txn.Put(dbi, []byte("k1"), []byte("v1"), 0)
	if err != nil {
		t.Fatalf("Put k1/v1 failed: %v", err)
	}
	err = txn.Put(dbi, []byte("k1"), []byte("v2"), 0)
	if err != nil {
		t.Fatalf("Put k1/v2 failed: %v", err)
	}
	err = txn.Put(dbi, []byte("k1"), []byte("v3"), 0)
	if err != nil {
		t.Fatalf("Put k1/v3 failed: %v", err)
	}
	// Insert a value for k2
	err = txn.Put(dbi, []byte("k2"), []byte("v1"), 0)
	if err != nil {
		t.Fatalf("Put k2/v1 failed: %v", err)
	}

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify initial count
	txn, err = env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenCursor failed: %v", err)
	}
	_, _, err = cur.Get([]byte("k1"), nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Get k1 failed: %v", err)
	}
	count, err := cur.Count()
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Count failed: %v", err)
	}
	if count != 3 {
		cur.Close()
		txn.Abort()
		t.Fatalf("Expected 3 values for k1, got %d", count)
	}
	t.Logf("Initial k1 count: %d", count)
	cur.Close()
	txn.Abort()

	// Delete middle value v2
	txn, err = env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn (delete) failed: %v", err)
	}
	t.Log("Deleting k1/v2...")
	err = txn.Del(dbi, []byte("k1"), []byte("v2"))
	if err != nil {
		txn.Abort()
		t.Fatalf("Del k1/v2 failed: %v", err)
	}
	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit (delete) failed: %v", err)
	}

	// Verify after delete
	txn, err = env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (verify) failed: %v", err)
	}
	cur, err = txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenCursor (verify) failed: %v", err)
	}
	_, _, err = cur.Get([]byte("k1"), nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Get k1 (verify) failed: %v", err)
	}
	count, _ = cur.Count()
	t.Logf("After delete k1 count: %d", count)

	// List remaining values
	_, v, err := cur.Get(nil, nil, FirstDup)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("FirstDup failed: %v", err)
	}
	var values []string
	for {
		values = append(values, string(v))
		_, v, err = cur.Get(nil, nil, NextDup)
		if err != nil {
			break
		}
	}
	t.Logf("Remaining values for k1: %v", values)

	cur.Close()
	txn.Abort()

	if count != 2 {
		t.Errorf("Expected 2 values after delete, got %d", count)
	}

	// v2 should not be in the list
	for _, val := range values {
		if val == "v2" {
			t.Errorf("v2 should have been deleted but still exists")
		}
	}

	// v1 and v3 should still exist
	hasV1, hasV3 := false, false
	for _, val := range values {
		if val == "v1" {
			hasV1 = true
		}
		if val == "v3" {
			hasV3 = true
		}
	}
	if !hasV1 {
		t.Errorf("v1 should still exist")
	}
	if !hasV3 {
		t.Errorf("v3 should still exist")
	}

	// Now delete v1, leaving only v3
	txn, err = env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn (delete2) failed: %v", err)
	}
	t.Log("Deleting k1/v1...")
	err = txn.Del(dbi, []byte("k1"), []byte("v1"))
	if err != nil {
		txn.Abort()
		t.Fatalf("Del k1/v1 failed: %v", err)
	}
	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit (delete2) failed: %v", err)
	}

	// Verify only v3 remains
	txn, err = env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (verify2) failed: %v", err)
	}
	cur, _ = txn.OpenCursor(dbi)
	k, v, err := cur.Get([]byte("k1"), nil, Set)
	if err != nil {
		t.Logf("Note: k1 lookup after deleting v1 failed: %v", err)
	} else {
		count, _ = cur.Count()
		t.Logf("After deleting v1, k1 count: %d, key=%q value=%q", count, k, v)
	}
	cur.Close()
	txn.Abort()

	// Delete v3 - should delete the key entirely
	txn, err = env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn (delete3) failed: %v", err)
	}

	// Check tree state before delete
	treeBefore := txn.GetTree(dbi)
	t.Logf("Before deleting v3: Items=%d", treeBefore.Items)

	t.Log("Deleting k1/v3...")
	err = txn.Del(dbi, []byte("k1"), []byte("v3"))
	if err != nil {
		txn.Abort()
		t.Fatalf("Del k1/v3 failed: %v", err)
	}

	// Check tree state after delete but before commit
	treeAfter := txn.GetTree(dbi)
	t.Logf("After deleting v3 (before commit): Items=%d", treeAfter.Items)

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit (delete3) failed: %v", err)
	}

	// Verify k1 is gone
	txn, err = env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (verify3) failed: %v", err)
	}

	// Dump tree state
	tree := txn.GetTree(dbi)
	t.Logf("Tree state: Root=%d, Items=%d, Height=%d", tree.Root, tree.Items, tree.Height)

	_, err = txn.Get(dbi, []byte("k1"))
	if err == nil {
		t.Error("k1 should not exist after deleting all values")
	} else if !IsNotFound(err) {
		t.Errorf("Expected NotFound for k1, got: %v", err)
	} else {
		t.Log("k1 correctly not found")
	}

	// k2 should still exist
	v2, err := txn.Get(dbi, []byte("k2"))
	if err != nil {
		t.Errorf("k2 should still exist: %v", err)
	} else {
		t.Logf("k2 value: %q", v2)
	}
	txn.Abort()
}

// TestDupSortSubTree tests conversion from sub-page to sub-tree when many duplicates are added
func TestDupSortSubTree(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-test-subtree-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create DUPSORT database
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("testdb", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert many duplicate values to force sub-tree conversion
	// The sub-page can hold roughly pageSize/2 - overhead bytes
	// With default 4096 byte pages, that's about 2000 bytes
	// Each value with header is ~18 bytes, so ~110 values should trigger conversion
	key := []byte("testkey")
	numValues := 200
	for i := 0; i < numValues; i++ {
		value := []byte(fmt.Sprintf("value-%04d", i))
		err = txn.Put(dbi, key, value, 0)
		if err != nil {
			t.Fatalf("Put value %d failed: %v", i, err)
		}
	}

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all values can be read back
	txn, err = env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenCursor failed: %v", err)
	}

	// Position at key and count
	_, _, err = cur.Get(key, nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Set failed: %v", err)
	}

	count, err := cur.Count()
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Count failed: %v", err)
	}

	if count != uint64(numValues) {
		cur.Close()
		txn.Abort()
		t.Fatalf("Expected %d values, got %d", numValues, count)
	}
	t.Logf("Count: %d", count)

	// Iterate through all values
	k, v, err := cur.Get(key, nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Set failed: %v", err)
	}

	readCount := 0
	for err == nil {
		if string(k) != "testkey" {
			t.Errorf("Unexpected key: %q", k)
		}
		readCount++
		k, v, err = cur.Get(nil, nil, NextDup)
	}

	if readCount != numValues {
		t.Errorf("Read %d values, expected %d", readCount, numValues)
	}
	t.Logf("Read %d values via iteration", readCount)

	// Verify first and last values
	k, v, err = cur.Get(key, nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Set failed: %v", err)
	}
	if string(v) != "value-0000" {
		t.Errorf("First value: expected 'value-0000', got %q", v)
	}

	_, v, err = cur.Get(nil, nil, LastDup)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("LastDup failed: %v", err)
	}
	expectedLast := fmt.Sprintf("value-%04d", numValues-1)
	if string(v) != expectedLast {
		t.Errorf("Last value: expected %q, got %q", expectedLast, v)
	}

	cur.Close()
	txn.Abort()
	t.Log("Sub-tree test passed")
}

// TestDupSortSubTreeDelete tests deleting values from a sub-tree (nested B+tree).
func TestDupSortSubTreeDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-subtree-delete-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	err = env.Open(dbPath, NoSubdir, 0644)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create DUPSORT database
	txn, err := env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("testdb", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert many duplicate values to force sub-tree conversion
	key := []byte("testkey")
	numValues := 200
	for i := 0; i < numValues; i++ {
		value := []byte(fmt.Sprintf("value-%04d", i))
		err = txn.Put(dbi, key, value, 0)
		if err != nil {
			t.Fatalf("Put value %d failed: %v", i, err)
		}
	}

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now delete half the values
	txn, err = env.BeginTxn(nil, TxnReadWrite)
	if err != nil {
		t.Fatalf("BeginTxn (write) failed: %v", err)
	}

	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenCursor failed: %v", err)
	}

	// Delete every other value, starting fresh for each lookup
	deletedCount := 0
	for i := 0; i < numValues; i += 2 {
		// Re-position cursor from key (reset sub-tree state)
		value := []byte(fmt.Sprintf("value-%04d", i))
		_, v, err := cur.Get(key, value, GetBoth)
		if err != nil {
			cur.Close()
			txn.Abort()
			t.Fatalf("GetBoth failed for value %d: %v", i, err)
		}
		if string(v) != string(value) {
			t.Logf("Warning: GetBoth returned value %q for requested %q", v, value)
		}

		err = cur.Del(0)
		if err != nil {
			cur.Close()
			txn.Abort()
			t.Fatalf("Del failed for value %d: %v", i, err)
		}
		deletedCount++
	}

	cur.Close()
	t.Logf("Deleted %d values", deletedCount)

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify the remaining values
	txn, err = env.BeginTxn(nil, TxnReadOnly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}

	cur, err = txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenCursor failed: %v", err)
	}

	// Count remaining values
	_, _, err = cur.Get(key, nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Set failed: %v", err)
	}

	count, err := cur.Count()
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Count failed: %v", err)
	}

	expectedCount := numValues - deletedCount
	if count != uint64(expectedCount) {
		cur.Close()
		txn.Abort()
		t.Fatalf("Expected %d remaining values, got %d", expectedCount, count)
	}
	t.Logf("Remaining count: %d", count)

	// Verify all remaining values are odd-indexed
	k, v, err := cur.Get(key, nil, Set)
	if err != nil {
		cur.Close()
		txn.Abort()
		t.Fatalf("Set failed: %v", err)
	}

	readCount := 0
	expectedIdx := 1 // First remaining value should be value-0001
	for err == nil {
		if string(k) != "testkey" {
			t.Errorf("Unexpected key: %q", k)
		}
		expected := fmt.Sprintf("value-%04d", expectedIdx)
		if string(v) != expected {
			t.Errorf("Value %d: expected %q, got %q", readCount, expected, v)
		}
		readCount++
		expectedIdx += 2 // Skip to next odd index
		k, v, err = cur.Get(nil, nil, NextDup)
	}

	if readCount != expectedCount {
		t.Errorf("Read %d values, expected %d", readCount, expectedCount)
	}

	cur.Close()
	txn.Abort()
	t.Log("Sub-tree delete test passed")
}

// TestPageSplitWithLargeValues tests that page splits correctly handle
// large values that require size-aware split point calculation.
func TestPageSplitWithLargeValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-split-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	// Use a small page size to trigger splits more easily
	if err := env.SetGeometry(-1, -1, 1<<20, -1, -1, 4096); err != nil {
		t.Fatalf("SetGeometry failed: %v", err)
	}

	if err := env.Open(dbPath, NoSubdir|NoMetaSync, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create a DBI
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert many small values first to fill up pages
	smallValue := make([]byte, 50)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%04d", i)
		if err := txn.Put(dbi, []byte(key), smallValue, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put small value failed at %d: %v", i, err)
		}
	}

	// Now insert a large value that will require a page split
	// The split must account for this large value's size
	largeValue := make([]byte, 1500)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Insert at various positions to test split point calculation
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("large%04d", i)
		if err := txn.Put(dbi, []byte(key), largeValue, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put large value failed at %d: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all values can be read back
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	// Check small values
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%04d", i)
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get small value %d failed: %v", i, err)
			continue
		}
		if len(val) != 50 {
			t.Errorf("Small value %d has wrong length: %d", i, len(val))
		}
	}

	// Check large values
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("large%04d", i)
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get large value %d failed: %v", i, err)
			continue
		}
		if len(val) != 1500 {
			t.Errorf("Large value %d has wrong length: %d", i, len(val))
		}
	}

	t.Log("Page split with large values test passed")
}

// TestPageSplitMixedSizes tests page splits with a mix of small and large entries.
func TestPageSplitMixedSizes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-mixed-split-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	if err := env.SetGeometry(-1, -1, 1<<20, -1, -1, 4096); err != nil {
		t.Fatalf("SetGeometry failed: %v", err)
	}

	if err := env.Open(dbPath, NoSubdir|NoMetaSync, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert alternating small and large values
	// This tests the split point calculation with varying entry sizes
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%05d", i)
		var value []byte
		if i%3 == 0 {
			// Large value (will likely need overflow or careful splitting)
			value = make([]byte, 800+i*10)
		} else if i%3 == 1 {
			// Medium value
			value = make([]byte, 200)
		} else {
			// Small value
			value = make([]byte, 20)
		}
		for j := range value {
			value[j] = byte((i + j) % 256)
		}

		if err := txn.Put(dbi, []byte(key), value, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed at %d (size=%d): %v", i, len(value), err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all entries
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%05d", i)
		var expectedLen int
		if i%3 == 0 {
			expectedLen = 800 + i*10
		} else if i%3 == 1 {
			expectedLen = 200
		} else {
			expectedLen = 20
		}

		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get %d failed: %v", i, err)
			continue
		}
		if len(val) != expectedLen {
			t.Errorf("Entry %d: expected len %d, got %d", i, expectedLen, len(val))
		}
	}

	t.Log("Mixed size page split test passed")
}

// TestPageSplitStress does a stress test with many insertions to exercise splits.
func TestPageSplitStress(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gdbx-split-stress-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	if err := env.SetGeometry(-1, -1, 10<<20, -1, -1, 4096); err != nil {
		t.Fatalf("SetGeometry failed: %v", err)
	}

	if err := env.Open(dbPath, NoSubdir|NoMetaSync, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert 1000 entries with varying sizes
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("stress%06d", i)
		// Varying sizes: 10 to 1000 bytes
		size := 10 + (i*7)%991
		value := make([]byte, size)
		for j := range value {
			value[j] = byte((i + j) % 256)
		}

		if err := txn.Put(dbi, []byte(key), value, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed at %d (size=%d): %v", i, size, err)
		}

		// Commit periodically to test multiple transactions
		if i > 0 && i%200 == 0 {
			if _, err := txn.Commit(); err != nil {
				t.Fatalf("Commit failed at %d: %v", i, err)
			}
			txn, err = env.BeginTxn(nil, 0)
			if err != nil {
				t.Fatalf("BeginTxn failed at %d: %v", i, err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Final commit failed: %v", err)
	}

	// Verify all entries
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("stress%06d", i)
		expectedSize := 10 + (i*7)%991

		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get %d failed: %v", i, err)
			continue
		}
		if len(val) != expectedSize {
			t.Errorf("Entry %d: expected len %d, got %d", i, expectedSize, len(val))
		}
	}

	t.Logf("Split stress test passed: %d entries", numEntries)
}

// TestPageSplitEdgeCases tests page splits where the split point needs to be
// at extreme positions (0 or numEntries) because of large nodes.
func TestPageSplitEdgeCases(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/edge.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Test: Fill page with small entries, then insert very large entries
	// that force the split algorithm to use edge split points
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert many small entries to nearly fill the page
	numSmall := 100
	for i := 0; i < numSmall; i++ {
		key := fmt.Sprintf("small%03d", i)
		val := fmt.Sprintf("v%03d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put small %d failed: %v", i, err)
		}
	}

	// Insert large entries that should trigger page splits
	// The large values should force the split algorithm to use edge split points
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("large%03d", i)
		val := make([]byte, 1500+i*50)
		for j := range val {
			val[j] = byte((i + j) % 256)
		}
		if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put large %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all entries are still accessible
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	// Check small entries
	for i := 0; i < numSmall; i++ {
		key := fmt.Sprintf("small%03d", i)
		expected := fmt.Sprintf("v%03d", i)
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get small %d failed: %v", i, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("Small entry %d: expected %q, got %q", i, expected, string(val))
		}
	}

	// Check large entries
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("large%03d", i)
		expectedLen := 1500 + i*50
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get large %d failed: %v", i, err)
			continue
		}
		if len(val) != expectedLen {
			t.Errorf("Large entry %d: expected len %d, got %d", i, expectedLen, len(val))
		}
	}

	t.Log("Edge case split test passed")
}

// TestConcurrentReadWriteTransactions tests that read transactions see consistent
// data when write transactions are committing concurrently.
// This tests the fix for the race condition where read txns could see new tree
// roots before the mmap was extended.
func TestConcurrentReadWriteTransactions(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/concurrent.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create initial data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert initial entries
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Run concurrent read and write transactions
	const numIterations = 50
	errors := make(chan error, numIterations*2)

	// Writer goroutine
	go func() {
		for i := 0; i < numIterations; i++ {
			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				errors <- fmt.Errorf("writer BeginTxn %d: %v", i, err)
				return
			}

			// Insert more entries that may trigger page splits and mmap growth
			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("new%05d_%05d", i, j)
				val := make([]byte, 500+j*10) // Varying sizes
				for k := range val {
					val[k] = byte(k % 256)
				}
				if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
					txn.Abort()
					errors <- fmt.Errorf("writer Put %d/%d: %v", i, j, err)
					return
				}
			}

			if _, err := txn.Commit(); err != nil {
				errors <- fmt.Errorf("writer Commit %d: %v", i, err)
				return
			}
		}
		errors <- nil
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < numIterations; i++ {
			txn, err := env.BeginTxn(nil, Readonly)
			if err != nil {
				errors <- fmt.Errorf("reader BeginTxn %d: %v", i, err)
				return
			}

			// Read all entries - should not crash even with concurrent writes
			cursor, err := txn.OpenCursor(dbi)
			if err != nil {
				txn.Abort()
				errors <- fmt.Errorf("reader OpenCursor %d: %v", i, err)
				return
			}

			count := 0
			for {
				_, _, err := cursor.Get(nil, nil, Next)
				if err != nil {
					if IsNotFound(err) {
						break
					}
					cursor.Close()
					txn.Abort()
					errors <- fmt.Errorf("reader cursor.Get %d: %v", i, err)
					return
				}
				count++
			}
			cursor.Close()
			txn.Abort()

			if count < 100 {
				errors <- fmt.Errorf("reader %d: expected at least 100 entries, got %d", i, count)
				return
			}
		}
		errors <- nil
	}()

	// Wait for both goroutines
	for i := 0; i < 2; i++ {
		if err := <-errors; err != nil {
			t.Error(err)
		}
	}

	t.Log("Concurrent read/write test passed")
}

// TestNamedDBIConcurrency tests concurrent access to named databases.
// This specifically tests the fix where info.tree was updated before mmap extended.
func TestNamedDBIConcurrency(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(20)

	path := dir + "/named_dbi.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create multiple named databases
	const numDBIs = 5
	dbis := make([]DBI, numDBIs)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	for i := 0; i < numDBIs; i++ {
		name := fmt.Sprintf("db%d", i)
		dbi, err := txn.OpenDBISimple(name, Create)
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenDBI %s failed: %v", name, err)
		}
		dbis[i] = dbi

		// Insert initial data
		for j := 0; j < 50; j++ {
			key := fmt.Sprintf("key%05d", j)
			val := fmt.Sprintf("val%05d", j)
			if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
				txn.Abort()
				t.Fatalf("Put failed: %v", err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Initial commit failed: %v", err)
	}

	// Concurrent writers and readers on different DBIs
	const numGoroutines = 10
	errors := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			dbi := dbis[id%numDBIs]

			for i := 0; i < 20; i++ {
				if id%2 == 0 {
					// Writer
					txn, err := env.BeginTxn(nil, 0)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d writer BeginTxn: %v", id, err)
						return
					}

					for j := 0; j < 10; j++ {
						key := fmt.Sprintf("g%d_i%d_j%d", id, i, j)
						val := make([]byte, 200+j*20)
						if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
							txn.Abort()
							errors <- fmt.Errorf("goroutine %d Put: %v", id, err)
							return
						}
					}

					if _, err := txn.Commit(); err != nil {
						errors <- fmt.Errorf("goroutine %d Commit: %v", id, err)
						return
					}
				} else {
					// Reader
					txn, err := env.BeginTxn(nil, Readonly)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d reader BeginTxn: %v", id, err)
						return
					}

					cursor, err := txn.OpenCursor(dbi)
					if err != nil {
						txn.Abort()
						errors <- fmt.Errorf("goroutine %d OpenCursor: %v", id, err)
						return
					}

					count := 0
					for {
						_, _, err := cursor.Get(nil, nil, Next)
						if err != nil {
							if IsNotFound(err) {
								break
							}
							cursor.Close()
							txn.Abort()
							errors <- fmt.Errorf("goroutine %d cursor error: %v", id, err)
							return
						}
						count++
					}
					cursor.Close()
					txn.Abort()
				}
			}
			errors <- nil
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		if err := <-errors; err != nil {
			t.Error(err)
		}
	}

	t.Log("Named DBI concurrency test passed")
}

// TestCloseWithActiveTransactions tests that Close() properly waits for
// all active transactions to complete before unmapping memory.
func TestCloseWithActiveTransactions(t *testing.T) {
	for iteration := 0; iteration < 10; iteration++ {
		dir := t.TempDir()
		env, err := NewEnv(Default)
		if err != nil {
			t.Fatalf("NewEnv failed: %v", err)
		}

		env.SetMaxDBs(10)

		path := dir + "/close_test.db"
		if err := env.Open(path, 0, 0644); err != nil {
			t.Fatalf("Open failed: %v", err)
		}

		// Create initial data
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatalf("BeginTxn failed: %v", err)
		}

		dbi, err := txn.OpenDBISimple("test", Create)
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenDBI failed: %v", err)
		}

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%05d", i)
			val := make([]byte, 500)
			if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put failed: %v", err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Start multiple read transactions
		const numReaders = 5
		readers := make([]*Txn, numReaders)
		for i := 0; i < numReaders; i++ {
			txn, err := env.BeginTxn(nil, Readonly)
			if err != nil {
				t.Fatalf("BeginTxn reader %d failed: %v", i, err)
			}
			readers[i] = txn
		}

		// Close env in a goroutine - should wait for readers
		done := make(chan bool)
		go func() {
			env.Close()
			done <- true
		}()

		// Abort readers one by one
		for i, txn := range readers {
			// Small delay to test the waiting behavior
			txn.Abort()
			t.Logf("Iteration %d: Aborted reader %d", iteration, i)
		}

		// Wait for close to complete
		<-done
		t.Logf("Iteration %d: Close completed", iteration)
	}

	t.Log("Close with active transactions test passed")
}

// TestPageSplitVeryLargeValues tests page splits with values near the maximum size.
func TestPageSplitVeryLargeValues(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/large_values.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert values of various sizes, including very large ones
	// Page size is 4096, so values near that size will test overflow handling
	sizes := []int{100, 500, 1000, 1500, 2000, 2500, 3000, 3500}

	for i, size := range sizes {
		key := fmt.Sprintf("key%03d", i)
		val := make([]byte, size)
		for j := range val {
			val[j] = byte((i + j) % 256)
		}

		if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put size %d failed: %v", size, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all entries
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	for i, size := range sizes {
		key := fmt.Sprintf("key%03d", i)
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get key%03d failed: %v", i, err)
			continue
		}
		if len(val) != size {
			t.Errorf("Key %03d: expected len %d, got %d", i, size, len(val))
			continue
		}
		for j := range val {
			if val[j] != byte((i+j)%256) {
				t.Errorf("Key %03d: data corrupted at byte %d", i, j)
				break
			}
		}
	}

	t.Log("Very large values test passed")
}

// TestPageSplitSequentialInserts tests page splits with sequential key inserts.
// This ensures the split point calculation works when keys are inserted in order.
func TestPageSplitSequentialInserts(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/sequential.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert many entries sequentially - this tests append-like behavior
	const numEntries = 500
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%06d", i) // Sequential keys
		val := make([]byte, 100+(i%50)*10)
		for j := range val {
			val[j] = byte((i + j) % 256)
		}
		if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify via cursor iteration
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	prevKey := ""
	for {
		k, _, err := cursor.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Cursor.Get failed: %v", err)
		}
		key := string(k)
		if key <= prevKey {
			t.Errorf("Keys out of order: %q <= %q", key, prevKey)
		}
		prevKey = key
		count++
	}

	if count != numEntries {
		t.Errorf("Expected %d entries, got %d", numEntries, count)
	}

	t.Logf("Sequential inserts test passed: %d entries", count)
}

// TestPageSplitReverseInserts tests page splits with reverse key inserts.
// This tests the opposite case where keys are always inserted at the beginning.
func TestPageSplitReverseInserts(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/reverse.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert in reverse order - always inserts at the beginning
	const numEntries = 500
	for i := numEntries - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%06d", i)
		val := make([]byte, 100+(i%50)*10)
		for j := range val {
			val[j] = byte((i + j) % 256)
		}
		if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	count := 0
	for {
		_, _, err := cursor.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Cursor.Get failed: %v", err)
		}
		count++
	}

	if count != numEntries {
		t.Errorf("Expected %d entries, got %d", numEntries, count)
	}

	t.Logf("Reverse inserts test passed: %d entries", count)
}

// TestPageSplitRandomInserts tests page splits with random key inserts.
func TestPageSplitRandomInserts(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/random.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert with pseudo-random keys (using a simple hash)
	const numEntries = 500
	inserted := make(map[string]int)
	for i := 0; i < numEntries; i++ {
		// Simple hash to generate pseudo-random order
		h := (i*31 + 17) % 100000
		key := fmt.Sprintf("key%06d", h)
		val := make([]byte, 100+(i%50)*10)
		for j := range val {
			val[j] = byte((i + j) % 256)
		}
		if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %d failed: %v", i, err)
		}
		inserted[key] = len(val)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer txn.Abort()

	for key, expectedLen := range inserted {
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get %s failed: %v", key, err)
			continue
		}
		if len(val) != expectedLen {
			t.Errorf("Key %s: expected len %d, got %d", key, expectedLen, len(val))
		}
	}

	t.Logf("Random inserts test passed: %d unique entries", len(inserted))
}

// TestMultipleCommitsWithMmapGrowth tests multiple commits that cause the mmap to grow.
// This is a stress test for the mmap growth and transaction synchronization.
func TestMultipleCommitsWithMmapGrowth(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/growth.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Multiple commits, each adding data to force mmap growth
	const numCommits = 20
	const entriesPerCommit = 50

	for c := 0; c < numCommits; c++ {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatalf("BeginTxn commit %d failed: %v", c, err)
		}

		var dbi DBI
		if c == 0 {
			dbi, err = txn.OpenDBISimple("test", Create)
		} else {
			dbi, err = txn.OpenDBISimple("test", 0)
		}
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenDBI commit %d failed: %v", c, err)
		}

		// Insert entries with large values to force growth
		for i := 0; i < entriesPerCommit; i++ {
			key := fmt.Sprintf("c%03d_k%05d", c, i)
			val := make([]byte, 500+c*20) // Increasing size per commit
			for j := range val {
				val[j] = byte((c + i + j) % 256)
			}
			if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put commit %d entry %d failed: %v", c, i, err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Commit %d failed: %v", c, err)
		}

		// Read back all data to verify consistency
		rtxn, err := env.BeginTxn(nil, Readonly)
		if err != nil {
			t.Fatalf("BeginTxn read %d failed: %v", c, err)
		}

		cursor, err := rtxn.OpenCursor(dbi)
		if err != nil {
			rtxn.Abort()
			t.Fatalf("OpenCursor read %d failed: %v", c, err)
		}

		count := 0
		for {
			_, _, err := cursor.Get(nil, nil, Next)
			if err != nil {
				if IsNotFound(err) {
					break
				}
				cursor.Close()
				rtxn.Abort()
				t.Fatalf("Cursor.Get read %d failed: %v", c, err)
			}
			count++
		}
		cursor.Close()
		rtxn.Abort()

		expectedCount := (c + 1) * entriesPerCommit
		if count != expectedCount {
			t.Errorf("Commit %d: expected %d entries, got %d", c, expectedCount, count)
		}
	}

	t.Logf("Multiple commits test passed: %d commits", numCommits)
}

// TestTransactionIsolation tests that read transactions see a consistent snapshot.
func TestTransactionIsolation(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	path := dir + "/isolation.db"
	if err := env.Open(path, 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create initial data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert 100 entries
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Start a read transaction (snapshot)
	rtxn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read) failed: %v", err)
	}
	defer rtxn.Abort()

	// Count entries in snapshot
	cursor, err := rtxn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}

	snapshotCount := 0
	for {
		_, _, err := cursor.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Cursor.Get failed: %v", err)
		}
		snapshotCount++
	}
	cursor.Close()

	if snapshotCount != 100 {
		t.Fatalf("Expected 100 entries in snapshot, got %d", snapshotCount)
	}

	// Add more data in a new write transaction
	wtxn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn (write) failed: %v", err)
	}

	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := wtxn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			wtxn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := wtxn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify original read transaction still sees only 100 entries (isolation)
	cursor, err = rtxn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor 2 failed: %v", err)
	}
	defer cursor.Close()

	isolatedCount := 0
	for {
		_, _, err := cursor.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Cursor.Get 2 failed: %v", err)
		}
		isolatedCount++
	}

	if isolatedCount != 100 {
		t.Errorf("Isolation broken: expected 100 entries, got %d", isolatedCount)
	}

	// New read transaction should see all 200 entries
	rtxn2, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn (read 2) failed: %v", err)
	}
	defer rtxn2.Abort()

	cursor2, err := rtxn2.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor 3 failed: %v", err)
	}
	defer cursor2.Close()

	newCount := 0
	for {
		_, _, err := cursor2.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Cursor.Get 3 failed: %v", err)
		}
		newCount++
	}

	if newCount != 200 {
		t.Errorf("New transaction should see 200 entries, got %d", newCount)
	}

	t.Log("Transaction isolation test passed")
}

// =============================================================================
// MDBX Compatibility Tests - Cursor Operations
// =============================================================================

// TestCursorSet tests the Set cursor operation (exact key match).
func TestCursorSet(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create test data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
		"date":   "brown",
		"elder":  "purple",
	}

	for k, v := range testData {
		if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Test Set operation
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	// Test successful Set
	k, v, err := cursor.Get([]byte("cherry"), nil, Set)
	if err != nil {
		t.Errorf("Set 'cherry' failed: %v", err)
	} else if string(k) != "cherry" || string(v) != "red" {
		t.Errorf("Set 'cherry': expected cherry/red, got %s/%s", k, v)
	}

	// Test Set for non-existent key
	_, _, err = cursor.Get([]byte("fig"), nil, Set)
	if !IsNotFound(err) {
		t.Errorf("Set 'fig' should return NotFound, got: %v", err)
	}

	t.Log("Cursor Set test passed")
}

// TestCursorSetKey tests the SetKey operation (returns both key and value).
func TestCursorSetKey(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	if err := txn.Put(dbi, []byte("key1"), []byte("value1"), 0); err != nil {
		txn.Abort()
		t.Fatalf("Put failed: %v", err)
	}
	if err := txn.Put(dbi, []byte("key2"), []byte("value2"), 0); err != nil {
		txn.Abort()
		t.Fatalf("Put failed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	k, v, err := cursor.Get([]byte("key1"), nil, SetKey)
	if err != nil {
		t.Errorf("SetKey failed: %v", err)
	} else if string(k) != "key1" || string(v) != "value1" {
		t.Errorf("SetKey: expected key1/value1, got %s/%s", k, v)
	}

	t.Log("Cursor SetKey test passed")
}

// TestCursorSetRange tests the SetRange operation (first key >= specified).
func TestCursorSetRange(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert keys: a10, a20, a30, a40, a50
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("a%d0", i)
		val := fmt.Sprintf("v%d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	// SetRange for "a25" should return "a30"
	k, v, err := cursor.Get([]byte("a25"), nil, SetRange)
	if err != nil {
		t.Errorf("SetRange 'a25' failed: %v", err)
	} else if string(k) != "a30" || string(v) != "v3" {
		t.Errorf("SetRange 'a25': expected a30/v3, got %s/%s", k, v)
	}

	// SetRange for exact match "a20" should return "a20"
	k, v, err = cursor.Get([]byte("a20"), nil, SetRange)
	if err != nil {
		t.Errorf("SetRange 'a20' failed: %v", err)
	} else if string(k) != "a20" || string(v) != "v2" {
		t.Errorf("SetRange 'a20': expected a20/v2, got %s/%s", k, v)
	}

	// SetRange for "a60" (beyond all keys) should return NotFound
	_, _, err = cursor.Get([]byte("a60"), nil, SetRange)
	if !IsNotFound(err) {
		t.Errorf("SetRange 'a60' should return NotFound, got: %v", err)
	}

	t.Log("Cursor SetRange test passed")
}

// TestCursorGetCurrent tests the GetCurrent operation.
func TestCursorGetCurrent(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	if err := txn.Put(dbi, []byte("key1"), []byte("value1"), 0); err != nil {
		txn.Abort()
		t.Fatalf("Put failed: %v", err)
	}
	if err := txn.Put(dbi, []byte("key2"), []byte("value2"), 0); err != nil {
		txn.Abort()
		t.Fatalf("Put failed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	// Position at first entry
	_, _, err = cursor.Get(nil, nil, First)
	if err != nil {
		t.Fatalf("First failed: %v", err)
	}

	// GetCurrent should return the same entry
	k, v, err := cursor.Get(nil, nil, GetCurrent)
	if err != nil {
		t.Errorf("GetCurrent failed: %v", err)
	} else if string(k) != "key1" || string(v) != "value1" {
		t.Errorf("GetCurrent: expected key1/value1, got %s/%s", k, v)
	}

	// Move to next
	_, _, err = cursor.Get(nil, nil, Next)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// GetCurrent should return key2
	k, v, err = cursor.Get(nil, nil, GetCurrent)
	if err != nil {
		t.Errorf("GetCurrent 2 failed: %v", err)
	} else if string(k) != "key2" || string(v) != "value2" {
		t.Errorf("GetCurrent 2: expected key2/value2, got %s/%s", k, v)
	}

	t.Log("Cursor GetCurrent test passed")
}

// TestDupSortCursorOperations tests DupSort-specific cursor operations.
func TestDupSortCursorOperations(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert multiple values per key
	// key1: val1, val2, val3
	// key2: val4, val5
	// key3: val6
	testData := []struct {
		key, val string
	}{
		{"key1", "val1"},
		{"key1", "val2"},
		{"key1", "val3"},
		{"key2", "val4"},
		{"key2", "val5"},
		{"key3", "val6"},
	}

	for _, d := range testData {
		if err := txn.Put(dbi, []byte(d.key), []byte(d.val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %s/%s failed: %v", d.key, d.val, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	// Test FirstDup - position at key1, then get first dup
	k, v, err := cursor.Get([]byte("key1"), nil, Set)
	if err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}
	k, v, err = cursor.Get(nil, nil, FirstDup)
	if err != nil {
		t.Errorf("FirstDup failed: %v", err)
	} else if string(k) != "key1" || string(v) != "val1" {
		t.Errorf("FirstDup: expected key1/val1, got %s/%s", k, v)
	}

	// Test LastDup
	k, v, err = cursor.Get(nil, nil, LastDup)
	if err != nil {
		t.Errorf("LastDup failed: %v", err)
	} else if string(k) != "key1" || string(v) != "val3" {
		t.Errorf("LastDup: expected key1/val3, got %s/%s", k, v)
	}

	// Test NextDup - go back to first, then iterate
	cursor.Get(nil, nil, FirstDup)
	k, v, err = cursor.Get(nil, nil, NextDup)
	if err != nil {
		t.Errorf("NextDup failed: %v", err)
	} else if string(k) != "key1" || string(v) != "val2" {
		t.Errorf("NextDup: expected key1/val2, got %s/%s", k, v)
	}

	// Test PrevDup
	cursor.Get(nil, nil, LastDup) // Go to val3
	k, v, err = cursor.Get(nil, nil, PrevDup)
	if err != nil {
		t.Errorf("PrevDup failed: %v", err)
	} else if string(k) != "key1" || string(v) != "val2" {
		t.Errorf("PrevDup: expected key1/val2, got %s/%s", k, v)
	}

	// Test NextNoDup - should move to key2
	cursor.Get([]byte("key1"), nil, Set) // Back to key1
	k, v, err = cursor.Get(nil, nil, NextNoDup)
	if err != nil {
		t.Errorf("NextNoDup failed: %v", err)
	} else if string(k) != "key2" {
		t.Errorf("NextNoDup: expected key2, got %s", k)
	}

	// Test PrevNoDup - should move back to key1 (last dup)
	k, v, err = cursor.Get(nil, nil, PrevNoDup)
	if err != nil {
		t.Errorf("PrevNoDup failed: %v", err)
	} else if string(k) != "key1" {
		t.Errorf("PrevNoDup: expected key1, got %s", k)
	}

	// Test GetBoth - exact key-value match
	k, v, err = cursor.Get([]byte("key1"), []byte("val2"), GetBoth)
	if err != nil {
		t.Errorf("GetBoth failed: %v", err)
	} else if string(k) != "key1" || string(v) != "val2" {
		t.Errorf("GetBoth: expected key1/val2, got %s/%s", k, v)
	}

	// Test GetBoth for non-existent value
	_, _, err = cursor.Get([]byte("key1"), []byte("val9"), GetBoth)
	if !IsNotFound(err) {
		t.Errorf("GetBoth non-existent should return NotFound, got: %v", err)
	}

	// Test GetBothRange - value >= specified
	k, v, err = cursor.Get([]byte("key2"), []byte("val45"), GetBothRange)
	if err != nil {
		t.Errorf("GetBothRange failed: %v", err)
	} else if string(k) != "key2" || string(v) != "val5" {
		t.Errorf("GetBothRange: expected key2/val5, got %s/%s", k, v)
	}

	t.Log("DupSort cursor operations test passed")
}

// TestCursorCount tests the Count operation for DUPSORT tables.
func TestCursorCount(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert duplicates: key1 has 5 values, key2 has 3 values
	for i := 0; i < 5; i++ {
		if err := txn.Put(dbi, []byte("key1"), []byte(fmt.Sprintf("val%d", i)), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := txn.Put(dbi, []byte("key2"), []byte(fmt.Sprintf("val%d", i)), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	// Position at key1 and count
	_, _, err = cursor.Get([]byte("key1"), nil, Set)
	if err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}

	count, err := cursor.Count()
	if err != nil {
		t.Errorf("Count failed: %v", err)
	} else if count != 5 {
		t.Errorf("Count key1: expected 5, got %d", count)
	}

	// Position at key2 and count
	_, _, err = cursor.Get([]byte("key2"), nil, Set)
	if err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}

	count, err = cursor.Count()
	if err != nil {
		t.Errorf("Count 2 failed: %v", err)
	} else if count != 3 {
		t.Errorf("Count key2: expected 3, got %d", count)
	}

	t.Log("Cursor Count test passed")
}

// =============================================================================
// MDBX Compatibility Tests - Put Flags
// =============================================================================

// TestPutNoDupData tests the NoDupData flag for DUPSORT tables.
func TestPutNoDupData(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert initial entry
	if err := txn.Put(dbi, []byte("key1"), []byte("val1"), 0); err != nil {
		txn.Abort()
		t.Fatalf("Put failed: %v", err)
	}

	// Try to insert same key-value pair with NoDupData - should fail
	err = txn.Put(dbi, []byte("key1"), []byte("val1"), NoDupData)
	if err == nil {
		txn.Abort()
		t.Fatalf("NoDupData should fail for duplicate key-value pair")
	}
	if !IsKeyExist(err) {
		txn.Abort()
		t.Fatalf("NoDupData error should be KeyExist, got: %v", err)
	}

	// Insert different value for same key with NoDupData - should succeed
	err = txn.Put(dbi, []byte("key1"), []byte("val2"), NoDupData)
	if err != nil {
		txn.Abort()
		t.Fatalf("NoDupData for new value should succeed: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Log("Put NoDupData test passed")
}

// TestPutAppend tests the Append flag for optimized sequential inserts.
func TestPutAppend(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert keys in ascending order using Append
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), Append); err != nil {
			txn.Abort()
			t.Fatalf("Put Append %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("val%05d", i)
		val, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Get %d failed: %v", i, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("Entry %d: expected %s, got %s", i, expected, string(val))
		}
	}

	t.Log("Put Append test passed")
}

// TestPutAppendDup tests the AppendDup flag for DUPSORT tables.
func TestPutAppendDup(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create|DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert sorted values for key1 using AppendDup
	for i := 0; i < 50; i++ {
		val := fmt.Sprintf("val%05d", i)
		if err := txn.Put(dbi, []byte("key1"), []byte(val), AppendDup); err != nil {
			txn.Abort()
			t.Fatalf("Put AppendDup %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify count
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	_, _, err = cursor.Get([]byte("key1"), nil, Set)
	if err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}

	count, err := cursor.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 50 {
		t.Errorf("Expected 50 duplicates, got %d", count)
	}

	t.Log("Put AppendDup test passed")
}

// =============================================================================
// MDBX Compatibility Tests - Database Flags
// =============================================================================

// TestDupFixed tests the DupFixed flag for fixed-size duplicate values.
func TestDupFixed(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	// DupFixed requires DupSort
	dbi, err := txn.OpenDBISimple("test", Create|DupSort|DupFixed)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert fixed-size values (8 bytes each)
	for i := 0; i < 100; i++ {
		val := make([]byte, 8)
		val[0] = byte(i)
		val[7] = byte(i)
		if err := txn.Put(dbi, []byte("key1"), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	_, _, err = cursor.Get([]byte("key1"), nil, Set)
	if err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}

	count, err := cursor.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected 100 duplicates, got %d", count)
	}

	// Iterate and verify values
	_, v, err := cursor.Get(nil, nil, FirstDup)
	if err != nil {
		t.Fatalf("FirstDup failed: %v", err)
	}

	idx := 0
	for {
		if len(v) != 8 {
			t.Errorf("Value %d: expected 8 bytes, got %d", idx, len(v))
		}
		if v[0] != byte(idx) || v[7] != byte(idx) {
			t.Errorf("Value %d: data mismatch", idx)
		}

		_, v, err = cursor.Get(nil, nil, NextDup)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("NextDup failed: %v", err)
		}
		idx++
	}

	if idx != 99 {
		t.Errorf("Expected 99 iterations (0-99), got %d", idx)
	}

	t.Log("DupFixed test passed")
}

// TestIntegerKey tests the IntegerKey flag.
func TestIntegerKey(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create|IntegerKey)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert uint64 keys
	for i := uint64(0); i < 100; i++ {
		key := make([]byte, 8)
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		key[3] = byte(i >> 24)
		key[4] = byte(i >> 32)
		key[5] = byte(i >> 40)
		key[6] = byte(i >> 48)
		key[7] = byte(i >> 56)

		val := fmt.Sprintf("val%d", i)
		if err := txn.Put(dbi, key, []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify via cursor iteration - keys should be in numeric order
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	expectedIdx := uint64(0)
	for {
		k, _, err := cursor.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Next failed: %v", err)
		}

		// Decode key
		keyVal := uint64(k[0]) | uint64(k[1])<<8 | uint64(k[2])<<16 | uint64(k[3])<<24 |
			uint64(k[4])<<32 | uint64(k[5])<<40 | uint64(k[6])<<48 | uint64(k[7])<<56

		if keyVal != expectedIdx {
			t.Errorf("Key order: expected %d, got %d", expectedIdx, keyVal)
		}
		expectedIdx++
	}

	if expectedIdx != 100 {
		t.Errorf("Expected 100 entries, got %d", expectedIdx)
	}

	t.Log("IntegerKey test passed")
}

// TestReverseKey tests the ReverseKey flag.
func TestReverseKey(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create|ReverseKey)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert keys
	keys := []string{"apple", "apricot", "banana", "cherry"}
	for _, k := range keys {
		if err := txn.Put(dbi, []byte(k), []byte("val"), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Iterate and collect keys - with ReverseKey, they should be in reverse byte order
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("OpenCursor failed: %v", err)
	}
	defer cursor.Close()

	var result []string
	for {
		k, _, err := cursor.Get(nil, nil, Next)
		if err != nil {
			if IsNotFound(err) {
				break
			}
			t.Fatalf("Next failed: %v", err)
		}
		result = append(result, string(k))
	}

	// With reverse comparison, keys ending in later letters come first
	// e.g., "cherry" ends in 'y', "banana" ends in 'a'
	t.Logf("ReverseKey order: %v", result)

	if len(result) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(result))
	}

	t.Log("ReverseKey test passed")
}

// =============================================================================
// MDBX Compatibility Tests - Transaction Features
// =============================================================================

// TestTransactionResetRenew tests Reset and Renew for read transactions.
func TestTransactionResetRenew(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create initial data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Start read transaction
	rtxn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	// Read some data
	val, err := rtxn.Get(dbi, []byte("key5"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "val5" {
		t.Errorf("Expected val5, got %s", val)
	}

	// Reset the transaction
	rtxn.Reset()

	// Add more data
	wtxn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn write failed: %v", err)
	}

	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		if err := wtxn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			wtxn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := wtxn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Renew the read transaction - should see new data
	if err := rtxn.Renew(); err != nil {
		t.Fatalf("Renew failed: %v", err)
	}

	// Should be able to see new data
	val, err = rtxn.Get(dbi, []byte("key15"))
	if err != nil {
		t.Fatalf("Get after Renew failed: %v", err)
	}
	if string(val) != "val15" {
		t.Errorf("Expected val15, got %s", val)
	}

	rtxn.Abort()

	t.Log("Transaction Reset/Renew test passed")
}

// TestDatabaseStat tests the Stat function.
func TestDatabaseStat(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Insert some entries
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%05d", i)
		val := make([]byte, 500)
		if err := txn.Put(dbi, []byte(key), val, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get stats before commit
	stat, err := txn.Stat(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("Stat failed: %v", err)
	}

	if stat.Entries != 100 {
		t.Errorf("Stat.Entries: expected 100, got %d", stat.Entries)
	}

	if stat.PageSize != 4096 {
		t.Errorf("Stat.PageSize: expected 4096, got %d", stat.PageSize)
	}

	t.Logf("Stat: Entries=%d, Depth=%d, BranchPages=%d, LeafPages=%d, OverflowPages=%d",
		stat.Entries, stat.Depth, stat.BranchPages, stat.LeafPages, stat.OverflowPages)

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	t.Log("Database Stat test passed")
}

// =============================================================================
// MDBX Compatibility Tests - Environment Features
// =============================================================================

// TestReadOnlyEnvironment tests opening an environment in read-only mode.
func TestReadOnlyEnvironment(t *testing.T) {
	dir := t.TempDir()

	// Create database with data
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
			txn.Abort()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	env.Close()

	// Reopen in read-only mode
	env2, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv 2 failed: %v", err)
	}
	defer env2.Close()

	env2.SetMaxDBs(10)
	if err := env2.Open(dir+"/test.db", ReadOnly, 0644); err != nil {
		t.Fatalf("Open ReadOnly failed: %v", err)
	}

	// Should be able to read
	txn, err = env2.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	val, err := txn.Get(dbi, []byte("key5"))
	if err != nil {
		txn.Abort()
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "val5" {
		t.Errorf("Expected val5, got %s", val)
	}

	txn.Abort()

	// Should NOT be able to write
	_, err = env2.BeginTxn(nil, 0)
	if err == nil {
		t.Error("Write transaction should fail on read-only environment")
	}

	t.Log("ReadOnly environment test passed")
}

// TestCursorDelete tests cursor-based deletion with various flags.
func TestCursorDelete(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Test with regular database
	t.Run("Regular", func(t *testing.T) {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatalf("BeginTxn failed: %v", err)
		}

		dbi, err := txn.OpenDBISimple("regular", Create)
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenDBI failed: %v", err)
		}

		// Insert entries
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			val := fmt.Sprintf("val%d", i)
			if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
				txn.Abort()
				t.Fatalf("Put failed: %v", err)
			}
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenCursor failed: %v", err)
		}

		// Position at key5 and delete
		_, _, err = cursor.Get([]byte("key5"), nil, Set)
		if err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("Set failed: %v", err)
		}

		if err := cursor.Del(0); err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("Del failed: %v", err)
		}

		cursor.Close()

		// Verify deletion
		_, err = txn.Get(dbi, []byte("key5"))
		if !IsNotFound(err) {
			txn.Abort()
			t.Errorf("key5 should be deleted, got: %v", err)
		}

		txn.Abort()
	})

	// Test with DupSort database
	t.Run("DupSort", func(t *testing.T) {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatalf("BeginTxn failed: %v", err)
		}

		dbi, err := txn.OpenDBISimple("dupsort", Create|DupSort)
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenDBI failed: %v", err)
		}

		// Insert duplicates
		for i := 0; i < 5; i++ {
			val := fmt.Sprintf("val%d", i)
			if err := txn.Put(dbi, []byte("key1"), []byte(val), 0); err != nil {
				txn.Abort()
				t.Fatalf("Put failed: %v", err)
			}
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenCursor failed: %v", err)
		}

		// Position at key1/val2 and delete just that value
		_, _, err = cursor.Get([]byte("key1"), []byte("val2"), GetBoth)
		if err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("GetBoth failed: %v", err)
		}

		if err := cursor.Del(0); err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("Del failed: %v", err)
		}

		// Verify val2 is deleted but others remain
		_, _, err = cursor.Get([]byte("key1"), []byte("val2"), GetBoth)
		if !IsNotFound(err) {
			cursor.Close()
			txn.Abort()
			t.Errorf("val2 should be deleted")
		}

		// Count remaining duplicates
		_, _, err = cursor.Get([]byte("key1"), nil, Set)
		if err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("Set failed: %v", err)
		}

		count, err := cursor.Count()
		if err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("Count failed: %v", err)
		}

		if count != 4 {
			cursor.Close()
			txn.Abort()
			t.Errorf("Expected 4 remaining duplicates, got %d", count)
		}

		cursor.Close()
		txn.Abort()
	})

	t.Log("Cursor Delete test passed")
}

// TestEmptyDatabase tests operations on an empty database.
func TestEmptyDatabase(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("empty", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Get on empty DB should return NotFound
	_, err = txn.Get(dbi, []byte("nonexistent"))
	if !IsNotFound(err) {
		txn.Abort()
		t.Errorf("Get on empty DB should return NotFound, got: %v", err)
	}

	// Cursor operations on empty DB
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenCursor failed: %v", err)
	}

	_, _, err = cursor.Get(nil, nil, First)
	if !IsNotFound(err) {
		t.Errorf("First on empty DB should return NotFound, got: %v", err)
	}

	_, _, err = cursor.Get(nil, nil, Last)
	if !IsNotFound(err) {
		t.Errorf("Last on empty DB should return NotFound, got: %v", err)
	}

	cursor.Close()

	// Stat on empty DB
	stat, err := txn.Stat(dbi)
	if err != nil {
		txn.Abort()
		t.Fatalf("Stat failed: %v", err)
	}

	if stat.Entries != 0 {
		t.Errorf("Empty DB should have 0 entries, got %d", stat.Entries)
	}

	txn.Abort()

	t.Log("Empty database test passed")
}

// TestLargeKeyValues tests handling of large keys and values.
func TestLargeKeyValues(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbi, err := txn.OpenDBISimple("test", Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("OpenDBI failed: %v", err)
	}

	// Test various key sizes (max key size depends on page size)
	keySizes := []int{1, 10, 100, 500, 1000}
	for _, size := range keySizes {
		key := make([]byte, size)
		for i := range key {
			key[i] = byte(i % 256)
		}
		val := []byte("value")

		err := txn.Put(dbi, key, val, 0)
		if err != nil {
			t.Logf("Key size %d: %v (may be expected if too large)", size, err)
		} else {
			t.Logf("Key size %d: OK", size)
		}
	}

	// Test various value sizes
	valueSizes := []int{1, 100, 1000, 5000, 10000, 50000}
	for i, size := range valueSizes {
		key := []byte(fmt.Sprintf("vkey%d", i))
		val := make([]byte, size)
		for j := range val {
			val[j] = byte(j % 256)
		}

		err := txn.Put(dbi, key, val, 0)
		if err != nil {
			t.Errorf("Value size %d failed: %v", size, err)
		} else {
			t.Logf("Value size %d: OK", size)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify large values
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	for i, size := range valueSizes {
		key := []byte(fmt.Sprintf("vkey%d", i))
		val, err := txn.Get(dbi, key)
		if err != nil {
			t.Errorf("Get value size %d failed: %v", size, err)
			continue
		}
		if len(val) != size {
			t.Errorf("Value size %d: expected %d bytes, got %d", size, size, len(val))
		}
	}

	t.Log("Large key/value test passed")
}

// TestMultipleNamedDatabases tests operations across multiple named databases.
func TestMultipleNamedDatabases(t *testing.T) {
	dir := t.TempDir()
	env, err := NewEnv(Default)
	if err != nil {
		t.Fatalf("NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(20)
	if err := env.Open(dir+"/test.db", 0, 0644); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create multiple named databases with different configurations
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}

	dbis := make([]DBI, 5)
	dbNames := []string{"default_db", "dupsort_db", "dupfixed_db", "intkey_db", "reverse_db"}
	dbFlags := []uint{0, DupSort, DupSort | DupFixed, IntegerKey, ReverseKey}

	for i, name := range dbNames {
		dbi, err := txn.OpenDBISimple(name, Create|dbFlags[i])
		if err != nil {
			txn.Abort()
			t.Fatalf("OpenDBI %s failed: %v", name, err)
		}
		dbis[i] = dbi

		// Insert data
		for j := 0; j < 10; j++ {
			var key []byte
			if dbFlags[i]&IntegerKey != 0 {
				key = make([]byte, 8)
				key[0] = byte(j)
			} else {
				key = []byte(fmt.Sprintf("key%d", j))
			}

			val := []byte(fmt.Sprintf("val%d_%s", j, name))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put to %s failed: %v", name, err)
			}

			// For DupSort, add extra values
			if dbFlags[i]&DupSort != 0 {
				for k := 1; k < 3; k++ {
					var dupVal []byte
					if dbFlags[i]&DupFixed != 0 {
						dupVal = make([]byte, 8)
						dupVal[0] = byte(j)
						dupVal[1] = byte(k)
					} else {
						dupVal = []byte(fmt.Sprintf("dup%d_%d", j, k))
					}
					if err := txn.Put(dbi, key, dupVal, 0); err != nil {
						txn.Abort()
						t.Fatalf("Put dup to %s failed: %v", name, err)
					}
				}
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify each database
	txn, err = env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Fatalf("BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	for i, name := range dbNames {
		dbi, err := txn.OpenDBISimple(name, dbFlags[i])
		if err != nil {
			t.Errorf("OpenDBI %s for read failed: %v", name, err)
			continue
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Errorf("Stat %s failed: %v", name, err)
			continue
		}

		expectedEntries := uint64(10)
		if dbFlags[i]&DupSort != 0 {
			expectedEntries = 30 // 10 keys * 3 values each
		}

		if stat.Entries != expectedEntries {
			t.Errorf("DB %s: expected %d entries, got %d", name, expectedEntries, stat.Entries)
		}

		t.Logf("DB %s: %d entries", name, stat.Entries)
	}

	t.Log("Multiple named databases test passed")
}
