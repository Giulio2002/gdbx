package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestCOWIsolation tests Copy-On-Write isolation between read and write transactions.
// A read-only transaction should see a consistent snapshot even while writes happen.
func TestCOWIsolation(t *testing.T) {
	path := t.TempDir() + "/cow_test.db"

	// Create and populate database
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Initial write: populate with 1000 entries
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	val := make([]byte, 8)
	initialValue := uint64(1000)

	for i := 0; i < 1000; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, initialValue)
		if err := txn.Put(dbi, key, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Start a read-only transaction (snapshot)
	roTxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer roTxn.Abort()

	roDbi, err := roTxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify initial values in read transaction
	binary.BigEndian.PutUint64(key, 0)
	roVal, err := roTxn.Get(roDbi, key)
	if err != nil {
		t.Fatal("Failed to read initial value:", err)
	}
	if binary.BigEndian.Uint64(roVal) != initialValue {
		t.Fatalf("Expected initial value %d, got %d", initialValue, binary.BigEndian.Uint64(roVal))
	}

	// Start a write transaction and modify all values
	rwTxn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	rwDbi, err := rwTxn.OpenDBISimple("test", 0)
	if err != nil {
		rwTxn.Abort()
		t.Fatal(err)
	}

	modifiedValue := uint64(2000)
	for i := 0; i < 1000; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, modifiedValue)
		if err := rwTxn.Put(rwDbi, key, val, 0); err != nil {
			rwTxn.Abort()
			t.Fatal(err)
		}
	}

	// Also add some new entries
	for i := 1000; i < 1500; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, modifiedValue)
		if err := rwTxn.Put(rwDbi, key, val, 0); err != nil {
			rwTxn.Abort()
			t.Fatal(err)
		}
	}

	// Commit the write transaction
	if _, err := rwTxn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Read transaction should STILL see the original values (COW isolation)
	for i := 0; i < 100; i++ { // Check first 100 entries
		binary.BigEndian.PutUint64(key, uint64(i))
		roVal, err := roTxn.Get(roDbi, key)
		if err != nil {
			t.Fatalf("Failed to read key %d in read txn: %v", i, err)
		}
		gotValue := binary.BigEndian.Uint64(roVal)
		if gotValue != initialValue {
			t.Fatalf("COW violation: key %d should have value %d (snapshot), got %d (modified)",
				i, initialValue, gotValue)
		}
	}

	// New entries should NOT be visible in read transaction
	binary.BigEndian.PutUint64(key, 1000)
	_, err = roTxn.Get(roDbi, key)
	if err != gdbx.ErrNotFoundError {
		t.Fatalf("COW violation: new key 1000 should not be visible in read txn, got err=%v", err)
	}

	// Close read transaction
	roTxn.Abort()

	// Start a new read transaction - should see the modifications
	newRoTxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer newRoTxn.Abort()

	newRoDbi, err := newRoTxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// New read transaction should see modified values
	for i := 0; i < 100; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		newVal, err := newRoTxn.Get(newRoDbi, key)
		if err != nil {
			t.Fatalf("Failed to read key %d in new read txn: %v", i, err)
		}
		gotValue := binary.BigEndian.Uint64(newVal)
		if gotValue != modifiedValue {
			t.Fatalf("Key %d should have modified value %d, got %d", i, modifiedValue, gotValue)
		}
	}

	// New entries should be visible
	binary.BigEndian.PutUint64(key, 1000)
	newVal, err := newRoTxn.Get(newRoDbi, key)
	if err != nil {
		t.Fatalf("Failed to read new key 1000: %v", err)
	}
	if binary.BigEndian.Uint64(newVal) != modifiedValue {
		t.Fatalf("New key 1000 should have value %d, got %d", modifiedValue, binary.BigEndian.Uint64(newVal))
	}

	t.Log("COW isolation test passed!")
}

// TestCOWCursorIteration tests that cursor iteration in a read transaction
// is not affected by concurrent writes.
func TestCOWCursorIteration(t *testing.T) {
	path := t.TempDir() + "/cow_cursor_test.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Initial write
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	val := make([]byte, 8)

	for i := 0; i < 500; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*10))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Start read transaction
	roTxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer roTxn.Abort()

	roDbi, err := roTxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := roTxn.OpenCursor(roDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Start iterating
	k, v, err := cursor.Get(nil, nil, gdbx.First)
	if err != nil {
		t.Fatal(err)
	}
	firstKey := binary.BigEndian.Uint64(k)
	firstVal := binary.BigEndian.Uint64(v)

	// Now do a write while cursor is active
	rwTxn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	rwDbi, err := rwTxn.OpenDBISimple("test", 0)
	if err != nil {
		rwTxn.Abort()
		t.Fatal(err)
	}

	// Modify existing entries
	for i := 0; i < 500; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100)) // Different multiplier
		if err := rwTxn.Put(rwDbi, key, val, 0); err != nil {
			rwTxn.Abort()
			t.Fatal(err)
		}
	}

	// Add new entries
	for i := 500; i < 1000; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100))
		if err := rwTxn.Put(rwDbi, key, val, 0); err != nil {
			rwTxn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := rwTxn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Continue iterating - should still see original values
	count := 1 // Already got first
	for {
		k, v, err = cursor.Get(nil, nil, gdbx.Next)
		if err == gdbx.ErrNotFoundError {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
		keyNum := binary.BigEndian.Uint64(k)
		valNum := binary.BigEndian.Uint64(v)
		expectedVal := keyNum * 10 // Original multiplier
		if valNum != expectedVal {
			t.Fatalf("COW violation during iteration: key %d has value %d, expected %d",
				keyNum, valNum, expectedVal)
		}
	}

	// Should have seen exactly 500 entries (original count)
	if count != 500 {
		t.Fatalf("COW violation: expected 500 entries during iteration, got %d", count)
	}

	t.Logf("COW cursor iteration test passed! First entry: key=%d val=%d, total entries=%d",
		firstKey, firstVal, count)
}

// TestCOWWithDupSort tests COW isolation with DUPSORT databases.
func TestCOWWithDupSort(t *testing.T) {
	path := t.TempDir() + "/cow_dupsort_test.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Initial write with DUPSORT
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("duptest", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Add multiple values per key
	for k := 0; k < 10; k++ {
		key := []byte(fmt.Sprintf("key%02d", k))
		for v := 0; v < 5; v++ {
			val := []byte(fmt.Sprintf("val%02d", v))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Start read transaction
	roTxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer roTxn.Abort()

	roDbi, err := roTxn.OpenDBISimple("duptest", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Count duplicates for key00 in read txn
	cursor, err := roTxn.OpenCursor(roDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	_, _, err = cursor.Get([]byte("key00"), nil, gdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	initialCount, err := cursor.Count()
	if err != nil {
		t.Fatal(err)
	}

	if initialCount != 5 {
		t.Fatalf("Expected 5 duplicates for key00, got %d", initialCount)
	}

	// Now modify duplicates in a write transaction
	rwTxn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	rwDbi, err := rwTxn.OpenDBISimple("duptest", 0)
	if err != nil {
		rwTxn.Abort()
		t.Fatal(err)
	}

	// Add more duplicates to key00
	for v := 5; v < 10; v++ {
		val := []byte(fmt.Sprintf("val%02d", v))
		if err := rwTxn.Put(rwDbi, []byte("key00"), val, 0); err != nil {
			rwTxn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := rwTxn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Re-check count in read transaction - should still be 5
	_, _, err = cursor.Get([]byte("key00"), nil, gdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	countAfter, err := cursor.Count()
	if err != nil {
		t.Fatal(err)
	}

	if countAfter != 5 {
		t.Fatalf("COW violation: expected 5 duplicates after concurrent write, got %d", countAfter)
	}

	// Iterate through duplicates - should see original values only
	_, v, err := cursor.Get(nil, nil, gdbx.FirstDup)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("val00")) {
		t.Fatalf("Expected first dup to be 'val00', got '%s'", v)
	}

	t.Log("COW DUPSORT test passed!")
}
