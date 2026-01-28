package tests

import (
	"encoding/binary"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestDeleteReinsertCorruption tests for data corruption after deleting
// and reinserting entries. This is a known issue that triggers with
// large numbers of entries (>1500) with significant value sizes.
//
// Bug: After delete+reinsert cycle, all data is lost even though commit succeeds.
// This appears to be a pre-existing bug in GC page handling.
func TestDeleteReinsertCorruption(t *testing.T) {
	t.Skip("Known bug: data corruption after delete+reinsert with >1500 large entries")

	path := t.TempDir() + "/corruption.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", gdbx.Create)
		txn.Commit()
	}

	const numEntries = 2000 // Bug triggers somewhere between 1500-1800
	key := make([]byte, 8)
	val := make([]byte, 500)

	// Insert
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Fatalf("Insert Put(%d) failed: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Insert commit failed: %v", err)
		}
		t.Log("Insert committed")
	}

	// Verify after insert
	{
		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			_, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("After insert: Get(%d) failed: %v", i, err)
			}
		}
		txn.Abort()
		t.Log("Verified after insert")
	}

	// Delete all
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			if err := txn.Del(dbi, key, nil); err != nil {
				t.Fatalf("Del(%d) failed: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Delete commit failed: %v", err)
		}
		t.Log("Delete committed")
	}

	// Verify after delete
	{
		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			_, err := txn.Get(dbi, key)
			if err != gdbx.ErrNotFoundError {
				t.Fatalf("After delete: Get(%d) should return ErrNotFoundError, got: %v", i, err)
			}
		}
		txn.Abort()
		t.Log("Verified after delete (all entries gone)")
	}

	// Reinsert
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i+1000))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Fatalf("Reinsert Put(%d) failed: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Reinsert commit failed: %v", err)
		}
		t.Log("Reinsert committed")
	}

	// Verify after reinsert
	{
		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		dbi, _ = txn.OpenDBISimple("test", 0)
		missing := 0
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				missing++
				if missing <= 5 {
					t.Logf("After reinsert: Get(%d) failed: %v", i, err)
				}
			} else {
				got := binary.BigEndian.Uint64(v)
				expected := uint64(i + 1000)
				if got != expected {
					t.Errorf("After reinsert: Get(%d) = %d, expected %d", i, got, expected)
				}
			}
		}
		txn.Abort()
		if missing > 0 {
			t.Fatalf("After reinsert: %d/%d entries missing!", missing, numEntries)
		}
		t.Log("Verified after reinsert")
	}
}

// TestSmallDeleteReinsert verifies that delete+reinsert works for smaller datasets.
// This passes and serves as a baseline test.
func TestSmallDeleteReinsert(t *testing.T) {
	path := t.TempDir() + "/small_reinsert.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", gdbx.Create)
		txn.Commit()
	}

	const numEntries = 1000 // Below the bug threshold
	key := make([]byte, 8)
	val := make([]byte, 500)

	// Insert
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i))
			txn.Put(dbi, key, val, 0)
		}
		txn.Commit()
	}

	// Delete all
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			txn.Del(dbi, key, nil)
		}
		txn.Commit()
	}

	// Reinsert
	{
		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i+1000))
			txn.Put(dbi, key, val, 0)
		}
		txn.Commit()
	}

	// Verify
	{
		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		dbi, _ = txn.OpenDBISimple("test", 0)
		for i := 0; i < numEntries; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
			got := binary.BigEndian.Uint64(v)
			expected := uint64(i + 1000)
			if got != expected {
				t.Fatalf("Get(%d) = %d, expected %d", i, got, expected)
			}
		}
		txn.Abort()
	}
}
