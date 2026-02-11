package tests

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// ==================== Regular (non-DupSort) table tests ====================

// TestDeleteRangeRegularBasic tests basic range delete on a regular table.
func TestDeleteRangeRegularBasic(t *testing.T) {
	path := t.TempDir() + "/dr_regular_basic.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	// Create table and insert 10 entries (keys 0..9)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for i := uint64(0); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val := make([]byte, 32)
			binary.BigEndian.PutUint64(val, i*100)
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [3, 7) — should delete keys 3, 4, 5, 6
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 3)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 7)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 4 {
			txn.Abort()
			t.Fatalf("expected 4 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify remaining entries
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		// Keys 0, 1, 2 should exist
		for i := uint64(0); i < 3; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			_, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) after delete range failed: %v", i, err)
			}
		}

		// Keys 3, 4, 5, 6 should NOT exist
		for i := uint64(3); i < 7; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			_, err := txn.Get(dbi, key)
			if err != gdbx.ErrNotFoundError {
				t.Fatalf("Get(%d) after delete range: expected ErrNotFound, got %v", i, err)
			}
		}

		// Keys 7, 8, 9 should exist
		for i := uint64(7); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			_, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) after delete range failed: %v", i, err)
			}
		}
	}
}

// TestDeleteRangeRegularAll tests deleting the entire table via range delete.
func TestDeleteRangeRegularAll(t *testing.T) {
	path := t.TempDir() + "/dr_regular_all.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < 100; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val := make([]byte, 32)
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete all entries (from=nil, to=nil)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		deleted, err := txn.DeleteRange(dbi, nil, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 100 {
			txn.Abort()
			t.Fatalf("expected 100 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify table is empty
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 0 {
			t.Fatalf("expected 0 entries, got %d", stat.Entries)
		}
	}
}

// TestDeleteRangeRegularEmpty tests range delete on an empty table.
func TestDeleteRangeRegularEmpty(t *testing.T) {
	path := t.TempDir() + "/dr_regular_empty.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete from empty table
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 100)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 0 {
			txn.Abort()
			t.Fatalf("expected 0 deleted, got %d", deleted)
		}

		txn.Abort()
	}
}

// TestDeleteRangeRegularNoMatch tests range delete with a range that contains no entries.
func TestDeleteRangeRegularNoMatch(t *testing.T) {
	path := t.TempDir() + "/dr_regular_nomatch.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		// Insert keys 0, 1, 2
		for i := uint64(0); i < 3; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if err := txn.Put(dbi, key, make([]byte, 32), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [100, 200) — no matching entries
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 100)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 200)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 0 {
			txn.Abort()
			t.Fatalf("expected 0 deleted, got %d", deleted)
		}

		txn.Abort()
	}
}

// TestDeleteRangeRegularNilTo tests range delete with to=nil (delete to the end).
func TestDeleteRangeRegularNilTo(t *testing.T) {
	path := t.TempDir() + "/dr_regular_nilto.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if err := txn.Put(dbi, key, make([]byte, 32), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete from key 5 to end (to=nil)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 5)

		deleted, err := txn.DeleteRange(dbi, from, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 5 {
			txn.Abort()
			t.Fatalf("expected 5 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify: keys 0..4 exist, keys 5..9 don't
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		for i := uint64(0); i < 5; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if _, err := txn.Get(dbi, key); err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
		}
		for i := uint64(5); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if _, err := txn.Get(dbi, key); err != gdbx.ErrNotFoundError {
				t.Fatalf("Get(%d) expected ErrNotFound, got %v", i, err)
			}
		}
	}
}

// TestDeleteRangeRegularNilFrom tests range delete with from=nil (delete from beginning).
func TestDeleteRangeRegularNilFrom(t *testing.T) {
	path := t.TempDir() + "/dr_regular_nilfrom.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if err := txn.Put(dbi, key, make([]byte, 32), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete from beginning to key 5 (from=nil, to=5)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 5)

		deleted, err := txn.DeleteRange(dbi, nil, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 5 {
			txn.Abort()
			t.Fatalf("expected 5 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify: keys 0..4 don't exist, keys 5..9 exist
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		for i := uint64(0); i < 5; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if _, err := txn.Get(dbi, key); err != gdbx.ErrNotFoundError {
				t.Fatalf("Get(%d) expected ErrNotFound, got %v", i, err)
			}
		}
		for i := uint64(5); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if _, err := txn.Get(dbi, key); err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
		}
	}
}

// TestDeleteRangeRegularLargeValues tests range delete with larger values (overflow pages).
func TestDeleteRangeRegularLargeValues(t *testing.T) {
	path := t.TempDir() + "/dr_regular_largevals.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < 20; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val := make([]byte, 507) // Large enough to cause splits
			for j := range val {
				val[j] = byte(i)
			}
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [5, 15)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 5)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 15)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 10 {
			txn.Abort()
			t.Fatalf("expected 10 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		// Keys 0..4 should exist
		for i := uint64(0); i < 5; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
			if len(val) != 507 {
				t.Fatalf("Get(%d): expected 507 bytes, got %d", i, len(val))
			}
		}

		// Keys 5..14 should NOT exist
		for i := uint64(5); i < 15; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			_, err := txn.Get(dbi, key)
			if err != gdbx.ErrNotFoundError {
				t.Fatalf("Get(%d) expected ErrNotFound, got %v", i, err)
			}
		}

		// Keys 15..19 should exist
		for i := uint64(15); i < 20; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
			if len(val) != 507 {
				t.Fatalf("Get(%d): expected 507 bytes, got %d", i, len(val))
			}
		}
	}
}

// TestDeleteRangeRegularSingleEntry tests deleting a range that contains exactly one entry.
func TestDeleteRangeRegularSingleEntry(t *testing.T) {
	path := t.TempDir() + "/dr_regular_single.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < 5; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if err := txn.Put(dbi, key, make([]byte, 16), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [2, 3) — only key 2
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 2)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 3)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 1 {
			txn.Abort()
			t.Fatalf("expected 1 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 4 {
			t.Fatalf("expected 4 entries remaining, got %d", stat.Entries)
		}

		key2 := make([]byte, 8)
		binary.BigEndian.PutUint64(key2, 2)
		_, err = txn.Get(dbi, key2)
		if err != gdbx.ErrNotFoundError {
			t.Fatalf("Get(2) expected ErrNotFound, got %v", err)
		}
	}
}

// TestDeleteRangeRegularLargeDataset tests range delete with a larger dataset
// to exercise B-tree rebalancing across multiple pages.
func TestDeleteRangeRegularLargeDataset(t *testing.T) {
	path := t.TempDir() + "/dr_regular_large.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	numEntries := uint64(1000)

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < numEntries; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val := make([]byte, 64)
			binary.BigEndian.PutUint64(val, i)
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [200, 800)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 200)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 800)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 600 {
			txn.Abort()
			t.Fatalf("expected 600 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 400 {
			t.Fatalf("expected 400 entries remaining, got %d", stat.Entries)
		}

		// Spot-check some entries
		for _, i := range []uint64{0, 50, 199, 800, 900, 999} {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			_, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
		}

		for _, i := range []uint64{200, 300, 500, 799} {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			_, err := txn.Get(dbi, key)
			if err != gdbx.ErrNotFoundError {
				t.Fatalf("Get(%d) expected ErrNotFound, got %v", i, err)
			}
		}
	}
}

// ==================== DupSort table tests ====================

// TestDeleteRangeDupSortBasic tests basic range delete on a DupSort table.
func TestDeleteRangeDupSortBasic(t *testing.T) {
	path := t.TempDir() + "/dr_dupsort_basic.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		// Insert 5 keys, each with 3 duplicate values
		for i := uint64(0); i < 5; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			for j := uint64(0); j < 3; j++ {
				val := make([]byte, 8)
				binary.BigEndian.PutUint64(val, j)
				if err := txn.Put(dbi, key, val, 0); err != nil {
					txn.Abort()
					t.Fatal(err)
				}
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [1, 4) — should delete keys 1, 2, 3 with all their dups
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 1)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 4)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		// 3 keys * 3 dups = 9 entries
		if deleted != 9 {
			txn.Abort()
			t.Fatalf("expected 9 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify remaining entries
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		// Count all remaining entries
		count := 0
		var keys []uint64
		for k, _, err := cursor.Get(nil, nil, gdbx.First); err == nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
			count++
			if len(k) >= 8 {
				keys = append(keys, binary.BigEndian.Uint64(k[:8]))
			}
		}

		// Should have 6 remaining entries (2 keys * 3 dups)
		if count != 6 {
			t.Fatalf("expected 6 remaining entries, got %d (keys: %v)", count, keys)
		}
	}
}

// TestDeleteRangeDupSortManyDups tests range delete on a DupSort table with many duplicates.
func TestDeleteRangeDupSortManyDups(t *testing.T) {
	path := t.TempDir() + "/dr_dupsort_manydups.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	dupsPerKey := uint64(50)

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		// Insert 10 keys, each with 50 duplicate values
		for i := uint64(0); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			for j := uint64(0); j < dupsPerKey; j++ {
				val := make([]byte, 16)
				binary.BigEndian.PutUint64(val, j)
				if err := txn.Put(dbi, key, val, 0); err != nil {
					txn.Abort()
					t.Fatal(err)
				}
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [3, 7) — should delete keys 3, 4, 5, 6 with all their dups
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 3)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 7)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		expected := int64(4 * dupsPerKey)
		if deleted != expected {
			txn.Abort()
			t.Fatalf("expected %d deleted, got %d", expected, deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify remaining entries
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		expectedRemaining := uint64(6 * dupsPerKey) // 6 keys * 50 dups
		if stat.Entries != expectedRemaining {
			t.Fatalf("expected %d remaining entries, got %d", expectedRemaining, stat.Entries)
		}

		// Verify key 0 exists with all dups
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		key0 := make([]byte, 8)
		binary.BigEndian.PutUint64(key0, 0)
		k, _, err := cursor.Get(key0, nil, gdbx.Set)
		if err != nil {
			t.Fatalf("Set(0) failed: %v", err)
		}
		if !bytes.Equal(k, key0) {
			t.Fatalf("Set(0) returned wrong key: %x", k)
		}
		cnt, err := cursor.Count()
		if err != nil {
			t.Fatalf("Count() failed: %v", err)
		}
		if cnt != dupsPerKey {
			t.Fatalf("key 0: expected %d dups, got %d", dupsPerKey, cnt)
		}

		// Verify key 3 does NOT exist
		key3 := make([]byte, 8)
		binary.BigEndian.PutUint64(key3, 3)
		_, _, err = cursor.Get(key3, nil, gdbx.Set)
		if !gdbx.IsNotFound(err) {
			t.Fatalf("Set(3) expected NotFound, got %v", err)
		}
	}
}

// TestDeleteRangeDupSortNilTo tests range delete to end on a DupSort table.
func TestDeleteRangeDupSortNilTo(t *testing.T) {
	path := t.TempDir() + "/dr_dupsort_nilto.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for i := uint64(0); i < 5; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			for j := uint64(0); j < 4; j++ {
				val := make([]byte, 8)
				binary.BigEndian.PutUint64(val, j)
				if err := txn.Put(dbi, key, val, 0); err != nil {
					txn.Abort()
					t.Fatal(err)
				}
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete from key 3 to end (to=nil)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 3)

		deleted, err := txn.DeleteRange(dbi, from, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		// 2 keys (3, 4) * 4 dups = 8
		if deleted != 8 {
			txn.Abort()
			t.Fatalf("expected 8 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify remaining: keys 0, 1, 2 with 4 dups each = 12 entries
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 12 {
			t.Fatalf("expected 12 remaining entries, got %d", stat.Entries)
		}
	}
}

// TestDeleteRangeDupSortAll tests deleting all entries from a DupSort table.
func TestDeleteRangeDupSortAll(t *testing.T) {
	path := t.TempDir() + "/dr_dupsort_all.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for i := uint64(0); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			for j := uint64(0); j < 5; j++ {
				val := make([]byte, 8)
				binary.BigEndian.PutUint64(val, j)
				if err := txn.Put(dbi, key, val, 0); err != nil {
					txn.Abort()
					t.Fatal(err)
				}
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete all (from=nil, to=nil)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		deleted, err := txn.DeleteRange(dbi, nil, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 50 {
			txn.Abort()
			t.Fatalf("expected 50 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify table is empty
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 0 {
			t.Fatalf("expected 0 entries, got %d", stat.Entries)
		}
	}
}

// TestDeleteRangeDupSortSingleValuePerKey tests DupSort where each key has exactly one value.
func TestDeleteRangeDupSortSingleValuePerKey(t *testing.T) {
	path := t.TempDir() + "/dr_dupsort_singleval.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		// Each key has exactly 1 value (degenerate DupSort case)
		for i := uint64(0); i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, i*10)
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [2, 8)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 2)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 8)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		// 6 keys * 1 dup = 6
		if deleted != 6 {
			txn.Abort()
			t.Fatalf("expected 6 deleted, got %d", deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 4 {
			t.Fatalf("expected 4 remaining entries, got %d", stat.Entries)
		}
	}
}

// TestDeleteRangeReadOnlyTransaction tests that DeleteRange fails on read-only transactions.
func TestDeleteRangeReadOnlyTransaction(t *testing.T) {
	path := t.TempDir() + "/dr_readonly.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		key := make([]byte, 8)
		if err := txn.Put(dbi, key, make([]byte, 8), 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Try DeleteRange on a read-only transaction
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		_, err = txn.DeleteRange(dbi, nil, nil)
		if err == nil {
			t.Fatal("expected error on read-only transaction, got nil")
		}
	}
}

// TestDeleteRangeMultipleCommits tests that DeleteRange works correctly
// across multiple transactions.
func TestDeleteRangeMultipleCommits(t *testing.T) {
	path := t.TempDir() + "/dr_multi_commit.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var dbi gdbx.DBI
	// Insert 30 entries
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		for i := uint64(0); i < 30; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if err := txn.Put(dbi, key, make([]byte, 32), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// First range delete: [0, 10)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 10)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 10 {
			txn.Abort()
			t.Fatalf("first delete: expected 10, got %d", deleted)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Second range delete: [20, 30)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 20)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 30)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if deleted != 10 {
			txn.Abort()
			t.Fatalf("second delete: expected 10, got %d", deleted)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify: only keys 10..19 remain
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Entries != 10 {
			t.Fatalf("expected 10 entries remaining, got %d", stat.Entries)
		}

		for i := uint64(10); i < 20; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			if _, err := txn.Get(dbi, key); err != nil {
				t.Fatalf("Get(%d) failed: %v", i, err)
			}
		}
	}
}

// TestDeleteRangeDupSortLargeDataset tests range delete on a DupSort table with many entries.
func TestDeleteRangeDupSortLargeDataset(t *testing.T) {
	path := t.TempDir() + "/dr_dupsort_large.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	numKeys := uint64(100)
	dupsPerKey := uint64(10)

	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for i := uint64(0); i < numKeys; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			for j := uint64(0); j < dupsPerKey; j++ {
				val := make([]byte, 16)
				binary.BigEndian.PutUint64(val, j)
				if err := txn.Put(dbi, key, val, 0); err != nil {
					txn.Abort()
					t.Fatal(err)
				}
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete range [25, 75) — 50 keys * 10 dups = 500 entries
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		from := make([]byte, 8)
		binary.BigEndian.PutUint64(from, 25)
		to := make([]byte, 8)
		binary.BigEndian.PutUint64(to, 75)

		deleted, err := txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		expected := int64(50 * dupsPerKey)
		if deleted != expected {
			txn.Abort()
			t.Fatalf("expected %d deleted, got %d", expected, deleted)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		stat, err := txn.Stat(dbi)
		if err != nil {
			t.Fatal(err)
		}
		expectedRemaining := uint64(50 * dupsPerKey)
		if stat.Entries != expectedRemaining {
			t.Fatalf("expected %d remaining entries, got %d", expectedRemaining, stat.Entries)
		}
	}
}
