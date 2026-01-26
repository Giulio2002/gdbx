package tests

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestDeleteRangePreservesOutOfRangeEntries reproduces a bug where deleting
// a range of entries (Seek+DeleteCurrent+Next pattern) corrupts entries
// outside the deleted range.
func TestDeleteRangePreservesOutOfRangeEntries(t *testing.T) {
	path := t.TempDir() + "/delete_range.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Open or create a simple (non-DUPSORT) table
	var dbi gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test_table", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Insert 4 entries with 8-byte keys (0, 1, 2, 3)
	// Each with 500+ bytes of value to match the erigon test pattern
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for i := uint64(0); i <= 3; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			value := make([]byte, 507)
			for j := range value {
				value[j] = byte(i)
			}
			if err := txn.Put(dbi, key, value, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put(%d) failed: %v", i, err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all entries exist
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			t.Fatal(err)
		}

		for i := uint64(0); i <= 3; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)
			val, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get(%d) before prune failed: %v", i, err)
			}
			if len(val) != 507 {
				t.Fatalf("Get(%d) before prune: expected 507 bytes, got %d", i, len(val))
			}
		}
		t.Log("All 4 entries verified before prune")
	}

	// Delete range: entries 1 and 2 (simulating prune from key 1 to key 3)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		fromPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(fromPrefix, 1)

		toPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(toPrefix, 3)

		delCount := 0
		for k, _, err := cursor.Get(fromPrefix, nil, gdbx.SetRange); k != nil && bytes.Compare(k, toPrefix) < 0; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
			if err != nil {
				cursor.Close()
				txn.Abort()
				t.Fatalf("cursor iteration failed: %v", err)
			}
			t.Logf("Deleting key: %x", k)
			if err := cursor.Del(0); err != nil {
				cursor.Close()
				txn.Abort()
				t.Fatalf("Del() failed: %v", err)
			}
			delCount++
		}
		if err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("cursor iteration error: %v", err)
		}

		cursor.Close()
		t.Logf("Deleted %d entries", delCount)

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify entry 0 and 3 still exist
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			t.Fatal(err)
		}

		// Entry 0 should still exist
		key0 := make([]byte, 8)
		binary.BigEndian.PutUint64(key0, 0)
		val0, err := txn.Get(dbi, key0)
		if err != nil {
			t.Fatalf("Get(0) after prune failed: %v", err)
		}
		if len(val0) != 507 {
			t.Fatalf("Get(0) after prune: expected 507 bytes, got %d", len(val0))
		}
		t.Log("Entry 0 verified after prune")

		// Entry 3 should still exist
		key3 := make([]byte, 8)
		binary.BigEndian.PutUint64(key3, 3)
		val3, err := txn.Get(dbi, key3)
		if err != nil {
			t.Fatalf("Get(3) after prune failed: %v", err)
		}
		if len(val3) != 507 {
			t.Fatalf("Get(3) after prune: expected 507 bytes, got %d", len(val3))
		}
		t.Log("Entry 3 verified after prune")

		// Entry 1 should NOT exist
		key1 := make([]byte, 8)
		binary.BigEndian.PutUint64(key1, 1)
		_, err = txn.Get(dbi, key1)
		if err != gdbx.ErrNotFoundError {
			t.Fatalf("Get(1) after prune: expected ErrNotFoundError, got %v", err)
		}
		t.Log("Entry 1 correctly not found after prune")

		// Entry 2 should NOT exist
		key2 := make([]byte, 8)
		binary.BigEndian.PutUint64(key2, 2)
		_, err = txn.Get(dbi, key2)
		if err != gdbx.ErrNotFoundError {
			t.Fatalf("Get(2) after prune: expected ErrNotFoundError, got %v", err)
		}
		t.Log("Entry 2 correctly not found after prune")
	}
}

// TestDeleteRangeMultiTable simulates the MarkedForkable scenario with two tables:
// - canonical: 8-byte key -> 32-byte hash
// - vals: 40-byte key (prefix + hash) -> value
// This tests the exact pattern used by erigon's marked forkable prune.
func TestDeleteRangeMultiTable(t *testing.T) {
	path := t.TempDir() + "/delete_range_multi.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	var canonicalDBI, valsDBI gdbx.DBI
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		canonicalDBI, err = txn.OpenDBISimple("canonical", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		valsDBI, err = txn.OpenDBISimple("vals", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Generate hashes for each entry
	hashes := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		hashes[i] = make([]byte, 32)
		for j := range hashes[i] {
			hashes[i][j] = byte(i*10 + j + 0x87) // Start with 0x87 like the erigon debug output
		}
	}

	// Insert 4 entries (0, 1, 2, 3) into both tables
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		canonicalDBI, _ = txn.OpenDBISimple("canonical", 0)
		valsDBI, _ = txn.OpenDBISimple("vals", 0)

		for i := uint64(0); i <= 3; i++ {
			// Canonical: key = encTs(i), value = hash
			canonicalKey := make([]byte, 8)
			binary.BigEndian.PutUint64(canonicalKey, i)
			if err := txn.Put(canonicalDBI, canonicalKey, hashes[i], 0); err != nil {
				txn.Abort()
				t.Fatalf("Put canonical(%d) failed: %v", i, err)
			}

			// Vals: key = encTs(i) + hash, value = data
			valsKey := make([]byte, 40)
			binary.BigEndian.PutUint64(valsKey, i)
			copy(valsKey[8:], hashes[i])
			value := make([]byte, 507)
			for j := range value {
				value[j] = byte(i)
			}
			if err := txn.Put(valsDBI, valsKey, value, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put vals(%d) failed: %v", i, err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all entries in SEPARATE read transaction (matching erigon pattern)
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		canonicalDBI, _ = txn.OpenDBISimple("canonical", 0)
		valsDBI, _ = txn.OpenDBISimple("vals", 0)

		for i := uint64(0); i <= 3; i++ {
			canonicalKey := make([]byte, 8)
			binary.BigEndian.PutUint64(canonicalKey, i)
			hash, err := txn.Get(canonicalDBI, canonicalKey)
			if err != nil {
				t.Fatalf("Get canonical(%d) before prune failed: %v", i, err)
			}

			valsKey := make([]byte, 40)
			copy(valsKey, canonicalKey)
			copy(valsKey[8:], hash)
			val, err := txn.Get(valsDBI, valsKey)
			if err != nil {
				t.Fatalf("Get vals(%d) before prune failed: %v", i, err)
			}
			if len(val) != 507 {
				t.Fatalf("Get vals(%d) before prune: expected 507 bytes, got %d", i, len(val))
			}
		}
		txn.Abort()
		t.Log("All 4 entries verified before prune")
	}

	// Prune: delete entries 1 and 2 from BOTH tables (simulating MarkedTx.Prune)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		canonicalDBI, _ = txn.OpenDBISimple("canonical", 0)
		valsDBI, _ = txn.OpenDBISimple("vals", 0)

		fromPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(fromPrefix, 1)

		toPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(toPrefix, 3)

		// Delete from canonical table
		{
			cursor, err := txn.OpenCursor(canonicalDBI)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}
			delCount := 0
			for k, _, err := cursor.Get(fromPrefix, nil, gdbx.SetRange); k != nil && bytes.Compare(k, toPrefix) < 0; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
				if err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("canonical cursor iteration failed: %v", err)
				}
				t.Logf("Deleting canonical key: %x", k)
				if err := cursor.Del(0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Del canonical failed: %v", err)
				}
				delCount++
			}
			cursor.Close()
			t.Logf("Deleted %d entries from canonical", delCount)
		}

		// Delete from vals table
		{
			cursor, err := txn.OpenCursor(valsDBI)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}
			delCount := 0
			for k, _, err := cursor.Get(fromPrefix, nil, gdbx.SetRange); k != nil && bytes.Compare(k[:8], toPrefix) < 0; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
				if err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("vals cursor iteration failed: %v", err)
				}
				t.Logf("Deleting vals key: %x", k)
				if err := cursor.Del(0); err != nil {
					cursor.Close()
					txn.Abort()
					t.Fatalf("Del vals failed: %v", err)
				}
				delCount++
			}
			cursor.Close()
			t.Logf("Deleted %d entries from vals", delCount)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify entry 0 still exists in SEPARATE read transaction
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()
		canonicalDBI, _ = txn.OpenDBISimple("canonical", 0)
		valsDBI, _ = txn.OpenDBISimple("vals", 0)

		// Check canonical entry 0
		canonicalKey := make([]byte, 8)
		binary.BigEndian.PutUint64(canonicalKey, 0)
		hash, err := txn.Get(canonicalDBI, canonicalKey)
		if err != nil {
			t.Fatalf("Get canonical(0) after prune failed: %v", err)
		}
		if len(hash) != 32 {
			t.Fatalf("Get canonical(0) after prune: expected 32 bytes, got %d", len(hash))
		}
		t.Log("Canonical entry 0 verified after prune")

		// Check vals entry 0 using the retrieved hash
		valsKey := make([]byte, 40)
		copy(valsKey, canonicalKey)
		copy(valsKey[8:], hash)
		t.Logf("Looking up vals key: %x", valsKey)
		val, err := txn.Get(valsDBI, valsKey)
		if err != nil {
			t.Fatalf("Get vals(0) after prune failed: %v", err)
		}
		if len(val) != 507 {
			t.Fatalf("Get vals(0) after prune: expected 507 bytes, got %d", len(val))
		}
		t.Log("Vals entry 0 verified after prune")

		// Check entry 3 still exists
		key3 := make([]byte, 8)
		binary.BigEndian.PutUint64(key3, 3)
		hash3, err := txn.Get(canonicalDBI, key3)
		if err != nil {
			t.Fatalf("Get canonical(3) after prune failed: %v", err)
		}
		valsKey3 := make([]byte, 40)
		copy(valsKey3, key3)
		copy(valsKey3[8:], hash3)
		val3, err := txn.Get(valsDBI, valsKey3)
		if err != nil {
			t.Fatalf("Get vals(3) after prune failed: %v", err)
		}
		if len(val3) != 507 {
			t.Fatalf("Get vals(3) after prune: expected 507 bytes, got %d", len(val3))
		}
		t.Log("Entry 3 verified after prune")
	}
}

// TestDeleteRangeWithLargerKeys tests the same delete range pattern
// with 40-byte keys (8 bytes prefix + 32 bytes hash) like MarkedForkable uses.
func TestDeleteRangeWithLargerKeys(t *testing.T) {
	path := t.TempDir() + "/delete_range_large.db"

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
		dbi, err = txn.OpenDBISimple("test_table", gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Insert 4 entries with 40-byte keys (8-byte prefix + 32-byte hash)
	hashes := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		hashes[i] = make([]byte, 32)
		for j := range hashes[i] {
			hashes[i][j] = byte(i*10 + j)
		}
	}

	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for i := uint64(0); i <= 3; i++ {
			key := make([]byte, 40)
			binary.BigEndian.PutUint64(key, i)
			copy(key[8:], hashes[i])

			value := make([]byte, 507)
			for j := range value {
				value[j] = byte(i)
			}
			if err := txn.Put(dbi, key, value, 0); err != nil {
				txn.Abort()
				t.Fatalf("Put(%d) failed: %v", i, err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Delete entries with prefix 1 and 2 (using 8-byte prefix for Seek)
	{
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		fromPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(fromPrefix, 1)

		toPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(toPrefix, 3)

		delCount := 0
		for k, _, err := cursor.Get(fromPrefix, nil, gdbx.SetRange); k != nil && bytes.Compare(k[:8], toPrefix) < 0; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
			if err != nil {
				cursor.Close()
				txn.Abort()
				t.Fatalf("cursor iteration failed: %v", err)
			}
			t.Logf("Deleting key: %x", k)
			if err := cursor.Del(0); err != nil {
				cursor.Close()
				txn.Abort()
				t.Fatalf("Del() failed: %v", err)
			}
			delCount++
		}
		if err != nil {
			cursor.Close()
			txn.Abort()
			t.Fatalf("cursor iteration error: %v", err)
		}

		cursor.Close()
		t.Logf("Deleted %d entries", delCount)

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify entry 0 and 3 still exist
	{
		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err = txn.OpenDBISimple("test_table", 0)
		if err != nil {
			t.Fatal(err)
		}

		// Entry 0 should still exist
		key0 := make([]byte, 40)
		binary.BigEndian.PutUint64(key0, 0)
		copy(key0[8:], hashes[0])
		val0, err := txn.Get(dbi, key0)
		if err != nil {
			t.Fatalf("Get(0) after prune failed: %v", err)
		}
		if len(val0) != 507 {
			t.Fatalf("Get(0) after prune: expected 507 bytes, got %d", len(val0))
		}
		t.Log("Entry 0 verified after prune")

		// Entry 3 should still exist
		key3 := make([]byte, 40)
		binary.BigEndian.PutUint64(key3, 3)
		copy(key3[8:], hashes[3])
		val3, err := txn.Get(dbi, key3)
		if err != nil {
			t.Fatalf("Get(3) after prune failed: %v", err)
		}
		if len(val3) != 507 {
			t.Fatalf("Get(3) after prune: expected 507 bytes, got %d", len(val3))
		}
		t.Log("Entry 3 verified after prune")
	}
}
