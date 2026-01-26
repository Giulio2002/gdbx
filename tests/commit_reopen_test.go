package tests

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestCommitReopenScenarios tests database integrity after commit and reopen
func TestCommitReopenScenarios(t *testing.T) {
	t.Run("BasicWriteCommitReopen", testBasicWriteCommitReopen)
	t.Run("MultipleTransactionsReopen", testMultipleTransactionsReopen)
	t.Run("DupSortCommitReopen", testDupSortCommitReopen)
	t.Run("LargeValueCommitReopen", testLargeValueCommitReopen)
	t.Run("DeleteThenReopen", testDeleteThenReopen)
	t.Run("UpdateThenReopen", testUpdateThenReopen)
	t.Run("MultipleDBIsReopen", testMultipleDBIsReopen)
	t.Run("EmptyCommitReopen", testEmptyCommitReopen)
	t.Run("SubTreeCommitReopen", testSubTreeCommitReopen)
	t.Run("AppendFlagBasic", testAppendFlagBasic)
	t.Run("AppendFlagMultipleKeys", testAppendFlagMultipleKeys)
	t.Run("AppendFlagWithDupSort", testAppendFlagWithDupSort)
}

func testBasicWriteCommitReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Write some data
	{
		env, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		env.SetMaxDBs(10)
		if err := env.Open(dir, 0, 0644); err != nil {
			t.Fatal(err)
		}

		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			t.Fatal(err)
		}

		// Write 100 key-value pairs
		for i := 0; i < 100; i++ {
			key := []byte{byte(i)}
			val := []byte{byte(i), byte(i + 1), byte(i + 2)}
			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		env.Close()
	}

	// Reopen and verify
	{
		env, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		env.SetMaxDBs(10)
		if err := env.Open(dir, 0, 0644); err != nil {
			t.Fatal(err)
		}
		defer env.Close()

		txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err := txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		// Verify all 100 key-value pairs
		for i := 0; i < 100; i++ {
			key := []byte{byte(i)}
			expected := []byte{byte(i), byte(i + 1), byte(i + 2)}
			val, err := txn.Get(dbi, key)
			if err != nil {
				t.Fatalf("Get key %d failed: %v", i, err)
			}
			if !bytes.Equal(val, expected) {
				t.Errorf("Key %d: got %v, want %v", i, val, expected)
			}
		}
	}
}

func testMultipleTransactionsReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Multiple write transactions
	{
		env, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		env.SetMaxDBs(10)
		if err := env.Open(dir, 0, 0644); err != nil {
			t.Fatal(err)
		}

		// Transaction 1: write keys 0-9
		txn1, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn1.OpenDBISimple("multi", gdbx.Create)
		for i := 0; i < 10; i++ {
			txn1.Put(dbi, []byte{byte(i)}, []byte("txn1"), 0)
		}
		txn1.Commit()

		// Transaction 2: write keys 10-19
		txn2, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn2.OpenDBISimple("multi", 0)
		for i := 10; i < 20; i++ {
			txn2.Put(dbi, []byte{byte(i)}, []byte("txn2"), 0)
		}
		_, _ = txn2.Commit()

		// Transaction 3: update keys 5-14
		txn3, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn3.OpenDBISimple("multi", 0)
		for i := 5; i < 15; i++ {
			txn3.Put(dbi, []byte{byte(i)}, []byte("txn3"), 0)
		}
		txn3.Commit()

		env.Close()
	}

	// Reopen and verify
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()
		dbi, _ := txn.OpenDBISimple("multi", 0)

		// Keys 0-4 should be "txn1"
		for i := 0; i < 5; i++ {
			val, _ := txn.Get(dbi, []byte{byte(i)})
			if string(val) != "txn1" {
				t.Errorf("Key %d: got %q, want txn1", i, val)
			}
		}
		// Keys 5-14 should be "txn3"
		for i := 5; i < 15; i++ {
			val, _ := txn.Get(dbi, []byte{byte(i)})
			if string(val) != "txn3" {
				t.Errorf("Key %d: got %q, want txn3", i, val)
			}
		}
		// Keys 15-19 should be "txn2"
		for i := 15; i < 20; i++ {
			val, _ := txn.Get(dbi, []byte{byte(i)})
			if string(val) != "txn2" {
				t.Errorf("Key %d: got %q, want txn2", i, val)
			}
		}
	}
}

func testDupSortCommitReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Write DUPSORT data
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple("dupsort", gdbx.Create|gdbx.DupSort)

		// Add multiple values per key
		for k := 0; k < 10; k++ {
			key := []byte{byte(k)}
			for v := 0; v < 5; v++ {
				val := []byte{byte(k), byte(v)}
				txn.Put(dbi, key, val, 0)
			}
		}
		_, _ = txn.Commit()
		env.Close()
	}

	// Reopen and verify
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()
		dbi, _ := txn.OpenDBISimple("dupsort", 0)

		cursor, _ := txn.OpenCursor(dbi)
		defer cursor.Close()

		// Iterate all key-value pairs
		count := 0
		for k, v, err := cursor.Get(nil, nil, gdbx.First); err == nil; k, v, err = cursor.Get(nil, nil, gdbx.Next) {
			if len(k) != 1 || len(v) != 2 {
				t.Errorf("Invalid kv: k=%v, v=%v", k, v)
			}
			count++
		}

		expected := 10 * 5
		if count != expected {
			t.Errorf("Count: got %d, want %d", count, expected)
		}
	}
}

func testLargeValueCommitReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	largeValue := make([]byte, 100000) // 100KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Write large value
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple("large", gdbx.Create)
		txn.Put(dbi, []byte("bigkey"), largeValue, 0)
		_, _ = txn.Commit()
		env.Close()
	}

	// Reopen and verify
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()
		dbi, _ := txn.OpenDBISimple("large", 0)

		val, err := txn.Get(dbi, []byte("bigkey"))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(val, largeValue) {
			t.Errorf("Large value mismatch: got len=%d, want len=%d", len(val), len(largeValue))
		}
	}
}

func testDeleteThenReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Write, delete some, commit
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple("delete", gdbx.Create)

		// Write 20 keys
		for i := 0; i < 20; i++ {
			txn.Put(dbi, []byte{byte(i)}, []byte("value"), 0)
		}

		// Delete even keys
		for i := 0; i < 20; i += 2 {
			txn.Del(dbi, []byte{byte(i)}, nil)
		}

		_, _ = txn.Commit()
		env.Close()
	}

	// Reopen and verify
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()
		dbi, _ := txn.OpenDBISimple("delete", 0)

		// Even keys should not exist
		for i := 0; i < 20; i += 2 {
			_, err := txn.Get(dbi, []byte{byte(i)})
			if !gdbx.IsNotFound(err) {
				t.Errorf("Key %d should not exist", i)
			}
		}

		// Odd keys should exist
		for i := 1; i < 20; i += 2 {
			val, err := txn.Get(dbi, []byte{byte(i)})
			if err != nil {
				t.Errorf("Key %d should exist: %v", i, err)
			}
			if string(val) != "value" {
				t.Errorf("Key %d: got %q, want value", i, val)
			}
		}
	}
}

func testUpdateThenReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Write initial, then update
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		// Initial write
		txn1, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn1.OpenDBISimple("update", gdbx.Create)
		for i := 0; i < 10; i++ {
			txn1.Put(dbi, []byte{byte(i)}, []byte("v1"), 0)
		}
		txn1.Commit()

		// Update
		txn2, _ := env.BeginTxn(nil, 0)
		dbi, _ = txn2.OpenDBISimple("update", 0)
		for i := 0; i < 10; i++ {
			txn2.Put(dbi, []byte{byte(i)}, []byte("v2"), 0)
		}
		_, _ = txn2.Commit()

		env.Close()
	}

	// Reopen and verify
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()
		dbi, _ := txn.OpenDBISimple("update", 0)

		for i := 0; i < 10; i++ {
			val, err := txn.Get(dbi, []byte{byte(i)})
			if err != nil {
				t.Errorf("Key %d: %v", i, err)
			}
			if string(val) != "v2" {
				t.Errorf("Key %d: got %q, want v2", i, val)
			}
		}
	}
}

func testMultipleDBIsReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Write to multiple DBIs
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi1, _ := txn.OpenDBISimple("db1", gdbx.Create)
		dbi2, _ := txn.OpenDBISimple("db2", gdbx.Create)
		dbi3, _ := txn.OpenDBISimple("db3", gdbx.Create|gdbx.DupSort)

		txn.Put(dbi1, []byte("key1"), []byte("val1"), 0)
		txn.Put(dbi2, []byte("key2"), []byte("val2"), 0)
		txn.Put(dbi3, []byte("key3"), []byte("val3a"), 0)
		txn.Put(dbi3, []byte("key3"), []byte("val3b"), 0)

		_, _ = txn.Commit()
		env.Close()
	}

	// Reopen and verify all DBIs
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()

		dbi1, _ := txn.OpenDBISimple("db1", 0)
		dbi2, _ := txn.OpenDBISimple("db2", 0)
		dbi3, _ := txn.OpenDBISimple("db3", 0)

		val1, _ := txn.Get(dbi1, []byte("key1"))
		val2, _ := txn.Get(dbi2, []byte("key2"))

		if string(val1) != "val1" {
			t.Errorf("db1: got %q, want val1", val1)
		}
		if string(val2) != "val2" {
			t.Errorf("db2: got %q, want val2", val2)
		}

		// Check DUPSORT values
		cursor, _ := txn.OpenCursor(dbi3)
		defer cursor.Close()
		k, v, _ := cursor.Get([]byte("key3"), nil, gdbx.Set)
		if string(k) != "key3" || string(v) != "val3a" {
			t.Errorf("db3 first: k=%q, v=%q", k, v)
		}
		_, v, _ = cursor.Get(nil, nil, gdbx.NextDup)
		if string(v) != "val3b" {
			t.Errorf("db3 second: v=%q", v)
		}
	}
}

func testEmptyCommitReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Create empty DB
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		txn.OpenDBISimple("empty", gdbx.Create)
		_, _ = txn.Commit()
		env.Close()
	}

	// Reopen and verify empty DB exists
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()

		dbi, err := txn.OpenDBISimple("empty", 0)
		if err != nil {
			t.Fatalf("Should find empty DB: %v", err)
		}

		// First should return not found on empty DB
		cursor, _ := txn.OpenCursor(dbi)
		defer cursor.Close()
		_, _, err = cursor.Get(nil, nil, gdbx.First)
		if !gdbx.IsNotFound(err) {
			t.Errorf("Empty DB First should return NotFound: %v", err)
		}
	}
}

func testSubTreeCommitReopen(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	// Create DUPSORT with many values (will create sub-tree)
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple("subtree", gdbx.Create|gdbx.DupSort)

		// Add many values to force sub-tree creation
		for i := 0; i < 1000; i++ {
			val := make([]byte, 8)
			val[0] = byte(i >> 24)
			val[1] = byte(i >> 16)
			val[2] = byte(i >> 8)
			val[3] = byte(i)
			txn.Put(dbi, []byte("key"), val, 0)
		}
		_, _ = txn.Commit()
		env.Close()
	}

	// Reopen and verify sub-tree
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(dir, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()
		dbi, _ := txn.OpenDBISimple("subtree", 0)

		cursor, _ := txn.OpenCursor(dbi)
		defer cursor.Close()

		// Count all values
		count := 0
		for _, _, err := cursor.Get([]byte("key"), nil, gdbx.Set); err == nil; _, _, err = cursor.Get(nil, nil, gdbx.NextDup) {
			count++
		}

		if count != 1000 {
			t.Errorf("Count: got %d, want 1000", count)
		}
	}
}

func testAppendFlagBasic(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, 0, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("append", gdbx.Create)

	// First insert without Append (establishes first key)
	err1 := txn.Put(dbi, []byte("aaa"), []byte("v1"), 0)
	if err1 != nil {
		t.Fatalf("First put failed: %v", err1)
	}

	// Append should work for key > last
	err2 := txn.Put(dbi, []byte("bbb"), []byte("v2"), gdbx.Append)
	if err2 != nil {
		t.Errorf("Append valid key should succeed: %v", err2)
	}

	// Append should fail for key < last
	err3 := txn.Put(dbi, []byte("aab"), []byte("v3"), gdbx.Append)
	if err3 == nil {
		t.Error("Append with out-of-order key should fail")
	}
	t.Logf("Append out-of-order error: %v", err3)

	// Append with equal key should work (update)
	err4 := txn.Put(dbi, []byte("bbb"), []byte("v2-updated"), gdbx.Append)
	if err4 != nil {
		t.Errorf("Append with same key should succeed: %v", err4)
	}

	txn.Abort()
}

func testAppendFlagMultipleKeys(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, 0, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("append_multi", gdbx.Create)

	// Insert keys in order using Append
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		err := txn.Put(dbi, []byte(k), []byte("value"), gdbx.Append)
		if err != nil {
			t.Errorf("Append %q failed: %v", k, err)
		}
	}

	// Verify all keys exist
	for _, k := range keys {
		val, err := txn.Get(dbi, []byte(k))
		if err != nil {
			t.Errorf("Get %q failed: %v", k, err)
		}
		if string(val) != "value" {
			t.Errorf("Get %q: got %q", k, val)
		}
	}

	txn.Abort()
}

func testAppendFlagWithDupSort(t *testing.T) {
	dir, cleanup := makeTempDir(t)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, 0, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("append_dup", gdbx.Create|gdbx.DupSort)

	// Test Append (key ordering) on DUPSORT
	txn.Put(dbi, []byte("a"), []byte("v1"), 0)

	// Append with larger key should work
	err1 := txn.Put(dbi, []byte("b"), []byte("v1"), gdbx.Append)
	if err1 != nil {
		t.Errorf("Append larger key failed: %v", err1)
	}

	// Append with smaller key should fail
	err2 := txn.Put(dbi, []byte("aa"), []byte("v1"), gdbx.Append)
	if err2 == nil {
		t.Error("Append smaller key should fail")
	}

	// Test AppendDup (value ordering) on same key
	txn.Put(dbi, []byte("c"), []byte("aaa"), 0)
	txn.Put(dbi, []byte("c"), []byte("bbb"), 0)

	// AppendDup with larger value should work
	err3 := txn.Put(dbi, []byte("c"), []byte("ccc"), gdbx.AppendDup)
	if err3 != nil {
		t.Errorf("AppendDup larger value failed: %v", err3)
	}

	// AppendDup with smaller value should fail
	err4 := txn.Put(dbi, []byte("c"), []byte("aab"), gdbx.AppendDup)
	if err4 == nil {
		t.Error("AppendDup smaller value should fail")
	}
	t.Logf("AppendDup smaller error: %v", err4)

	txn.Abort()
}

func makeTempDir(t *testing.T) (string, func()) {
	dir, err := os.MkdirTemp("", "gdbx_test_*")
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "db"), func() {
		os.RemoveAll(dir)
	}
}
