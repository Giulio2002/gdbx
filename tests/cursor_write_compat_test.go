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

// TestCursorPutCompat tests that cursor.Put in gdbx produces databases readable by libmdbx.
func TestCursorPutCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "cursor-put-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Write with gdbx cursor.Put
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("testdb", gdbx.Create)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Insert 100 key-value pairs using cursor.Put
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		if err := cursor.Put(key, val, 0); err != nil {
			cursor.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("cursor.Put failed: %v", err)
		}
	}

	cursor.Close()
	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}
	env.Close()

	// Phase 2: Read with libmdbx and verify
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	if err := mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	mdbxDbi, err := mdbxTxn.OpenDBI("testdb", 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all 100 entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		expectedVal := []byte(fmt.Sprintf("value-%04d", i))

		val, err := mdbxTxn.Get(mdbxDbi, key)
		if err != nil {
			t.Fatalf("libmdbx Get(%s) failed: %v", key, err)
		}
		if !bytes.Equal(val, expectedVal) {
			t.Fatalf("libmdbx Get(%s) = %q, want %q", key, val, expectedVal)
		}
	}

	t.Logf("cursor.Put compatibility: gdbx wrote 100 entries, libmdbx read all correctly")
}

// TestCursorPutDupCompat tests cursor.Put for DUPSORT databases.
func TestCursorPutDupCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "cursor-putdup-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Write with gdbx cursor.Put (DUPSORT)
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("dupdb", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Insert 5 keys with 10 values each using cursor.Put
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		for j := 0; j < 10; j++ {
			val := []byte(fmt.Sprintf("val-%04d", j))
			if err := cursor.Put(key, val, 0); err != nil {
				cursor.Close()
				txn.Abort()
				env.Close()
				t.Fatalf("cursor.Put failed: %v", err)
			}
		}
	}

	cursor.Close()
	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatal(err)
	}
	env.Close()

	// Phase 2: Read with libmdbx and verify
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	if err := mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}
	defer mdbxEnv.Close()

	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxTxn.Abort()

	mdbxDbi, err := mdbxTxn.OpenDBI("dupdb", mdbx.DupSort, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mdbxCursor, err := mdbxTxn.OpenCursor(mdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer mdbxCursor.Close()

	// Verify all entries using GetBoth
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		for j := 0; j < 10; j++ {
			val := []byte(fmt.Sprintf("val-%04d", j))
			_, gotVal, err := mdbxCursor.Get(key, val, mdbx.GetBoth)
			if err != nil {
				t.Fatalf("libmdbx GetBoth(%s, %s) failed: %v", key, val, err)
			}
			if !bytes.Equal(gotVal, val) {
				t.Fatalf("libmdbx GetBoth(%s, %s) = %q", key, val, gotVal)
			}
		}
	}

	// Verify count for each key
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		_, _, err := mdbxCursor.Get(key, nil, mdbx.Set)
		if err != nil {
			t.Fatalf("libmdbx Set(%s) failed: %v", key, err)
		}
		count, err := mdbxCursor.Count()
		if err != nil {
			t.Fatalf("libmdbx Count failed: %v", err)
		}
		if count != 10 {
			t.Fatalf("key %s has %d values, want 10", key, count)
		}
	}

	t.Logf("cursor.Put DUPSORT compatibility: gdbx wrote 5 keys x 10 values, libmdbx verified all")
}

// TestCursorDelCompat tests cursor.Del compatibility.
func TestCursorDelCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "cursor-del-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Write with gdbx, then delete some entries
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Insert 100 entries
	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("testdb", gdbx.Create)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		txn.Put(dbi, key, val, 0)
	}
	_, _ = txn.Commit()

	// Delete every other entry using cursor.Del
	txn, _ = env.BeginTxn(nil, 0)
	cursor, _ := txn.OpenCursor(dbi)
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%04d", i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err != nil {
			cursor.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("cursor.Get failed: %v", err)
		}
		if err := cursor.Del(0); err != nil {
			cursor.Close()
			txn.Abort()
			env.Close()
			t.Fatalf("cursor.Del failed: %v", err)
		}
	}
	cursor.Close()
	_, _ = txn.Commit()
	env.Close()

	// Phase 2: Verify with libmdbx
	mdbxEnv, _ := mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644)
	defer mdbxEnv.Close()

	mdbxTxn, _ := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	defer mdbxTxn.Abort()
	mdbxDbi, _ := mdbxTxn.OpenDBI("testdb", 0, nil, nil)

	// Verify even keys are deleted, odd keys remain
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val, err := mdbxTxn.Get(mdbxDbi, key)

		if i%2 == 0 {
			// Even keys should be deleted
			if !mdbx.IsNotFound(err) {
				t.Fatalf("key %s should be deleted, got val=%q err=%v", key, val, err)
			}
		} else {
			// Odd keys should exist
			if err != nil {
				t.Fatalf("key %s should exist: %v", key, err)
			}
			expectedVal := []byte(fmt.Sprintf("value-%04d", i))
			if !bytes.Equal(val, expectedVal) {
				t.Fatalf("key %s = %q, want %q", key, val, expectedVal)
			}
		}
	}

	t.Logf("cursor.Del compatibility: gdbx deleted 50 entries, libmdbx verified correctly")
}

// TestCursorDelDupCompat tests Txn.Del for DUPSORT databases.
func TestCursorDelDupCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "cursor-deldup-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Write DUPSORT data with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	env.SetMaxDBs(10)
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("dupdb", gdbx.Create|gdbx.DupSort)

	// Insert 5 keys with 20 values each
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		for j := 0; j < 20; j++ {
			val := []byte(fmt.Sprintf("val-%04d", j))
			txn.Put(dbi, key, val, 0)
		}
	}
	_, _ = txn.Commit()

	// Delete specific duplicate values using Txn.Del
	key := []byte("key-02")

	// Delete val-0005 from key-02
	txn, _ = env.BeginTxn(nil, 0)
	dbi, _ = txn.OpenDBISimple("dupdb", gdbx.DupSort)
	if err := txn.Del(dbi, key, []byte("val-0005")); err != nil {
		txn.Abort()
		env.Close()
		t.Fatalf("Txn.Del for val-0005 failed: %v", err)
	}
	_, _ = txn.Commit()

	// Delete val-0010 from key-02
	txn, _ = env.BeginTxn(nil, 0)
	dbi, _ = txn.OpenDBISimple("dupdb", gdbx.DupSort)
	if err := txn.Del(dbi, key, []byte("val-0010")); err != nil {
		txn.Abort()
		env.Close()
		t.Fatalf("Txn.Del for val-0010 failed: %v", err)
	}
	_, _ = txn.Commit()
	env.Close()

	// Phase 2: Verify with libmdbx
	mdbxEnv, _ := mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644)
	defer mdbxEnv.Close()

	mdbxTxn, _ := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	defer mdbxTxn.Abort()
	mdbxDbi, _ := mdbxTxn.OpenDBI("dupdb", mdbx.DupSort, nil, nil)
	mdbxCursor, _ := mdbxTxn.OpenCursor(mdbxDbi)
	defer mdbxCursor.Close()

	// Verify key-02 has 18 values (20 - 2 deleted)
	_, _, err = mdbxCursor.Get([]byte("key-02"), nil, mdbx.Set)
	if err != nil {
		t.Fatal(err)
	}
	count, _ := mdbxCursor.Count()
	if count != 18 {
		t.Fatalf("key-02 should have 18 values, got %d", count)
	}

	// Verify val-0005 and val-0010 are deleted
	for _, valIdx := range []int{5, 10} {
		val := []byte(fmt.Sprintf("val-%04d", valIdx))
		_, _, err := mdbxCursor.Get([]byte("key-02"), val, mdbx.GetBoth)
		if !mdbx.IsNotFound(err) {
			t.Fatalf("val-%04d should be deleted from key-02", valIdx)
		}
	}

	// Verify other values still exist
	for j := 0; j < 20; j++ {
		if j == 5 || j == 10 {
			continue
		}
		val := []byte(fmt.Sprintf("val-%04d", j))
		_, _, err := mdbxCursor.Get([]byte("key-02"), val, mdbx.GetBoth)
		if err != nil {
			t.Fatalf("val-%04d should exist in key-02: %v", j, err)
		}
	}

	// Verify other keys are untouched (still have 20 values each)
	for i := 0; i < 5; i++ {
		if i == 2 {
			continue
		}
		key := []byte(fmt.Sprintf("key-%02d", i))
		_, _, err = mdbxCursor.Get(key, nil, mdbx.Set)
		if err != nil {
			t.Fatal(err)
		}
		count, _ := mdbxCursor.Count()
		if count != 20 {
			t.Fatalf("key-%02d should have 20 values, got %d", i, count)
		}
	}

	t.Logf("cursor.Del DUPSORT compatibility: gdbx deleted 2 dup values, libmdbx verified correctly")
}

// TestReverseWriteCompat tests libmdbx writes, gdbx reads (reverse direction).
func TestReverseWriteCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "reverse-write-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Write with libmdbx cursor.Put
	mdbxEnv, _ := mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Create, 0644)

	mdbxTxn, _ := mdbxEnv.BeginTxn(nil, 0)
	mdbxDbi, _ := mdbxTxn.OpenDBI("testdb", mdbx.Create, nil, nil)
	mdbxCursor, _ := mdbxTxn.OpenCursor(mdbxDbi)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		mdbxCursor.Put(key, val, 0)
	}
	mdbxCursor.Close()
	mdbxTxn.Commit()
	mdbxEnv.Close()

	// Phase 2: Read with gdbx and verify
	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple("testdb", 0)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		expectedVal := []byte(fmt.Sprintf("value-%04d", i))

		val, err := txn.Get(dbi, key)
		if err != nil {
			t.Fatalf("gdbx Get(%s) failed: %v", key, err)
		}
		if !bytes.Equal(val, expectedVal) {
			t.Fatalf("gdbx Get(%s) = %q, want %q", key, val, expectedVal)
		}
	}

	t.Logf("Reverse compatibility: libmdbx wrote 100 entries, gdbx read all correctly")
}

// TestReverseDelCompat tests libmdbx deletes, gdbx verifies.
func TestReverseDelCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "reverse-del-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Write with libmdbx
	mdbxEnv, _ := mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Create, 0644)

	mdbxTxn, _ := mdbxEnv.BeginTxn(nil, 0)
	mdbxDbi, _ := mdbxTxn.OpenDBI("testdb", mdbx.Create, nil, nil)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		mdbxTxn.Put(mdbxDbi, key, val, 0)
	}
	mdbxTxn.Commit()

	// Delete every other entry with libmdbx cursor.Del
	mdbxTxn, _ = mdbxEnv.BeginTxn(nil, 0)
	mdbxCursor, _ := mdbxTxn.OpenCursor(mdbxDbi)
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%04d", i))
		mdbxCursor.Get(key, nil, mdbx.Set)
		mdbxCursor.Del(0)
	}
	mdbxCursor.Close()
	mdbxTxn.Commit()
	mdbxEnv.Close()

	// Phase 2: Verify with gdbx
	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple("testdb", 0)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val, err := txn.Get(dbi, key)

		if i%2 == 0 {
			if !gdbx.IsNotFound(err) {
				t.Fatalf("key %s should be deleted, got val=%q err=%v", key, val, err)
			}
		} else {
			if err != nil {
				t.Fatalf("key %s should exist: %v", key, err)
			}
			expectedVal := []byte(fmt.Sprintf("value-%04d", i))
			if !bytes.Equal(val, expectedVal) {
				t.Fatalf("key %s = %q, want %q", key, val, expectedVal)
			}
		}
	}

	t.Logf("Reverse del compatibility: libmdbx deleted 50 entries, gdbx verified correctly")
}
