// Package tests contains compatibility tests between libmdbx and gdbx.
// These tests create databases using libmdbx (via CGO) and verify
// that gdbx can read them correctly.
package tests

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// testDB holds paths and cleanup for a test database
type testDB struct {
	path    string
	cleanup func()
}

// newTestDB creates a temporary directory for a test database
func newTestDB(t *testing.T) *testDB {
	t.Helper()
	dir, err := os.MkdirTemp("", "gdbx-compat-*")
	if err != nil {
		t.Fatal(err)
	}
	return &testDB{
		path: dir,
		cleanup: func() {
			os.RemoveAll(dir)
		},
	}
}

// TestBasicReadWrite tests basic key-value operations
func TestBasicReadWrite(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create database with libmdbx
	entries := map[string]string{
		"key1":  "value1",
		"key2":  "value2",
		"key3":  "value3",
		"hello": "world",
		"foo":   "bar",
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	// Read with gdbx and verify
	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		for k, expected := range entries {
			val, err := txn.Get(dbi, []byte(k))
			if err != nil {
				t.Errorf("Get(%q) error: %v", k, err)
				continue
			}
			if string(val) != expected {
				t.Errorf("Get(%q) = %q, want %q", k, val, expected)
			}
		}
	})
}

// TestEmptyDatabase tests reading an empty database
func TestEmptyDatabase(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create empty database with libmdbx
	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		// Don't insert anything
	})

	// Read with gdbx
	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		_, _, err = cursor.Get(nil, nil, gdbx.First)
		if !gdbx.IsNotFound(err) {
			t.Errorf("Expected NotFound on empty db, got: %v", err)
		}
	})
}

// TestLargeValues tests reading large values (overflow pages)
func TestLargeValues(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create large values that will use overflow pages
	largeValue := make([]byte, 100000) // 100KB
	rand.Read(largeValue)

	entries := map[string][]byte{
		"small":  []byte("tiny"),
		"medium": bytes.Repeat([]byte("x"), 1000),
		"large":  largeValue,
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), v, 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		for k, expected := range entries {
			val, err := txn.Get(dbi, []byte(k))
			if err != nil {
				t.Errorf("Get(%q) error: %v", k, err)
				continue
			}
			if !bytes.Equal(val, expected) {
				t.Errorf("Get(%q) length = %d, want %d", k, len(val), len(expected))
			}
		}
	})
}

// TestManyEntries tests reading a database with many entries
func TestManyEntries(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	numEntries := 10000
	entries := make(map[string][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%08d", i)
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(i))
		entries[key] = value
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), v, 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		// Verify count via cursor iteration
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		count := 0
		_, _, err = cursor.Get(nil, nil, gdbx.First)
		for err == nil {
			count++
			_, _, err = cursor.Get(nil, nil, gdbx.Next)
		}
		if !gdbx.IsNotFound(err) {
			t.Fatal(err)
		}

		if count != numEntries {
			t.Errorf("Counted %d entries, want %d", count, numEntries)
		}

		// Verify random samples
		for i := 0; i < 100; i++ {
			idx := i * 100
			key := fmt.Sprintf("key-%08d", idx)
			val, err := txn.Get(dbi, []byte(key))
			if err != nil {
				t.Errorf("Get(%q) error: %v", key, err)
				continue
			}
			expected := entries[key]
			if !bytes.Equal(val, expected) {
				t.Errorf("Get(%q) = %x, want %x", key, val, expected)
			}
		}
	})
}

// TestNamedDatabases tests multiple named databases
func TestNamedDatabases(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	tables := map[string]map[string]string{
		"users": {
			"alice": "admin",
			"bob":   "user",
			"carol": "guest",
		},
		"config": {
			"version": "1.0.0",
			"debug":   "false",
		},
		"counters": {
			"visits": "12345",
			"errors": "0",
		},
	}

	// Create with libmdbx
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetOption(mdbx.OptMaxDB, 10)
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)

	if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	for tableName, entries := range tables {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		dbi, err := txn.OpenDBI(tableName, mdbx.Create, nil, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	env.Close()

	// Read with gdbx
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	gdbxEnv.SetMaxDBs(10)
	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	for tableName, entries := range tables {
		dbi, err := txn.OpenDBISimple(tableName, 0)
		if err != nil {
			t.Errorf("OpenDBI(%q) error: %v", tableName, err)
			continue
		}

		for k, expected := range entries {
			val, err := txn.Get(dbi, []byte(k))
			if err != nil {
				t.Errorf("Get(%q.%q) error: %v", tableName, k, err)
				continue
			}
			if string(val) != expected {
				t.Errorf("Get(%q.%q) = %q, want %q", tableName, k, val, expected)
			}
		}
	}
}

// TestCursorIteration tests forward and backward iteration
func TestCursorIteration(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create ordered entries
	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff"}
	entries := make(map[string]string)
	for _, k := range keys {
		entries[k] = "value-" + k
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		// Forward iteration
		var forwardKeys []string
		k, _, err := cursor.Get(nil, nil, gdbx.First)
		for err == nil {
			forwardKeys = append(forwardKeys, string(k))
			k, _, err = cursor.Get(nil, nil, gdbx.Next)
		}

		if len(forwardKeys) != len(keys) {
			t.Errorf("Forward: got %d keys, want %d", len(forwardKeys), len(keys))
		}

		for i, got := range forwardKeys {
			if got != keys[i] {
				t.Errorf("Forward[%d] = %q, want %q", i, got, keys[i])
			}
		}

		// Backward iteration
		var backwardKeys []string
		k, _, err = cursor.Get(nil, nil, gdbx.Last)
		for err == nil {
			backwardKeys = append(backwardKeys, string(k))
			k, _, err = cursor.Get(nil, nil, gdbx.Prev)
		}

		if len(backwardKeys) != len(keys) {
			t.Errorf("Backward: got %d keys, want %d", len(backwardKeys), len(keys))
		}

		for i, got := range backwardKeys {
			expected := keys[len(keys)-1-i]
			if got != expected {
				t.Errorf("Backward[%d] = %q, want %q", i, got, expected)
			}
		}
	})
}

// TestCursorSeek tests cursor seek operations
func TestCursorSeek(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	entries := map[string]string{
		"key10": "val10",
		"key20": "val20",
		"key30": "val30",
		"key40": "val40",
		"key50": "val50",
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		// Exact match
		k, v, err := cursor.Get([]byte("key30"), nil, gdbx.Set)
		if err != nil {
			t.Errorf("Set(key30) error: %v", err)
		} else if string(k) != "key30" || string(v) != "val30" {
			t.Errorf("Set(key30) = %q:%q, want key30:val30", k, v)
		}

		// SetRange - find key >= "key25"
		k, _, err = cursor.Get([]byte("key25"), nil, gdbx.SetRange)
		if err != nil {
			t.Errorf("SetRange(key25) error: %v", err)
		} else if string(k) != "key30" {
			t.Errorf("SetRange(key25) = %q, want key30", k)
		}

		// SetRange - exact match
		k, _, err = cursor.Get([]byte("key40"), nil, gdbx.SetRange)
		if err != nil {
			t.Errorf("SetRange(key40) error: %v", err)
		} else if string(k) != "key40" {
			t.Errorf("SetRange(key40) = %q, want key40", k)
		}

		// Set - non-existent key
		_, _, err = cursor.Get([]byte("key99"), nil, gdbx.Set)
		if !gdbx.IsNotFound(err) {
			t.Errorf("Set(key99) expected NotFound, got: %v", err)
		}
	})
}

// TestBinaryKeys tests keys with binary data (not just strings)
func TestBinaryKeys(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create binary keys
	entries := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i*1000))
		value := make([]byte, 16)
		binary.LittleEndian.PutUint64(value, uint64(i))
		binary.LittleEndian.PutUint64(value[8:], uint64(i*2))
		entries[string(key)] = value
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), v, 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		for k, expected := range entries {
			val, err := txn.Get(dbi, []byte(k))
			if err != nil {
				t.Errorf("Get binary key error: %v", err)
				continue
			}
			if !bytes.Equal(val, expected) {
				t.Errorf("Get binary key mismatch")
			}
		}

		// Verify iteration order (should be sorted by binary comparison)
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		var prevKey []byte
		k, _, err := cursor.Get(nil, nil, gdbx.First)
		for err == nil {
			if prevKey != nil && bytes.Compare(prevKey, k) >= 0 {
				t.Errorf("Keys not in sorted order: %x >= %x", prevKey, k)
			}
			prevKey = append([]byte{}, k...)
			k, _, err = cursor.Get(nil, nil, gdbx.Next)
		}
	})
}

// TestVariableKeySizes tests keys of different sizes
func TestVariableKeySizes(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	entries := make(map[string]string)
	// Keys from 1 to 100 bytes
	for size := 1; size <= 100; size++ {
		key := bytes.Repeat([]byte{byte(size)}, size)
		entries[string(key)] = fmt.Sprintf("value-for-size-%d", size)
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		for k, expected := range entries {
			val, err := txn.Get(dbi, []byte(k))
			if err != nil {
				t.Errorf("Get(key len %d) error: %v", len(k), err)
				continue
			}
			if string(val) != expected {
				t.Errorf("Get(key len %d) = %q, want %q", len(k), val, expected)
			}
		}
	})
}

// TestPageSizes tests databases with different page sizes
func TestPageSizes(t *testing.T) {
	pageSizes := []int{4096, 8192, 16384}

	for _, pageSize := range pageSizes {
		t.Run(fmt.Sprintf("PageSize%d", pageSize), func(t *testing.T) {
			// Lock OS thread for mdbx-go transaction safety
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			db := newTestDB(t)
			defer db.cleanup()

			entries := make(map[string]string)
			for i := 0; i < 1000; i++ {
				entries[fmt.Sprintf("key-%04d", i)] = fmt.Sprintf("value-%04d", i)
			}

			// Create with specific page size
			env, err := mdbx.NewEnv(mdbx.Label("test"))
			if err != nil {
				t.Fatal(err)
			}

			env.SetGeometry(-1, -1, 1<<30, -1, -1, pageSize)

			if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
				env.Close()
				t.Fatal(err)
			}

			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				env.Close()
				t.Fatal(err)
			}

			dbi, err := txn.OpenRoot(0)
			if err != nil {
				txn.Abort()
				env.Close()
				t.Fatal(err)
			}

			for k, v := range entries {
				if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
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

			// Read with gdbx
			readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
				for k, expected := range entries {
					val, err := txn.Get(dbi, []byte(k))
					if err != nil {
						t.Errorf("Get(%q) error: %v", k, err)
						continue
					}
					if string(val) != expected {
						t.Errorf("Get(%q) = %q, want %q", k, val, expected)
					}
				}
			})
		})
	}
}

// TestDeepTree tests a database with a deep B-tree (many levels)
func TestDeepTree(t *testing.T) {
	// Pin this goroutine to a single OS thread for the duration of the test.
	// libmdbx requires that transactions are used from the same thread they were created on.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Create many entries to force a deeper tree
	numEntries := 100000

	// Create with libmdbx
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)

	if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenRoot(0)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	for i := 0; i < numEntries; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		value := make([]byte, 32)
		binary.BigEndian.PutUint64(value, uint64(i*2))
		if err := txn.Put(dbi, key, value, 0); err != nil {
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

	t.Logf("Created tree with %d entries", numEntries)

	// Read with gdbx
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxTxn.Abort()

	// Verify random lookups
	for i := 0; i < 1000; i++ {
		idx := i * 100
		if idx >= numEntries {
			idx = numEntries - 1
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(idx))

		val, err := gdbxTxn.Get(gdbx.MainDBI, key)
		if err != nil {
			t.Errorf("Get(%d) error: %v", idx, err)
			continue
		}
		expected := uint64(idx * 2)
		got := binary.BigEndian.Uint64(val)
		if got != expected {
			t.Errorf("Get(%d) = %d, want %d", idx, got, expected)
		}
	}

	// Verify iteration count
	cursor, err := gdbxTxn.OpenCursor(gdbx.MainDBI)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	_, _, err = cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		count++
		_, _, err = cursor.Get(nil, nil, gdbx.Next)
	}

	if count != numEntries {
		t.Errorf("Iteration count = %d, want %d", count, numEntries)
	}
}

// TestStatistics tests that gdbx returns correct statistics
func TestStatistics(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	numEntries := 5000

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for i := 0; i < numEntries; i++ {
			key := fmt.Sprintf("key-%06d", i)
			value := fmt.Sprintf("value-%06d", i)
			if err := txn.Put(dbi, []byte(key), []byte(value), 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	// Get gdbx stats
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxTxn.Abort()

	gdbxStat, err := gdbxTxn.Stat(gdbx.MainDBI)
	if err != nil {
		t.Fatal(err)
	}

	// Check stats are reasonable
	if gdbxStat.Entries != uint64(numEntries) {
		t.Errorf("Entries: got %d, want %d", gdbxStat.Entries, numEntries)
	}
	if gdbxStat.Depth == 0 {
		t.Errorf("Depth should be > 0")
	}
	if gdbxStat.LeafPages == 0 {
		t.Errorf("LeafPages should be > 0")
	}
}

// TestListSubdatabases tests listing all named databases
func TestListSubdatabases(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create multiple named databases with libmdbx
	dbNames := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetOption(mdbx.OptMaxDB, 10)
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)

	if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	for _, name := range dbNames {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		dbi, err := txn.OpenDBI(name, mdbx.Create, nil, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		// Put one entry to ensure it's not empty
		if err := txn.Put(dbi, []byte("key"), []byte("value"), 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	env.Close()

	// Read with gdbx and list databases
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	gdbxEnv.SetMaxDBs(10)
	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	// Iterate MainDBI to list subdatabases
	cursor, err := txn.OpenCursor(gdbx.MainDBI)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	foundDBs := make(map[string]bool)
	k, _, err := cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		foundDBs[string(k)] = true
		k, _, err = cursor.Get(nil, nil, gdbx.Next)
	}

	for _, name := range dbNames {
		if !foundDBs[name] {
			t.Errorf("Missing database: %q", name)
		}
	}
}

// TestUpdateThenRead tests that gdbx can read after multiple updates
func TestUpdateThenRead(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create with libmdbx, update multiple times
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)

	if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	// Transaction 1: insert keys 0-99
	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenRoot(0)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		txn.Put(dbi, []byte(key), []byte("v1"), 0)
	}
	_, _ = txn.Commit()

	// Transaction 2: update keys 50-99
	txn, _ = env.BeginTxn(nil, 0)
	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		txn.Put(dbi, []byte(key), []byte("v2"), 0)
	}
	_, _ = txn.Commit()

	// Transaction 3: delete keys 25-49
	txn, _ = env.BeginTxn(nil, 0)
	for i := 25; i < 50; i++ {
		key := fmt.Sprintf("key-%03d", i)
		txn.Del(dbi, []byte(key), nil)
	}
	_, _ = txn.Commit()

	env.Close()

	// Read with gdbx
	readWithGdbx(t, db.path, func(txn *gdbx.Txn, dbi gdbx.DBI) {
		// Keys 0-24: should have "v1"
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("key-%03d", i)
			val, err := txn.Get(dbi, []byte(key))
			if err != nil {
				t.Errorf("Get(%s) error: %v", key, err)
				continue
			}
			if string(val) != "v1" {
				t.Errorf("Get(%s) = %q, want v1", key, val)
			}
		}

		// Keys 25-49: should be deleted
		for i := 25; i < 50; i++ {
			key := fmt.Sprintf("key-%03d", i)
			_, err := txn.Get(dbi, []byte(key))
			if !gdbx.IsNotFound(err) {
				t.Errorf("Get(%s) should be NotFound, got: %v", key, err)
			}
		}

		// Keys 50-99: should have "v2"
		for i := 50; i < 100; i++ {
			key := fmt.Sprintf("key-%03d", i)
			val, err := txn.Get(dbi, []byte(key))
			if err != nil {
				t.Errorf("Get(%s) error: %v", key, err)
				continue
			}
			if string(val) != "v2" {
				t.Errorf("Get(%s) = %q, want v2", key, val)
			}
		}
	})
}

// Helper functions

func createWithLibmdbx(t *testing.T, path string, fn func(txn *mdbx.Txn, dbi mdbx.DBI)) {
	t.Helper()

	// Lock OS thread for mdbx-go transaction safety
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)

	if err := env.Open(path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenRoot(0)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	fn(txn, dbi)

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
}

func readWithGdbx(t *testing.T, path string, fn func(txn *gdbx.Txn, dbi gdbx.DBI)) {
	t.Helper()

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	if err := env.Open(path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	fn(txn, gdbx.MainDBI)
}
