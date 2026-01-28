package tests

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestCommitPersistenceWithWriteMap tests that data persists after commit with WriteMap mode.
func TestCommitPersistenceWithWriteMap(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-writemap-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Create and populate database with WriteMap
	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	// Use WriteMap flag
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}

	// Insert data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert various sizes of data
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("small value")},
		{[]byte("key2"), bytes.Repeat([]byte("x"), 100)},
		{[]byte("key3"), bytes.Repeat([]byte("y"), 500)},
		{[]byte("key4"), bytes.Repeat([]byte("z"), 1000)},
	}

	for _, td := range testData {
		if err := txn.Put(dbi, td.key, td.value, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %s failed: %v", td.key, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Close the environment
	env.Close()

	// Reopen and verify data persisted
	env, err = gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for _, td := range testData {
		got, err := txn.Get(dbi, td.key)
		if err != nil {
			txn.Abort()
			t.Fatalf("Get %s failed: %v", td.key, err)
		}
		if !bytes.Equal(got, td.value) {
			t.Errorf("Key %s: got len %d, want len %d", td.key, len(got), len(td.value))
		}
	}
	txn.Abort()

	t.Log("WriteMap commit persistence: PASSED")
}

// TestCommitPersistenceWithoutWriteMap tests that data persists after commit without WriteMap mode.
func TestCommitPersistenceWithoutWriteMap(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-nowritemap-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Create and populate database WITHOUT WriteMap
	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	// No WriteMap flag
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		t.Fatal(err)
	}

	// Insert data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert various sizes of data
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("small value")},
		{[]byte("key2"), bytes.Repeat([]byte("x"), 100)},
		{[]byte("key3"), bytes.Repeat([]byte("y"), 500)},
		{[]byte("key4"), bytes.Repeat([]byte("z"), 1000)},
	}

	for _, td := range testData {
		if err := txn.Put(dbi, td.key, td.value, 0); err != nil {
			txn.Abort()
			t.Fatalf("Put %s failed: %v", td.key, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Close the environment
	env.Close()

	// Reopen and verify data persisted
	env, err = gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(dbPath, gdbx.NoSubdir, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	for _, td := range testData {
		got, err := txn.Get(dbi, td.key)
		if err != nil {
			txn.Abort()
			t.Fatalf("Get %s failed: %v", td.key, err)
		}
		if !bytes.Equal(got, td.value) {
			t.Errorf("Key %s: got len %d, want len %d", td.key, len(got), len(td.value))
		}
	}
	txn.Abort()

	t.Log("Non-WriteMap commit persistence: PASSED")
}

// TestCommitPersistenceLargeValues tests persistence of large values (near overflow threshold)
func TestCommitPersistenceLargeValues(t *testing.T) {
	for _, mode := range []struct {
		name  string
		flags uint
	}{
		{"WriteMap", gdbx.NoSubdir | gdbx.WriteMap},
		{"NoWriteMap", gdbx.NoSubdir},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "gdbx-large-persist-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			dbPath := filepath.Join(dir, "test.db")

			env, err := gdbx.NewEnv("")
			if err != nil {
				t.Fatal(err)
			}
			if err := env.SetMaxDBs(10); err != nil {
				t.Fatal(err)
			}
			if err := env.Open(dbPath, mode.flags, 0644); err != nil {
				t.Fatal(err)
			}

			maxVal := env.MaxValSize()
			t.Logf("MaxValSize: %d", maxVal)

			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				t.Fatal(err)
			}

			dbi, err := txn.OpenDBISimple("test", gdbx.Create)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}

			// Insert values at various sizes relative to maxVal
			testData := []struct {
				key   []byte
				value []byte
			}{
				{[]byte("small"), []byte("tiny")},
				{[]byte("medium"), bytes.Repeat([]byte("m"), 500)},
				{[]byte("large"), bytes.Repeat([]byte("l"), 1500)},
				{[]byte("max"), bytes.Repeat([]byte("M"), maxVal)},
			}

			for _, td := range testData {
				if err := txn.Put(dbi, td.key, td.value, 0); err != nil {
					txn.Abort()
					t.Fatalf("Put %s (len %d) failed: %v", td.key, len(td.value), err)
				}
			}

			if _, err := txn.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			env.Close()

			// Reopen and verify
			env, err = gdbx.NewEnv("")
			if err != nil {
				t.Fatal(err)
			}
			if err := env.SetMaxDBs(10); err != nil {
				t.Fatal(err)
			}
			if err := env.Open(dbPath, mode.flags, 0644); err != nil {
				t.Fatal(err)
			}
			defer env.Close()

			txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
			if err != nil {
				t.Fatal(err)
			}

			dbi, err = txn.OpenDBISimple("test", 0)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}

			for _, td := range testData {
				got, err := txn.Get(dbi, td.key)
				if err != nil {
					txn.Abort()
					t.Fatalf("Get %s failed: %v", td.key, err)
				}
				if !bytes.Equal(got, td.value) {
					t.Errorf("Key %s: got len %d, want len %d", td.key, len(got), len(td.value))
				}
			}
			txn.Abort()
		})
	}
}

// TestMultipleCommitsPersistence tests that multiple commits are all persisted
func TestMultipleCommitsPersistence(t *testing.T) {
	for _, mode := range []struct {
		name  string
		flags uint
	}{
		{"WriteMap", gdbx.NoSubdir | gdbx.WriteMap},
		{"NoWriteMap", gdbx.NoSubdir},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "gdbx-multi-commit-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			dbPath := filepath.Join(dir, "test.db")

			env, err := gdbx.NewEnv("")
			if err != nil {
				t.Fatal(err)
			}
			if err := env.SetMaxDBs(10); err != nil {
				t.Fatal(err)
			}
			if err := env.Open(dbPath, mode.flags, 0644); err != nil {
				t.Fatal(err)
			}

			var dbi gdbx.DBI

			// Create DBI in first transaction
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
				t.Fatalf("Commit 0 failed: %v", err)
			}

			// Multiple commits with different data
			for i := 0; i < 5; i++ {
				txn, err := env.BeginTxn(nil, 0)
				if err != nil {
					t.Fatal(err)
				}

				key := []byte{byte('A' + i)}
				value := bytes.Repeat([]byte{byte('a' + i)}, 100*(i+1))

				if err := txn.Put(dbi, key, value, 0); err != nil {
					txn.Abort()
					t.Fatalf("Put in commit %d failed: %v", i+1, err)
				}

				if _, err := txn.Commit(); err != nil {
					t.Fatalf("Commit %d failed: %v", i+1, err)
				}
			}

			env.Close()

			// Reopen and verify all data
			env, err = gdbx.NewEnv("")
			if err != nil {
				t.Fatal(err)
			}
			if err := env.SetMaxDBs(10); err != nil {
				t.Fatal(err)
			}
			if err := env.Open(dbPath, mode.flags, 0644); err != nil {
				t.Fatal(err)
			}
			defer env.Close()

			txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
			if err != nil {
				t.Fatal(err)
			}

			dbi, err = txn.OpenDBISimple("test", 0)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}

			for i := 0; i < 5; i++ {
				key := []byte{byte('A' + i)}
				expectedValue := bytes.Repeat([]byte{byte('a' + i)}, 100*(i+1))

				got, err := txn.Get(dbi, key)
				if err != nil {
					txn.Abort()
					t.Fatalf("Get key %s failed: %v", key, err)
				}
				if !bytes.Equal(got, expectedValue) {
					t.Errorf("Key %s: got len %d, want len %d", key, len(got), len(expectedValue))
				}
			}
			txn.Abort()
		})
	}
}

// TestCommitWithPageSplits tests that commits involving page splits persist correctly
func TestCommitWithPageSplits(t *testing.T) {
	for _, mode := range []struct {
		name  string
		flags uint
	}{
		{"WriteMap", gdbx.NoSubdir | gdbx.WriteMap},
		{"NoWriteMap", gdbx.NoSubdir},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "gdbx-split-persist-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			dbPath := filepath.Join(dir, "test.db")

			env, err := gdbx.NewEnv("")
			if err != nil {
				t.Fatal(err)
			}
			if err := env.SetMaxDBs(10); err != nil {
				t.Fatal(err)
			}
			if err := env.Open(dbPath, mode.flags, 0644); err != nil {
				t.Fatal(err)
			}

			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				t.Fatal(err)
			}

			dbi, err := txn.OpenDBISimple("test", gdbx.Create)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}

			// Insert enough entries to cause multiple page splits
			numEntries := 200
			entries := make(map[string][]byte)

			for i := 0; i < numEntries; i++ {
				key := []byte{byte(i >> 8), byte(i & 0xFF), 'k', 'e', 'y'}
				value := bytes.Repeat([]byte{byte(i)}, 50)
				entries[string(key)] = value

				if err := txn.Put(dbi, key, value, 0); err != nil {
					txn.Abort()
					t.Fatalf("Put %d failed: %v", i, err)
				}
			}

			if _, err := txn.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			env.Close()

			// Reopen and verify all entries
			env, err = gdbx.NewEnv("")
			if err != nil {
				t.Fatal(err)
			}
			if err := env.SetMaxDBs(10); err != nil {
				t.Fatal(err)
			}
			if err := env.Open(dbPath, mode.flags, 0644); err != nil {
				t.Fatal(err)
			}
			defer env.Close()

			txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
			if err != nil {
				t.Fatal(err)
			}

			dbi, err = txn.OpenDBISimple("test", 0)
			if err != nil {
				txn.Abort()
				t.Fatal(err)
			}

			for keyStr, expectedValue := range entries {
				key := []byte(keyStr)
				got, err := txn.Get(dbi, key)
				if err != nil {
					txn.Abort()
					t.Fatalf("Get key %x failed: %v", key, err)
				}
				if !bytes.Equal(got, expectedValue) {
					t.Errorf("Key %x: got len %d, want len %d", key, len(got), len(expectedValue))
				}
			}
			txn.Abort()

			t.Logf("Verified %d entries after page splits", numEntries)
		})
	}
}
