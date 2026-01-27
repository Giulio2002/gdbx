package tests

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// TestBigValueCompatibility tests that gdbx and mdbx produce identical results
// when writing and reading big values (values that use overflow pages).
func TestBigValueCompatibility(t *testing.T) {
	// Test various value sizes that trigger overflow pages
	// With 4KB page size, values > ~4000 bytes use overflow pages
	valueSizes := []int{
		5000,   // Just over one page
		8192,   // 8KB - two overflow pages
		16384,  // 16KB - four overflow pages
		50000,  // ~50KB - many overflow pages
		100000, // 100KB - stress test
	}

	for _, size := range valueSizes {
		t.Run(formatTestSize(size), func(t *testing.T) {
			testBigValueCompatibility(t, size)
		})
	}
}

func formatTestSize(n int) string {
	switch {
	case n >= 1000000:
		return fmt.Sprintf("%dMB", n/1000000)
	case n >= 1000:
		return fmt.Sprintf("%dKB", n/1000)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func testBigValueCompatibility(t *testing.T, valueSize int) {
	tmpDir := t.TempDir()
	gdbxPath := filepath.Join(tmpDir, "gdbx.db")
	mdbxPath := filepath.Join(tmpDir, "mdbx.db")

	numKeys := 100

	// Generate random test data
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(keys[i], uint64(i))

		values[i] = make([]byte, valueSize)
		rand.Read(values[i])
	}

	// Calculate required map size
	mapSize := int64(numKeys) * int64(valueSize) * 3
	if mapSize < 256*1024*1024 {
		mapSize = 256 * 1024 * 1024
	}

	// ============ Test 1: gdbx write, gdbx read ============
	t.Run("GdbxWriteGdbxRead", func(t *testing.T) {
		// Write with gdbx
		genv, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		defer genv.Close()
		genv.SetMaxDBs(10)
		genv.SetGeometry(-1, -1, mapSize, -1, -1, 4096)
		if err := genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
			t.Fatal(err)
		}

		// Write all values
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
				t.Fatalf("gdbx put failed for key %d: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}

		// Read back and verify
		rtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer rtxn.Abort()
		rdbi, err := rtxn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			got, err := rtxn.Get(rdbi, keys[i])
			if err != nil {
				t.Fatalf("gdbx get failed for key %d: %v", i, err)
			}
			if !bytes.Equal(got, values[i]) {
				t.Fatalf("gdbx read mismatch for key %d: got %d bytes, want %d bytes", i, len(got), len(values[i]))
			}
		}
	})

	// Clean up for next test
	os.Remove(gdbxPath)

	// ============ Test 2: mdbx write, mdbx read ============
	t.Run("MdbxWriteMdbxRead", func(t *testing.T) {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		menv, err := mdbxgo.NewEnv(mdbxgo.Label("bigval-test"))
		if err != nil {
			t.Fatal(err)
		}
		defer menv.Close()
		menv.SetOption(mdbxgo.OptMaxDB, 10)
		menv.SetGeometry(-1, -1, int(mapSize), -1, -1, 4096)
		if err := menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync|mdbxgo.WriteMap, 0644); err != nil {
			t.Fatal(err)
		}

		// Write all values
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBI("test", mdbxgo.Create, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
				t.Fatalf("mdbx put failed for key %d: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}

		// Read back and verify
		rtxn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			t.Fatal(err)
		}
		defer rtxn.Abort()
		rdbi, err := rtxn.OpenDBI("test", 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			got, err := rtxn.Get(rdbi, keys[i])
			if err != nil {
				t.Fatalf("mdbx get failed for key %d: %v", i, err)
			}
			if !bytes.Equal(got, values[i]) {
				t.Fatalf("mdbx read mismatch for key %d: got %d bytes, want %d bytes", i, len(got), len(values[i]))
			}
		}
	})

	// Clean up for next test
	os.Remove(mdbxPath)

	// ============ Test 3: gdbx write, mdbx read (cross-compatibility) ============
	t.Run("GdbxWriteMdbxRead", func(t *testing.T) {
		sharedPath := filepath.Join(tmpDir, "shared.db")

		// Write with gdbx
		genv, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		genv.SetMaxDBs(10)
		genv.SetGeometry(-1, -1, mapSize, -1, -1, 4096)
		if err := genv.Open(sharedPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
			t.Fatal(err)
		}

		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			genv.Close()
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			genv.Close()
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
				genv.Close()
				t.Fatalf("gdbx put failed for key %d: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			genv.Close()
			t.Fatal(err)
		}
		genv.Close()

		// Read with mdbx
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		menv, err := mdbxgo.NewEnv(mdbxgo.Label("bigval-compat"))
		if err != nil {
			t.Fatal(err)
		}
		defer menv.Close()
		menv.SetOption(mdbxgo.OptMaxDB, 10)
		menv.SetGeometry(-1, -1, int(mapSize), -1, -1, 4096)
		if err := menv.Open(sharedPath, mdbxgo.NoSubdir|mdbxgo.Readonly, 0644); err != nil {
			t.Fatal(err)
		}

		rtxn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			t.Fatal(err)
		}
		defer rtxn.Abort()
		rdbi, err := rtxn.OpenDBI("test", 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			got, err := rtxn.Get(rdbi, keys[i])
			if err != nil {
				t.Fatalf("mdbx read of gdbx data failed for key %d: %v", i, err)
			}
			if !bytes.Equal(got, values[i]) {
				t.Fatalf("cross-compat mismatch for key %d: got %d bytes, want %d bytes", i, len(got), len(values[i]))
			}
		}

		os.Remove(sharedPath)
	})

	// ============ Test 4: mdbx write, gdbx read (cross-compatibility) ============
	t.Run("MdbxWriteGdbxRead", func(t *testing.T) {
		sharedPath := filepath.Join(tmpDir, "shared2.db")

		// Write with mdbx
		runtime.LockOSThread()

		menv, err := mdbxgo.NewEnv(mdbxgo.Label("bigval-compat2"))
		if err != nil {
			t.Fatal(err)
		}
		menv.SetOption(mdbxgo.OptMaxDB, 10)
		menv.SetGeometry(-1, -1, int(mapSize), -1, -1, 4096)
		if err := menv.Open(sharedPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync|mdbxgo.WriteMap, 0644); err != nil {
			t.Fatal(err)
		}

		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			menv.Close()
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBI("test", mdbxgo.Create, nil, nil)
		if err != nil {
			menv.Close()
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
				menv.Close()
				t.Fatalf("mdbx put failed for key %d: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			menv.Close()
			t.Fatal(err)
		}
		menv.Close()
		runtime.UnlockOSThread()

		// Read with gdbx
		genv, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		defer genv.Close()
		genv.SetMaxDBs(10)
		genv.SetGeometry(-1, -1, mapSize, -1, -1, 4096)
		if err := genv.Open(sharedPath, gdbx.NoSubdir|gdbx.TxnReadOnly, 0644); err != nil {
			t.Fatal(err)
		}

		rtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		defer rtxn.Abort()
		rdbi, err := rtxn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			got, err := rtxn.Get(rdbi, keys[i])
			if err != nil {
				t.Fatalf("gdbx read of mdbx data failed for key %d: %v", i, err)
			}
			if !bytes.Equal(got, values[i]) {
				t.Fatalf("cross-compat mismatch for key %d: got %d bytes, want %d bytes", i, len(got), len(values[i]))
			}
		}

		os.Remove(sharedPath)
	})

	// ============ Test 5: Update same values multiple times and verify ============
	t.Run("RepeatedUpdates", func(t *testing.T) {
		sharedPath := filepath.Join(tmpDir, "updates.db")

		// Open gdbx
		genv, err := gdbx.NewEnv(gdbx.Default)
		if err != nil {
			t.Fatal(err)
		}
		defer genv.Close()
		genv.SetMaxDBs(10)
		genv.SetGeometry(-1, -1, mapSize, -1, -1, 4096)
		if err := genv.Open(sharedPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
			t.Fatal(err)
		}

		// Initial write
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("test", gdbx.Create)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
				t.Fatalf("initial put failed for key %d: %v", i, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}

		// Update values multiple times (simulates benchmark scenario)
		for round := 0; round < 5; round++ {
			// Generate new values with same size
			newValues := make([][]byte, numKeys)
			for i := 0; i < numKeys; i++ {
				newValues[i] = make([]byte, valueSize)
				rand.Read(newValues[i])
			}

			// Update all values
			txn, err := genv.BeginTxn(nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			dbi, err := txn.OpenDBISimple("test", 0)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < numKeys; i++ {
				if err := txn.Put(dbi, keys[i], newValues[i], 0); err != nil {
					t.Fatalf("round %d: update failed for key %d: %v", round, i, err)
				}
			}
			if _, err := txn.Commit(); err != nil {
				t.Fatal(err)
			}

			// Verify reads match
			rtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
			if err != nil {
				t.Fatal(err)
			}
			rdbi, err := rtxn.OpenDBISimple("test", 0)
			if err != nil {
				rtxn.Abort()
				t.Fatal(err)
			}
			for i := 0; i < numKeys; i++ {
				got, err := rtxn.Get(rdbi, keys[i])
				if err != nil {
					rtxn.Abort()
					t.Fatalf("round %d: read failed for key %d: %v", round, i, err)
				}
				if !bytes.Equal(got, newValues[i]) {
					rtxn.Abort()
					t.Fatalf("round %d: mismatch for key %d", round, i)
				}
			}
			rtxn.Abort()

			// Update values array for next verification
			values = newValues
		}

		// Final verification with mdbx
		genv.Close()

		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		menv, err := mdbxgo.NewEnv(mdbxgo.Label("bigval-updates"))
		if err != nil {
			t.Fatal(err)
		}
		defer menv.Close()
		menv.SetOption(mdbxgo.OptMaxDB, 10)
		menv.SetGeometry(-1, -1, int(mapSize), -1, -1, 4096)
		if err := menv.Open(sharedPath, mdbxgo.NoSubdir|mdbxgo.Readonly, 0644); err != nil {
			t.Fatal(err)
		}

		rtxn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			t.Fatal(err)
		}
		defer rtxn.Abort()
		rdbi, err := rtxn.OpenDBI("test", 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			got, err := rtxn.Get(rdbi, keys[i])
			if err != nil {
				t.Fatalf("final mdbx verification failed for key %d: %v", i, err)
			}
			if !bytes.Equal(got, values[i]) {
				t.Fatalf("final mdbx verification mismatch for key %d", i)
			}
		}
	})
}

// TestBigValueUpdateSameSize specifically tests the in-place update optimization
// where values are updated with new data of the same size.
func TestBigValueUpdateSameSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	valueSize := 8192 // 8KB
	numKeys := 50
	numUpdates := 10

	// Generate initial data
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(keys[i], uint64(i))
	}

	// Open gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	env.SetGeometry(-1, -1, 512*1024*1024, -1, -1, 4096)
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}

	// Track current values
	currentValues := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		currentValues[i] = make([]byte, valueSize)
		rand.Read(currentValues[i])
	}

	// Initial write
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numKeys; i++ {
		if err := txn.Put(dbi, keys[i], currentValues[i], 0); err != nil {
			t.Fatalf("initial put failed: %v", err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Perform multiple rounds of same-size updates
	for round := 0; round < numUpdates; round++ {
		// Generate new random values (same size)
		for i := 0; i < numKeys; i++ {
			rand.Read(currentValues[i])
		}

		// Update
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			if err := txn.Put(dbi, keys[i], currentValues[i], 0); err != nil {
				t.Fatalf("round %d: update failed: %v", round, err)
			}
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}

		// Verify immediately after commit
		rtxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			t.Fatal(err)
		}
		rdbi, err := rtxn.OpenDBISimple("test", 0)
		if err != nil {
			rtxn.Abort()
			t.Fatal(err)
		}
		for i := 0; i < numKeys; i++ {
			got, err := rtxn.Get(rdbi, keys[i])
			if err != nil {
				rtxn.Abort()
				t.Fatalf("round %d key %d: get failed: %v", round, i, err)
			}
			if !bytes.Equal(got, currentValues[i]) {
				rtxn.Abort()
				t.Fatalf("round %d key %d: value mismatch - got %d bytes, want %d bytes",
					round, i, len(got), len(currentValues[i]))
			}
		}
		rtxn.Abort()
	}

	// Close and reopen to verify persistence
	env.Close()

	env2, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env2.Close()
	env2.SetMaxDBs(10)
	env2.SetGeometry(-1, -1, 512*1024*1024, -1, -1, 4096)
	if err := env2.Open(dbPath, gdbx.NoSubdir|gdbx.TxnReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	rtxn, err := env2.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer rtxn.Abort()
	rdbi, err := rtxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numKeys; i++ {
		got, err := rtxn.Get(rdbi, keys[i])
		if err != nil {
			t.Fatalf("after reopen key %d: get failed: %v", i, err)
		}
		if !bytes.Equal(got, currentValues[i]) {
			t.Fatalf("after reopen key %d: value mismatch", i)
		}
	}
}
