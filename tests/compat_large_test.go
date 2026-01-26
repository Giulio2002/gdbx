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

// Test configuration - adjust these to control test size
var (
	// Table1Entries is the number of key-value pairs in table "1" (regular table)
	Table1Entries = 100

	// Table2Keys is the number of keys in table "2" (DUPSORT table)
	Table2Keys = 10

	// Table2ValuesPerKey is the number of duplicate values per key in table "2"
	Table2ValuesPerKey = 100
)

// TestLargeCompatibility is a comprehensive compatibility test between gdbx and libmdbx.
// It creates databases with two tables:
// - Table "1": Regular table with 10k key-value pairs
// - Table "2": DUPSORT table with 1k keys, each having 10k duplicate values
//
// Tests both directions:
// - gdbx writes → libmdbx reads
// - libmdbx writes → gdbx reads
func TestLargeCompatibility(t *testing.T) {
	t.Run("GdbxWriteLibmdbxRead", testGdbxWriteLibmdbxRead)
	t.Run("LibmdbxWriteGdbxRead", testLibmdbxWriteGdbxRead)
}

// testGdbxWriteLibmdbxRead tests that libmdbx can read databases created by gdbx
func testGdbxWriteLibmdbxRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compat-gdbx-write-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// === Phase 1: Write with gdbx ===
	t.Log("Phase 1: Writing with gdbx...")
	writeWithGdbx(t, dbPath)

	// === Phase 2: Read and verify with libmdbx ===
	t.Log("Phase 2: Reading with libmdbx...")
	verifyWithLibmdbx(t, dbPath)

	t.Log("gdbx→libmdbx compatibility test PASSED")
}

// testLibmdbxWriteGdbxRead tests that gdbx can read databases created by libmdbx
func testLibmdbxWriteGdbxRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compat-libmdbx-write-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// === Phase 1: Write with libmdbx ===
	t.Log("Phase 1: Writing with libmdbx...")
	writeWithLibmdbx(t, dbPath)

	// === Phase 2: Read and verify with gdbx ===
	t.Log("Phase 2: Reading with gdbx...")
	verifyWithGdbx(t, dbPath)

	t.Log("libmdbx→gdbx compatibility test PASSED")
}

// ============================================================================
// gdbx write functions
// ============================================================================

func writeWithGdbx(t *testing.T, dbPath string) {
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatalf("gdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096) // 1GB max

	err = env.Open(dbPath, gdbx.NoSubdir|gdbx.NoReadAhead, 0644)
	if err != nil {
		t.Fatalf("gdbx Open failed: %v", err)
	}

	// Create tables and write data
	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		t.Fatalf("gdbx BeginTxn failed: %v", err)
	}

	// Table "1" - regular table
	dbi1, err := txn.OpenDBISimple("1", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("gdbx OpenDBI '1' failed: %v", err)
	}

	// Table "2" - DUPSORT table
	dbi2, err := txn.OpenDBISimple("2", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("gdbx OpenDBI '2' failed: %v", err)
	}

	// Write entries to table "1"
	t.Logf("  Writing %d entries to table '1'...", Table1Entries)
	for i := 0; i < Table1Entries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("value-%05d", i))
		if err := txn.Put(dbi1, key, val, 0); err != nil {
			txn.Abort()
			t.Fatalf("gdbx Put to '1' failed at %d: %v", i, err)
		}
	}

	// Write keys × values to table "2" (DUPSORT)
	t.Logf("  Writing %d keys × %d values to table '2' (DUPSORT)...", Table2Keys, Table2ValuesPerKey)
	for k := 0; k < Table2Keys; k++ {
		key := []byte(fmt.Sprintf("dupkey-%04d", k))
		for v := 0; v < Table2ValuesPerKey; v++ {
			val := []byte(fmt.Sprintf("subval-%05d", v))
			if err := txn.Put(dbi2, key, val, 0); err != nil {
				txn.Abort()
				t.Fatalf("gdbx Put to '2' failed at key=%d val=%d: %v", k, v, err)
			}
		}
		if Table2Keys >= 100 && (k+1)%(Table2Keys/10) == 0 {
			t.Logf("    Wrote %d keys...", k+1)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("gdbx Commit failed: %v", err)
	}

	t.Log("  gdbx write complete")
}

// ============================================================================
// libmdbx write functions
// ============================================================================

func writeWithLibmdbx(t *testing.T, dbPath string) {
	// mdbx-go requires transactions to be used from the same OS thread
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	err = env.Open(dbPath, mdbx.NoSubdir, 0644)
	if err != nil {
		t.Fatalf("mdbx Open failed: %v", err)
	}

	// Create tables and write data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}

	// Table "1" - regular table
	dbi1, err := txn.OpenDBISimple("1", mdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("mdbx OpenDBI '1' failed: %v", err)
	}

	// Table "2" - DUPSORT table
	dbi2, err := txn.OpenDBISimple("2", mdbx.Create|mdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("mdbx OpenDBI '2' failed: %v", err)
	}

	// Write entries to table "1"
	t.Logf("  Writing %d entries to table '1'...", Table1Entries)
	for i := 0; i < Table1Entries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("value-%05d", i))
		if err := txn.Put(dbi1, key, val, 0); err != nil {
			txn.Abort()
			t.Fatalf("mdbx Put to '1' failed at %d: %v", i, err)
		}
	}

	// Write keys × values to table "2" (DUPSORT)
	t.Logf("  Writing %d keys × %d values to table '2' (DUPSORT)...", Table2Keys, Table2ValuesPerKey)
	for k := 0; k < Table2Keys; k++ {
		key := []byte(fmt.Sprintf("dupkey-%04d", k))
		for v := 0; v < Table2ValuesPerKey; v++ {
			val := []byte(fmt.Sprintf("subval-%05d", v))
			if err := txn.Put(dbi2, key, val, 0); err != nil {
				txn.Abort()
				t.Fatalf("mdbx Put to '2' failed at key=%d val=%d: %v", k, v, err)
			}
		}
		if Table2Keys >= 100 && (k+1)%(Table2Keys/10) == 0 {
			t.Logf("    Wrote %d keys...", k+1)
		}
	}

	_, err = txn.Commit()
	if err != nil {
		t.Fatalf("mdbx Commit failed: %v", err)
	}

	t.Log("  libmdbx write complete")
}

// ============================================================================
// Verification with libmdbx (reading gdbx-created database)
// ============================================================================

func verifyWithLibmdbx(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetOption(mdbx.OptMaxDB, 10)

	err = env.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644)
	if err != nil {
		t.Fatalf("mdbx Open (readonly) failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	// Open tables
	dbi1, err := txn.OpenDBISimple("1", 0)
	if err != nil {
		t.Fatalf("mdbx OpenDBI '1' failed: %v", err)
	}

	dbi2, err := txn.OpenDBISimple("2", mdbx.DupSort)
	if err != nil {
		t.Fatalf("mdbx OpenDBI '2' failed: %v", err)
	}

	// Verify table "1" with direct Get
	t.Log("  Verifying table '1' with Get...")
	verifyTable1GetMdbx(t, txn, dbi1)

	// Verify table "1" with cursor iteration
	t.Log("  Verifying table '1' with cursor iteration...")
	verifyTable1CursorMdbx(t, txn, dbi1)

	// Verify table "2" with direct Get
	t.Log("  Verifying table '2' with GetBoth...")
	verifyTable2GetMdbx(t, txn, dbi2)

	// Verify table "2" with cursor iteration (Next)
	t.Log("  Verifying table '2' with cursor Next...")
	verifyTable2CursorNextMdbx(t, txn, dbi2)

	// Verify table "2" with cursor NextDup
	t.Log("  Verifying table '2' with cursor NextDup...")
	verifyTable2CursorNextDupMdbx(t, txn, dbi2)

	// Verify table "2" with cursor Count
	t.Log("  Verifying table '2' with cursor Count...")
	verifyTable2CursorCountMdbx(t, txn, dbi2)

	t.Log("  libmdbx verification complete")
}

func verifyTable1GetMdbx(t *testing.T, txn *mdbx.Txn, dbi mdbx.DBI) {
	for i := 0; i < Table1Entries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expectedVal := []byte(fmt.Sprintf("value-%05d", i))

		val, err := txn.Get(dbi, key)
		if err != nil {
			t.Fatalf("mdbx Get failed at %d: %v", i, err)
		}
		if !bytes.Equal(val, expectedVal) {
			t.Fatalf("mdbx Get mismatch at %d: got %q, want %q", i, val, expectedVal)
		}
	}
}

func verifyTable1CursorMdbx(t *testing.T, txn *mdbx.Txn, dbi mdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("mdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	count := 0
	k, v, err := cur.Get(nil, nil, mdbx.First)
	for err == nil {
		expectedKey := []byte(fmt.Sprintf("key-%05d", count))
		expectedVal := []byte(fmt.Sprintf("value-%05d", count))

		if !bytes.Equal(k, expectedKey) {
			t.Fatalf("mdbx cursor key mismatch at %d: got %q, want %q", count, k, expectedKey)
		}
		if !bytes.Equal(v, expectedVal) {
			t.Fatalf("mdbx cursor value mismatch at %d: got %q, want %q", count, v, expectedVal)
		}

		count++
		k, v, err = cur.Get(nil, nil, mdbx.Next)
	}

	if count != Table1Entries {
		t.Fatalf("mdbx cursor iteration count: got %d, want %d", count, Table1Entries)
	}
}

func verifyTable2GetMdbx(t *testing.T, txn *mdbx.Txn, dbi mdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("mdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// Spot check some key-value pairs (using configured sizes)
	lastKey := Table2Keys - 1
	lastVal := Table2ValuesPerKey - 1
	midKey := Table2Keys / 2
	midVal := Table2ValuesPerKey / 2
	testCases := []struct{ keyIdx, valIdx int }{
		{0, 0}, {0, lastVal}, {midKey, midVal}, {lastKey, 0}, {lastKey, lastVal},
	}

	for _, tc := range testCases {
		key := []byte(fmt.Sprintf("dupkey-%04d", tc.keyIdx))
		val := []byte(fmt.Sprintf("subval-%05d", tc.valIdx))

		_, v, err := cur.Get(key, val, mdbx.GetBoth)
		if err != nil {
			t.Fatalf("mdbx GetBoth failed for key=%d val=%d: %v", tc.keyIdx, tc.valIdx, err)
		}
		if !bytes.Equal(v, val) {
			t.Fatalf("mdbx GetBoth mismatch: got %q, want %q", v, val)
		}
	}
}

func verifyTable2CursorNextMdbx(t *testing.T, txn *mdbx.Txn, dbi mdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("mdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// Iterate through all entries and verify count
	count := 0
	k, v, err := cur.Get(nil, nil, mdbx.First)
	for err == nil {
		keyIdx := count / Table2ValuesPerKey
		valIdx := count % Table2ValuesPerKey

		expectedKey := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))
		expectedVal := []byte(fmt.Sprintf("subval-%05d", valIdx))

		if !bytes.Equal(k, expectedKey) {
			t.Fatalf("mdbx Next key mismatch at %d: got %q, want %q", count, k, expectedKey)
		}
		if !bytes.Equal(v, expectedVal) {
			t.Fatalf("mdbx Next value mismatch at %d: got %q, want %q", count, v, expectedVal)
		}

		count++
		k, v, err = cur.Get(nil, nil, mdbx.Next)

		// Progress logging for large datasets
		logInterval := Table2Keys * Table2ValuesPerKey / 10
		if logInterval > 0 && count%logInterval == 0 {
			t.Logf("    Verified %d entries...", count)
		}
	}

	expectedTotal := Table2Keys * Table2ValuesPerKey
	if count != expectedTotal {
		t.Fatalf("mdbx Next iteration count: got %d, want %d", count, expectedTotal)
	}
}

func verifyTable2CursorNextDupMdbx(t *testing.T, txn *mdbx.Txn, dbi mdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("mdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// For each key, verify all duplicates with NextDup
	for keyIdx := 0; keyIdx < Table2Keys; keyIdx++ {
		key := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))

		// Position at first value
		k, v, err := cur.Get(key, nil, mdbx.Set)
		if err != nil {
			t.Fatalf("mdbx Set failed for key %d: %v", keyIdx, err)
		}

		// Verify first value
		expectedVal := []byte(fmt.Sprintf("subval-%05d", 0))
		if !bytes.Equal(v, expectedVal) {
			t.Fatalf("mdbx first dup mismatch at key %d: got %q, want %q", keyIdx, v, expectedVal)
		}

		// Iterate through duplicates
		dupCount := 1
		for {
			k, v, err = cur.Get(nil, nil, mdbx.NextDup)
			if err != nil {
				break
			}

			expectedKey := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))
			expectedVal := []byte(fmt.Sprintf("subval-%05d", dupCount))

			if !bytes.Equal(k, expectedKey) {
				t.Fatalf("mdbx NextDup key changed: got %q, want %q", k, expectedKey)
			}
			if !bytes.Equal(v, expectedVal) {
				t.Fatalf("mdbx NextDup value mismatch at key=%d dup=%d: got %q, want %q",
					keyIdx, dupCount, v, expectedVal)
			}

			dupCount++
		}

		if dupCount != Table2ValuesPerKey {
			t.Fatalf("mdbx NextDup count for key %d: got %d, want %d", keyIdx, dupCount, Table2ValuesPerKey)
		}

		// Progress logging for large datasets
		logInterval := Table2Keys / 10
		if logInterval > 0 && (keyIdx+1)%logInterval == 0 {
			t.Logf("    Verified %d keys with NextDup...", keyIdx+1)
		}
	}
}

func verifyTable2CursorCountMdbx(t *testing.T, txn *mdbx.Txn, dbi mdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("mdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// Verify Count() for some keys (using configured sizes)
	testKeys := []int{0}
	if Table2Keys > 1 {
		testKeys = append(testKeys, Table2Keys/10, Table2Keys/2, Table2Keys-1)
	}
	for _, keyIdx := range testKeys {
		if keyIdx >= Table2Keys {
			continue
		}
		key := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))

		_, _, err := cur.Get(key, nil, mdbx.Set)
		if err != nil {
			t.Fatalf("mdbx Set failed for key %d: %v", keyIdx, err)
		}

		count, err := cur.Count()
		if err != nil {
			t.Fatalf("mdbx Count failed for key %d: %v", keyIdx, err)
		}

		if count != uint64(Table2ValuesPerKey) {
			t.Fatalf("mdbx Count for key %d: got %d, want %d", keyIdx, count, Table2ValuesPerKey)
		}
	}
}

// ============================================================================
// Verification with gdbx (reading libmdbx-created database)
// ============================================================================

func verifyWithGdbx(t *testing.T, dbPath string) {
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatalf("gdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetMaxDBs(10)

	err = env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	if err != nil {
		t.Fatalf("gdbx Open (readonly) failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatalf("gdbx BeginTxn failed: %v", err)
	}
	defer txn.Abort()

	// Open tables
	dbi1, err := txn.OpenDBISimple("1", 0)
	if err != nil {
		t.Fatalf("gdbx OpenDBI '1' failed: %v", err)
	}

	dbi2, err := txn.OpenDBISimple("2", gdbx.DupSort)
	if err != nil {
		t.Fatalf("gdbx OpenDBI '2' failed: %v", err)
	}

	// Verify table "1" with direct Get
	t.Log("  Verifying table '1' with Get...")
	verifyTable1GetGdbx(t, txn, dbi1)

	// Verify table "1" with cursor iteration
	t.Log("  Verifying table '1' with cursor iteration...")
	verifyTable1CursorGdbx(t, txn, dbi1)

	// Verify table "2" with direct Get
	t.Log("  Verifying table '2' with GetBoth...")
	verifyTable2GetGdbx(t, txn, dbi2)

	// Verify table "2" with cursor iteration (Next)
	t.Log("  Verifying table '2' with cursor Next...")
	verifyTable2CursorNextGdbx(t, txn, dbi2)

	// Verify table "2" with cursor NextDup
	t.Log("  Verifying table '2' with cursor NextDup...")
	verifyTable2CursorNextDupGdbx(t, txn, dbi2)

	// Verify table "2" with cursor Count
	t.Log("  Verifying table '2' with cursor Count...")
	verifyTable2CursorCountGdbx(t, txn, dbi2)

	t.Log("  gdbx verification complete")
}

func verifyTable1GetGdbx(t *testing.T, txn *gdbx.Txn, dbi gdbx.DBI) {
	for i := 0; i < Table1Entries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expectedVal := []byte(fmt.Sprintf("value-%05d", i))

		val, err := txn.Get(dbi, key)
		if err != nil {
			t.Fatalf("gdbx Get failed at %d: %v", i, err)
		}
		if !bytes.Equal(val, expectedVal) {
			t.Fatalf("gdbx Get mismatch at %d: got %q, want %q", i, val, expectedVal)
		}
	}
}

func verifyTable1CursorGdbx(t *testing.T, txn *gdbx.Txn, dbi gdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("gdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	count := 0
	k, v, err := cur.Get(nil, nil, gdbx.First)
	for err == nil {
		expectedKey := []byte(fmt.Sprintf("key-%05d", count))
		expectedVal := []byte(fmt.Sprintf("value-%05d", count))

		if !bytes.Equal(k, expectedKey) {
			t.Fatalf("gdbx cursor key mismatch at %d: got %q, want %q", count, k, expectedKey)
		}
		if !bytes.Equal(v, expectedVal) {
			t.Fatalf("gdbx cursor value mismatch at %d: got %q, want %q", count, v, expectedVal)
		}

		count++
		k, v, err = cur.Get(nil, nil, gdbx.Next)
	}

	if count != Table1Entries {
		t.Fatalf("gdbx cursor iteration count: got %d, want %d", count, Table1Entries)
	}
}

func verifyTable2GetGdbx(t *testing.T, txn *gdbx.Txn, dbi gdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("gdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// Spot check some key-value pairs (using configured sizes)
	lastKey := Table2Keys - 1
	lastVal := Table2ValuesPerKey - 1
	midKey := Table2Keys / 2
	midVal := Table2ValuesPerKey / 2
	testCases := []struct{ keyIdx, valIdx int }{
		{0, 0}, {0, lastVal}, {midKey, midVal}, {lastKey, 0}, {lastKey, lastVal},
	}

	for _, tc := range testCases {
		key := []byte(fmt.Sprintf("dupkey-%04d", tc.keyIdx))
		val := []byte(fmt.Sprintf("subval-%05d", tc.valIdx))

		_, v, err := cur.Get(key, val, gdbx.GetBoth)
		if err != nil {
			t.Fatalf("gdbx GetBoth failed for key=%d val=%d: %v", tc.keyIdx, tc.valIdx, err)
		}
		if !bytes.Equal(v, val) {
			t.Fatalf("gdbx GetBoth mismatch: got %q, want %q", v, val)
		}
	}
}

func verifyTable2CursorNextGdbx(t *testing.T, txn *gdbx.Txn, dbi gdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("gdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// Iterate through all entries and verify count
	count := 0
	k, v, err := cur.Get(nil, nil, gdbx.First)
	for err == nil {
		keyIdx := count / Table2ValuesPerKey
		valIdx := count % Table2ValuesPerKey

		expectedKey := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))
		expectedVal := []byte(fmt.Sprintf("subval-%05d", valIdx))

		if !bytes.Equal(k, expectedKey) {
			t.Fatalf("gdbx Next key mismatch at %d: got %q, want %q", count, k, expectedKey)
		}
		if !bytes.Equal(v, expectedVal) {
			t.Fatalf("gdbx Next value mismatch at %d: got %q, want %q", count, v, expectedVal)
		}

		count++
		k, v, err = cur.Get(nil, nil, gdbx.Next)

		// Progress logging for large datasets
		logInterval := Table2Keys * Table2ValuesPerKey / 10
		if logInterval > 0 && count%logInterval == 0 {
			t.Logf("    Verified %d entries...", count)
		}
	}

	expectedTotal := Table2Keys * Table2ValuesPerKey
	if count != expectedTotal {
		t.Fatalf("gdbx Next iteration count: got %d, want %d", count, expectedTotal)
	}
}

func verifyTable2CursorNextDupGdbx(t *testing.T, txn *gdbx.Txn, dbi gdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("gdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// For each key, verify all duplicates with NextDup
	for keyIdx := 0; keyIdx < Table2Keys; keyIdx++ {
		key := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))

		// Position at first value
		k, v, err := cur.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Fatalf("gdbx Set failed for key %d: %v", keyIdx, err)
		}

		// Verify first value
		expectedVal := []byte(fmt.Sprintf("subval-%05d", 0))
		if !bytes.Equal(v, expectedVal) {
			t.Fatalf("gdbx first dup mismatch at key %d: got %q, want %q", keyIdx, v, expectedVal)
		}

		// Iterate through duplicates
		dupCount := 1
		for {
			k, v, err = cur.Get(nil, nil, gdbx.NextDup)
			if err != nil {
				break
			}

			expectedKey := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))
			expectedVal := []byte(fmt.Sprintf("subval-%05d", dupCount))

			if !bytes.Equal(k, expectedKey) {
				t.Fatalf("gdbx NextDup key changed: got %q, want %q", k, expectedKey)
			}
			if !bytes.Equal(v, expectedVal) {
				t.Fatalf("gdbx NextDup value mismatch at key=%d dup=%d: got %q, want %q",
					keyIdx, dupCount, v, expectedVal)
			}

			dupCount++
		}

		if dupCount != Table2ValuesPerKey {
			t.Fatalf("gdbx NextDup count for key %d: got %d, want %d", keyIdx, dupCount, Table2ValuesPerKey)
		}

		// Progress logging for large datasets
		logInterval := Table2Keys / 10
		if logInterval > 0 && (keyIdx+1)%logInterval == 0 {
			t.Logf("    Verified %d keys with NextDup...", keyIdx+1)
		}
	}
}

func verifyTable2CursorCountGdbx(t *testing.T, txn *gdbx.Txn, dbi gdbx.DBI) {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatalf("gdbx OpenCursor failed: %v", err)
	}
	defer cur.Close()

	// Verify Count() for some keys (using configured sizes)
	testKeys := []int{0}
	if Table2Keys > 1 {
		testKeys = append(testKeys, Table2Keys/10, Table2Keys/2, Table2Keys-1)
	}
	for _, keyIdx := range testKeys {
		if keyIdx >= Table2Keys {
			continue
		}
		key := []byte(fmt.Sprintf("dupkey-%04d", keyIdx))

		_, _, err := cur.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Fatalf("gdbx Set failed for key %d: %v", keyIdx, err)
		}

		count, err := cur.Count()
		if err != nil {
			t.Fatalf("gdbx Count failed for key %d: %v", keyIdx, err)
		}

		if count != uint64(Table2ValuesPerKey) {
			t.Fatalf("gdbx Count for key %d: got %d, want %d", keyIdx, count, Table2ValuesPerKey)
		}
	}
}

// ============================================================================
// Cross-verification: Compare gdbx and libmdbx cursor outputs side by side
// ============================================================================

func TestCursorOutputMatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compat-cursor-match-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create database with libmdbx
	t.Log("Creating database with libmdbx...")
	createSmallTestDB(t, dbPath)

	// Compare cursor outputs
	t.Log("Comparing cursor outputs...")
	compareCursorOutputs(t, dbPath)

	t.Log("Cursor output match test PASSED")
}

func createSmallTestDB(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetOption(mdbx.OptMaxDB, 10)
	env.SetGeometry(-1, -1, 1<<28, -1, -1, 4096)

	err = env.Open(dbPath, mdbx.NoSubdir, 0644)
	if err != nil {
		t.Fatalf("mdbx Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}

	// Regular table
	dbi1, err := txn.OpenDBISimple("1", mdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatalf("mdbx OpenDBI '1' failed: %v", err)
	}

	// DUPSORT table
	dbi2, err := txn.OpenDBISimple("2", mdbx.Create|mdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("mdbx OpenDBI '2' failed: %v", err)
	}

	// Small data for quick testing
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("value-%03d", i))
		txn.Put(dbi1, key, val, 0)
	}

	// DUPSORT: 10 keys with 50 values each
	for k := 0; k < 10; k++ {
		key := []byte(fmt.Sprintf("dk-%02d", k))
		for v := 0; v < 50; v++ {
			val := []byte(fmt.Sprintf("sv-%03d", v))
			txn.Put(dbi2, key, val, 0)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("mdbx Commit failed: %v", err)
	}
}

func compareCursorOutputs(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Open with gdbx
	gEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatalf("gdbx NewEnv failed: %v", err)
	}
	defer gEnv.Close()
	gEnv.SetMaxDBs(10)
	if err := gEnv.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644); err != nil {
		t.Fatalf("gdbx Open failed: %v", err)
	}

	// Open with mdbx
	mEnv, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer mEnv.Close()
	mEnv.SetOption(mdbx.OptMaxDB, 10)
	if err := mEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644); err != nil {
		t.Fatalf("mdbx Open failed: %v", err)
	}

	// Begin transactions
	gTxn, err := gEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatalf("gdbx BeginTxn failed: %v", err)
	}
	defer gTxn.Abort()

	mTxn, err := mEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}
	defer mTxn.Abort()

	// Compare table "1"
	t.Log("  Comparing table '1' cursor outputs...")
	compareTable1Cursors(t, gTxn, mTxn)

	// Compare table "2" (DUPSORT)
	t.Log("  Comparing table '2' cursor outputs (Next)...")
	compareTable2CursorsNext(t, gTxn, mTxn)

	t.Log("  Comparing table '2' cursor outputs (NextDup)...")
	compareTable2CursorsNextDup(t, gTxn, mTxn)

	t.Log("  Comparing table '2' cursor outputs (NextNoDup)...")
	compareTable2CursorsNextNoDup(t, gTxn, mTxn)
}

func compareTable1Cursors(t *testing.T, gTxn *gdbx.Txn, mTxn *mdbx.Txn) {
	gDbi, _ := gTxn.OpenDBISimple("1", 0)
	mDbi, _ := mTxn.OpenDBISimple("1", 0)

	gCur, _ := gTxn.OpenCursor(gDbi)
	mCur, _ := mTxn.OpenCursor(mDbi)
	defer gCur.Close()
	defer mCur.Close()

	// Compare First
	gk, gv, gErr := gCur.Get(nil, nil, gdbx.First)
	mk, mv, mErr := mCur.Get(nil, nil, mdbx.First)

	count := 0
	for gErr == nil && mErr == nil {
		if !bytes.Equal(gk, mk) {
			t.Fatalf("Table '1' key mismatch at %d: gdbx=%q, mdbx=%q", count, gk, mk)
		}
		if !bytes.Equal(gv, mv) {
			t.Fatalf("Table '1' value mismatch at %d: gdbx=%q, mdbx=%q", count, gv, mv)
		}

		count++
		gk, gv, gErr = gCur.Get(nil, nil, gdbx.Next)
		mk, mv, mErr = mCur.Get(nil, nil, mdbx.Next)
	}

	if (gErr == nil) != (mErr == nil) {
		t.Fatalf("Table '1' iteration end mismatch: gdbx_err=%v, mdbx_err=%v", gErr, mErr)
	}

	t.Logf("    Compared %d entries", count)
}

func compareTable2CursorsNext(t *testing.T, gTxn *gdbx.Txn, mTxn *mdbx.Txn) {
	gDbi, _ := gTxn.OpenDBISimple("2", gdbx.DupSort)
	mDbi, _ := mTxn.OpenDBISimple("2", mdbx.DupSort)

	gCur, _ := gTxn.OpenCursor(gDbi)
	mCur, _ := mTxn.OpenCursor(mDbi)
	defer gCur.Close()
	defer mCur.Close()

	// Compare with Next (traverses all key-value pairs)
	gk, gv, gErr := gCur.Get(nil, nil, gdbx.First)
	mk, mv, mErr := mCur.Get(nil, nil, mdbx.First)

	count := 0
	for gErr == nil && mErr == nil {
		if !bytes.Equal(gk, mk) {
			t.Fatalf("Table '2' Next key mismatch at %d: gdbx=%q, mdbx=%q", count, gk, mk)
		}
		if !bytes.Equal(gv, mv) {
			t.Fatalf("Table '2' Next value mismatch at %d: gdbx=%q, mdbx=%q", count, gv, mv)
		}

		count++
		gk, gv, gErr = gCur.Get(nil, nil, gdbx.Next)
		mk, mv, mErr = mCur.Get(nil, nil, mdbx.Next)
	}

	if (gErr == nil) != (mErr == nil) {
		t.Fatalf("Table '2' Next iteration end mismatch: gdbx_err=%v, mdbx_err=%v", gErr, mErr)
	}

	t.Logf("    Compared %d entries with Next", count)
}

func compareTable2CursorsNextDup(t *testing.T, gTxn *gdbx.Txn, mTxn *mdbx.Txn) {
	gDbi, _ := gTxn.OpenDBISimple("2", gdbx.DupSort)
	mDbi, _ := mTxn.OpenDBISimple("2", mdbx.DupSort)

	gCur, _ := gTxn.OpenCursor(gDbi)
	mCur, _ := mTxn.OpenCursor(mDbi)
	defer gCur.Close()
	defer mCur.Close()

	// For each key, compare NextDup behavior
	for k := 0; k < 10; k++ {
		key := []byte(fmt.Sprintf("dk-%02d", k))

		gk, gv, gErr := gCur.Get(key, nil, gdbx.Set)
		mk, mv, mErr := mCur.Get(key, nil, mdbx.Set)

		if gErr != nil || mErr != nil {
			t.Fatalf("Set failed for key %d: gdbx=%v, mdbx=%v", k, gErr, mErr)
		}

		dupCount := 0
		for gErr == nil && mErr == nil {
			if !bytes.Equal(gk, mk) {
				t.Fatalf("NextDup key mismatch at key=%d dup=%d: gdbx=%q, mdbx=%q", k, dupCount, gk, mk)
			}
			if !bytes.Equal(gv, mv) {
				t.Fatalf("NextDup value mismatch at key=%d dup=%d: gdbx=%q, mdbx=%q", k, dupCount, gv, mv)
			}

			dupCount++
			gk, gv, gErr = gCur.Get(nil, nil, gdbx.NextDup)
			mk, mv, mErr = mCur.Get(nil, nil, mdbx.NextDup)
		}

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("NextDup end mismatch at key=%d: gdbx_err=%v, mdbx_err=%v", k, gErr, mErr)
		}
	}
}

func compareTable2CursorsNextNoDup(t *testing.T, gTxn *gdbx.Txn, mTxn *mdbx.Txn) {
	gDbi, _ := gTxn.OpenDBISimple("2", gdbx.DupSort)
	mDbi, _ := mTxn.OpenDBISimple("2", mdbx.DupSort)

	gCur, _ := gTxn.OpenCursor(gDbi)
	mCur, _ := mTxn.OpenCursor(mDbi)
	defer gCur.Close()
	defer mCur.Close()

	// Compare with NextNoDup (skips to next key)
	gk, gv, gErr := gCur.Get(nil, nil, gdbx.First)
	mk, mv, mErr := mCur.Get(nil, nil, mdbx.First)

	count := 0
	for gErr == nil && mErr == nil {
		if !bytes.Equal(gk, mk) {
			t.Fatalf("NextNoDup key mismatch at %d: gdbx=%q, mdbx=%q", count, gk, mk)
		}
		if !bytes.Equal(gv, mv) {
			t.Fatalf("NextNoDup value mismatch at %d: gdbx=%q, mdbx=%q", count, gv, mv)
		}

		count++
		gk, gv, gErr = gCur.Get(nil, nil, gdbx.NextNoDup)
		mk, mv, mErr = mCur.Get(nil, nil, mdbx.NextNoDup)
	}

	if (gErr == nil) != (mErr == nil) {
		t.Fatalf("NextNoDup iteration end mismatch: gdbx_err=%v, mdbx_err=%v", gErr, mErr)
	}

	t.Logf("    Compared %d keys with NextNoDup", count)
}

// ============================================================================
// Seek and read test: seek to every 100th key, read 10 entries with Next/NextDup
// ============================================================================

func TestSeekAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compat-seek-read-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a database with 1000 keys, 100 values each for DUPSORT
	t.Log("Creating database...")
	createSeekTestDB(t, dbPath)

	// Test seeking and reading
	t.Log("Testing seek and read operations...")
	testSeekAndReadOperations(t, dbPath)

	t.Log("Seek and read test PASSED")
}

func createSeekTestDB(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetOption(mdbx.OptMaxDB, 10)
	env.SetGeometry(-1, -1, 1<<28, -1, -1, 4096)

	err = env.Open(dbPath, mdbx.NoSubdir, 0644)
	if err != nil {
		t.Fatalf("mdbx Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}

	// DUPSORT table with 1000 keys × 100 values
	dbi, err := txn.OpenDBISimple("dupdata", mdbx.Create|mdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("mdbx OpenDBI failed: %v", err)
	}

	for k := 0; k < 1000; k++ {
		key := []byte(fmt.Sprintf("key%04d", k))
		for v := 0; v < 100; v++ {
			val := []byte(fmt.Sprintf("val%04d", v))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatalf("mdbx Put failed at key=%d val=%d: %v", k, v, err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("mdbx Commit failed: %v", err)
	}
}

func testSeekAndReadOperations(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Open with gdbx
	gEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatalf("gdbx NewEnv failed: %v", err)
	}
	defer gEnv.Close()
	gEnv.SetMaxDBs(10)
	if err := gEnv.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644); err != nil {
		t.Fatalf("gdbx Open failed: %v", err)
	}

	// Open with mdbx
	mEnv, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer mEnv.Close()
	mEnv.SetOption(mdbx.OptMaxDB, 10)
	if err := mEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644); err != nil {
		t.Fatalf("mdbx Open failed: %v", err)
	}

	// Begin transactions
	gTxn, err := gEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatalf("gdbx BeginTxn failed: %v", err)
	}
	defer gTxn.Abort()

	mTxn, err := mEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}
	defer mTxn.Abort()

	// Open cursors
	gDbi, _ := gTxn.OpenDBISimple("dupdata", gdbx.DupSort)
	mDbi, _ := mTxn.OpenDBISimple("dupdata", mdbx.DupSort)

	gCur, _ := gTxn.OpenCursor(gDbi)
	mCur, _ := mTxn.OpenCursor(mDbi)
	defer gCur.Close()
	defer mCur.Close()

	// Test 1: Set (exact key match) + Next
	t.Log("  Testing Set + Next...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		key := []byte(fmt.Sprintf("key%04d", seekKey))

		gk, gv, gErr := gCur.Get(key, nil, gdbx.Set)
		mk, mv, mErr := mCur.Get(key, nil, mdbx.Set)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("Set mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("Set result mismatch at key %d: gdbx=(%q,%q), mdbx=(%q,%q)",
				seekKey, gk, gv, mk, mv)
		}

		// Read 10 entries with Next
		for i := 0; i < 10; i++ {
			gk, gv, gErr = gCur.Get(nil, nil, gdbx.Next)
			mk, mv, mErr = mCur.Get(nil, nil, mdbx.Next)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("Set+Next mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("Set+Next result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    Set + Next: OK")

	// Test 2: SetKey + Next
	t.Log("  Testing SetKey + Next...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		key := []byte(fmt.Sprintf("key%04d", seekKey))

		gk, gv, gErr := gCur.Get(key, nil, gdbx.SetKey)
		mk, mv, mErr := mCur.Get(key, nil, mdbx.SetKey)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("SetKey mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("SetKey result mismatch at key %d: gdbx=(%q,%q), mdbx=(%q,%q)",
				seekKey, gk, gv, mk, mv)
		}

		// Read 10 entries with Next
		for i := 0; i < 10; i++ {
			gk, gv, gErr = gCur.Get(nil, nil, gdbx.Next)
			mk, mv, mErr = mCur.Get(nil, nil, mdbx.Next)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("SetKey+Next mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("SetKey+Next result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    SetKey + Next: OK")

	// Test 3: SetRange (>= search) + Next
	t.Log("  Testing SetRange + Next...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		// Use a key that doesn't exist exactly to test range behavior
		key := []byte(fmt.Sprintf("key%04d5", seekKey)) // e.g., "key00005" between "key0000" and "key0001"

		gk, gv, gErr := gCur.Get(key, nil, gdbx.SetRange)
		mk, mv, mErr := mCur.Get(key, nil, mdbx.SetRange)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("SetRange mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("SetRange result mismatch at key %d: gdbx=(%q,%q), mdbx=(%q,%q)",
				seekKey, gk, gv, mk, mv)
		}

		// Read 10 entries with Next
		for i := 0; i < 10; i++ {
			gk, gv, gErr = gCur.Get(nil, nil, gdbx.Next)
			mk, mv, mErr = mCur.Get(nil, nil, mdbx.Next)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("SetRange+Next mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("SetRange+Next result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    SetRange + Next: OK")

	// Test 4: GetBoth (exact key + value) + NextDup
	t.Log("  Testing GetBoth + NextDup...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		key := []byte(fmt.Sprintf("key%04d", seekKey))
		val := []byte(fmt.Sprintf("val%04d", 50)) // Middle value

		gk, gv, gErr := gCur.Get(key, val, gdbx.GetBoth)
		mk, mv, mErr := mCur.Get(key, val, mdbx.GetBoth)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("GetBoth mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("GetBoth result mismatch at key %d: gdbx=(%q,%q), mdbx=(%q,%q)",
				seekKey, gk, gv, mk, mv)
		}

		// Read 10 entries with NextDup
		for i := 0; i < 10; i++ {
			gk, gv, gErr = gCur.Get(nil, nil, gdbx.NextDup)
			mk, mv, mErr = mCur.Get(nil, nil, mdbx.NextDup)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("GetBoth+NextDup mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("GetBoth+NextDup result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    GetBoth + NextDup: OK")

	// Test 5: GetBothRange (exact key, value >= search) + NextDup
	t.Log("  Testing GetBothRange + NextDup...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		key := []byte(fmt.Sprintf("key%04d", seekKey))
		// Use a value that doesn't exist exactly: "val00505" should find "val0051"
		val := []byte(fmt.Sprintf("val%04d5", 50))

		gk, gv, gErr := gCur.Get(key, val, gdbx.GetBothRange)
		mk, mv, mErr := mCur.Get(key, val, mdbx.GetBothRange)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("GetBothRange mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("GetBothRange result mismatch at key %d: gdbx=(%q,%q), mdbx=(%q,%q)",
				seekKey, gk, gv, mk, mv)
		}

		// Read 10 entries with NextDup
		for i := 0; i < 10; i++ {
			gk, gv, gErr = gCur.Get(nil, nil, gdbx.NextDup)
			mk, mv, mErr = mCur.Get(nil, nil, mdbx.NextDup)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("GetBothRange+NextDup mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("GetBothRange+NextDup result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    GetBothRange + NextDup: OK")

	// Test 6: FirstDup + NextDup
	// Note: FirstDup may return empty key in mdbx-go (key is implicit from current position)
	t.Log("  Testing FirstDup + NextDup...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		key := []byte(fmt.Sprintf("key%04d", seekKey))

		// First position at the key
		_, _, gErr := gCur.Get(key, nil, gdbx.Set)
		_, _, mErr := mCur.Get(key, nil, mdbx.Set)
		if gErr != nil || mErr != nil {
			continue
		}

		// Now get FirstDup - only compare values (key behavior differs)
		_, gv, gErr := gCur.Get(nil, nil, gdbx.FirstDup)
		_, mv, mErr := mCur.Get(nil, nil, mdbx.FirstDup)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("FirstDup mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gv, mv) {
			t.Fatalf("FirstDup value mismatch at key %d: gdbx=%q, mdbx=%q", seekKey, gv, mv)
		}

		// Read 10 entries with NextDup
		for i := 0; i < 10; i++ {
			gk, gv, gErr := gCur.Get(nil, nil, gdbx.NextDup)
			mk, mv, mErr := mCur.Get(nil, nil, mdbx.NextDup)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("FirstDup+NextDup mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("FirstDup+NextDup result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    FirstDup + NextDup: OK")

	// Test 7: LastDup + PrevDup
	// Note: LastDup may return empty key in mdbx-go (key is implicit from current position)
	t.Log("  Testing LastDup + PrevDup...")
	for seekKey := 0; seekKey < 1000; seekKey += 100 {
		key := []byte(fmt.Sprintf("key%04d", seekKey))

		// First position at the key
		_, _, gErr := gCur.Get(key, nil, gdbx.Set)
		_, _, mErr := mCur.Get(key, nil, mdbx.Set)
		if gErr != nil || mErr != nil {
			continue
		}

		// Now get LastDup - only compare values (key behavior differs)
		_, gv, gErr := gCur.Get(nil, nil, gdbx.LastDup)
		_, mv, mErr := mCur.Get(nil, nil, mdbx.LastDup)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("LastDup mismatch at key %d: gdbx_err=%v, mdbx_err=%v", seekKey, gErr, mErr)
		}
		if gErr != nil {
			continue
		}
		if !bytes.Equal(gv, mv) {
			t.Fatalf("LastDup value mismatch at key %d: gdbx=%q, mdbx=%q", seekKey, gv, mv)
		}

		// Read 10 entries with PrevDup
		for i := 0; i < 10; i++ {
			gk, gv, gErr := gCur.Get(nil, nil, gdbx.PrevDup)
			mk, mv, mErr := mCur.Get(nil, nil, mdbx.PrevDup)

			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("LastDup+PrevDup mismatch at key %d, iter %d: gdbx_err=%v, mdbx_err=%v",
					seekKey, i, gErr, mErr)
			}
			if gErr != nil {
				break
			}
			if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
				t.Fatalf("LastDup+PrevDup result mismatch at key %d, iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					seekKey, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    LastDup + PrevDup: OK")

	// Test 8: Prev and PrevNoDup
	t.Log("  Testing Prev and PrevNoDup...")
	// Start from end
	gk, gv, gErr := gCur.Get(nil, nil, gdbx.Last)
	mk, mv, mErr := mCur.Get(nil, nil, mdbx.Last)
	if (gErr == nil) != (mErr == nil) {
		t.Fatalf("Last mismatch: gdbx_err=%v, mdbx_err=%v", gErr, mErr)
	}
	if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
		t.Fatalf("Last result mismatch: gdbx=(%q,%q), mdbx=(%q,%q)", gk, gv, mk, mv)
	}

	// Read 100 entries with Prev
	for i := 0; i < 100; i++ {
		gk, gv, gErr = gCur.Get(nil, nil, gdbx.Prev)
		mk, mv, mErr = mCur.Get(nil, nil, mdbx.Prev)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("Prev mismatch at iter %d: gdbx_err=%v, mdbx_err=%v", i, gErr, mErr)
		}
		if gErr != nil {
			break
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("Prev result mismatch at iter %d: gdbx=(%q,%q), mdbx=(%q,%q)", i, gk, gv, mk, mv)
		}
	}
	t.Log("    Prev: OK")

	// Test PrevNoDup
	gk, gv, gErr = gCur.Get(nil, nil, gdbx.Last)
	mk, mv, mErr = mCur.Get(nil, nil, mdbx.Last)
	for i := 0; i < 10; i++ {
		gk, gv, gErr = gCur.Get(nil, nil, gdbx.PrevNoDup)
		mk, mv, mErr = mCur.Get(nil, nil, mdbx.PrevNoDup)

		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("PrevNoDup mismatch at iter %d: gdbx_err=%v, mdbx_err=%v", i, gErr, mErr)
		}
		if gErr != nil {
			break
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("PrevNoDup result mismatch at iter %d: gdbx=(%q,%q), mdbx=(%q,%q)", i, gk, gv, mk, mv)
		}
	}
	t.Log("    PrevNoDup: OK")
}

// ============================================================================
// Comprehensive cursor operation combination tests
// ============================================================================

func TestCursorOperationCombinations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compat-cursor-combo-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a database with various data patterns
	t.Log("Creating database...")
	createComboTestDB(t, dbPath)

	// Run combination tests
	t.Log("Testing cursor operation combinations...")
	testCursorCombinations(t, dbPath)

	t.Log("Cursor operation combination tests PASSED")
}

func createComboTestDB(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Default)
	if err != nil {
		t.Fatalf("mdbx NewEnv failed: %v", err)
	}
	defer env.Close()

	env.SetOption(mdbx.OptMaxDB, 10)
	env.SetGeometry(-1, -1, 1<<28, -1, -1, 4096)

	err = env.Open(dbPath, mdbx.NoSubdir, 0644)
	if err != nil {
		t.Fatalf("mdbx Open failed: %v", err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("mdbx BeginTxn failed: %v", err)
	}

	// Create DUPSORT table with varying numbers of duplicates per key
	dbi, err := txn.OpenDBISimple("combo", mdbx.Create|mdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatalf("mdbx OpenDBI failed: %v", err)
	}

	// Key patterns:
	// - Some keys with 1 value (no duplicates)
	// - Some keys with few values (2-5)
	// - Some keys with many values (50-100)
	// - Some keys with very many values (500+)
	patterns := []struct {
		prefix    string
		numKeys   int
		numValues int
	}{
		{"single", 100, 1},
		{"few", 50, 5},
		{"medium", 30, 50},
		{"many", 20, 200},
	}

	for _, p := range patterns {
		for k := 0; k < p.numKeys; k++ {
			key := []byte(fmt.Sprintf("%s-%04d", p.prefix, k))
			for v := 0; v < p.numValues; v++ {
				val := []byte(fmt.Sprintf("value-%06d", v))
				if err := txn.Put(dbi, key, val, 0); err != nil {
					txn.Abort()
					t.Fatalf("mdbx Put failed at %s key=%d val=%d: %v", p.prefix, k, v, err)
				}
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("mdbx Commit failed: %v", err)
	}
}

func testCursorCombinations(t *testing.T, dbPath string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Open with both libraries
	gEnv, _ := gdbx.NewEnv(gdbx.Default)
	defer gEnv.Close()
	gEnv.SetMaxDBs(10)
	gEnv.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)

	mEnv, _ := mdbx.NewEnv(mdbx.Default)
	defer mEnv.Close()
	mEnv.SetOption(mdbx.OptMaxDB, 10)
	mEnv.Open(dbPath, mdbx.NoSubdir|mdbx.Readonly, 0644)

	gTxn, _ := gEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	defer gTxn.Abort()
	mTxn, _ := mEnv.BeginTxn(nil, mdbx.Readonly)
	defer mTxn.Abort()

	gDbi, _ := gTxn.OpenDBISimple("combo", gdbx.DupSort)
	mDbi, _ := mTxn.OpenDBISimple("combo", mdbx.DupSort)

	gCur, _ := gTxn.OpenCursor(gDbi)
	mCur, _ := mTxn.OpenCursor(mDbi)
	defer gCur.Close()
	defer mCur.Close()

	// Test 1: First, then alternating Next and PrevDup
	t.Log("  Testing First + alternating Next/PrevDup...")
	gCur.Get(nil, nil, gdbx.First)
	mCur.Get(nil, nil, mdbx.First)

	for i := 0; i < 100; i++ {
		// Next
		gk, gv, gErr := gCur.Get(nil, nil, gdbx.Next)
		mk, mv, mErr := mCur.Get(nil, nil, mdbx.Next)
		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("Next mismatch at iter %d: gdbx_err=%v, mdbx_err=%v", i, gErr, mErr)
		}
		if gErr != nil {
			break
		}
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Fatalf("Next result mismatch at iter %d", i)
		}

		// PrevDup (may fail if on first dup)
		gk, gv, gErr = gCur.Get(nil, nil, gdbx.PrevDup)
		mk, mv, mErr = mCur.Get(nil, nil, mdbx.PrevDup)
		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("PrevDup mismatch at iter %d: gdbx_err=%v, mdbx_err=%v", i, gErr, mErr)
		}
		if gErr == nil && (!bytes.Equal(gk, mk) || !bytes.Equal(gv, mv)) {
			t.Fatalf("PrevDup result mismatch at iter %d", i)
		}
	}
	t.Log("    First + Next/PrevDup: OK")

	// Test 2: SetRange then NextNoDup then PrevNoDup
	t.Log("  Testing SetRange + NextNoDup + PrevNoDup...")
	for _, prefix := range []string{"few", "medium", "many"} {
		key := []byte(fmt.Sprintf("%s-0010", prefix))

		gCur.Get(key, nil, gdbx.SetRange)
		mCur.Get(key, nil, mdbx.SetRange)

		// NextNoDup a few times
		for i := 0; i < 5; i++ {
			gk, gv, gErr := gCur.Get(nil, nil, gdbx.NextNoDup)
			mk, mv, mErr := mCur.Get(nil, nil, mdbx.NextNoDup)
			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("NextNoDup mismatch for %s at iter %d", prefix, i)
			}
			if gErr == nil && (!bytes.Equal(gk, mk) || !bytes.Equal(gv, mv)) {
				t.Fatalf("NextNoDup result mismatch for %s at iter %d", prefix, i)
			}
		}

		// PrevNoDup back
		for i := 0; i < 3; i++ {
			gk, gv, gErr := gCur.Get(nil, nil, gdbx.PrevNoDup)
			mk, mv, mErr := mCur.Get(nil, nil, mdbx.PrevNoDup)
			if (gErr == nil) != (mErr == nil) {
				t.Fatalf("PrevNoDup mismatch for %s at iter %d", prefix, i)
			}
			if gErr == nil && (!bytes.Equal(gk, mk) || !bytes.Equal(gv, mv)) {
				t.Fatalf("PrevNoDup result mismatch for %s at iter %d: gdbx=(%q,%q), mdbx=(%q,%q)",
					prefix, i, gk, gv, mk, mv)
			}
		}
	}
	t.Log("    SetRange + NextNoDup + PrevNoDup: OK")

	// Test 3: Set + FirstDup + NextDup + LastDup + PrevDup round trip
	t.Log("  Testing dup navigation round trip...")
	testKeys := []string{"few-0025", "medium-0015", "many-0010"}
	for _, keyStr := range testKeys {
		key := []byte(keyStr)

		// Set to position at key
		_, _, gErr := gCur.Get(key, nil, gdbx.Set)
		_, _, mErr := mCur.Get(key, nil, mdbx.Set)
		if gErr != nil || mErr != nil {
			continue
		}

		// Collect gdbx values with NextDup
		var gdbxVals []string
		_, gv, gErr := gCur.Get(nil, nil, gdbx.FirstDup)
		for gErr == nil {
			gdbxVals = append(gdbxVals, string(gv))
			_, gv, gErr = gCur.Get(nil, nil, gdbx.NextDup)
		}

		// Collect mdbx values with NextDup
		var mdbxVals []string
		_, mv, mErr := mCur.Get(nil, nil, mdbx.FirstDup)
		for mErr == nil {
			mdbxVals = append(mdbxVals, string(mv))
			_, mv, mErr = mCur.Get(nil, nil, mdbx.NextDup)
		}

		if len(gdbxVals) != len(mdbxVals) {
			t.Fatalf("Dup count mismatch for %s: gdbx=%d, mdbx=%d", keyStr, len(gdbxVals), len(mdbxVals))
		}
		for i := range gdbxVals {
			if gdbxVals[i] != mdbxVals[i] {
				t.Fatalf("Dup value mismatch for %s at %d", keyStr, i)
			}
		}

		// Now test PrevDup from LastDup
		gCur.Get(key, nil, gdbx.Set)
		mCur.Get(key, nil, mdbx.Set)

		var gdbxRevVals []string
		_, gv, gErr = gCur.Get(nil, nil, gdbx.LastDup)
		for gErr == nil {
			gdbxRevVals = append(gdbxRevVals, string(gv))
			_, gv, gErr = gCur.Get(nil, nil, gdbx.PrevDup)
		}

		var mdbxRevVals []string
		_, mv, mErr = mCur.Get(nil, nil, mdbx.LastDup)
		for mErr == nil {
			mdbxRevVals = append(mdbxRevVals, string(mv))
			_, mv, mErr = mCur.Get(nil, nil, mdbx.PrevDup)
		}

		if len(gdbxRevVals) != len(mdbxRevVals) {
			t.Fatalf("Reverse dup count mismatch for %s: gdbx=%d, mdbx=%d", keyStr, len(gdbxRevVals), len(mdbxRevVals))
		}
		for i := range gdbxRevVals {
			if gdbxRevVals[i] != mdbxRevVals[i] {
				t.Fatalf("Reverse dup value mismatch for %s at %d", keyStr, i)
			}
		}
	}
	t.Log("    Dup navigation round trip: OK")

	// Test 4: Seek to middle of dups, then navigate both directions
	t.Log("  Testing mid-dup seek and bidirectional navigation...")
	for _, keyStr := range []string{"medium-0010", "many-0005"} {
		key := []byte(keyStr)
		val := []byte("value-000050") // Middle value

		// GetBothRange to position in middle
		_, gv, gErr := gCur.Get(key, val, gdbx.GetBothRange)
		_, mv, mErr := mCur.Get(key, val, mdbx.GetBothRange)
		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("GetBothRange mismatch for %s", keyStr)
		}
		if gErr == nil && !bytes.Equal(gv, mv) {
			t.Fatalf("GetBothRange value mismatch for %s: gdbx=%q, mdbx=%q", keyStr, gv, mv)
		}

		// NextDup a few times
		for i := 0; i < 10; i++ {
			gk, gv, gErr := gCur.Get(nil, nil, gdbx.NextDup)
			mk, mv, mErr := mCur.Get(nil, nil, mdbx.NextDup)
			if (gErr == nil) != (mErr == nil) {
				break
			}
			if gErr == nil && (!bytes.Equal(gk, mk) || !bytes.Equal(gv, mv)) {
				t.Fatalf("NextDup from middle mismatch for %s at iter %d", keyStr, i)
			}
		}

		// PrevDup back past the start
		for i := 0; i < 20; i++ {
			gk, gv, gErr := gCur.Get(nil, nil, gdbx.PrevDup)
			mk, mv, mErr := mCur.Get(nil, nil, mdbx.PrevDup)
			if (gErr == nil) != (mErr == nil) {
				break
			}
			if gErr == nil && (!bytes.Equal(gk, mk) || !bytes.Equal(gv, mv)) {
				t.Fatalf("PrevDup past middle mismatch for %s at iter %d", keyStr, i)
			}
		}
	}
	t.Log("    Mid-dup seek and bidirectional: OK")

	// Test 5: Full iteration forward then backward
	t.Log("  Testing full forward then backward iteration...")
	var gForward, mForward []string
	gCur.Get(nil, nil, gdbx.First)
	mCur.Get(nil, nil, mdbx.First)

	// Collect first 200 entries forward
	for i := 0; i < 200; i++ {
		gk, gv, gErr := gCur.Get(nil, nil, gdbx.Next)
		mk, mv, mErr := mCur.Get(nil, nil, mdbx.Next)
		if gErr != nil || mErr != nil {
			break
		}
		gForward = append(gForward, string(gk)+":"+string(gv))
		mForward = append(mForward, string(mk)+":"+string(mv))
	}

	// Now go backward
	var gBackward, mBackward []string
	for i := 0; i < 200; i++ {
		gk, gv, gErr := gCur.Get(nil, nil, gdbx.Prev)
		mk, mv, mErr := mCur.Get(nil, nil, mdbx.Prev)
		if gErr != nil || mErr != nil {
			break
		}
		gBackward = append(gBackward, string(gk)+":"+string(gv))
		mBackward = append(mBackward, string(mk)+":"+string(mv))
	}

	// Verify forward
	for i := range gForward {
		if i >= len(mForward) || gForward[i] != mForward[i] {
			t.Fatalf("Forward iteration mismatch at %d", i)
		}
	}

	// Verify backward
	for i := range gBackward {
		if i >= len(mBackward) || gBackward[i] != mBackward[i] {
			t.Fatalf("Backward iteration mismatch at %d: gdbx=%s, mdbx=%s", i, gBackward[i], mBackward[i])
		}
	}
	t.Log("    Full forward/backward iteration: OK")

	// Test 6: Count verification at various positions
	t.Log("  Testing Count at various positions...")
	countTests := []struct {
		key           string
		expectedCount uint64
	}{
		{"single-0050", 1},
		{"few-0025", 5},
		{"medium-0015", 50},
		{"many-0010", 200},
	}

	for _, ct := range countTests {
		key := []byte(ct.key)
		gCur.Get(key, nil, gdbx.Set)
		mCur.Get(key, nil, mdbx.Set)

		gCount, gErr := gCur.Count()
		mCount, mErr := mCur.Count()
		if (gErr == nil) != (mErr == nil) {
			t.Fatalf("Count error mismatch for %s: gdbx_err=%v, mdbx_err=%v", ct.key, gErr, mErr)
		}
		if gErr == nil {
			if gCount != mCount {
				t.Fatalf("Count mismatch for %s: gdbx=%d, mdbx=%d", ct.key, gCount, mCount)
			}
			if gCount != ct.expectedCount {
				t.Fatalf("Count unexpected for %s: got=%d, want=%d", ct.key, gCount, ct.expectedCount)
			}
		}
	}
	t.Log("    Count verification: OK")
}
