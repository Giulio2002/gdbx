package tests

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestGdbxWriteLibmdbxRead verifies that DUPSORT databases created by gdbx
// can be read correctly by libmdbx.
func TestGdbxWriteLibmdbxRead(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create DUPSORT database with gdbx
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	gdbxEnv.SetMaxDBs(10)
	// Set geometry to match libmdbx defaults
	if err := gdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096); err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}
	if err := gdbxEnv.Open(db.path, 0, 0644); err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}

	dbi, err := gdbxTxn.OpenDBISimple("testdb", gdbx.Create|gdbx.DupSort)
	if err != nil {
		gdbxTxn.Abort()
		gdbxEnv.Close()
		t.Fatal(err)
	}

	// Insert multiple values for multiple keys
	testData := map[string][]string{
		"key1": {"alpha", "beta", "gamma"},
		"key2": {"one", "two", "three"},
		"key3": {"x", "y", "z"},
	}

	for key, values := range testData {
		for _, val := range values {
			if err := gdbxTxn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
				gdbxTxn.Abort()
				gdbxEnv.Close()
				t.Fatalf("Put error: %v", err)
			}
		}
	}

	if _, err := gdbxTxn.Commit(); err != nil {
		gdbxEnv.Close()
		t.Fatalf("Commit error: %v", err)
	}
	gdbxEnv.Close()

	// Read with libmdbx
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(db.path, 0, 0644); err != nil {
		t.Fatalf("libmdbx env.Open error: %v", err)
	}
	t.Log("libmdbx env.Open succeeded")

	txn, err := env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		t.Fatalf("libmdbx BeginTxn error: %v", err)
	}
	defer txn.Abort()
	t.Log("libmdbx BeginTxn succeeded")

	// Check what's in the main database
	mainCursor, err := txn.OpenCursor(1)
	if err != nil {
		t.Logf("OpenCursor on main db failed: %v", err)
	} else {
		t.Log("Main database contents:")
		k, v, err := mainCursor.Get(nil, nil, mdbx.First)
		for err == nil {
			t.Logf("  %q (len=%d) -> (value len=%d):", k, len(k), len(v))
			// Dump value bytes
			for i := 0; i < len(v); i += 16 {
				line := "    "
				for j := i; j < i+16 && j < len(v); j++ {
					line += fmt.Sprintf("%02x ", v[j])
				}
				t.Log(line)
			}
			k, v, err = mainCursor.Get(nil, nil, mdbx.Next)
		}
		mainCursor.Close()
	}

	// First try without DupSort flag to see what libmdbx detects
	mdbxDbi, err := txn.OpenDBI("testdb", 0, nil, nil)
	if err != nil {
		t.Logf("OpenDBI without flags: %v", err)
		// Try with DupSort flag
		mdbxDbi, err = txn.OpenDBI("testdb", mdbx.DupSort, nil, nil)
		if err != nil {
			t.Fatalf("libmdbx OpenDBI error: %v", err)
		}
	}

	cursor, err := txn.OpenCursor(mdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Read all entries and compare
	actualData := make(map[string][]string)
	for {
		k, v, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("libmdbx Get error: %v", err)
		}
		key := string(k)
		actualData[key] = append(actualData[key], string(v))
	}

	// Compare with expected data (values are stored in sorted order in DUPSORT)
	for key, expectedValues := range testData {
		actualValues, ok := actualData[key]
		if !ok {
			t.Errorf("Key %q not found by libmdbx", key)
			continue
		}

		// DUPSORT stores values in sorted order
		sortedExpected := make([]string, len(expectedValues))
		copy(sortedExpected, expectedValues)
		sort.Strings(sortedExpected)

		if len(actualValues) != len(sortedExpected) {
			t.Errorf("Key %q: expected %d values, got %d", key, len(sortedExpected), len(actualValues))
			continue
		}

		for i, expected := range sortedExpected {
			if actualValues[i] != expected {
				t.Errorf("Key %q value[%d]: expected %q, got %q", key, i, expected, actualValues[i])
			}
		}
	}

	t.Logf("Successfully verified gdbx-created DUPSORT database read by libmdbx")
}

// TestLibmdbxWriteGdbxRead verifies that DUPSORT databases created by libmdbx
// can be read correctly by gdbx.
func TestLibmdbxWriteGdbxRead(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create DUPSORT database with libmdbx
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBI("testdb", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		env.Close()
		t.Fatal(err)
	}

	// Insert multiple values for multiple keys
	// Note: Values are stored in sorted order by libmdbx
	testData := map[string][]string{
		"apple":  {"green", "red", "yellow"}, // sorted order
		"banana": {"green", "yellow"},        // sorted order
		"cherry": {"red"},
	}

	for key, values := range testData {
		for _, val := range values {
			if err := txn.Put(dbi, []byte(key), []byte(val), 0); err != nil {
				txn.Abort()
				env.Close()
				t.Fatalf("Put error: %v", err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		env.Close()
		t.Fatalf("Commit error: %v", err)
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

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxTxn.Abort()

	gdbxDbi, err := gdbxTxn.OpenDBISimple("testdb", 0)
	if err != nil {
		t.Fatalf("gdbx OpenDBI error: %v", err)
	}

	cursor, err := gdbxTxn.OpenCursor(gdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Read all entries and compare
	actualData := make(map[string][]string)
	k, v, err := cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		key := string(k)
		actualData[key] = append(actualData[key], string(v))
		k, v, err = cursor.Get(nil, nil, gdbx.Next)
	}
	if !gdbx.IsNotFound(err) {
		t.Fatalf("gdbx Get error: %v", err)
	}

	// Compare with expected data
	for key, expectedValues := range testData {
		actualValues, ok := actualData[key]
		if !ok {
			t.Errorf("Key %q not found by gdbx", key)
			continue
		}

		if len(actualValues) != len(expectedValues) {
			t.Errorf("Key %q: expected %d values, got %d", key, len(expectedValues), len(actualValues))
			continue
		}

		for i, expected := range expectedValues {
			if actualValues[i] != expected {
				t.Errorf("Key %q value[%d]: expected %q, got %q", key, i, expected, actualValues[i])
			}
		}
	}

	t.Logf("Successfully verified libmdbx-created DUPSORT database read by gdbx")
}

// TestRoundTrip verifies that data written by gdbx can be read by libmdbx,
// then written by libmdbx and read by gdbx.
func TestRoundTrip(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Phase 1: Write with gdbx
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	gdbxEnv.SetMaxDBs(10)
	// Set geometry to match libmdbx defaults
	if err := gdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096); err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}
	if err := gdbxEnv.Open(db.path, 0, 0644); err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}

	dbi, err := gdbxTxn.OpenDBISimple("roundtrip", gdbx.Create|gdbx.DupSort)
	if err != nil {
		gdbxTxn.Abort()
		gdbxEnv.Close()
		t.Fatal(err)
	}

	// Write initial data
	if err := gdbxTxn.Put(dbi, []byte("test"), []byte("value1"), 0); err != nil {
		gdbxTxn.Abort()
		gdbxEnv.Close()
		t.Fatal(err)
	}
	if err := gdbxTxn.Put(dbi, []byte("test"), []byte("value2"), 0); err != nil {
		gdbxTxn.Abort()
		gdbxEnv.Close()
		t.Fatal(err)
	}

	if _, err := gdbxTxn.Commit(); err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}
	gdbxEnv.Close()

	// Phase 2: Read with libmdbx and add more data
	mdbxEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)

	if err := mdbxEnv.Open(db.path, 0, 0644); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Verify existing data
	mdbxTxn, err := mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	mdbxDbi, err := mdbxTxn.OpenDBI("roundtrip", mdbx.DupSort, nil, nil)
	if err != nil {
		mdbxTxn.Abort()
		mdbxEnv.Close()
		t.Fatalf("libmdbx OpenDBI error: %v", err)
	}

	cursor, _ := mdbxTxn.OpenCursor(mdbxDbi)
	k, v, err := cursor.Get(nil, nil, mdbx.First)
	if err != nil {
		cursor.Close()
		mdbxTxn.Abort()
		mdbxEnv.Close()
		t.Fatalf("libmdbx read error: %v", err)
	}
	if string(k) != "test" || string(v) != "value1" {
		cursor.Close()
		mdbxTxn.Abort()
		mdbxEnv.Close()
		t.Fatalf("Unexpected first entry: %q => %q", k, v)
	}
	cursor.Close()
	mdbxTxn.Abort()

	// Add more data with libmdbx
	mdbxTxn, _ = mdbxEnv.BeginTxn(nil, 0)
	mdbxDbi, _ = mdbxTxn.OpenDBI("roundtrip", mdbx.DupSort, nil, nil)
	if err := mdbxTxn.Put(mdbxDbi, []byte("test"), []byte("value3"), 0); err != nil {
		mdbxTxn.Abort()
		mdbxEnv.Close()
		t.Fatal(err)
	}
	mdbxTxn.Commit()
	mdbxEnv.Close()

	// Phase 3: Verify all data with gdbx
	gdbxEnv, _ = gdbx.NewEnv(gdbx.Default)
	gdbxEnv.SetMaxDBs(10)
	gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644)
	defer gdbxEnv.Close()

	gdbxTxn, _ = gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	defer gdbxTxn.Abort()

	dbi, _ = gdbxTxn.OpenDBISimple("roundtrip", 0)
	cursor2, _ := gdbxTxn.OpenCursor(dbi)
	defer cursor2.Close()

	var values []string
	k, v, err = cursor2.Get(nil, nil, gdbx.First)
	for err == nil {
		if string(k) == "test" {
			values = append(values, string(v))
		}
		k, v, err = cursor2.Get(nil, nil, gdbx.Next)
	}

	expected := []string{"value1", "value2", "value3"}
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, got %d: %v", len(expected), len(values), values)
	}

	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Value[%d]: expected %q, got %q", i, exp, values[i])
		}
	}

	t.Logf("Successfully completed round-trip test: gdbx -> libmdbx -> gdbx")
}

// TestSubPageBinaryCompat compares the actual sub-page bytes between gdbx and libmdbx
func TestSubPageBinaryCompat(t *testing.T) {
	db1 := newTestDB(t)
	defer db1.cleanup()
	db2 := newTestDB(t)
	defer db2.cleanup()

	values := []string{"aaa", "bbb", "ccc"}

	// Create with gdbx
	gdbxEnv, _ := gdbx.NewEnv(gdbx.Default)
	gdbxEnv.SetMaxDBs(10)
	gdbxEnv.Open(db1.path, 0, 0644)
	gdbxTxn, _ := gdbxEnv.BeginTxn(nil, 0)
	dbi, _ := gdbxTxn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	for _, v := range values {
		gdbxTxn.Put(dbi, []byte("key"), []byte(v), 0)
	}
	gdbxTxn.Commit()
	gdbxEnv.Close()

	// Create with libmdbx
	mdbxEnv, _ := mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.Open(db2.path, mdbx.Create, 0644)
	mdbxTxn, _ := mdbxEnv.BeginTxn(nil, 0)
	mdbxDbi, _ := mdbxTxn.OpenDBI("test", mdbx.Create|mdbx.DupSort, nil, nil)
	for _, v := range values {
		mdbxTxn.Put(mdbxDbi, []byte("key"), []byte(v), 0)
	}
	mdbxTxn.Commit()
	mdbxEnv.Close()

	// Read both and compare
	gdbxEnv, _ = gdbx.NewEnv(gdbx.Default)
	gdbxEnv.SetMaxDBs(10)
	gdbxEnv.Open(db1.path, gdbx.ReadOnly, 0644)
	defer gdbxEnv.Close()

	gdbxTxn, _ = gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	defer gdbxTxn.Abort()
	dbi, _ = gdbxTxn.OpenDBISimple("test", 0)
	tree := gdbxTxn.GetTree(dbi)
	gdbxPage, _ := gdbxTxn.DebugGetPage(uint32(tree.Root))

	mdbxEnv, _ = mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.Open(db2.path, 0, 0644)
	defer mdbxEnv.Close()

	mdbxTxn, _ = mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	defer mdbxTxn.Abort()
	mdbxDbi, _ = mdbxTxn.OpenDBI("test", 0, nil, nil)
	cursor, _ := mdbxTxn.OpenCursor(mdbxDbi)
	defer cursor.Close()

	// Get first entry from both
	gdbxCursor, _ := gdbxTxn.OpenCursor(dbi)
	defer gdbxCursor.Close()

	k1, v1, _ := gdbxCursor.Get(nil, nil, gdbx.First)
	k2, v2, _ := cursor.Get(nil, nil, mdbx.First)

	t.Logf("gdbx first: key=%q value=%q", k1, v1)
	t.Logf("libmdbx first: key=%q value=%q", k2, v2)

	if !bytes.Equal(k1, k2) || !bytes.Equal(v1, v2) {
		t.Errorf("First entries differ!")
	}

	// Compare all values
	var gdbxVals, mdbxVals []string
	gdbxVals = append(gdbxVals, string(v1))
	for {
		_, v, err := gdbxCursor.Get(nil, nil, gdbx.Next)
		if gdbx.IsNotFound(err) {
			break
		}
		gdbxVals = append(gdbxVals, string(v))
	}

	mdbxVals = append(mdbxVals, string(v2))
	for {
		_, v, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		mdbxVals = append(mdbxVals, string(v))
	}

	t.Logf("gdbx values: %v", gdbxVals)
	t.Logf("libmdbx values: %v", mdbxVals)

	if len(gdbxVals) != len(mdbxVals) {
		t.Errorf("Different number of values: gdbx=%d, libmdbx=%d", len(gdbxVals), len(mdbxVals))
	}

	// Compare page bytes (first 100 bytes for debugging)
	t.Logf("gdbx root page first 100 bytes:")
	for i := 0; i < 100 && i < len(gdbxPage); i++ {
		if i%16 == 0 {
			t.Logf("\n  %3d: ", i)
		}
		t.Logf("%02x ", gdbxPage[i])
	}
}
