// Package tests contains reproduction tests for Erigon issues
package tests

import (
	"os"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestNextDupsReproErigon reproduces the exact scenario from Erigon's TestNextDups
// This test uses gdbx from start to finish, matching the exact Erigon test pattern
func TestNextDupsReproErigon(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// First run with mdbx-go to get expected behavior
	t.Log("=== Testing with mdbx-go ===")
	mdbxKeys, mdbxValues := runNextDupsWithMdbx(t, db.path)
	t.Logf("mdbx iteration: keys=%v, values=%v", mdbxKeys, mdbxValues)

	// Now run the exact same scenario with gdbx
	t.Log("=== Testing with gdbx ===")
	gdbxKeys, gdbxValues := runNextDupsWithGdbx(t)
	t.Logf("gdbx iteration: keys=%v, values=%v", gdbxKeys, gdbxValues)

	// Compare results
	expectedKeys := []string{"key", "key2", "key2", "key3"}
	expectedValues := []string{"value1.7", "value1.1", "value1.2", "value1.6"}

	if len(mdbxKeys) != len(expectedKeys) {
		t.Errorf("mdbx: expected %d keys, got %d: %v", len(expectedKeys), len(mdbxKeys), mdbxKeys)
	}
	if len(gdbxKeys) != len(expectedKeys) {
		t.Errorf("gdbx: expected %d keys, got %d: %v", len(expectedKeys), len(gdbxKeys), gdbxKeys)
	}

	for i := range expectedKeys {
		if i < len(mdbxKeys) && mdbxKeys[i] != expectedKeys[i] {
			t.Errorf("mdbx key[%d]: got %q, want %q", i, mdbxKeys[i], expectedKeys[i])
		}
		if i < len(gdbxKeys) && gdbxKeys[i] != expectedKeys[i] {
			t.Errorf("gdbx key[%d]: got %q, want %q", i, gdbxKeys[i], expectedKeys[i])
		}
		if i < len(mdbxValues) && mdbxValues[i] != expectedValues[i] {
			t.Errorf("mdbx value[%d]: got %q, want %q", i, mdbxValues[i], expectedValues[i])
		}
		if i < len(gdbxValues) && gdbxValues[i] != expectedValues[i] {
			t.Errorf("gdbx value[%d]: got %q, want %q", i, gdbxValues[i], expectedValues[i])
		}
	}
}

func runNextDupsWithMdbx(t *testing.T, path string) ([]string, []string) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("Table", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Insert base case data (Erigon's BaseCase)
	baseData := []struct {
		key, value string
	}{
		{"key1", "value1.1"},
		{"key3", "value3.1"},
		{"key1", "value1.3"},
		{"key3", "value3.3"},
	}
	for _, kv := range baseData {
		txn.Put(dbi, []byte(kv.key), []byte(kv.value), 0)
	}

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact all base data (replicating Erigon's test)
	for _, kv := range baseData {
		_, _, err := cursor.Get([]byte(kv.key), []byte(kv.value), mdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Put new data
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0)

	// Test Current() and iteration
	k, v, err := cursor.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx Current(): k=%q, v=%q, err=%v", k, v, err)

	var keys, values []string
	for k != nil {
		keys = append(keys, string(k))
		values = append(values, string(v))
		k, v, err = cursor.Get(nil, nil, mdbx.Next)
		if err != nil {
			break
		}
	}
	return keys, values
}

func runNextDupsWithGdbx(t *testing.T) ([]string, []string) {
	db := newTestDB(t)
	defer db.cleanup()

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(db.path, gdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	// Insert base case data (Erigon's BaseCase)
	baseData := []struct {
		key, value string
	}{
		{"key1", "value1.1"},
		{"key3", "value3.1"},
		{"key1", "value1.3"},
		{"key3", "value3.3"},
	}
	for _, kv := range baseData {
		txn.Put(dbi, []byte(kv.key), []byte(kv.value), 0)
	}

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact all base data (replicating Erigon's test)
	for _, kv := range baseData {
		_, _, err := cursor.Get([]byte(kv.key), []byte(kv.value), gdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Put new data - note: tx.Put followed by cursor.Put
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0)

	// Test Current() and iteration
	k, v, err := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx Current(): k=%q, v=%q, err=%v", k, v, err)

	var keys, values []string
	for k != nil {
		keys = append(keys, string(k))
		values = append(values, string(v))
		k, v, err = cursor.Get(nil, nil, gdbx.Next)
		if err != nil {
			break
		}
	}
	return keys, values
}

// TestNextDupsCurrentAfterPut tests Current() behavior after Put() operations
func TestNextDupsCurrentAfterPut(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create fresh database with gdbx
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(db.path, gdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Put multiple entries
	puts := []struct {
		key, value string
	}{
		{"key2", "value1.1"},
		{"key2", "value1.2"},
		{"key3", "value1.6"},
		{"key", "value1.7"},
	}

	for _, kv := range puts {
		if err := cursor.Put([]byte(kv.key), []byte(kv.value), 0); err != nil {
			t.Fatalf("Put(%s, %s) failed: %v", kv.key, kv.value, err)
		}
	}

	// After the last Put, cursor should be positioned at (key, value1.7)
	// which is the smallest key in sorted order
	k, v, err := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("After puts - Current(): k=%q, v=%q, err=%v", k, v, err)

	if err != nil {
		t.Fatalf("Current() error: %v", err)
	}
	if string(k) != "key" {
		t.Errorf("Current() key: got %q, want %q", k, "key")
	}

	// Now test Next
	k, v, err = cursor.Get(nil, nil, gdbx.Next)
	t.Logf("Next(): k=%q, v=%q, err=%v", k, v, err)

	if err != nil && !gdbx.IsNotFound(err) {
		t.Fatalf("Next() error: %v", err)
	}
	if k == nil {
		t.Error("Next() returned nil key - iteration stopped too early")
	} else {
		t.Logf("Next() succeeded: k=%q, v=%q", k, v)
	}

	// Count all entries by iterating from First
	cursor.Get(nil, nil, gdbx.First)
	count := 0
	for {
		k, v, err := cursor.Get(nil, nil, gdbx.GetCurrent)
		if err != nil || k == nil {
			break
		}
		count++
		t.Logf("Entry %d: k=%q, v=%q", count, k, v)
		_, _, err = cursor.Get(nil, nil, gdbx.Next)
		if err != nil {
			break
		}
	}
	t.Logf("Total entries: %d (expected 4)", count)

	if count != 4 {
		t.Errorf("Expected 4 entries, got %d", count)
	}
}

// TestErigonIterationPattern tests the exact iteration pattern from Erigon
// including the Prev() calls that restore cursor position
func TestErigonIterationPattern(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(db.path, gdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, gdbx.TxnReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	// First insert base case data (like Erigon's BaseCase)
	baseData := []struct {
		key, value string
	}{
		{"key1", "value1.1"},
		{"key3", "value3.1"},
		{"key1", "value1.3"},
		{"key3", "value3.3"},
	}
	for _, kv := range baseData {
		txn.Put(dbi, []byte(kv.key), []byte(kv.value), 0)
	}

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact all base data (like Erigon's TestNextDups lines 537-540)
	for _, kv := range baseData {
		_, _, err := cursor.Get([]byte(kv.key), []byte(kv.value), gdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Insert test data matching Erigon's TestNextDups EXACT order
	// Note: Erigon uses txn.Put for first one, then cursor.Put for rest
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0) // Last Put - cursor should be here

	// Test Current() first
	k, v, err := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("Current(): k=%q, v=%q, err=%v", k, v, err)

	// Erigon's iteration function: collect entries then call Prev to restore
	erigonIteration := func(startK, startV []byte) ([]string, []string) {
		var keys, values []string
		i := 0
		for k, v := startK, startV; k != nil; {
			keys = append(keys, string(k))
			values = append(values, string(v))
			i++
			k, v, err = cursor.Get(nil, nil, gdbx.Next)
			if err != nil {
				break
			}
		}
		// Restore position like Erigon does
		for ind := i; ind > 1; ind-- {
			cursor.Get(nil, nil, gdbx.Prev)
		}
		return keys, values
	}

	// First iteration from Current position
	keys1, values1 := erigonIteration(k, v)
	t.Logf("First iteration: keys=%v, values=%v", keys1, values1)

	expectedKeys := []string{"key", "key2", "key2", "key3"}
	if len(keys1) != len(expectedKeys) {
		t.Errorf("First iteration: expected %d keys, got %d: %v", len(expectedKeys), len(keys1), keys1)
	}

	// Now call FirstDup (like Erigon test line 553)
	_, fv, err := cursor.Get(nil, nil, gdbx.FirstDup)
	t.Logf("FirstDup(): v=%q, err=%v", fv, err)

	// Check cursor position after FirstDup
	ck, cv, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("After FirstDup, GetCurrent(): k=%q, v=%q", ck, cv)

	// Second iteration using k from before (Erigon passes k from first Current)
	keys2, values2 := erigonIteration(k, fv)
	t.Logf("Second iteration (after FirstDup): keys=%v, values=%v", keys2, values2)

	if len(keys2) != len(expectedKeys) {
		t.Errorf("Second iteration: expected %d keys, got %d: %v", len(expectedKeys), len(keys2), keys2)
	}
}

// TestFirstDupThenIteration tests that FirstDup doesn't break iteration
func TestFirstDupThenIteration(t *testing.T) {
	// Test with mdbx-go first to establish expected behavior
	dir, err := os.MkdirTemp("", "firstdup-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	runtime.LockOSThread()

	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		runtime.UnlockOSThread()
		t.Fatal(err)
	}

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(dir, mdbx.Create, 0644); err != nil {
		env.Close()
		runtime.UnlockOSThread()
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		env.Close()
		runtime.UnlockOSThread()
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBI("Table", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		env.Close()
		runtime.UnlockOSThread()
		t.Fatal(err)
	}

	// Insert test data
	testData := []struct {
		key, value string
	}{
		{"key", "value1.7"},
		{"key2", "value1.1"},
		{"key2", "value1.2"},
		{"key3", "value1.6"},
	}

	for _, kv := range testData {
		txn.Put(dbi, []byte(kv.key), []byte(kv.value), 0)
	}

	cursor, _ := txn.OpenCursor(dbi)

	// First position with First
	k, v, err := cursor.Get(nil, nil, mdbx.First)
	t.Logf("mdbx First(): k=%q, v=%q, err=%v", k, v, err)

	// Now call FirstDup
	_, fv, err := cursor.Get(nil, nil, mdbx.FirstDup)
	t.Logf("mdbx FirstDup(): v=%q, err=%v", fv, err)

	// Now iterate and collect
	var mdbxKeys, mdbxValues []string
	for k != nil {
		mdbxKeys = append(mdbxKeys, string(k))
		mdbxValues = append(mdbxValues, string(v))
		k, v, err = cursor.Get(nil, nil, mdbx.Next)
		if err != nil {
			break
		}
	}
	t.Logf("mdbx iteration after FirstDup: keys=%v, values=%v", mdbxKeys, mdbxValues)

	cursor.Close()
	txn.Commit()
	env.Close()
	runtime.UnlockOSThread()

	// Now test with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	genv.SetMaxDBs(10)
	if err := genv.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}

	gtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gtxn.Abort()

	gdbi, err := gtxn.OpenDBISimple("Table", 0)
	if err != nil {
		t.Fatal(err)
	}

	gcursor, err := gtxn.OpenCursor(gdbi)
	if err != nil {
		t.Fatal(err)
	}
	defer gcursor.Close()

	// First position
	k, v, err = gcursor.Get(nil, nil, gdbx.First)
	t.Logf("gdbx First(): k=%q, v=%q, err=%v", k, v, err)

	// Call FirstDup
	_, fv, err = gcursor.Get(nil, nil, gdbx.FirstDup)
	t.Logf("gdbx FirstDup(): v=%q, err=%v", fv, err)

	// Now iterate
	var gdbxKeys, gdbxValues []string
	for k != nil {
		gdbxKeys = append(gdbxKeys, string(k))
		gdbxValues = append(gdbxValues, string(v))
		k, v, err = gcursor.Get(nil, nil, gdbx.Next)
		if err != nil {
			break
		}
	}
	t.Logf("gdbx iteration after FirstDup: keys=%v, values=%v", gdbxKeys, gdbxValues)

	// Compare
	if len(mdbxKeys) != len(gdbxKeys) {
		t.Errorf("Length mismatch: mdbx=%d, gdbx=%d", len(mdbxKeys), len(gdbxKeys))
	}

	for i := range mdbxKeys {
		if i >= len(gdbxKeys) {
			break
		}
		if mdbxKeys[i] != gdbxKeys[i] {
			t.Errorf("key[%d]: mdbx=%q, gdbx=%q", i, mdbxKeys[i], gdbxKeys[i])
		}
		if mdbxValues[i] != gdbxValues[i] {
			t.Errorf("value[%d]: mdbx=%q, gdbx=%q", i, mdbxValues[i], gdbxValues[i])
		}
	}
}
