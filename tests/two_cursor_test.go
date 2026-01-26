// Package tests tests for multi-cursor behavior
package tests

import (
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestTwoCursors tests behavior when two cursors are open on the same table
// This replicates Erigon's pattern where BaseCase creates a cursor, and TestNextDups creates another
func TestTwoCursors(t *testing.T) {
	t.Log("=== Running with mdbx-go ===")
	mdbxKeys, mdbxValues := runTwoCursorsWithMdbx(t)

	t.Log("=== Running with gdbx ===")
	gdbxKeys, gdbxValues := runTwoCursorsWithGdbx(t)

	// Compare
	t.Logf("mdbx keys: %v", mdbxKeys)
	t.Logf("gdbx keys: %v", gdbxKeys)
	t.Logf("mdbx values: %v", mdbxValues)
	t.Logf("gdbx values: %v", gdbxValues)

	if len(mdbxKeys) != len(gdbxKeys) {
		t.Errorf("Key count mismatch: mdbx=%d, gdbx=%d", len(mdbxKeys), len(gdbxKeys))
	}
	for i := range mdbxKeys {
		if i >= len(gdbxKeys) {
			break
		}
		if mdbxKeys[i] != gdbxKeys[i] {
			t.Errorf("Keys[%d]: mdbx=%q, gdbx=%q", i, mdbxKeys[i], gdbxKeys[i])
		}
	}
}

func runTwoCursorsWithMdbx(t *testing.T) ([]string, []string) {
	dir := t.TempDir()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	env, _ := mdbx.NewEnv(mdbx.Label("test"))
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)
	env.Open(dir, mdbx.Create, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, 0)
	defer txn.Abort()

	dbi, _ := txn.OpenDBI("Table", mdbx.Create|mdbx.DupSort, nil, nil)

	// === Create FIRST cursor (like BaseCase does) ===
	cursor1, _ := txn.OpenCursor(dbi)
	defer cursor1.Close()

	// Use cursor1 to insert BaseCase data
	cursor1.Put([]byte("key1"), []byte("value1.1"), 0)
	cursor1.Put([]byte("key3"), []byte("value3.1"), 0)
	cursor1.Put([]byte("key1"), []byte("value1.3"), 0)
	cursor1.Put([]byte("key3"), []byte("value3.3"), 0)

	t.Log("mdbx: First cursor inserted base data")

	// === Create SECOND cursor (like TestNextDups does) ===
	cursor2, _ := txn.OpenCursor(dbi)
	defer cursor2.Close()

	t.Log("mdbx: Second cursor created")

	// DeleteExact using cursor2
	for _, kv := range []struct{ k, v string }{
		{"key1", "value1.1"},
		{"key1", "value1.3"},
		{"key3", "value3.1"},
		{"key3", "value3.3"},
	} {
		_, _, err := cursor2.Get([]byte(kv.k), []byte(kv.v), mdbx.GetBoth)
		if err == nil {
			cursor2.Del(0)
			t.Logf("mdbx: cursor2 deleted %s:%s", kv.k, kv.v)
		}
	}

	// Put new data using both txn.Put and cursor2.Put (like Erigon)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor2.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor2.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor2.Put([]byte("key"), []byte("value1.7"), 0)

	t.Log("mdbx: New data inserted via cursor2")

	// Current() on cursor2
	k, v, _ := cursor2.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx: cursor2.Current() -> k=%q, v=%q", k, v)

	// First iteration
	var keys []string
	var values []string
	i := 0
	for kk, vv := k, v; kk != nil; {
		keys = append(keys, string(kk))
		values = append(values, string(vv))
		t.Logf("mdbx: iter1 entry[%d]: k=%q, v=%q", i, kk, vv)
		i++
		kk, vv, _ = cursor2.Get(nil, nil, mdbx.Next)
	}
	t.Logf("mdbx: First iteration collected %d entries", i)

	// Prev to restore
	for ind := i; ind > 1; ind-- {
		cursor2.Get(nil, nil, mdbx.Prev)
	}

	// FirstDup
	_, fdv, _ := cursor2.Get(nil, nil, mdbx.FirstDup)
	t.Logf("mdbx: FirstDup() -> v=%q", fdv)

	// Check current after FirstDup
	ck, cv, _ := cursor2.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx: Current() after FirstDup -> k=%q, v=%q", ck, cv)

	// Second iteration
	var keys2 []string
	var values2 []string
	j := 0
	for kk, vv := k, fdv; kk != nil; {
		keys2 = append(keys2, string(kk))
		values2 = append(values2, string(vv))
		t.Logf("mdbx: iter2 entry[%d]: k=%q, v=%q", j, kk, vv)
		j++
		kk, vv, _ = cursor2.Get(nil, nil, mdbx.Next)
	}
	t.Logf("mdbx: Second iteration collected %d entries: keys=%v", j, keys2)

	return keys2, values2
}

func runTwoCursorsWithGdbx(t *testing.T) ([]string, []string) {
	dir := t.TempDir()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, gdbx.Create, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadWrite)
	defer txn.Abort()

	dbi, _ := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)

	// === Create FIRST cursor (like BaseCase does) ===
	cursor1, _ := txn.OpenCursor(dbi)
	defer cursor1.Close()

	// Use cursor1 to insert BaseCase data
	cursor1.Put([]byte("key1"), []byte("value1.1"), 0)
	cursor1.Put([]byte("key3"), []byte("value3.1"), 0)
	cursor1.Put([]byte("key1"), []byte("value1.3"), 0)
	cursor1.Put([]byte("key3"), []byte("value3.3"), 0)

	t.Log("gdbx: First cursor inserted base data")

	// === Create SECOND cursor (like TestNextDups does) ===
	cursor2, _ := txn.OpenCursor(dbi)
	defer cursor2.Close()

	t.Log("gdbx: Second cursor created")

	// DeleteExact using cursor2
	for _, kv := range []struct{ k, v string }{
		{"key1", "value1.1"},
		{"key1", "value1.3"},
		{"key3", "value3.1"},
		{"key3", "value3.3"},
	} {
		_, _, err := cursor2.Get([]byte(kv.k), []byte(kv.v), gdbx.GetBoth)
		if err == nil {
			cursor2.Del(0)
			t.Logf("gdbx: cursor2 deleted %s:%s", kv.k, kv.v)
		}
	}

	// Put new data using both txn.Put and cursor2.Put (like Erigon)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor2.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor2.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor2.Put([]byte("key"), []byte("value1.7"), 0)

	t.Log("gdbx: New data inserted via cursor2")

	// Current() on cursor2
	k, v, _ := cursor2.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx: cursor2.Current() -> k=%q, v=%q", k, v)

	// First iteration
	var keys []string
	var values []string
	i := 0
	for kk, vv := k, v; kk != nil; {
		keys = append(keys, string(kk))
		values = append(values, string(vv))
		t.Logf("gdbx: iter1 entry[%d]: k=%q, v=%q", i, kk, vv)
		i++
		kk, vv, _ = cursor2.Get(nil, nil, gdbx.Next)
	}
	t.Logf("gdbx: First iteration collected %d entries", i)

	// Prev to restore
	for ind := i; ind > 1; ind-- {
		cursor2.Get(nil, nil, gdbx.Prev)
	}

	// FirstDup
	_, fdv, _ := cursor2.Get(nil, nil, gdbx.FirstDup)
	t.Logf("gdbx: FirstDup() -> v=%q", fdv)

	// Check current after FirstDup
	ck, cv, _ := cursor2.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx: Current() after FirstDup -> k=%q, v=%q", ck, cv)

	// Second iteration
	var keys2 []string
	var values2 []string
	j := 0
	for kk, vv := k, fdv; kk != nil; {
		keys2 = append(keys2, string(kk))
		values2 = append(values2, string(vv))
		t.Logf("gdbx: iter2 entry[%d]: k=%q, v=%q", j, kk, vv)
		j++
		kk, vv, _ = cursor2.Get(nil, nil, gdbx.Next)
	}
	t.Logf("gdbx: Second iteration collected %d entries: keys=%v", j, keys2)

	return keys2, values2
}
