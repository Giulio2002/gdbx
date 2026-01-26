// Package tests contains reproduction tests for cursor bugs
package tests

import (
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestPrevAfterNextReachesEnd reproduces the bug where Prev() doesn't work
// correctly after Next() reaches the end of the database.
// This is the root cause of Erigon's TestNextDups failure.
func TestPrevAfterNextReachesEnd(t *testing.T) {
	// First test with mdbx-go to get expected behavior
	t.Log("=== mdbx-go behavior ===")
	mdbxResults := runPrevAfterEndWithMdbx(t)

	// Then test with gdbx
	t.Log("=== gdbx behavior ===")
	gdbxResults := runPrevAfterEndWithGdbx(t)

	// Compare
	if mdbxResults.afterNextEndCurrent != gdbxResults.afterNextEndCurrent {
		t.Errorf("After Next()=nil, Current() differs:\n  mdbx: %q\n  gdbx: %q",
			mdbxResults.afterNextEndCurrent, gdbxResults.afterNextEndCurrent)
	}

	for i, mp := range mdbxResults.prevResults {
		if i >= len(gdbxResults.prevResults) {
			t.Errorf("Prev() %d: mdbx returned %q, gdbx missing", i+1, mp)
			continue
		}
		gp := gdbxResults.prevResults[i]
		if mp != gp {
			t.Errorf("Prev() %d differs:\n  mdbx: %q\n  gdbx: %q", i+1, mp, gp)
		}
	}

	if mdbxResults.finalCurrent != gdbxResults.finalCurrent {
		t.Errorf("Final Current() differs:\n  mdbx: %q\n  gdbx: %q",
			mdbxResults.finalCurrent, gdbxResults.finalCurrent)
	}
}

type testResults struct {
	afterNextEndCurrent string
	prevResults         []string
	finalCurrent        string
}

func runPrevAfterEndWithMdbx(t *testing.T) testResults {
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

	// Insert test data: 4 entries across 3 keys
	txn.Put(dbi, []byte("key"), []byte("value1.7"), 0)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	txn.Put(dbi, []byte("key2"), []byte("value1.2"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value1.6"), 0)

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// Iterate through all entries until Next() returns nil
	k, v, _ := cursor.Get(nil, nil, mdbx.First)
	count := 0
	for k != nil {
		t.Logf("mdbx entry %d: k=%q, v=%q", count, k, v)
		count++
		k, v, _ = cursor.Get(nil, nil, mdbx.Next)
	}
	t.Logf("mdbx: collected %d entries, Next() returned nil", count)

	var results testResults

	// Check cursor position after Next() returned nil
	ck, cv, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	results.afterNextEndCurrent = string(ck) + ":" + string(cv)
	t.Logf("mdbx: after Next()=nil, Current() = %q:%q", ck, cv)

	// Call Prev() count-1 times (to get back to first entry)
	for i := 0; i < count-1; i++ {
		pk, pv, _ := cursor.Get(nil, nil, mdbx.Prev)
		results.prevResults = append(results.prevResults, string(pk)+":"+string(pv))
		t.Logf("mdbx: Prev() %d = %q:%q", i+1, pk, pv)
	}

	// Check final position
	fk, fv, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	results.finalCurrent = string(fk) + ":" + string(fv)
	t.Logf("mdbx: final Current() = %q:%q", fk, fv)

	return results
}

// TestPrevAfterEndErigonPattern reproduces the exact Erigon TestNextDups pattern
func TestPrevAfterEndErigonPattern(t *testing.T) {
	t.Log("=== mdbx-go behavior ===")
	mdbxResults := runErigonPatternWithMdbx(t)

	t.Log("=== gdbx behavior ===")
	gdbxResults := runErigonPatternWithGdbx(t)

	// Compare
	if mdbxResults.afterNextEndCurrent != gdbxResults.afterNextEndCurrent {
		t.Errorf("After Next()=nil, Current() differs:\n  mdbx: %q\n  gdbx: %q",
			mdbxResults.afterNextEndCurrent, gdbxResults.afterNextEndCurrent)
	}

	for i, mp := range mdbxResults.prevResults {
		if i >= len(gdbxResults.prevResults) {
			t.Errorf("Prev() %d: mdbx returned %q, gdbx missing", i+1, mp)
			continue
		}
		gp := gdbxResults.prevResults[i]
		if mp != gp {
			t.Errorf("Prev() %d differs:\n  mdbx: %q\n  gdbx: %q", i+1, mp, gp)
		}
	}

	if mdbxResults.finalCurrent != gdbxResults.finalCurrent {
		t.Errorf("Final Current() differs:\n  mdbx: %q\n  gdbx: %q",
			mdbxResults.finalCurrent, gdbxResults.finalCurrent)
	}
}

func runErigonPatternWithMdbx(t *testing.T) testResults {
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

	// First insert BaseCase data (like Erigon's BaseCase)
	txn.Put(dbi, []byte("key1"), []byte("value1.1"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.1"), 0)
	txn.Put(dbi, []byte("key1"), []byte("value1.3"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.3"), 0)

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact all base data (like Erigon's TestNextDups)
	baseData := []struct{ k, v string }{
		{"key1", "value1.1"},
		{"key1", "value1.3"},
		{"key3", "value3.1"},
		{"key3", "value3.3"},
	}
	for _, kv := range baseData {
		_, _, err := cursor.Get([]byte(kv.k), []byte(kv.v), mdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Now insert new data (like Erigon's test)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0) // via txn
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0) // Last - cursor should be here

	// Get current position (should be at "key" after last Put)
	k, v, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx: after puts, Current() = %q:%q", k, v)

	// Iterate from current position (like Erigon's iteration function)
	count := 0
	for k != nil {
		t.Logf("mdbx entry %d: k=%q, v=%q", count, k, v)
		count++
		k, v, _ = cursor.Get(nil, nil, mdbx.Next)
	}
	t.Logf("mdbx: collected %d entries, Next() returned nil", count)

	var results testResults

	// Check cursor position after Next() returned nil
	ck, cv, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	results.afterNextEndCurrent = string(ck) + ":" + string(cv)
	t.Logf("mdbx: after Next()=nil, Current() = %q:%q", ck, cv)

	// Call Prev() count-1 times (to get back to first entry)
	for i := 0; i < count-1; i++ {
		pk, pv, _ := cursor.Get(nil, nil, mdbx.Prev)
		results.prevResults = append(results.prevResults, string(pk)+":"+string(pv))
		t.Logf("mdbx: Prev() %d = %q:%q", i+1, pk, pv)
	}

	// Check final position
	fk, fv, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	results.finalCurrent = string(fk) + ":" + string(fv)
	t.Logf("mdbx: final Current() = %q:%q", fk, fv)

	return results
}

func runErigonPatternWithGdbx(t *testing.T) testResults {
	dir := t.TempDir()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, gdbx.Create, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadWrite)
	defer txn.Abort()

	dbi, _ := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)

	// First insert BaseCase data (like Erigon's BaseCase)
	txn.Put(dbi, []byte("key1"), []byte("value1.1"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.1"), 0)
	txn.Put(dbi, []byte("key1"), []byte("value1.3"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.3"), 0)

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact all base data (like Erigon's TestNextDups)
	baseData := []struct{ k, v string }{
		{"key1", "value1.1"},
		{"key1", "value1.3"},
		{"key3", "value3.1"},
		{"key3", "value3.3"},
	}
	for _, kv := range baseData {
		_, _, err := cursor.Get([]byte(kv.k), []byte(kv.v), gdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Now insert new data (like Erigon's test)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0) // via txn
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0) // Last - cursor should be here

	// Get current position (should be at "key" after last Put)
	k, v, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx: after puts, Current() = %q:%q", k, v)

	// Iterate from current position (like Erigon's iteration function)
	count := 0
	for k != nil {
		t.Logf("gdbx entry %d: k=%q, v=%q", count, k, v)
		count++
		k, v, _ = cursor.Get(nil, nil, gdbx.Next)
	}
	t.Logf("gdbx: collected %d entries, Next() returned nil", count)

	var results testResults

	// Check cursor position after Next() returned nil
	ck, cv, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	results.afterNextEndCurrent = string(ck) + ":" + string(cv)
	t.Logf("gdbx: after Next()=nil, Current() = %q:%q", ck, cv)

	// Call Prev() count-1 times (to get back to first entry)
	for i := 0; i < count-1; i++ {
		pk, pv, _ := cursor.Get(nil, nil, gdbx.Prev)
		results.prevResults = append(results.prevResults, string(pk)+":"+string(pv))
		t.Logf("gdbx: Prev() %d = %q:%q", i+1, pk, pv)
	}

	// Check final position
	fk, fv, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	results.finalCurrent = string(fk) + ":" + string(fv)
	t.Logf("gdbx: final Current() = %q:%q", fk, fv)

	return results
}

func runPrevAfterEndWithGdbx(t *testing.T) testResults {
	dir := t.TempDir()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, gdbx.Create, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadWrite)
	defer txn.Abort()

	dbi, _ := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)

	// Insert test data: 4 entries across 3 keys
	txn.Put(dbi, []byte("key"), []byte("value1.7"), 0)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	txn.Put(dbi, []byte("key2"), []byte("value1.2"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value1.6"), 0)

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// Iterate through all entries until Next() returns nil
	k, v, _ := cursor.Get(nil, nil, gdbx.First)
	count := 0
	for k != nil {
		t.Logf("gdbx entry %d: k=%q, v=%q", count, k, v)
		count++
		k, v, _ = cursor.Get(nil, nil, gdbx.Next)
	}
	t.Logf("gdbx: collected %d entries, Next() returned nil", count)

	var results testResults

	// Check cursor position after Next() returned nil
	ck, cv, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	results.afterNextEndCurrent = string(ck) + ":" + string(cv)
	t.Logf("gdbx: after Next()=nil, Current() = %q:%q", ck, cv)

	// Call Prev() count-1 times (to get back to first entry)
	for i := 0; i < count-1; i++ {
		pk, pv, _ := cursor.Get(nil, nil, gdbx.Prev)
		results.prevResults = append(results.prevResults, string(pk)+":"+string(pv))
		t.Logf("gdbx: Prev() %d = %q:%q", i+1, pk, pv)
	}

	// Check final position
	fk, fv, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	results.finalCurrent = string(fk) + ":" + string(fv)
	t.Logf("gdbx: final Current() = %q:%q", fk, fv)

	return results
}
