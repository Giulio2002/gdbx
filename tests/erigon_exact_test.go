// Package tests contains exact reproduction of Erigon's TestNextDups
package tests

import (
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestErigonExact exactly replicates Erigon's TestNextDups pattern
func TestErigonExact(t *testing.T) {
	t.Log("=== Running with mdbx-go ===")
	mdbxKeys1, mdbxValues1, mdbxKeys2, mdbxValues2 := runErigonExactWithMdbx(t)

	t.Log("=== Running with gdbx ===")
	gdbxKeys1, gdbxValues1, gdbxKeys2, gdbxValues2 := runErigonExactWithGdbx(t)

	// Compare first iteration
	t.Logf("mdbx iteration1 keys: %v", mdbxKeys1)
	t.Logf("gdbx iteration1 keys: %v", gdbxKeys1)
	if len(mdbxKeys1) != len(gdbxKeys1) {
		t.Errorf("Iteration1 key count: mdbx=%d, gdbx=%d", len(mdbxKeys1), len(gdbxKeys1))
	}
	for i := range mdbxKeys1 {
		if i >= len(gdbxKeys1) {
			break
		}
		if mdbxKeys1[i] != gdbxKeys1[i] {
			t.Errorf("Iteration1 keys[%d]: mdbx=%q, gdbx=%q", i, mdbxKeys1[i], gdbxKeys1[i])
		}
	}

	// Compare second iteration (after FirstDup)
	t.Logf("mdbx iteration2 keys: %v", mdbxKeys2)
	t.Logf("gdbx iteration2 keys: %v", gdbxKeys2)
	if len(mdbxKeys2) != len(gdbxKeys2) {
		t.Errorf("Iteration2 key count: mdbx=%d, gdbx=%d", len(mdbxKeys2), len(gdbxKeys2))
	}
	for i := range mdbxKeys2 {
		if i >= len(gdbxKeys2) {
			break
		}
		if mdbxKeys2[i] != gdbxKeys2[i] {
			t.Errorf("Iteration2 keys[%d]: mdbx=%q, gdbx=%q", i, mdbxKeys2[i], gdbxKeys2[i])
		}
	}

	// Also compare values
	t.Logf("mdbx iteration1 values: %v", mdbxValues1)
	t.Logf("gdbx iteration1 values: %v", gdbxValues1)
	t.Logf("mdbx iteration2 values: %v", mdbxValues2)
	t.Logf("gdbx iteration2 values: %v", gdbxValues2)
}

// iterationMdbx replicates Erigon's iteration function exactly
func iterationMdbx(t *testing.T, cursor *mdbx.Cursor, start []byte, val []byte) ([]string, []string) {
	t.Helper()
	var keys []string
	var values []string
	i := 0
	for k, v := start, val; k != nil; {
		t.Logf("mdbx iteration entry[%d]: k=%q, v=%q", i, k, v)
		keys = append(keys, string(k))
		values = append(values, string(v))
		i++
		k, v, _ = cursor.Get(nil, nil, mdbx.Next)
		t.Logf("mdbx iteration Next() -> k=%q, v=%q", k, v)
	}
	// Restore position with Prev calls
	for ind := i; ind > 1; ind-- {
		pk, pv, _ := cursor.Get(nil, nil, mdbx.Prev)
		t.Logf("mdbx iteration Prev() -> k=%q, v=%q", pk, pv)
	}
	return keys, values
}

// iterationGdbx replicates Erigon's iteration function exactly
func iterationGdbx(t *testing.T, cursor *gdbx.Cursor, start []byte, val []byte) ([]string, []string) {
	t.Helper()
	var keys []string
	var values []string
	i := 0
	for k, v := start, val; k != nil; {
		t.Logf("gdbx iteration entry[%d]: k=%q, v=%q", i, k, v)
		keys = append(keys, string(k))
		values = append(values, string(v))
		i++
		k, v, _ = cursor.Get(nil, nil, gdbx.Next)
		t.Logf("gdbx iteration Next() -> k=%q, v=%q", k, v)
	}
	// Restore position with Prev calls
	for ind := i; ind > 1; ind-- {
		pk, pv, _ := cursor.Get(nil, nil, gdbx.Prev)
		t.Logf("gdbx iteration Prev() -> k=%q, v=%q", pk, pv)
	}
	return keys, values
}

func runErigonExactWithMdbx(t *testing.T) ([]string, []string, []string, []string) {
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

	// BaseCase data (like Erigon's BaseCase)
	txn.Put(dbi, []byte("key1"), []byte("value1.1"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.1"), 0)
	txn.Put(dbi, []byte("key1"), []byte("value1.3"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.3"), 0)

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact (like Erigon's TestNextDups lines 537-540)
	for _, kv := range []struct{ k, v string }{
		{"key1", "value1.1"},
		{"key1", "value1.3"},
		{"key3", "value3.1"},
		{"key3", "value3.3"},
	} {
		_, _, err := cursor.Get([]byte(kv.k), []byte(kv.v), mdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Put new data (like Erigon's TestNextDups lines 542-545)
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0)

	// Line 547: k, v, err := c.Current()
	k, v, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx: Current() after puts -> k=%q, v=%q", k, v)

	// Line 549: keys, values := iteration(t, c, k, v)
	keys1, values1 := iterationMdbx(t, cursor, k, v)
	t.Logf("mdbx: First iteration collected: keys=%v, values=%v", keys1, values1)

	// Check cursor position after first iteration
	ck, cv, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx: Current() after first iteration -> k=%q, v=%q", ck, cv)

	// Line 553: v, err = c.FirstDup()
	_, fdv, _ := cursor.Get(nil, nil, mdbx.FirstDup)
	t.Logf("mdbx: FirstDup() -> v=%q", fdv)

	// Check cursor position after FirstDup
	ck2, cv2, _ := cursor.Get(nil, nil, mdbx.GetCurrent)
	t.Logf("mdbx: Current() after FirstDup -> k=%q, v=%q", ck2, cv2)

	// Line 555: keys, values = iteration(t, c, k, v) - NOTE: uses original k from line 547!
	keys2, values2 := iterationMdbx(t, cursor, k, fdv)
	t.Logf("mdbx: Second iteration collected: keys=%v, values=%v", keys2, values2)

	return keys1, values1, keys2, values2
}

func runErigonExactWithGdbx(t *testing.T) ([]string, []string, []string, []string) {
	dir := t.TempDir()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dir, gdbx.Create, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadWrite)
	defer txn.Abort()

	dbi, _ := txn.OpenDBISimple("Table", gdbx.Create|gdbx.DupSort)

	// BaseCase data
	txn.Put(dbi, []byte("key1"), []byte("value1.1"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.1"), 0)
	txn.Put(dbi, []byte("key1"), []byte("value1.3"), 0)
	txn.Put(dbi, []byte("key3"), []byte("value3.3"), 0)

	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// DeleteExact
	for _, kv := range []struct{ k, v string }{
		{"key1", "value1.1"},
		{"key1", "value1.3"},
		{"key3", "value3.1"},
		{"key3", "value3.3"},
	} {
		_, _, err := cursor.Get([]byte(kv.k), []byte(kv.v), gdbx.GetBoth)
		if err == nil {
			cursor.Del(0)
		}
	}

	// Put new data
	txn.Put(dbi, []byte("key2"), []byte("value1.1"), 0)
	cursor.Put([]byte("key2"), []byte("value1.2"), 0)
	cursor.Put([]byte("key3"), []byte("value1.6"), 0)
	cursor.Put([]byte("key"), []byte("value1.7"), 0)

	// Current after puts
	k, v, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx: Current() after puts -> k=%q, v=%q", k, v)

	// First iteration
	keys1, values1 := iterationGdbx(t, cursor, k, v)
	t.Logf("gdbx: First iteration collected: keys=%v, values=%v", keys1, values1)

	// Check cursor position after first iteration
	ck, cv, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx: Current() after first iteration -> k=%q, v=%q", ck, cv)

	// FirstDup
	_, fdv, _ := cursor.Get(nil, nil, gdbx.FirstDup)
	t.Logf("gdbx: FirstDup() -> v=%q", fdv)

	// Check cursor position after FirstDup
	ck2, cv2, _ := cursor.Get(nil, nil, gdbx.GetCurrent)
	t.Logf("gdbx: Current() after FirstDup -> k=%q, v=%q", ck2, cv2)

	// Second iteration with original k and FirstDup's v
	keys2, values2 := iterationGdbx(t, cursor, k, fdv)
	t.Logf("gdbx: Second iteration collected: keys=%v, values=%v", keys2, values2)

	return keys1, values1, keys2, values2
}
