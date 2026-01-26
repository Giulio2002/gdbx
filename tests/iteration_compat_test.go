// Package tests contains compatibility tests for iteration patterns.
// These tests ensure gdbx behaves identically to libmdbx for cursor operations.
package tests

import (
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// iterationHelper performs the iteration pattern used in Erigon tests:
// iterate from start, collecting keys/values, then restore cursor via Prev calls.
// Returns collected keys and values.
func gdbxIteration(t *testing.T, c *gdbx.Cursor, startK, startV []byte) ([]string, []string) {
	t.Helper()
	var keys []string
	var values []string

	k, v := startK, startV
	i := 0
	for k != nil {
		keys = append(keys, string(k))
		values = append(values, string(v))
		i++
		var err error
		k, v, err = c.Get(nil, nil, gdbx.Next)
		if err != nil {
			break
		}
	}

	// Restore cursor position via Prev calls (matches Erigon's iteration helper)
	for ind := i; ind > 1; ind-- {
		c.Get(nil, nil, gdbx.Prev)
	}

	return keys, values
}

// mdbxIteration performs the same iteration pattern with mdbx-go.
func mdbxIteration(t *testing.T, c *mdbx.Cursor, startK, startV []byte) ([]string, []string) {
	t.Helper()
	var keys []string
	var values []string

	k, v := startK, startV
	i := 0
	for k != nil {
		keys = append(keys, string(k))
		values = append(values, string(v))
		i++
		var err error
		k, v, err = c.Get(nil, nil, mdbx.Next)
		if err != nil {
			break
		}
	}

	// Restore cursor position via Prev calls
	for ind := i; ind > 1; ind-- {
		c.Get(nil, nil, mdbx.Prev)
	}

	return keys, values
}

// TestIterationPatternCompat tests that gdbx and mdbx-go produce identical results
// for the iteration + Prev restore + FirstDup/NextNoDup pattern used in Erigon.
func TestIterationPatternCompat(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Test data: key with single value, key2 with 2 values, key3 with single value
	testData := []struct {
		key, value string
	}{
		{"key", "value1.7"},
		{"key2", "value1.1"},
		{"key2", "value1.2"},
		{"key3", "value1.6"},
	}

	// Create database with libmdbx
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	menv.SetOption(mdbx.OptMaxDB, 10)

	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		menv.Close()
		t.Fatal(err)
	}

	// Insert test data with libmdbx
	mtxn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		menv.Close()
		t.Fatal(err)
	}

	mdbi, err := mtxn.OpenDBI("Table", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		mtxn.Abort()
		menv.Close()
		t.Fatal(err)
	}

	for _, kv := range testData {
		if err := mtxn.Put(mdbi, []byte(kv.key), []byte(kv.value), 0); err != nil {
			mtxn.Abort()
			menv.Close()
			t.Fatal(err)
		}
	}

	mtxn.Commit()

	// Open read-only txn for mdbx comparison
	mtxn, err = menv.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		menv.Close()
		t.Fatal(err)
	}
	defer mtxn.Abort()

	mdbi, err = mtxn.OpenDBI("Table", 0, nil, nil)
	if err != nil {
		menv.Close()
		t.Fatal(err)
	}

	mcursor, err := mtxn.OpenCursor(mdbi)
	if err != nil {
		menv.Close()
		t.Fatal(err)
	}
	defer mcursor.Close()

	// Open with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	if err := genv.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}

	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
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

	// Test 1: Basic iteration from First
	t.Run("BasicIteration", func(t *testing.T) {
		mk, mv, _ := mcursor.Get(nil, nil, mdbx.First)
		gk, gv, _ := gcursor.Get(nil, nil, gdbx.First)

		mkeys, mvals := mdbxIteration(t, mcursor, mk, mv)
		gkeys, gvals := gdbxIteration(t, gcursor, gk, gv)

		if len(mkeys) != len(gkeys) {
			t.Errorf("BasicIteration: mdbx got %d entries, gdbx got %d", len(mkeys), len(gkeys))
		}
		for i := range mkeys {
			if i < len(gkeys) && mkeys[i] != gkeys[i] {
				t.Errorf("BasicIteration key[%d]: mdbx=%q, gdbx=%q", i, mkeys[i], gkeys[i])
			}
			if i < len(gvals) && mvals[i] != gvals[i] {
				t.Errorf("BasicIteration val[%d]: mdbx=%q, gdbx=%q", i, mvals[i], gvals[i])
			}
		}
		t.Logf("BasicIteration: mdbx=%v, gdbx=%v", mkeys, gkeys)
	})

	// Test 2: FirstDup then iteration (same pattern as Erigon TestNextDups line 556)
	t.Run("FirstDupThenIteration", func(t *testing.T) {
		// Position at first entry
		mk, mv, _ := mcursor.Get(nil, nil, mdbx.First)
		gk, gv, _ := gcursor.Get(nil, nil, gdbx.First)

		// First iteration
		mkeys1, _ := mdbxIteration(t, mcursor, mk, mv)
		gkeys1, _ := gdbxIteration(t, gcursor, gk, gv)

		// Call FirstDup
		_, mv, _ = mcursor.Get(nil, nil, mdbx.FirstDup)
		_, gv, _ = gcursor.Get(nil, nil, gdbx.FirstDup)

		// Second iteration with same k from first call
		mkeys2, mvals2 := mdbxIteration(t, mcursor, mk, mv)
		gkeys2, gvals2 := gdbxIteration(t, gcursor, gk, gv)

		if len(mkeys1) != len(gkeys1) {
			t.Errorf("First iteration: mdbx got %d, gdbx got %d", len(mkeys1), len(gkeys1))
		}
		if len(mkeys2) != len(gkeys2) {
			t.Errorf("After FirstDup: mdbx got %d entries, gdbx got %d", len(mkeys2), len(gkeys2))
		}
		for i := range mkeys2 {
			if i < len(gkeys2) && mkeys2[i] != gkeys2[i] {
				t.Errorf("After FirstDup key[%d]: mdbx=%q, gdbx=%q", i, mkeys2[i], gkeys2[i])
			}
			if i < len(gvals2) && mvals2[i] != gvals2[i] {
				t.Errorf("After FirstDup val[%d]: mdbx=%q, gdbx=%q", i, mvals2[i], gvals2[i])
			}
		}
		t.Logf("FirstDupThenIteration: mdbx=%v, gdbx=%v", mkeys2, gkeys2)
	})

	// Test 3: NextNoDup then iteration
	t.Run("NextNoDupThenIteration", func(t *testing.T) {
		// Position at first entry
		mk, mv, _ := mcursor.Get(nil, nil, mdbx.First)
		gk, gv, _ := gcursor.Get(nil, nil, gdbx.First)

		// First iteration
		mdbxIteration(t, mcursor, mk, mv)
		gdbxIteration(t, gcursor, gk, gv)

		// Call NextNoDup
		mk, mv, _ = mcursor.Get(nil, nil, mdbx.NextNoDup)
		gk, gv, _ = gcursor.Get(nil, nil, gdbx.NextNoDup)

		// Iteration after NextNoDup
		mkeys, mvals := mdbxIteration(t, mcursor, mk, mv)
		gkeys, gvals := gdbxIteration(t, gcursor, gk, gv)

		if len(mkeys) != len(gkeys) {
			t.Errorf("After NextNoDup: mdbx got %d entries, gdbx got %d", len(mkeys), len(gkeys))
		}
		for i := range mkeys {
			if i < len(gkeys) && mkeys[i] != gkeys[i] {
				t.Errorf("After NextNoDup key[%d]: mdbx=%q, gdbx=%q", i, mkeys[i], gkeys[i])
			}
			if i < len(gvals) && mvals[i] != gvals[i] {
				t.Errorf("After NextNoDup val[%d]: mdbx=%q, gdbx=%q", i, mvals[i], gvals[i])
			}
		}
		t.Logf("NextNoDupThenIteration: mdbx=%v, gdbx=%v", mkeys, gkeys)
	})

	// Test 4: Multiple iterations with LastDup
	t.Run("LastDupThenIteration", func(t *testing.T) {
		// Position at key2
		mcursor.Get([]byte("key2"), nil, mdbx.Set)
		gcursor.Get([]byte("key2"), nil, gdbx.Set)

		// Call LastDup - note: mdbx-go returns empty key for LastDup, so we use GetCurrent for key
		_, mv, _ := mcursor.Get(nil, nil, mdbx.LastDup)
		_, gv, _ := gcursor.Get(nil, nil, gdbx.LastDup)

		// Get current key after LastDup
		mk, _, _ := mcursor.Get(nil, nil, mdbx.GetCurrent)
		gk, _, _ := gcursor.Get(nil, nil, gdbx.GetCurrent)

		// Iteration after LastDup
		mkeys, mvals := mdbxIteration(t, mcursor, mk, mv)
		gkeys, gvals := gdbxIteration(t, gcursor, gk, gv)

		if len(mkeys) != len(gkeys) {
			t.Errorf("After LastDup: mdbx got %d entries, gdbx got %d", len(mkeys), len(gkeys))
		}
		for i := range mkeys {
			if i < len(gkeys) && mkeys[i] != gkeys[i] {
				t.Errorf("After LastDup key[%d]: mdbx=%q, gdbx=%q", i, mkeys[i], gkeys[i])
			}
			if i < len(gvals) && mvals[i] != gvals[i] {
				t.Errorf("After LastDup val[%d]: mdbx=%q, gdbx=%q", i, mvals[i], gvals[i])
			}
		}
		t.Logf("LastDupThenIteration: mdbx=%v, gdbx=%v", mkeys, gkeys)
	})

	// Test 5: PrevNoDup then iteration
	t.Run("PrevNoDupThenIteration", func(t *testing.T) {
		// Position at last entry
		mk, mv, _ := mcursor.Get(nil, nil, mdbx.Last)
		gk, gv, _ := gcursor.Get(nil, nil, gdbx.Last)

		// Call PrevNoDup
		mk, mv, _ = mcursor.Get(nil, nil, mdbx.PrevNoDup)
		gk, gv, _ = gcursor.Get(nil, nil, gdbx.PrevNoDup)

		// Iteration after PrevNoDup
		mkeys, mvals := mdbxIteration(t, mcursor, mk, mv)
		gkeys, gvals := gdbxIteration(t, gcursor, gk, gv)

		if len(mkeys) != len(gkeys) {
			t.Errorf("After PrevNoDup: mdbx got %d entries, gdbx got %d", len(mkeys), len(gkeys))
		}
		for i := range mkeys {
			if i < len(gkeys) && mkeys[i] != gkeys[i] {
				t.Errorf("After PrevNoDup key[%d]: mdbx=%q, gdbx=%q", i, mkeys[i], gkeys[i])
			}
			if i < len(gvals) && mvals[i] != gvals[i] {
				t.Errorf("After PrevNoDup val[%d]: mdbx=%q, gdbx=%q", i, mvals[i], gvals[i])
			}
		}
		t.Logf("PrevNoDupThenIteration: mdbx=%v, gdbx=%v", mkeys, gkeys)
	})
}

// TestIterationWithMoreDuplicates tests with more duplicates per key.
func TestIterationWithMoreDuplicates(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Test data with more duplicates
	testData := []struct {
		key, value string
	}{
		{"a", "a1"},
		{"a", "a2"},
		{"a", "a3"},
		{"b", "b1"},
		{"b", "b2"},
		{"c", "c1"},
	}

	// Create database with libmdbx
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer menv.Close()

	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	menv.SetOption(mdbx.OptMaxDB, 10)

	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	mtxn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	mdbi, err := mtxn.OpenDBI("Table", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		mtxn.Abort()
		t.Fatal(err)
	}

	for _, kv := range testData {
		if err := mtxn.Put(mdbi, []byte(kv.key), []byte(kv.value), 0); err != nil {
			mtxn.Abort()
			t.Fatal(err)
		}
	}

	mtxn.Commit()

	// Open with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	if err := genv.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}

	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	// Compare with mdbx-go
	mtxn, _ = menv.BeginTxn(nil, mdbx.Readonly)
	defer mtxn.Abort()
	mdbi, _ = mtxn.OpenDBI("Table", 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()

	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	defer gtxn.Abort()
	gdbi, _ := gtxn.OpenDBISimple("Table", 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	defer gcursor.Close()

	// Test iteration from each key
	for _, startKey := range []string{"a", "b", "c"} {
		t.Run("From_"+startKey, func(t *testing.T) {
			mcursor.Get([]byte(startKey), nil, mdbx.Set)
			gcursor.Get([]byte(startKey), nil, gdbx.Set)

			mk, mv, _ := mcursor.Get(nil, nil, mdbx.GetCurrent)
			gk, gv, _ := gcursor.Get(nil, nil, gdbx.GetCurrent)

			mkeys, _ := mdbxIteration(t, mcursor, mk, mv)
			gkeys, _ := gdbxIteration(t, gcursor, gk, gv)

			if len(mkeys) != len(gkeys) {
				t.Errorf("From %s: mdbx got %d, gdbx got %d", startKey, len(mkeys), len(gkeys))
				t.Logf("mdbx: %v", mkeys)
				t.Logf("gdbx: %v", gkeys)
			} else {
				for i := range mkeys {
					if mkeys[i] != gkeys[i] {
						t.Errorf("From %s key[%d]: mdbx=%q, gdbx=%q", startKey, i, mkeys[i], gkeys[i])
					}
				}
			}
		})
	}
}
