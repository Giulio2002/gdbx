package tests

import (
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestGetBothRangeNoMatch tests what happens when GetBothRange
// searches for a value that's higher than all existing values.
// Expected behavior: return nil/NotFound (no value >= search value)
func TestGetBothRangeNoMatch(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create DUPSORT database with libmdbx
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBI("test", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Put key3 -> value3.1 only
	if err := txn.Put(dbi, []byte("key3"), []byte("value3.1"), 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test with libmdbx first
	t.Log("=== Testing libmdbx GetBothRange behavior ===")
	txn, _ = env.BeginTxn(nil, mdbx.Readonly)
	cursor, _ := txn.OpenCursor(dbi)

	// Search for key3 with value >= value3.2
	// Since only value3.1 exists and value3.1 < value3.2, should return NotFound
	k, v, err := cursor.Get([]byte("key3"), []byte("value3.2"), mdbx.GetBothRange)
	t.Logf("libmdbx GetBothRange(key3, value3.2): k=%q, v=%q, err=%v, isNotFound=%v",
		k, v, err, mdbx.IsNotFound(err))

	if !mdbx.IsNotFound(err) {
		t.Errorf("libmdbx: expected NotFound, got k=%q, v=%q, err=%v", k, v, err)
	}

	cursor.Close()
	txn.Abort()
	env.Close()

	// Now test with gdbx
	t.Log("\n=== Testing gdbx GetBothRange behavior ===")
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	if err := gdbxEnv.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}

	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxTxn.Abort()

	gdbxDbi, err := gdbxTxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	gdbxCursor, err := gdbxTxn.OpenCursor(gdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxCursor.Close()

	// Search for key3 with value >= value3.2
	gk, gv, gerr := gdbxCursor.Get([]byte("key3"), []byte("value3.2"), gdbx.GetBothRange)
	t.Logf("gdbx GetBothRange(key3, value3.2): k=%q, v=%q, err=%v, isNotFound=%v",
		gk, gv, gerr, gdbx.IsNotFound(gerr))

	if !gdbx.IsNotFound(gerr) {
		t.Errorf("gdbx: expected NotFound, got k=%q, v=%q, err=%v", gk, gv, gerr)
	}

	// Also test the case where value3.0 is searched (should find value3.1)
	t.Log("\n=== Testing GetBothRange with value that should match ===")
	gk2, gv2, gerr2 := gdbxCursor.Get([]byte("key3"), []byte("value3.0"), gdbx.GetBothRange)
	t.Logf("gdbx GetBothRange(key3, value3.0): k=%q, v=%q, err=%v",
		gk2, gv2, gerr2)

	if gerr2 != nil {
		t.Errorf("gdbx: expected value3.1, got error=%v", gerr2)
	}
	if string(gv2) != "value3.1" {
		t.Errorf("gdbx: expected value3.1, got %q", gv2)
	}
}
