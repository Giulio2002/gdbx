package tests

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

func TestDupSortBasic(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

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

	// Create a DUPSORT database
	dbi, err := txn.OpenDBI("dupsort_test", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert multiple values for the same key
	key := []byte("key1")
	values := []string{"value1", "value2", "value3", "value4", "value5"}
	for _, v := range values {
		if err := txn.Put(dbi, key, []byte(v), 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	// Also insert another key with duplicates
	key2 := []byte("key2")
	values2 := []string{"a", "b", "c"}
	for _, v := range values2 {
		if err := txn.Put(dbi, key2, []byte(v), 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify with libmdbx
	fmt.Println("=== libmdbx iteration ===")
	txn, _ = env.BeginTxn(nil, mdbx.Readonly)
	cursor, _ := txn.OpenCursor(dbi)
	for {
		k, v, err := cursor.Get(nil, nil, mdbx.Next)
		if mdbx.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("  %q => %q\n", k, v)
	}
	cursor.Close()
	txn.Abort()
	env.Close()

	// Read with gdbx
	fmt.Println("\n=== gdbx iteration ===")
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

	gdbxDbi, err := gdbxTxn.OpenDBISimple("dupsort_test", 0)
	if err != nil {
		t.Fatalf("OpenDBI error: %v", err)
	}

	// Debug: check tree flags
	tree := gdbxTxn.GetTree(gdbxDbi)
	fmt.Printf("gdbx tree flags: 0x%04x (DupSort=%v)\n", tree.Flags, tree.Flags&uint16(gdbx.DupSort) != 0)

	gdbxCursor, err := gdbxTxn.OpenCursor(gdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxCursor.Close()

	count := 0
	k, v, err := gdbxCursor.Get(nil, nil, gdbx.First)
	for err == nil {
		fmt.Printf("  %q => %q\n", k, v)
		count++
		k, v, err = gdbxCursor.Get(nil, nil, gdbx.Next)
	}
	if !gdbx.IsNotFound(err) {
		t.Fatalf("unexpected error: %v", err)
	}

	// We should have 8 total key-value pairs (5 + 3)
	expectedCount := len(values) + len(values2)
	if count != expectedCount {
		t.Errorf("got %d entries, want %d", count, expectedCount)
	}
}

func TestDupSortOperations(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

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

	dbi, err := txn.OpenDBI("dupsort_test", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert values
	for _, k := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			v := fmt.Sprintf("val%d", i)
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	env.Close()

	// Test gdbx DUPSORT operations
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

	gdbxDbi, err := gdbxTxn.OpenDBISimple("dupsort_test", 0)
	if err != nil {
		t.Fatalf("OpenDBI error: %v", err)
	}

	cursor, err := gdbxTxn.OpenCursor(gdbxDbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Test FirstDup
	fmt.Println("=== Testing FirstDup ===")
	k, v, err := cursor.Get([]byte("b"), nil, gdbx.Set)
	if err != nil {
		t.Logf("Set to 'b' error: %v", err)
	} else {
		fmt.Printf("Set: %q => %q\n", k, v)

		// Try FirstDup
		k, v, err = cursor.Get(nil, nil, gdbx.FirstDup)
		if err != nil {
			t.Logf("FirstDup error: %v (expected - not implemented)", err)
		} else {
			fmt.Printf("FirstDup: %q => %q\n", k, v)
		}

		// Try NextDup
		k, v, err = cursor.Get(nil, nil, gdbx.NextDup)
		if err != nil {
			t.Logf("NextDup error: %v (expected - not implemented)", err)
		} else {
			fmt.Printf("NextDup: %q => %q\n", k, v)
		}
	}

	// Test that regular Next still works
	fmt.Println("\n=== Testing regular Next through all entries ===")
	cursor.Get(nil, nil, gdbx.First)
	count := 0
	k, v, err = cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		fmt.Printf("  %q => %q\n", k, v)
		count++
		k, v, err = cursor.Get(nil, nil, gdbx.Next)
	}
	fmt.Printf("Total entries: %d\n", count)
}
