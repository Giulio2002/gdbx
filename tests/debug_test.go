package tests

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

func TestDebugIteration(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Use the same createWithLibmdbx helper as the failing test
	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff"}
	entries := make(map[string]string)
	for _, k := range keys {
		entries[k] = "value-" + k
	}

	createWithLibmdbx(t, db.path, func(txn *mdbx.Txn, dbi mdbx.DBI) {
		for k, v := range entries {
			if err := txn.Put(dbi, []byte(k), []byte(v), 0); err != nil {
				t.Fatal(err)
			}
		}
	})

	// Verify with libmdbx
	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	if err := env.Open(db.path, mdbx.Readonly, 0644); err != nil {
		env.Close()
		t.Fatal(err)
	}
	txn, _ := env.BeginTxn(nil, mdbx.Readonly)
	dbi, _ := txn.OpenRoot(0)

	// Verify with libmdbx
	txn, _ = env.BeginTxn(nil, mdbx.Readonly)
	cursor, _ := txn.OpenCursor(dbi)
	fmt.Println("=== libmdbx iteration ===")
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
	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gdbxTxn, err := gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxTxn.Abort()

	gdbxCursor, err := gdbxTxn.OpenCursor(gdbx.MainDBI)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxCursor.Close()

	fmt.Println("=== gdbx iteration ===")
	k, v, err := gdbxCursor.Get(nil, nil, gdbx.First)
	for err == nil {
		fmt.Printf("  %q => %q\n", k, v)
		k, v, err = gdbxCursor.Get(nil, nil, gdbx.Next)
	}
	if !gdbx.IsNotFound(err) {
		t.Fatal(err)
	}
}
