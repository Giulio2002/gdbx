package tests

import (
	"fmt"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestMetadataCompare compares database metadata created by gdbx vs libmdbx
func TestMetadataCompare(t *testing.T) {
	// Create database with gdbx
	db1 := newTestDB(t)
	defer db1.cleanup()

	gdbxEnv, _ := gdbx.NewEnv(gdbx.Default)
	gdbxEnv.SetMaxDBs(10)
	gdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	gdbxEnv.Open(db1.path, 0, 0644)
	gdbxTxn, _ := gdbxEnv.BeginTxn(nil, 0)
	gdbxDbi, _ := gdbxTxn.OpenDBISimple("testdb", gdbx.Create|gdbx.DupSort)
	gdbxTxn.Put(gdbxDbi, []byte("key"), []byte("val"), 0)
	gdbxTxn.Commit()

	// Read the main database record
	gdbxTxn, _ = gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	mainCursor, _ := gdbxTxn.OpenCursor(gdbx.MainDBI)
	k, v, err := mainCursor.Get([]byte("testdb"), nil, gdbx.Set)
	if err != nil {
		t.Fatalf("gdbx main lookup: %v", err)
	}
	fmt.Printf("gdbx main db entry for 'testdb':\n")
	fmt.Printf("  key: %q (len=%d)\n", k, len(k))
	fmt.Printf("  value (len=%d):\n", len(v))
	for i := 0; i < len(v); i += 16 {
		fmt.Printf("    %3d: ", i)
		for j := i; j < i+16 && j < len(v); j++ {
			fmt.Printf("%02x ", v[j])
		}
		fmt.Println()
	}
	mainCursor.Close()
	gdbxTxn.Abort()
	gdbxEnv.Close()

	// Create similar database with libmdbx
	db2 := newTestDB(t)
	defer db2.cleanup()

	mdbxEnv, _ := mdbx.NewEnv(mdbx.Label("test"))
	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mdbxEnv.SetOption(mdbx.OptMaxDB, 10)
	mdbxEnv.Open(db2.path, mdbx.Create, 0644)
	mdbxTxn, _ := mdbxEnv.BeginTxn(nil, 0)
	mdbxDbi, _ := mdbxTxn.OpenDBI("testdb", mdbx.Create|mdbx.DupSort, nil, nil)
	mdbxTxn.Put(mdbxDbi, []byte("key"), []byte("val"), 0)
	mdbxTxn.Commit()

	// Read main db record with libmdbx
	mdbxTxn, _ = mdbxEnv.BeginTxn(nil, mdbx.Readonly)
	// First list all named databases via the testdb database handle
	testDbi, err := mdbxTxn.OpenDBI("testdb", mdbx.DupSort, nil, nil)
	if err != nil {
		t.Logf("Opening testdb: %v", err)
	}
	_ = testDbi

	// Try DBI 0 (main database) - need to iterate differently
	// The main/unnamed database uses DBI 1 in libmdbx
	mainCursor2, err := mdbxTxn.OpenCursor(1) // DBI 1 is the main database
	if err != nil {
		t.Fatalf("OpenCursor on main db: %v", err)
	}

	fmt.Printf("\nlibmdbx main db entries:\n")
	var k2, v2 []byte
	k2, v2, err = mainCursor2.Get(nil, nil, mdbx.First)
	found := false
	for err == nil {
		fmt.Printf("  key: %q (len=%d), value_len=%d\n", k2, len(k2), len(v2))
		if string(k2) == "testdb" {
			found = true
			break
		}
		k2, v2, err = mainCursor2.Get(nil, nil, mdbx.Next)
	}
	if !found {
		if err != nil && !mdbx.IsNotFound(err) {
			t.Fatalf("libmdbx main lookup error: %v", err)
		}
		t.Fatal("testdb not found in libmdbx main db")
	}
	fmt.Printf("\nlibmdbx main db entry for 'testdb':\n")
	fmt.Printf("  key: %q (len=%d)\n", k2, len(k2))
	fmt.Printf("  value (len=%d):\n", len(v2))
	for i := 0; i < len(v2); i += 16 {
		fmt.Printf("    %3d: ", i)
		for j := i; j < i+16 && j < len(v2); j++ {
			fmt.Printf("%02x ", v2[j])
		}
		fmt.Println()
	}
	mainCursor2.Close()
	mdbxTxn.Abort()
	mdbxEnv.Close()
}
