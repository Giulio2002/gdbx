package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

func TestDebugBenchmarkStructure(t *testing.T) {
	// Create same structure as comparison benchmark
	dir, _ := os.MkdirTemp("", "debug_bench_*")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test.db")

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("dupsort", gdbx.Create|gdbx.DupSort)

	// Same as comparison benchmark: 10000 keys, 100 values each
	numKeys := 10000
	numDupVals := 100
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("dupkey-%06d", i))
		for j := 0; j < numDupVals; j++ {
			val := []byte(fmt.Sprintf("val-%06d", j))
			txn.Put(dbi, key, val, 0)
		}
	}
	_, _ = txn.Commit()
	env.Close()

	// Read back
	env, _ = gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(path, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	defer env.Close()

	txn, _ = env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ = txn.OpenDBISimple("dupsort", 0)

	stat, _ := txn.DBIStat(dbi)
	t.Logf("Main tree: Depth=%d, BranchPages=%d, LeafPages=%d, Entries=%d",
		stat.Depth, stat.BranchPages, stat.LeafPages, stat.Entries)

	// Also test NextNoDup performance in isolation
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	count := 0
	for {
		_, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			break
		}
		count++
	}
	t.Logf("Total keys iterated: %d", count)
}
