package benchmarks

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// Benchmark DUPSORT cursor operations with large sub-trees
// This exercises the sub-tree descent path which is critical for Erigon

func setupDupsortDB(b *testing.B, numKeys, numDupVals int) (string, func()) {
	dir, err := os.MkdirTemp("", "bench_dupsort_*")
	if err != nil {
		b.Fatal(err)
	}

	dbPath := filepath.Join(dir, "test.db")

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.SetGeometry(-1, -1, 4<<30, -1, -1, 4096)
	env.Open(dbPath, gdbx.NoSubdir, 0644)

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("dupsort", gdbx.Create|gdbx.DupSort)

	key := make([]byte, 32)
	val := make([]byte, 32)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		for j := 0; j < numDupVals; j++ {
			binary.BigEndian.PutUint64(val[:8], uint64(j))
			txn.Put(dbi, key, val, 0)
		}
	}
	_, _ = txn.Commit()
	env.Close()

	return dbPath, func() { os.RemoveAll(dir) }
}

func BenchmarkDupsortNextNoDup_Gdbx(b *testing.B) {
	dbPath, cleanup := setupDupsortDB(b, 1000, 10000)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	defer env.Close()

	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple("dupsort", 0)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	// Warm up
	for i := 0; i < 10; i++ {
		cursor.Get(nil, nil, gdbx.First)
		for {
			_, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
			if err != nil {
				break
			}
		}
	}

	b.ResetTimer()
	count := 0
	for i := 0; i < b.N; i++ {
		cursor.Get(nil, nil, gdbx.First)
		for {
			_, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
			if err != nil {
				break
			}
			count++
		}
	}
	b.ReportMetric(float64(count)/float64(b.N), "keys/iter")
}

func BenchmarkDupsortNextNoDup_Mdbx(b *testing.B) {
	dbPath, cleanup := setupDupsortDB(b, 1000, 10000)
	defer cleanup()

	env, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
	if err != nil {
		b.Fatal(err)
	}
	env.SetOption(mdbxgo.OptMaxDB, 10)
	if err := env.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.Readonly, 0644); err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	txn, err := env.BeginTxn(nil, mdbxgo.Readonly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()
	dbi, err := txn.OpenDBI("dupsort", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Fatal(err)
	}
	defer cursor.Close()

	// Warm up
	for i := 0; i < 10; i++ {
		cursor.Get(nil, nil, mdbxgo.First)
		for {
			_, _, err := cursor.Get(nil, nil, mdbxgo.NextNoDup)
			if err != nil {
				break
			}
		}
	}

	b.ResetTimer()
	count := 0
	for i := 0; i < b.N; i++ {
		cursor.Get(nil, nil, mdbxgo.First)
		for {
			_, _, err := cursor.Get(nil, nil, mdbxgo.NextNoDup)
			if err != nil {
				break
			}
			count++
		}
	}
	b.ReportMetric(float64(count)/float64(b.N), "keys/iter")
}

func BenchmarkDupsortSetFirstDup_Gdbx(b *testing.B) {
	dbPath, cleanup := setupDupsortDB(b, 1000, 10000)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	defer env.Close()

	// Collect keys
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	dbi, _ := txn.OpenDBISimple("dupsort", 0)
	cursor, _ := txn.OpenCursor(dbi)

	keys := make([][]byte, 0, 1000)
	cursor.Get(nil, nil, gdbx.First)
	for i := 0; i < 1000; i++ {
		k, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		keys = append(keys, keyCopy)
	}
	cursor.Close()
	txn.Abort()

	// Benchmark
	txn, _ = env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ = txn.OpenDBISimple("dupsort", 0)
	cursor, _ = txn.OpenCursor(dbi)
	defer cursor.Close()

	// Warm up
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			cursor.Get(key, nil, gdbx.Set)
			cursor.Get(nil, nil, gdbx.FirstDup)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % len(keys)
		cursor.Get(keys[keyIdx], nil, gdbx.Set)
		cursor.Get(nil, nil, gdbx.FirstDup)
	}
}

func BenchmarkDupsortSetFirstDup_Mdbx(b *testing.B) {
	dbPath, cleanup := setupDupsortDB(b, 1000, 10000)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)

	// Collect keys
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	dbi, _ := txn.OpenDBISimple("dupsort", 0)
	cursor, _ := txn.OpenCursor(dbi)

	keys := make([][]byte, 0, 1000)
	cursor.Get(nil, nil, gdbx.First)
	for i := 0; i < 1000; i++ {
		k, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		keys = append(keys, keyCopy)
	}
	cursor.Close()
	txn.Abort()
	env.Close()

	// Benchmark with mdbx-go
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
	if err != nil {
		b.Fatal(err)
	}
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	if err := menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.Readonly, 0644); err != nil {
		b.Fatal(err)
	}
	defer menv.Close()

	mtxn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
	if err != nil {
		b.Fatal(err)
	}
	defer mtxn.Abort()
	mdbi, err := mtxn.OpenDBI("dupsort", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	mcursor, err := mtxn.OpenCursor(mdbi)
	if err != nil {
		b.Fatal(err)
	}
	defer mcursor.Close()

	// Warm up
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			mcursor.Get(key, nil, mdbxgo.Set)
			mcursor.Get(nil, nil, mdbxgo.FirstDup)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % len(keys)
		mcursor.Get(keys[keyIdx], nil, mdbxgo.Set)
		mcursor.Get(nil, nil, mdbxgo.FirstDup)
	}
}

func BenchmarkDupsortSetLastDup_Gdbx(b *testing.B) {
	dbPath, cleanup := setupDupsortDB(b, 1000, 10000)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)
	defer env.Close()

	// Collect keys
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	dbi, _ := txn.OpenDBISimple("dupsort", 0)
	cursor, _ := txn.OpenCursor(dbi)

	keys := make([][]byte, 0, 1000)
	cursor.Get(nil, nil, gdbx.First)
	for i := 0; i < 1000; i++ {
		k, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		keys = append(keys, keyCopy)
	}
	cursor.Close()
	txn.Abort()

	// Benchmark
	txn, _ = env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ = txn.OpenDBISimple("dupsort", 0)
	cursor, _ = txn.OpenCursor(dbi)
	defer cursor.Close()

	// Warm up
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			cursor.Get(key, nil, gdbx.Set)
			cursor.Get(nil, nil, gdbx.LastDup)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % len(keys)
		cursor.Get(keys[keyIdx], nil, gdbx.Set)
		cursor.Get(nil, nil, gdbx.LastDup)
	}
}

func BenchmarkDupsortSetLastDup_Mdbx(b *testing.B) {
	dbPath, cleanup := setupDupsortDB(b, 1000, 10000)
	defer cleanup()

	env, _ := gdbx.NewEnv(gdbx.Default)
	env.SetMaxDBs(10)
	env.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644)

	// Collect keys
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	dbi, _ := txn.OpenDBISimple("dupsort", 0)
	cursor, _ := txn.OpenCursor(dbi)

	keys := make([][]byte, 0, 1000)
	cursor.Get(nil, nil, gdbx.First)
	for i := 0; i < 1000; i++ {
		k, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		keys = append(keys, keyCopy)
	}
	cursor.Close()
	txn.Abort()
	env.Close()

	// Benchmark with mdbx-go
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
	if err != nil {
		b.Fatal(err)
	}
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	if err := menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.Readonly, 0644); err != nil {
		b.Fatal(err)
	}
	defer menv.Close()

	mtxn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
	if err != nil {
		b.Fatal(err)
	}
	defer mtxn.Abort()
	mdbi, err := mtxn.OpenDBI("dupsort", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	mcursor, err := mtxn.OpenCursor(mdbi)
	if err != nil {
		b.Fatal(err)
	}
	defer mcursor.Close()

	// Warm up
	for i := 0; i < 10; i++ {
		for _, key := range keys {
			mcursor.Get(key, nil, mdbxgo.Set)
			mcursor.Get(nil, nil, mdbxgo.LastDup)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % len(keys)
		mcursor.Get(keys[keyIdx], nil, mdbxgo.Set)
		mcursor.Get(nil, nil, mdbxgo.LastDup)
	}
}
