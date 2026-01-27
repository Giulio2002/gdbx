package benchmarks

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// BenchmarkDBSizes benchmarks operations at different database sizes
// Run with: go test -bench=BenchmarkDBSizes -benchtime=1s -run=^$ ./tests/
//
// Databases are cached in testdata/benchdb/ to speed up subsequent runs.
// To clear the cache: rm -rf tests/testdata/benchdb/
func BenchmarkDBSizes(b *testing.B) {
	b.Cleanup(CleanupBenchCache)

	sizes := []int{100_000, 1_000_000, 10_000_000}

	for _, size := range sizes {
		sizeName := formatSize(size)
		b.Run(fmt.Sprintf("Plain_%s", sizeName), func(b *testing.B) {
			benchmarkPlainDB(b, size)
		})
	}

	// DUPSORT with varying configurations
	dupConfigs := []struct {
		keys, valsPerKey int
	}{
		{10_000, 10},    // 100k total
		{100_000, 10},   // 1M total
		{1_000_000, 10}, // 10M total
	}

	for _, cfg := range dupConfigs {
		total := cfg.keys * cfg.valsPerKey
		sizeName := formatSize(total)
		b.Run(fmt.Sprintf("DupSort_%s", sizeName), func(b *testing.B) {
			benchmarkDupSortDB(b, cfg.keys, cfg.valsPerKey)
		})
	}
}

func formatSize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func benchmarkPlainDB(b *testing.B, numKeys int) {
	genv, menv, sampleKeys := getCachedPlainDB(b, numKeys)

	// Run benchmarks
	b.Run("SeqRead_gdbx", func(b *testing.B) {
		benchSeqReadGdbx(b, genv, "bench", numKeys)
	})

	b.Run("SeqRead_mdbx", func(b *testing.B) {
		benchSeqReadMdbx(b, menv, "bench", numKeys)
	})

	b.Run("RandGet_gdbx", func(b *testing.B) {
		benchRandGetGdbx(b, genv, "bench", sampleKeys)
	})

	b.Run("RandGet_mdbx", func(b *testing.B) {
		benchRandGetMdbx(b, menv, "bench", sampleKeys)
	})

	b.Run("RandSet_gdbx", func(b *testing.B) {
		benchRandSetGdbx(b, genv, "bench", sampleKeys)
	})

	b.Run("RandSet_mdbx", func(b *testing.B) {
		benchRandSetMdbx(b, menv, "bench", sampleKeys)
	})
}

func benchmarkDupSortDB(b *testing.B, numKeys, valsPerKey int) {
	genv, menv, sampleKeys := getCachedDupSortDB(b, numKeys, valsPerKey)

	// Run benchmarks
	b.Run("SeqRead_gdbx", func(b *testing.B) {
		benchSeqReadGdbx(b, genv, "dupbench", numKeys*valsPerKey)
	})

	b.Run("SeqRead_mdbx", func(b *testing.B) {
		benchSeqReadMdbx(b, menv, "dupbench", numKeys*valsPerKey)
	})

	b.Run("NextNoDup_gdbx", func(b *testing.B) {
		benchNextNoDupGdbx(b, genv, "dupbench", numKeys)
	})

	b.Run("NextNoDup_mdbx", func(b *testing.B) {
		benchNextNoDupMdbx(b, menv, "dupbench", numKeys)
	})

	b.Run("RandSet_gdbx", func(b *testing.B) {
		benchRandSetGdbx(b, genv, "dupbench", sampleKeys)
	})

	b.Run("RandSet_mdbx", func(b *testing.B) {
		benchRandSetMdbx(b, menv, "dupbench", sampleKeys)
	})

	b.Run("Count_gdbx", func(b *testing.B) {
		benchCountGdbx(b, genv, "dupbench", sampleKeys)
	})

	b.Run("Count_mdbx", func(b *testing.B) {
		benchCountMdbx(b, menv, "dupbench", sampleKeys)
	})
}

// Benchmark functions - gdbx
func benchSeqReadGdbx(b *testing.B, env *gdbx.Env, tableName string, expected int) {
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple(tableName, 0)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor.Get(nil, nil, gdbx.First)
		count := 1
		for {
			_, _, err := cursor.Get(nil, nil, gdbx.Next)
			if err != nil {
				break
			}
			count++
		}
		if count < expected/2 {
			b.Fatalf("Only read %d entries, expected ~%d", count, expected)
		}
	}
}

func benchRandGetGdbx(b *testing.B, env *gdbx.Env, tableName string, sampleKeys [][]byte) {
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple(tableName, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range sampleKeys {
			txn.Get(dbi, k)
		}
	}
}

func benchRandSetGdbx(b *testing.B, env *gdbx.Env, tableName string, sampleKeys [][]byte) {
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple(tableName, 0)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range sampleKeys {
			cursor.Get(k, nil, gdbx.Set)
		}
	}
}

func benchNextNoDupGdbx(b *testing.B, env *gdbx.Env, tableName string, expected int) {
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple(tableName, 0)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor.Get(nil, nil, gdbx.First)
		count := 1
		for {
			_, _, err := cursor.Get(nil, nil, gdbx.NextNoDup)
			if err != nil {
				break
			}
			count++
		}
	}
}

func benchCountGdbx(b *testing.B, env *gdbx.Env, tableName string, sampleKeys [][]byte) {
	txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBISimple(tableName, 0)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range sampleKeys {
			cursor.Get(k, nil, gdbx.Set)
			cursor.Count()
		}
	}
}

// Benchmark functions - mdbx-go
func benchSeqReadMdbx(b *testing.B, env *mdbxgo.Env, tableName string, expected int) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, _ := env.BeginTxn(nil, mdbxgo.Readonly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor.Get(nil, nil, mdbxgo.First)
		count := 1
		for {
			_, _, err := cursor.Get(nil, nil, mdbxgo.Next)
			if err != nil {
				break
			}
			count++
		}
	}
}

func benchRandGetMdbx(b *testing.B, env *mdbxgo.Env, tableName string, sampleKeys [][]byte) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, _ := env.BeginTxn(nil, mdbxgo.Readonly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range sampleKeys {
			txn.Get(dbi, k)
		}
	}
}

func benchRandSetMdbx(b *testing.B, env *mdbxgo.Env, tableName string, sampleKeys [][]byte) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, _ := env.BeginTxn(nil, mdbxgo.Readonly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range sampleKeys {
			cursor.Get(k, nil, mdbxgo.Set)
		}
	}
}

func benchNextNoDupMdbx(b *testing.B, env *mdbxgo.Env, tableName string, expected int) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, _ := env.BeginTxn(nil, mdbxgo.Readonly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor.Get(nil, nil, mdbxgo.First)
		count := 1
		for {
			_, _, err := cursor.Get(nil, nil, mdbxgo.NextNoDup)
			if err != nil {
				break
			}
			count++
		}
	}
}

func benchCountMdbx(b *testing.B, env *mdbxgo.Env, tableName string, sampleKeys [][]byte) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, _ := env.BeginTxn(nil, mdbxgo.Readonly)
	defer txn.Abort()
	dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)
	cursor, _ := txn.OpenCursor(dbi)
	defer cursor.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range sampleKeys {
			cursor.Get(k, nil, mdbxgo.Set)
			cursor.Count()
		}
	}
}
