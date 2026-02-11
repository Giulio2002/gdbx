package benchmarks

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// BenchmarkDeleteRange benchmarks range deletion on regular (non-DupSort) tables.
// Each iteration creates a fresh write transaction, performs DeleteRange, then aborts
// to preserve the database for the next iteration.
//
// Comparison approaches:
//   - gdbx_DeleteRange: B-tree-aware bulk deletion (frees entire subtrees)
//   - gdbx_CursorLoop: cursor SetRange + Del + Next loop
//   - gdbx_Del_PerKey: txn.Del() called per key (re-traverses tree each time)
//   - mdbx_CursorLoop: mdbx-go cursor loop baseline
func BenchmarkDeleteRange(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000, 1_000_000}
	deletePercents := []int{1, 10, 50, 90}

	for _, size := range sizes {
		for _, pct := range deletePercents {
			sizeName := formatDeleteSize(size)
			deleteCount := size * pct / 100
			startKey := (size - deleteCount) / 2
			endKey := startKey + deleteCount

			b.Run(fmt.Sprintf("Plain_%s_Del%d%%/gdbx_DeleteRange", sizeName, pct), func(b *testing.B) {
				benchDeleteRangeGdbx(b, size, uint64(startKey), uint64(endKey))
			})

			b.Run(fmt.Sprintf("Plain_%s_Del%d%%/gdbx_CursorLoop", sizeName, pct), func(b *testing.B) {
				benchDeleteRangeCursorGdbx(b, size, uint64(startKey), uint64(endKey))
			})

			b.Run(fmt.Sprintf("Plain_%s_Del%d%%/gdbx_Del_PerKey", sizeName, pct), func(b *testing.B) {
				benchDeletePerKeyGdbx(b, size, uint64(startKey), uint64(endKey))
			})

			b.Run(fmt.Sprintf("Plain_%s_Del%d%%/mdbx_CursorLoop", sizeName, pct), func(b *testing.B) {
				benchDeleteRangeCursorMdbx(b, size, uint64(startKey), uint64(endKey))
			})

			b.Run(fmt.Sprintf("Plain_%s_Del%d%%/mdbx_Del_PerKey", sizeName, pct), func(b *testing.B) {
				benchDeletePerKeyMdbx(b, size, uint64(startKey), uint64(endKey))
			})
		}
	}
}

// BenchmarkDeleteRangeDupSort benchmarks range deletion on DupSort tables.
//
// Comparison approaches:
//   - gdbx_DeleteRange: B-tree-aware bulk deletion (frees key subtrees in bulk)
//   - gdbx_CursorLoop_NoDupData: cursor NextNoDup + Del(NoDupData) — deletes all values per key at once
//   - gdbx_CursorLoop_PerValue: cursor Next + Del — deletes each value individually (worst case)
//   - mdbx_CursorLoop_NoDupData: mdbx-go cursor NextNoDup + Del(NoDupData) baseline
//   - mdbx_CursorLoop_PerValue: mdbx-go cursor Next + Del — per-value baseline
func BenchmarkDeleteRangeDupSort(b *testing.B) {
	configs := []struct {
		numKeys    int
		valsPerKey int
	}{
		{100, 10},
		{1000, 10},
		{1000, 50},
		{10000, 10},
	}

	for _, cfg := range configs {
		total := cfg.numKeys * cfg.valsPerKey
		deleteKeys := cfg.numKeys / 2
		startKey := (cfg.numKeys - deleteKeys) / 2
		endKey := startKey + deleteKeys

		b.Run(fmt.Sprintf("DupSort_%dk_%dv_Del50%%/gdbx_DeleteRange", cfg.numKeys, cfg.valsPerKey), func(b *testing.B) {
			benchDeleteRangeDupSortGdbx(b, cfg.numKeys, cfg.valsPerKey, uint64(startKey), uint64(endKey))
		})

		b.Run(fmt.Sprintf("DupSort_%dk_%dv_Del50%%/gdbx_CursorLoop_NoDupData", cfg.numKeys, cfg.valsPerKey), func(b *testing.B) {
			benchDeleteRangeDupSortCursorGdbx(b, cfg.numKeys, cfg.valsPerKey, uint64(startKey), uint64(endKey))
		})

		b.Run(fmt.Sprintf("DupSort_%dk_%dv_Del50%%/gdbx_CursorLoop_PerValue", cfg.numKeys, cfg.valsPerKey), func(b *testing.B) {
			benchDeleteRangeDupSortCursorPerValueGdbx(b, cfg.numKeys, cfg.valsPerKey, uint64(startKey), uint64(endKey))
		})

		b.Run(fmt.Sprintf("DupSort_%dk_%dv_Del50%%/mdbx_CursorLoop_NoDupData", cfg.numKeys, cfg.valsPerKey), func(b *testing.B) {
			benchDeleteRangeDupSortCursorMdbx(b, cfg.numKeys, cfg.valsPerKey, uint64(startKey), uint64(endKey))
		})

		b.Run(fmt.Sprintf("DupSort_%dk_%dv_Del50%%/mdbx_CursorLoop_PerValue", cfg.numKeys, cfg.valsPerKey), func(b *testing.B) {
			benchDeleteRangeDupSortCursorPerValueMdbx(b, cfg.numKeys, cfg.valsPerKey, uint64(startKey), uint64(endKey))
		})

		_ = total
	}
}

func formatDeleteSize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ============ gdbx DeleteRange (API method) ============

func benchDeleteRangeGdbx(b *testing.B, numKeys int, fromKey, toKey uint64) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("bench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		_, err = txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		// Abort to preserve data for next iteration
		txn.Abort()
	}
}

// ============ gdbx cursor loop (manual pattern) ============

func benchDeleteRangeCursorGdbx(b *testing.B, numKeys int, fromKey, toKey uint64) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("bench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cmp := txn.Cmp
		for k, _, err := cursor.Get(from, nil, gdbx.SetRange); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
			if cmp(dbi, k, to) >= 0 {
				break
			}
			cursor.Del(0)
		}

		cursor.Close()
		txn.Abort()
	}
}

// ============ gdbx txn.Del per-key loop ============

func benchDeletePerKeyGdbx(b *testing.B, numKeys int, fromKey, toKey uint64) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	// Pre-generate all keys in the range
	keys := make([][]byte, 0, toKey-fromKey)
	for k := fromKey; k < toKey; k++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, k)
		keys = append(keys, key)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("bench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		for _, key := range keys {
			txn.Del(dbi, key, nil)
		}

		txn.Abort()
	}
}

// ============ mdbx txn.Del per-key loop ============

func benchDeletePerKeyMdbx(b *testing.B, numKeys int, fromKey, toKey uint64) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Pre-generate all keys in the range
	keys := make([][]byte, 0, toKey-fromKey)
	for k := fromKey; k < toKey; k++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, k)
		keys = append(keys, key)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBI("bench", 0, nil, nil)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		for _, key := range keys {
			txn.Del(dbi, key, nil)
		}

		txn.Abort()
	}
}

// ============ mdbx cursor loop ============

func benchDeleteRangeCursorMdbx(b *testing.B, numKeys int, fromKey, toKey uint64) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBI("bench", 0, nil, nil)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		for k, _, err := cursor.Get(from, nil, mdbxgo.SetRange); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, mdbxgo.Next) {
			if txn.Cmp(dbi, k, to) >= 0 {
				break
			}
			cursor.Del(0)
		}

		cursor.Close()
		txn.Abort()
	}
}

// ============ DupSort benchmarks ============

func benchDeleteRangeDupSortGdbx(b *testing.B, numKeys, valsPerKey int, fromKey, toKey uint64) {
	genv, _, _ := getCachedDupSortDB(b, numKeys, valsPerKey)

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("dupbench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		_, err = txn.DeleteRange(dbi, from, to)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		txn.Abort()
	}
}

func benchDeleteRangeDupSortCursorGdbx(b *testing.B, numKeys, valsPerKey int, fromKey, toKey uint64) {
	genv, _, _ := getCachedDupSortDB(b, numKeys, valsPerKey)

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("dupbench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cmp := txn.Cmp
		for k, _, err := cursor.Get(from, nil, gdbx.SetRange); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, gdbx.NextNoDup) {
			if cmp(dbi, k, to) >= 0 {
				break
			}
			cursor.Del(gdbx.NoDupData)
		}

		cursor.Close()
		txn.Abort()
	}
}

func benchDeleteRangeDupSortCursorMdbx(b *testing.B, numKeys, valsPerKey int, fromKey, toKey uint64) {
	_, menv, _ := getCachedDupSortDB(b, numKeys, valsPerKey)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBI("dupbench", 0, nil, nil)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		for k, _, err := cursor.Get(from, nil, mdbxgo.SetRange); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, mdbxgo.NextNoDup) {
			if txn.Cmp(dbi, k, to) >= 0 {
				break
			}
			cursor.Del(mdbxgo.NoDupData)
		}

		cursor.Close()
		txn.Abort()
	}
}

// ============ DupSort per-value cursor benchmarks (worst case: delete values one by one) ============

func benchDeleteRangeDupSortCursorPerValueGdbx(b *testing.B, numKeys, valsPerKey int, fromKey, toKey uint64) {
	genv, _, _ := getCachedDupSortDB(b, numKeys, valsPerKey)

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBISimple("dupbench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cmp := txn.Cmp
		// Delete each individual value (not NoDupData — one at a time)
		for k, _, err := cursor.Get(from, nil, gdbx.SetRange); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
			if cmp(dbi, k, to) >= 0 {
				break
			}
			cursor.Del(0)
		}

		cursor.Close()
		txn.Abort()
	}
}

func benchDeleteRangeDupSortCursorPerValueMdbx(b *testing.B, numKeys, valsPerKey int, fromKey, toKey uint64) {
	_, menv, _ := getCachedDupSortDB(b, numKeys, valsPerKey)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	from := make([]byte, 8)
	to := make([]byte, 8)
	binary.BigEndian.PutUint64(from, fromKey)
	binary.BigEndian.PutUint64(to, toKey)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		dbi, err := txn.OpenDBI("dupbench", 0, nil, nil)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}

		// Delete each individual value (not NoDupData — one at a time)
		for k, _, err := cursor.Get(from, nil, mdbxgo.SetRange); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, mdbxgo.Next) {
			if txn.Cmp(dbi, k, to) >= 0 {
				break
			}
			cursor.Del(0)
		}

		cursor.Close()
		txn.Abort()
	}
}
