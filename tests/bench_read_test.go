package tests

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
	"github.com/tecbot/gorocksdb"
)

// BenchmarkReadOps benchmarks read operations on pre-populated databases.
func BenchmarkReadOps(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		sizeName := formatReadSize(size)

		// Sequential Read (cursor iteration)
		b.Run(fmt.Sprintf("SeqRead_%s/gdbx", sizeName), func(b *testing.B) {
			benchSeqReadGdbxOp(b, size)
		})
		b.Run(fmt.Sprintf("SeqRead_%s/mdbx", sizeName), func(b *testing.B) {
			benchSeqReadMdbxOp(b, size)
		})
		b.Run(fmt.Sprintf("SeqRead_%s/bolt", sizeName), func(b *testing.B) {
			benchSeqReadBoltOp(b, size)
		})
		b.Run(fmt.Sprintf("SeqRead_%s/rocksdb", sizeName), func(b *testing.B) {
			benchSeqReadRocksDBOp(b, size)
		})

		// Random Get (point lookups)
		b.Run(fmt.Sprintf("RandGet_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandGetGdbxOp(b, size)
		})
		b.Run(fmt.Sprintf("RandGet_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandGetMdbxOp(b, size)
		})
		b.Run(fmt.Sprintf("RandGet_%s/bolt", sizeName), func(b *testing.B) {
			benchRandGetBoltOp(b, size)
		})
		b.Run(fmt.Sprintf("RandGet_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandGetRocksDBOp(b, size)
		})
	}
}

func formatReadSize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ============ Sequential Read (cursor iteration, per-entry cost) ============

func benchSeqReadGdbxOp(b *testing.B, numKeys int) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	txn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Fatal(err)
	}
	defer cursor.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			cursor.Get(nil, nil, gdbx.First)
		} else {
			cursor.Get(nil, nil, gdbx.Next)
		}
	}
}

func benchSeqReadMdbxOp(b *testing.B, numKeys int) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Fatal(err)
	}
	defer cursor.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			cursor.Get(nil, nil, mdbxgo.First)
		} else {
			cursor.Get(nil, nil, mdbxgo.Next)
		}
	}
}

func benchSeqReadBoltOp(b *testing.B, numKeys int) {
	db := getCachedBoltDB(b, numKeys)

	tx, err := db.Begin(false)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	cursor := bucket.Cursor()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			cursor.First()
		} else {
			cursor.Next()
		}
	}
}

func benchSeqReadRocksDBOp(b *testing.B, numKeys int) {
	db := getCachedRocksDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	defer iter.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			iter.SeekToFirst()
		} else {
			iter.Next()
		}
	}
}

// ============ Random Get (point lookups, per-operation cost) ============

func benchRandGetGdbxOp(b *testing.B, numKeys int) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	txn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		txn.Get(dbi, key)
	}
}

func benchRandGetMdbxOp(b *testing.B, numKeys int) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		txn.Get(dbi, key)
	}
}

func benchRandGetBoltOp(b *testing.B, numKeys int) {
	db := getCachedBoltDB(b, numKeys)

	tx, err := db.Begin(false)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	key := make([]byte, 8)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		bucket.Get(key)
	}
}

func benchRandGetRocksDBOp(b *testing.B, numKeys int) {
	db := getCachedRocksDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	key := make([]byte, 8)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		val, _ := db.Get(ro, key)
		if val != nil {
			val.Free()
		}
	}
}
