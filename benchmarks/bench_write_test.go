package benchmarks

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	"github.com/tecbot/gorocksdb"
)

func newRocksWriteOpts() *gorocksdb.WriteOptions {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // Disable WAL for fair comparison (others don't sync either)
	return wo
}

// BenchmarkWriteOps benchmarks write operations on pre-populated databases.
// Opens transaction and DBI once, then measures pure Put performance.
func BenchmarkWriteOps(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		sizeName := formatWriteSize(size)

		// Sequential Put (updates to existing keys)
		b.Run(fmt.Sprintf("SeqPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchSeqPutGdbx(b, size)
		})
		b.Run(fmt.Sprintf("SeqPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchSeqPutMdbx(b, size)
		})
		b.Run(fmt.Sprintf("SeqPut_%s/bolt", sizeName), func(b *testing.B) {
			benchSeqPutBolt(b, size)
		})
		b.Run(fmt.Sprintf("SeqPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchSeqPutRocksDB(b, size)
		})

		// Random Put (updates to random existing keys)
		b.Run(fmt.Sprintf("RandPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandPutGdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandPutMdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandPut_%s/bolt", sizeName), func(b *testing.B) {
			benchRandPutBolt(b, size)
		})
		b.Run(fmt.Sprintf("RandPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandPutRocksDB(b, size)
		})

		// Cursor Put
		b.Run(fmt.Sprintf("CursorPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchCursorPutGdbx(b, size)
		})
		b.Run(fmt.Sprintf("CursorPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchCursorPutMdbx(b, size)
		})
		b.Run(fmt.Sprintf("CursorPut_%s/bolt", sizeName), func(b *testing.B) {
			benchCursorPutBolt(b, size)
		})
		b.Run(fmt.Sprintf("CursorPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchCursorPutRocksDB(b, size)
		})
	}
}

func formatWriteSize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ============ Sequential Put (updates existing keys) ============

func benchSeqPutGdbx(b *testing.B, numKeys int) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Open transaction and DBI once before timing
	txn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, key, val, 0)
	}
}

func benchSeqPutMdbx(b *testing.B, numKeys int) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Open transaction and DBI once before timing
	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, key, val, 0)
	}
}

// ============ Random Put (updates random existing keys) ============

func benchRandPutGdbx(b *testing.B, numKeys int) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	// Fisher-Yates shuffle
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	// Open transaction and DBI once before timing
	txn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		j := order[i%numKeys]
		binary.BigEndian.PutUint64(key, uint64(j))
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, key, val, 0)
	}
}

func benchRandPutMdbx(b *testing.B, numKeys int) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	// Open transaction and DBI once before timing
	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		j := order[i%numKeys]
		binary.BigEndian.PutUint64(key, uint64(j))
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, key, val, 0)
	}
}

// ============ Cursor Put ============

func benchCursorPutGdbx(b *testing.B, numKeys int) {
	genv, _, _ := getCachedPlainDB(b, numKeys)

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Open transaction, DBI, and cursor once before timing
	txn, err := genv.BeginTxn(nil, 0)
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
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		cursor.Put(key, val, 0)
	}
}

func benchCursorPutMdbx(b *testing.B, numKeys int) {
	_, menv, _ := getCachedPlainDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Open transaction, DBI, and cursor once before timing
	txn, err := menv.BeginTxn(nil, 0)
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
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		cursor.Put(key, val, 0)
	}
}

// ============ BoltDB Benchmarks ============

func benchSeqPutBolt(b *testing.B, numKeys int) {
	db := getCachedBoltDB(b, numKeys)

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Open write transaction once before timing
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		bucket.Put(key, val)
	}
}

func benchRandPutBolt(b *testing.B, numKeys int) {
	db := getCachedBoltDB(b, numKeys)

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Pre-generate random order
	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	// Open write transaction once before timing
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		j := order[i%numKeys]
		binary.BigEndian.PutUint64(key, uint64(j))
		binary.BigEndian.PutUint64(val, uint64(i))
		bucket.Put(key, val)
	}
}

func benchCursorPutBolt(b *testing.B, numKeys int) {
	db := getCachedBoltDB(b, numKeys)

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Open write transaction once before timing
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	cursor := bucket.Cursor()
	_ = cursor // BoltDB cursor doesn't have Put, use bucket.Put

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		// BoltDB cursor doesn't support Put, using bucket.Put
		bucket.Put(key, val)
	}
}

// ============ RocksDB Benchmarks ============

func benchSeqPutRocksDB(b *testing.B, numKeys int) {
	db := getCachedRocksDB(b, numKeys)

	wo := newRocksWriteOpts()
	defer wo.Destroy()

	key := make([]byte, 8)
	val := make([]byte, 32)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		db.Put(wo, key, val)
	}
}

func benchRandPutRocksDB(b *testing.B, numKeys int) {
	db := getCachedRocksDB(b, numKeys)

	wo := newRocksWriteOpts()
	defer wo.Destroy()

	key := make([]byte, 8)
	val := make([]byte, 32)

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
		j := order[i%numKeys]
		binary.BigEndian.PutUint64(key, uint64(j))
		binary.BigEndian.PutUint64(val, uint64(i))
		db.Put(wo, key, val)
	}
}

func benchCursorPutRocksDB(b *testing.B, numKeys int) {
	db := getCachedRocksDB(b, numKeys)

	wo := newRocksWriteOpts()
	defer wo.Destroy()

	key := make([]byte, 8)
	val := make([]byte, 32)

	// RocksDB doesn't have cursor-based Put, use regular Put
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		binary.BigEndian.PutUint64(val, uint64(i))
		db.Put(wo, key, val)
	}
}
