package benchmarks

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
	"github.com/tecbot/gorocksdb"
)

// BenchmarkReadLongKeys benchmarks read operations with 64-byte keys.
func BenchmarkReadLongKeys(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		sizeName := formatLongSize(size)

		// Sequential Read (cursor iteration)
		b.Run(fmt.Sprintf("SeqRead_%s/gdbx", sizeName), func(b *testing.B) {
			benchSeqReadLongGdbx(b, size)
		})
		b.Run(fmt.Sprintf("SeqRead_%s/mdbx", sizeName), func(b *testing.B) {
			benchSeqReadLongMdbx(b, size)
		})
		b.Run(fmt.Sprintf("SeqRead_%s/bolt", sizeName), func(b *testing.B) {
			benchSeqReadLongBolt(b, size)
		})
		b.Run(fmt.Sprintf("SeqRead_%s/rocksdb", sizeName), func(b *testing.B) {
			benchSeqReadLongRocksDB(b, size)
		})

		// Random Get (point lookups)
		b.Run(fmt.Sprintf("RandGet_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandGetLongGdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandGet_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandGetLongMdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandGet_%s/bolt", sizeName), func(b *testing.B) {
			benchRandGetLongBolt(b, size)
		})
		b.Run(fmt.Sprintf("RandGet_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandGetLongRocksDB(b, size)
		})

		// Random Seek (cursor seek)
		b.Run(fmt.Sprintf("RandSeek_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandSeekLongGdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandSeek_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandSeekLongMdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandSeek_%s/bolt", sizeName), func(b *testing.B) {
			benchRandSeekLongBolt(b, size)
		})
		b.Run(fmt.Sprintf("RandSeek_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandSeekLongRocksDB(b, size)
		})
	}
}

// ============ Sequential Read (64-byte keys) ============

func benchSeqReadLongGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, _ := getCachedLongKeyDB(b, numKeys)

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

func benchSeqReadLongMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, _ := getCachedLongKeyDB(b, numKeys)

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

func benchSeqReadLongBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, _ := getCachedLongKeyDB(b, numKeys)

	tx, err := boltDB.Begin(false)
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

func benchSeqReadLongRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, _ := getCachedLongKeyDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := rocksDB.NewIterator(ro)
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

// ============ Random Get (64-byte keys) ============

func benchRandGetLongGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, keys := getCachedLongKeyDB(b, numKeys)

	txn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

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
		txn.Get(dbi, keys[order[i%numKeys]])
	}
}

func benchRandGetLongMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, keys := getCachedLongKeyDB(b, numKeys)

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
		txn.Get(dbi, keys[order[i%numKeys]])
	}
}

func benchRandGetLongBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, keys := getCachedLongKeyDB(b, numKeys)

	tx, err := boltDB.Begin(false)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

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
		bucket.Get(keys[order[i%numKeys]])
	}
}

func benchRandGetLongRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, keys := getCachedLongKeyDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

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
		val, _ := rocksDB.Get(ro, keys[order[i%numKeys]])
		if val != nil {
			val.Free()
		}
	}
}

// ============ Random Seek (64-byte keys) ============

func benchRandSeekLongGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, keys := getCachedLongKeyDB(b, numKeys)

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
		cursor.Get(keys[order[i%numKeys]], nil, gdbx.Set)
	}
}

func benchRandSeekLongMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, keys := getCachedLongKeyDB(b, numKeys)

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
		cursor.Get(keys[order[i%numKeys]], nil, mdbxgo.Set)
	}
}

func benchRandSeekLongBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, keys := getCachedLongKeyDB(b, numKeys)

	tx, err := boltDB.Begin(false)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	cursor := bucket.Cursor()

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
		cursor.Seek(keys[order[i%numKeys]])
	}
}

func benchRandSeekLongRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, keys := getCachedLongKeyDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := rocksDB.NewIterator(ro)
	defer iter.Close()

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
		iter.Seek(keys[order[i%numKeys]])
	}
}
