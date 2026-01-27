package tests

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
	"github.com/tecbot/gorocksdb"
	bolt "go.etcd.io/bbolt"
)

const bigValSize = 8 * 1024 // 8KB values

// BenchmarkBigValues benchmarks read/write operations with 8KB values.
func BenchmarkBigValues(b *testing.B) {
	sizes := []int{100, 1_000, 10_000}

	for _, size := range sizes {
		sizeName := formatBigValSize(size)

		// ============ WRITES ============

		// Sequential Put
		b.Run(fmt.Sprintf("Write/SeqPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchSeqPutBigGdbx(b, size)
		})
		b.Run(fmt.Sprintf("Write/SeqPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchSeqPutBigMdbx(b, size)
		})
		b.Run(fmt.Sprintf("Write/SeqPut_%s/bolt", sizeName), func(b *testing.B) {
			benchSeqPutBigBolt(b, size)
		})
		b.Run(fmt.Sprintf("Write/SeqPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchSeqPutBigRocksDB(b, size)
		})

		// Random Put
		b.Run(fmt.Sprintf("Write/RandPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandPutBigGdbx(b, size)
		})
		b.Run(fmt.Sprintf("Write/RandPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandPutBigMdbx(b, size)
		})
		b.Run(fmt.Sprintf("Write/RandPut_%s/bolt", sizeName), func(b *testing.B) {
			benchRandPutBigBolt(b, size)
		})
		b.Run(fmt.Sprintf("Write/RandPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandPutBigRocksDB(b, size)
		})

		// ============ READS ============

		// Sequential Read
		b.Run(fmt.Sprintf("Read/SeqRead_%s/gdbx", sizeName), func(b *testing.B) {
			benchSeqReadBigGdbx(b, size)
		})
		b.Run(fmt.Sprintf("Read/SeqRead_%s/mdbx", sizeName), func(b *testing.B) {
			benchSeqReadBigMdbx(b, size)
		})
		b.Run(fmt.Sprintf("Read/SeqRead_%s/bolt", sizeName), func(b *testing.B) {
			benchSeqReadBigBolt(b, size)
		})
		b.Run(fmt.Sprintf("Read/SeqRead_%s/rocksdb", sizeName), func(b *testing.B) {
			benchSeqReadBigRocksDB(b, size)
		})

		// Random Get
		b.Run(fmt.Sprintf("Read/RandGet_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandGetBigGdbx(b, size)
		})
		b.Run(fmt.Sprintf("Read/RandGet_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandGetBigMdbx(b, size)
		})
		b.Run(fmt.Sprintf("Read/RandGet_%s/bolt", sizeName), func(b *testing.B) {
			benchRandGetBigBolt(b, size)
		})
		b.Run(fmt.Sprintf("Read/RandGet_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandGetBigRocksDB(b, size)
		})
	}
}

func formatBigValSize(n int) string {
	switch {
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ============ Big Value Cache ============

var (
	bigValMu      sync.Mutex
	bigGdbxEnvs   = make(map[string]*gdbx.Env)
	bigMdbxEnvs   = make(map[string]*mdbxgo.Env)
	bigBoltDBs    = make(map[string]*bolt.DB)
	bigRocksDBs   = make(map[string]*gorocksdb.DB)
	bigValCache   = make(map[string][]byte) // Shared big value for writes
)

func getCachedBigValDB(b *testing.B, size int) (*gdbx.Env, *mdbxgo.Env, *bolt.DB, *gorocksdb.DB, []byte) {
	bigValMu.Lock()
	defer bigValMu.Unlock()

	key := fmt.Sprintf("bigval_%d", size)
	gdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("bigval_%d_gdbx.db", size))
	mdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("bigval_%d_mdbx.db", size))
	boltPath := filepath.Join(benchCacheDir, fmt.Sprintf("bigval_%d_bolt.db", size))
	rocksPath := filepath.Join(benchCacheDir, fmt.Sprintf("bigval_%d_rocks.db", size))

	// Check if already loaded
	if genv, ok := bigGdbxEnvs[key]; ok {
		return genv, bigMdbxEnvs[key], bigBoltDBs[key], bigRocksDBs[key], bigValCache[key]
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(benchCacheDir, 0755); err != nil {
		b.Fatal(err)
	}

	// Generate big value (same for all DBs)
	bigVal := make([]byte, bigValSize)
	rand.Read(bigVal)

	// Check if databases exist
	gdbxExists := fileExists(gdbxPath)
	mdbxExists := fileExists(mdbxPath)
	boltExists := fileExists(boltPath)
	rocksExists := fileExists(rocksPath)

	// Calculate needed size: size * 8KB * 2 (some overhead)
	mapSize := int64(size) * bigValSize * 3
	if mapSize < 256*1024*1024 {
		mapSize = 256 * 1024 * 1024 // Min 256MB
	}

	// Setup gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	genv.SetMaxDBs(10)
	genv.SetGeometry(-1, -1, mapSize*2, -1, -1, 4096)
	if err := genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		b.Fatal(err)
	}
	if err := genv.PreExtendMmap(mapSize); err != nil {
		genv.Close()
		b.Fatal(err)
	}

	// Setup mdbx-go
	runtime.LockOSThread()
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("bench-bigval"))
	if err != nil {
		genv.Close()
		b.Fatal(err)
	}
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.SetGeometry(-1, -1, int(mapSize*2), -1, -1, 4096)
	if err := menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync|mdbxgo.WriteMap, 0644); err != nil {
		genv.Close()
		b.Fatal(err)
	}
	runtime.UnlockOSThread()

	// Setup BoltDB
	boltDB, err := bolt.Open(boltPath, 0644, &bolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
	})
	if err != nil {
		genv.Close()
		menv.Close()
		b.Fatal(err)
	}

	// Setup RocksDB
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(64 * 1024 * 1024)
	opts.SetMaxWriteBufferNumber(3)
	rocksDB, err := gorocksdb.OpenDb(opts, rocksPath)
	if err != nil {
		genv.Close()
		menv.Close()
		boltDB.Close()
		b.Fatal(err)
	}

	// Populate if needed
	if !gdbxExists {
		b.Logf("Creating cached gdbx big-value DB with %d entries...", size)
		populateBigValDBGdbx(b, genv, size, bigVal)
	} else {
		b.Logf("Using cached gdbx big-value DB with %d entries", size)
	}

	if !mdbxExists {
		b.Logf("Creating cached mdbx big-value DB with %d entries...", size)
		populateBigValDBMdbx(b, menv, size, bigVal)
	} else {
		b.Logf("Using cached mdbx big-value DB with %d entries", size)
	}

	if !boltExists {
		b.Logf("Creating cached BoltDB big-value DB with %d entries...", size)
		populateBigValDBBolt(b, boltDB, size, bigVal)
	} else {
		b.Logf("Using cached BoltDB big-value DB with %d entries", size)
	}

	if !rocksExists {
		b.Logf("Creating cached RocksDB big-value DB with %d entries...", size)
		populateBigValDBRocks(b, rocksDB, size, bigVal)
	} else {
		b.Logf("Using cached RocksDB big-value DB with %d entries", size)
	}

	// Cache
	bigGdbxEnvs[key] = genv
	bigMdbxEnvs[key] = menv
	bigBoltDBs[key] = boltDB
	bigRocksDBs[key] = rocksDB
	bigValCache[key] = bigVal

	return genv, menv, boltDB, rocksDB, bigVal
}

func populateBigValDBGdbx(b *testing.B, env *gdbx.Env, numKeys int, bigVal []byte) {
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("bench", gdbx.Create)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Put(dbi, key, bigVal, gdbx.Upsert); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

func populateBigValDBMdbx(b *testing.B, env *mdbxgo.Env, numKeys int, bigVal []byte) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBI("bench", mdbxgo.Create, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Put(dbi, key, bigVal, mdbxgo.Upsert); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

func populateBigValDBBolt(b *testing.B, db *bolt.DB, numKeys int, bigVal []byte) {
	key := make([]byte, 8)
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			if err := bucket.Put(key, bigVal); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

func populateBigValDBRocks(b *testing.B, db *gorocksdb.DB, numKeys int, bigVal []byte) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	key := make([]byte, 8)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := db.Put(wo, key, bigVal); err != nil {
			b.Fatal(err)
		}
	}
}

// ============ WRITE: Sequential Put (8KB values) ============

func benchSeqPutBigGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, bigVal := getCachedBigValDB(b, numKeys)

	txn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		txn.Put(dbi, key, bigVal, 0)
	}
}

func benchSeqPutBigMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, bigVal := getCachedBigValDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		txn.Put(dbi, key, bigVal, 0)
	}
}

func benchSeqPutBigBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, bigVal := getCachedBigValDB(b, numKeys)

	tx, err := boltDB.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		bucket.Put(key, bigVal)
	}
}

func benchSeqPutBigRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, bigVal := getCachedBigValDB(b, numKeys)

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)
	defer wo.Destroy()

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i%numKeys))
		rocksDB.Put(wo, key, bigVal)
	}
}

// ============ WRITE: Random Put (8KB values) ============

func benchRandPutBigGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, bigVal := getCachedBigValDB(b, numKeys)

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	txn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		txn.Put(dbi, key, bigVal, 0)
	}
}

func benchRandPutBigMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, bigVal := getCachedBigValDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		txn.Put(dbi, key, bigVal, 0)
	}
}

func benchRandPutBigBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, bigVal := getCachedBigValDB(b, numKeys)

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	tx, err := boltDB.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		bucket.Put(key, bigVal)
	}
}

func benchRandPutBigRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, bigVal := getCachedBigValDB(b, numKeys)

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)
	defer wo.Destroy()

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		rocksDB.Put(wo, key, bigVal)
	}
}

// ============ READ: Sequential Read (8KB values) ============

func benchSeqReadBigGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, _ := getCachedBigValDB(b, numKeys)

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
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			cursor.Get(nil, nil, gdbx.First)
		} else {
			cursor.Get(nil, nil, gdbx.Next)
		}
	}
}

func benchSeqReadBigMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, _ := getCachedBigValDB(b, numKeys)

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
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			cursor.Get(nil, nil, mdbxgo.First)
		} else {
			cursor.Get(nil, nil, mdbxgo.Next)
		}
	}
}

func benchSeqReadBigBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, _ := getCachedBigValDB(b, numKeys)

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
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			cursor.First()
		} else {
			cursor.Next()
		}
	}
}

func benchSeqReadBigRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, _ := getCachedBigValDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := rocksDB.NewIterator(ro)
	defer iter.Close()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		if i%numKeys == 0 {
			iter.SeekToFirst()
		} else {
			iter.Next()
		}
	}
}

// ============ READ: Random Get (8KB values) ============

func benchRandGetBigGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, _ := getCachedBigValDB(b, numKeys)

	txn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		txn.Get(dbi, key)
	}
}

func benchRandGetBigMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, _ := getCachedBigValDB(b, numKeys)

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

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		txn.Get(dbi, key)
	}
}

func benchRandGetBigBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, _ := getCachedBigValDB(b, numKeys)

	tx, err := boltDB.Begin(false)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("bench"))
	if bucket == nil {
		b.Fatal("bucket not found")
	}

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		bucket.Get(key)
	}
}

func benchRandGetBigRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, _ := getCachedBigValDB(b, numKeys)

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	order := make([]int, numKeys)
	for i := range order {
		order[i] = i
	}
	for i := len(order) - 1; i > 0; i-- {
		j := int(uint64(i*17+31) % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}

	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(bigValSize)

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(order[i%numKeys]))
		val, _ := rocksDB.Get(ro, key)
		if val != nil {
			val.Free()
		}
	}
}
