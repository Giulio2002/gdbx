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

// BenchmarkWriteLongKeys benchmarks write operations with 64-byte keys.
func BenchmarkWriteLongKeys(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		sizeName := formatLongSize(size)

		// Sequential Put
		b.Run(fmt.Sprintf("SeqPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchSeqPutLongGdbx(b, size)
		})
		b.Run(fmt.Sprintf("SeqPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchSeqPutLongMdbx(b, size)
		})
		b.Run(fmt.Sprintf("SeqPut_%s/bolt", sizeName), func(b *testing.B) {
			benchSeqPutLongBolt(b, size)
		})
		b.Run(fmt.Sprintf("SeqPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchSeqPutLongRocksDB(b, size)
		})

		// Random Put
		b.Run(fmt.Sprintf("RandPut_%s/gdbx", sizeName), func(b *testing.B) {
			benchRandPutLongGdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandPut_%s/mdbx", sizeName), func(b *testing.B) {
			benchRandPutLongMdbx(b, size)
		})
		b.Run(fmt.Sprintf("RandPut_%s/bolt", sizeName), func(b *testing.B) {
			benchRandPutLongBolt(b, size)
		})
		b.Run(fmt.Sprintf("RandPut_%s/rocksdb", sizeName), func(b *testing.B) {
			benchRandPutLongRocksDB(b, size)
		})
	}
}

func formatLongSize(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ============ Long Key Cache ============

var (
	longKeyMu      sync.Mutex
	longGdbxEnvs   = make(map[string]*gdbx.Env)
	longMdbxEnvs   = make(map[string]*mdbxgo.Env)
	longBoltDBs    = make(map[string]*bolt.DB)
	longRocksDBs   = make(map[string]*gorocksdb.DB)
	longKeyCache   = make(map[string][][]byte) // Pre-generated 64-byte keys
)

func getCachedLongKeyDB(b *testing.B, size int) (*gdbx.Env, *mdbxgo.Env, *bolt.DB, *gorocksdb.DB, [][]byte) {
	longKeyMu.Lock()
	defer longKeyMu.Unlock()

	key := fmt.Sprintf("long64_%d", size)
	gdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("long64_%d_gdbx.db", size))
	mdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("long64_%d_mdbx.db", size))
	boltPath := filepath.Join(benchCacheDir, fmt.Sprintf("long64_%d_bolt.db", size))
	rocksPath := filepath.Join(benchCacheDir, fmt.Sprintf("long64_%d_rocks.db", size))

	// Check if already loaded
	if genv, ok := longGdbxEnvs[key]; ok {
		return genv, longMdbxEnvs[key], longBoltDBs[key], longRocksDBs[key], longKeyCache[key]
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(benchCacheDir, 0755); err != nil {
		b.Fatal(err)
	}

	// Generate keys first (same keys for all DBs)
	keys := generateLongKeys(size)

	// Check if databases exist
	gdbxExists := fileExists(gdbxPath)
	mdbxExists := fileExists(mdbxPath)
	boltExists := fileExists(boltPath)
	rocksExists := fileExists(rocksPath)

	// Setup gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	genv.SetMaxDBs(10)
	genv.SetGeometry(-1, -1, 1<<33, -1, -1, 4096) // 8GB max for larger keys
	if err := genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		b.Fatal(err)
	}
	if err := genv.PreExtendMmap(256 * 1024 * 1024); err != nil {
		genv.Close()
		b.Fatal(err)
	}

	// Setup mdbx-go
	runtime.LockOSThread()
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("bench-long"))
	if err != nil {
		genv.Close()
		b.Fatal(err)
	}
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.SetGeometry(-1, -1, 1<<33, -1, -1, 4096)
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
		b.Logf("Creating cached gdbx long-key DB with %d keys...", size)
		populateLongKeyDBGdbx(b, genv, keys)
	} else {
		b.Logf("Using cached gdbx long-key DB with %d keys", size)
	}

	if !mdbxExists {
		b.Logf("Creating cached mdbx long-key DB with %d keys...", size)
		populateLongKeyDBMdbx(b, menv, keys)
	} else {
		b.Logf("Using cached mdbx long-key DB with %d keys", size)
	}

	if !boltExists {
		b.Logf("Creating cached BoltDB long-key DB with %d keys...", size)
		populateLongKeyDBBolt(b, boltDB, keys)
	} else {
		b.Logf("Using cached BoltDB long-key DB with %d keys", size)
	}

	if !rocksExists {
		b.Logf("Creating cached RocksDB long-key DB with %d keys...", size)
		populateLongKeyDBRocks(b, rocksDB, keys)
	} else {
		b.Logf("Using cached RocksDB long-key DB with %d keys", size)
	}

	// Cache
	longGdbxEnvs[key] = genv
	longMdbxEnvs[key] = menv
	longBoltDBs[key] = boltDB
	longRocksDBs[key] = rocksDB
	longKeyCache[key] = keys

	return genv, menv, boltDB, rocksDB, keys
}

func generateLongKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 64)
		// First 8 bytes are sequential index (for ordering)
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		// Rest is random (to simulate realistic key distribution)
		rand.Read(key[8:])
		keys[i] = key
	}
	return keys
}

func populateLongKeyDBGdbx(b *testing.B, env *gdbx.Env, keys [][]byte) {
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("bench", gdbx.Create)
	if err != nil {
		b.Fatal(err)
	}

	batchSize := 50_000
	val := make([]byte, 32)

	for i, k := range keys {
		binary.BigEndian.PutUint64(val, uint64(i))
		if err := txn.Put(dbi, k, val, gdbx.Upsert); err != nil {
			b.Fatal(err)
		}
		if (i+1)%batchSize == 0 {
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
			txn, err = env.BeginTxn(nil, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

func populateLongKeyDBMdbx(b *testing.B, env *mdbxgo.Env, keys [][]byte) {
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

	batchSize := 50_000
	val := make([]byte, 32)

	for i, k := range keys {
		binary.BigEndian.PutUint64(val, uint64(i))
		if err := txn.Put(dbi, k, val, mdbxgo.Upsert); err != nil {
			b.Fatal(err)
		}
		if (i+1)%batchSize == 0 {
			if _, err := txn.Commit(); err != nil {
				b.Fatal(err)
			}
			txn, err = env.BeginTxn(nil, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

func populateLongKeyDBBolt(b *testing.B, db *bolt.DB, keys [][]byte) {
	batchSize := 50_000
	val := make([]byte, 32)

	for start := 0; start < len(keys); start += batchSize {
		end := start + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		err := db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
			if err != nil {
				return err
			}
			for i := start; i < end; i++ {
				binary.BigEndian.PutUint64(val, uint64(i))
				if err := bucket.Put(keys[i], val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func populateLongKeyDBRocks(b *testing.B, db *gorocksdb.DB, keys [][]byte) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	batchSize := 50_000
	val := make([]byte, 32)

	for i, k := range keys {
		binary.BigEndian.PutUint64(val, uint64(i))
		batch.Put(k, val)
		if (i+1)%batchSize == 0 {
			if err := db.Write(wo, batch); err != nil {
				b.Fatal(err)
			}
			batch.Clear()
		}
	}
	if batch.Count() > 0 {
		if err := db.Write(wo, batch); err != nil {
			b.Fatal(err)
		}
	}
}

// ============ Sequential Put (64-byte keys) ============

func benchSeqPutLongGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, keys := getCachedLongKeyDB(b, numKeys)

	val := make([]byte, 32)

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
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, keys[i%numKeys], val, 0)
	}
}

func benchSeqPutLongMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, keys := getCachedLongKeyDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	val := make([]byte, 32)

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
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, keys[i%numKeys], val, 0)
	}
}

func benchSeqPutLongBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, keys := getCachedLongKeyDB(b, numKeys)

	val := make([]byte, 32)

	tx, err := boltDB.Begin(true)
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
		binary.BigEndian.PutUint64(val, uint64(i))
		bucket.Put(keys[i%numKeys], val)
	}
}

func benchSeqPutLongRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, keys := getCachedLongKeyDB(b, numKeys)

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)
	defer wo.Destroy()

	val := make([]byte, 32)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(val, uint64(i))
		rocksDB.Put(wo, keys[i%numKeys], val)
	}
}

// ============ Random Put (64-byte keys) ============

func benchRandPutLongGdbx(b *testing.B, numKeys int) {
	genv, _, _, _, keys := getCachedLongKeyDB(b, numKeys)

	val := make([]byte, 32)

	// Shuffle key order
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, keys[order[i%numKeys]], val, 0)
	}
}

func benchRandPutLongMdbx(b *testing.B, numKeys int) {
	_, menv, _, _, keys := getCachedLongKeyDB(b, numKeys)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	val := make([]byte, 32)

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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(val, uint64(i))
		txn.Put(dbi, keys[order[i%numKeys]], val, 0)
	}
}

func benchRandPutLongBolt(b *testing.B, numKeys int) {
	_, _, boltDB, _, keys := getCachedLongKeyDB(b, numKeys)

	val := make([]byte, 32)

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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(val, uint64(i))
		bucket.Put(keys[order[i%numKeys]], val)
	}
}

func benchRandPutLongRocksDB(b *testing.B, numKeys int) {
	_, _, _, rocksDB, keys := getCachedLongKeyDB(b, numKeys)

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)
	defer wo.Destroy()

	val := make([]byte, 32)

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
		binary.BigEndian.PutUint64(val, uint64(i))
		rocksDB.Put(wo, keys[order[i%numKeys]], val)
	}
}
