package benchmarks

import (
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

// Cached benchmark database directory
const benchCacheDir = "testdata/benchdb"

var (
	cacheMu     sync.Mutex
	gdbxEnvs    = make(map[string]*gdbx.Env)
	mdbxEnvs    = make(map[string]*mdbxgo.Env)
	boltDBs     = make(map[string]*bolt.DB)
	rocksDBs    = make(map[string]*gorocksdb.DB)
	sampleCache = make(map[string][][]byte)
)

// getCachedPlainDB returns a cached plain database, creating it if needed.
// The database is stored in testdata/benchdb/plain_<size>_gdbx.db
func getCachedPlainDB(b *testing.B, size int) (*gdbx.Env, *mdbxgo.Env, [][]byte) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	key := fmt.Sprintf("plain_%d", size)
	gdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("plain_%d_gdbx.db", size))
	mdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("plain_%d_mdbx.db", size))

	// Check if already loaded in memory
	if genv, ok := gdbxEnvs[key]; ok {
		return genv, mdbxEnvs[key], sampleCache[key]
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(benchCacheDir, 0755); err != nil {
		b.Fatal(err)
	}

	// Check if databases exist on disk
	gdbxExists := fileExists(gdbxPath)
	mdbxExists := fileExists(mdbxPath)

	// Setup gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	genv.SetMaxDBs(10)
	genv.SetGeometry(-1, -1, 1<<32, -1, -1, 4096) // 4GB max
	if err := genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		b.Fatal(err)
	}
	// Pre-extend mmap for WriteMap mode to avoid heap allocations during writes
	if err := genv.PreExtendMmap(128 * 1024 * 1024); err != nil { // 128MB
		genv.Close()
		b.Fatal(err)
	}

	// Setup mdbx-go
	runtime.LockOSThread()
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	if err != nil {
		genv.Close()
		b.Fatal(err)
	}
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.SetGeometry(-1, -1, 1<<32, -1, -1, 4096) // 4GB max
	if err := menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync|mdbxgo.WriteMap, 0644); err != nil {
		genv.Close()
		b.Fatal(err)
	}
	runtime.UnlockOSThread()

	// Populate if needed
	if !gdbxExists {
		b.Logf("Creating cached gdbx plain DB with %d keys...", size)
		populatePlainDBCached(b, genv, size)
	} else {
		b.Logf("Using cached gdbx plain DB with %d keys", size)
	}

	if !mdbxExists {
		b.Logf("Creating cached mdbx plain DB with %d keys...", size)
		populatePlainDBMdbxCached(b, menv, size)
	} else {
		b.Logf("Using cached mdbx plain DB with %d keys", size)
	}

	// Collect sample keys
	samples := collectSampleKeysCached(b, genv, "bench", size)

	// Cache in memory
	gdbxEnvs[key] = genv
	mdbxEnvs[key] = menv
	sampleCache[key] = samples

	return genv, menv, samples
}

// getCachedDupSortDB returns a cached dupsort database, creating it if needed.
func getCachedDupSortDB(b *testing.B, numKeys, valsPerKey int) (*gdbx.Env, *mdbxgo.Env, [][]byte) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	total := numKeys * valsPerKey
	key := fmt.Sprintf("dupsort_%d", total)
	gdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("dupsort_%d_gdbx.db", total))
	mdbxPath := filepath.Join(benchCacheDir, fmt.Sprintf("dupsort_%d_mdbx.db", total))

	// Check if already loaded in memory
	if genv, ok := gdbxEnvs[key]; ok {
		return genv, mdbxEnvs[key], sampleCache[key]
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(benchCacheDir, 0755); err != nil {
		b.Fatal(err)
	}

	// Check if databases exist on disk
	gdbxExists := fileExists(gdbxPath)
	mdbxExists := fileExists(mdbxPath)

	// Setup gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	genv.SetMaxDBs(10)
	genv.SetGeometry(-1, -1, 1<<32, -1, -1, 4096) // 4GB max
	if err := genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		b.Fatal(err)
	}
	// Pre-extend mmap for WriteMap mode to avoid heap allocations during writes
	if err := genv.PreExtendMmap(128 * 1024 * 1024); err != nil { // 128MB
		genv.Close()
		b.Fatal(err)
	}

	// Setup mdbx-go
	runtime.LockOSThread()
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	if err != nil {
		genv.Close()
		b.Fatal(err)
	}
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.SetGeometry(-1, -1, 1<<32, -1, -1, 4096)
	if err := menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync|mdbxgo.WriteMap, 0644); err != nil {
		genv.Close()
		b.Fatal(err)
	}
	runtime.UnlockOSThread()

	// Populate if needed
	if !gdbxExists {
		b.Logf("Creating cached gdbx dupsort DB with %d keys x %d vals...", numKeys, valsPerKey)
		populateDupSortDBCached(b, genv, numKeys, valsPerKey)
	} else {
		b.Logf("Using cached gdbx dupsort DB with %d total entries", total)
	}

	if !mdbxExists {
		b.Logf("Creating cached mdbx dupsort DB with %d keys x %d vals...", numKeys, valsPerKey)
		populateDupSortDBMdbxCached(b, menv, numKeys, valsPerKey)
	} else {
		b.Logf("Using cached mdbx dupsort DB with %d total entries", total)
	}

	// Collect sample keys
	samples := collectSampleKeysCached(b, genv, "dupbench", numKeys)

	// Cache in memory
	gdbxEnvs[key] = genv
	mdbxEnvs[key] = menv
	sampleCache[key] = samples

	return genv, menv, samples
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func populatePlainDBCached(b *testing.B, env *gdbx.Env, numKeys int) {
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("bench", gdbx.Create)
	if err != nil {
		b.Fatal(err)
	}

	batchSize := 100_000
	key := make([]byte, 8)
	val := make([]byte, 32)

	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i))

		if err := txn.Put(dbi, key, val, gdbx.Upsert); err != nil {
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

func populatePlainDBMdbxCached(b *testing.B, env *mdbxgo.Env, numKeys int) {
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

	batchSize := 100_000
	key := make([]byte, 8)
	val := make([]byte, 32)

	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i))

		if err := txn.Put(dbi, key, val, mdbxgo.Upsert); err != nil {
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

func populateDupSortDBCached(b *testing.B, env *gdbx.Env, numKeys, valsPerKey int) {
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("dupbench", gdbx.Create|gdbx.DupSort)
	if err != nil {
		b.Fatal(err)
	}

	batchSize := 100_000
	key := make([]byte, 8)
	val := make([]byte, 16)
	count := 0

	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := 0; j < valsPerKey; j++ {
			binary.BigEndian.PutUint64(val, uint64(j))

			if err := txn.Put(dbi, key, val, gdbx.Upsert); err != nil {
				b.Fatal(err)
			}

			count++
			if count%batchSize == 0 {
				if _, err := txn.Commit(); err != nil {
					b.Fatal(err)
				}
				txn, err = env.BeginTxn(nil, 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

func populateDupSortDBMdbxCached(b *testing.B, env *mdbxgo.Env, numKeys, valsPerKey int) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBI("dupbench", mdbxgo.Create|mdbxgo.DupSort, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	batchSize := 100_000
	key := make([]byte, 8)
	val := make([]byte, 16)
	count := 0

	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := 0; j < valsPerKey; j++ {
			binary.BigEndian.PutUint64(val, uint64(j))

			if err := txn.Put(dbi, key, val, mdbxgo.Upsert); err != nil {
				b.Fatal(err)
			}

			count++
			if count%batchSize == 0 {
				if _, err := txn.Commit(); err != nil {
					b.Fatal(err)
				}
				txn, err = env.BeginTxn(nil, 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		b.Fatal(err)
	}
}

func collectSampleKeysCached(b *testing.B, env *gdbx.Env, tableName string, numKeys int) [][]byte {
	txn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBISimple(tableName, 0)
	if err != nil {
		b.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		b.Fatal(err)
	}
	defer cursor.Close()

	// Collect every 1000th key
	samples := make([][]byte, 0, numKeys/1000+1)
	i := 0
	for k, _, err := cursor.Get(nil, nil, gdbx.First); k != nil && err == nil; k, _, err = cursor.Get(nil, nil, gdbx.NextNoDup) {
		if i%1000 == 0 {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			samples = append(samples, keyCopy)
		}
		i++
	}

	return samples
}

// getCachedBoltDB returns a cached BoltDB database, creating it if needed.
func getCachedBoltDB(b *testing.B, size int) *bolt.DB {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	key := fmt.Sprintf("bolt_%d", size)
	boltPath := filepath.Join(benchCacheDir, fmt.Sprintf("plain_%d_bolt.db", size))

	// Check if already loaded in memory
	if db, ok := boltDBs[key]; ok {
		return db
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(benchCacheDir, 0755); err != nil {
		b.Fatal(err)
	}

	// Check if database exists on disk
	boltExists := fileExists(boltPath)

	// Setup BoltDB
	db, err := bolt.Open(boltPath, 0644, &bolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
	})
	if err != nil {
		b.Fatal(err)
	}

	// Populate if needed
	if !boltExists {
		b.Logf("Creating cached BoltDB with %d keys...", size)
		populateBoltDBCached(b, db, size)
	} else {
		b.Logf("Using cached BoltDB with %d keys", size)
	}

	// Cache in memory
	boltDBs[key] = db

	return db
}

func populateBoltDBCached(b *testing.B, db *bolt.DB, numKeys int) {
	batchSize := 100_000
	key := make([]byte, 8)
	val := make([]byte, 32)

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
		if err != nil {
			return err
		}

		for i := 0; i < numKeys; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i))

			if err := bucket.Put(key, val); err != nil {
				return err
			}

			// Commit in batches
			if (i+1)%batchSize == 0 && i+1 < numKeys {
				return nil // Will be called again
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	// Handle remaining batches
	written := batchSize
	for written < numKeys {
		batchEnd := written + batchSize
		if batchEnd > numKeys {
			batchEnd = numKeys
		}

		err := db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			for i := written; i < batchEnd; i++ {
				binary.BigEndian.PutUint64(key, uint64(i))
				binary.BigEndian.PutUint64(val, uint64(i))
				if err := bucket.Put(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		written = batchEnd
	}
}

// getCachedRocksDB returns a cached RocksDB database, creating it if needed.
func getCachedRocksDB(b *testing.B, size int) *gorocksdb.DB {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	key := fmt.Sprintf("rocks_%d", size)
	rocksPath := filepath.Join(benchCacheDir, fmt.Sprintf("plain_%d_rocks.db", size))

	// Check if already loaded in memory
	if db, ok := rocksDBs[key]; ok {
		return db
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(benchCacheDir, 0755); err != nil {
		b.Fatal(err)
	}

	// Check if database exists on disk
	rocksExists := fileExists(rocksPath)

	// Setup RocksDB
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(64 * 1024 * 1024) // 64MB write buffer
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(64 * 1024 * 1024)

	db, err := gorocksdb.OpenDb(opts, rocksPath)
	if err != nil {
		b.Fatal(err)
	}

	// Populate if needed
	if !rocksExists {
		b.Logf("Creating cached RocksDB with %d keys...", size)
		populateRocksDBCached(b, db, size)
	} else {
		b.Logf("Using cached RocksDB with %d keys", size)
	}

	// Cache in memory
	rocksDBs[key] = db

	return db
}

func populateRocksDBCached(b *testing.B, db *gorocksdb.DB, numKeys int) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	key := make([]byte, 8)
	val := make([]byte, 32)

	// Use WriteBatch for better performance
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	batchSize := 100_000

	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i))

		batch.Put(key, val)

		if (i+1)%batchSize == 0 {
			if err := db.Write(wo, batch); err != nil {
				b.Fatal(err)
			}
			batch.Clear()
		}
	}

	// Write remaining
	if batch.Count() > 0 {
		if err := db.Write(wo, batch); err != nil {
			b.Fatal(err)
		}
	}
}

// CleanupBenchCache closes all cached environments.
// Call this in TestMain or after benchmarks complete.
func CleanupBenchCache() {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	for _, env := range gdbxEnvs {
		env.Close()
	}
	for _, env := range mdbxEnvs {
		env.Close()
	}
	for _, db := range boltDBs {
		db.Close()
	}
	for _, db := range rocksDBs {
		db.Close()
	}
	gdbxEnvs = make(map[string]*gdbx.Env)
	mdbxEnvs = make(map[string]*mdbxgo.Env)
	boltDBs = make(map[string]*bolt.DB)
	rocksDBs = make(map[string]*gorocksdb.DB)
	sampleCache = make(map[string][][]byte)
}

// DeleteBenchCache removes all cached database files.
func DeleteBenchCache() error {
	return os.RemoveAll(benchCacheDir)
}
