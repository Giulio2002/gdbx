//go:build rocksdb

package benchmarks

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tecbot/gorocksdb"
)

var rocksDBs = make(map[string]*gorocksdb.DB)

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

func cleanupRocksDBCache() {
	for _, db := range rocksDBs {
		db.Close()
	}
	rocksDBs = make(map[string]*gorocksdb.DB)
}
