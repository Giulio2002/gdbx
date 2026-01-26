package tests

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestMmapExtension tests that mmap extension works correctly in WriteMap mode
func TestMmapExtension(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/mmap_test.db"

	// Create env with WriteMap mode
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	// Set geometry: start small, grow to 4GB
	env.SetGeometry(-1, -1, 4<<30, -1, -1, 4096)

	flags := gdbx.NoSubdir | gdbx.NoMetaSync | gdbx.WriteMap | gdbx.Create
	if err := env.Open(dbPath, flags, 0644); err != nil {
		t.Fatal(err)
	}

	// Check file size after open
	info, _ := os.Stat(dbPath)
	t.Logf("Initial file size: %d bytes", info.Size())

	// Insert 100k keys
	numKeys := 100_000
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	val := make([]byte, 32)

	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i))
		if err := txn.Put(dbi, key, val, gdbx.Upsert); err != nil {
			t.Fatalf("Put failed at key %d: %v", i, err)
		}

		if i > 0 && i%10000 == 0 {
			info, _ := os.Stat(dbPath)
			t.Logf("After %d keys: file size = %d bytes (%.2f MB)", i, info.Size(), float64(info.Size())/(1024*1024))
		}
	}

	// Check before commit
	info, _ = os.Stat(dbPath)
	t.Logf("Before commit: file size = %d bytes (%.2f MB)", info.Size(), float64(info.Size())/(1024*1024))

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Check after commit
	info, _ = os.Stat(dbPath)
	t.Logf("After commit: file size = %d bytes (%.2f MB)", info.Size(), float64(info.Size())/(1024*1024))
}
