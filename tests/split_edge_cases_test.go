package tests

import (
	"bytes"
	"os"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestSplitWithMaxNodes tests page splits when nodes are at maximum size
func TestSplitWithMaxNodes(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-split-max-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := env.MaxKeySize()
	// Calculate max value that keeps node inline (not overflow)
	// nodeSize = 8 + keyLen + valLen <= pageCapacity (4074)
	// valLen <= 4074 - 8 - keyLen = 4066 - keyLen

	t.Run("two_max_nodes", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test1", gdbx.Create)

		// Insert two nodes that each take up half the page
		// This should work without splitting
		halfPageNode := 4074 / 2 // ~2037 bytes total per node
		keySize := 100
		valSize := halfPageNode - 8 - keySize - 2 // subtract header and entry ptr

		key1 := make([]byte, keySize)
		key1[0] = 'a'
		val1 := make([]byte, valSize)

		key2 := make([]byte, keySize)
		key2[0] = 'b'
		val2 := make([]byte, valSize)

		if err := txn.Put(dbi, key1, val1, 0); err != nil {
			t.Errorf("Put key1 failed: %v", err)
			return
		}
		t.Logf("Put key1 (node=%d): OK", 8+keySize+valSize)

		if err := txn.Put(dbi, key2, val2, 0); err != nil {
			t.Errorf("Put key2 failed: %v", err)
			return
		}
		t.Logf("Put key2 (node=%d): OK", 8+keySize+valSize)

		// Now add a third node - should trigger split
		key3 := make([]byte, keySize)
		key3[0] = 'c'
		val3 := make([]byte, valSize)

		if err := txn.Put(dbi, key3, val3, 0); err != nil {
			t.Errorf("Put key3 (split) failed: %v", err)
			return
		}
		t.Logf("Put key3 (split triggered): OK")
	})

	t.Run("nodes_at_split_boundary", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test2", gdbx.Create)

		// Create nodes that barely fit 2 per page
		// Then add a third that requires split
		keySize := maxKey
		valSize := 100 // Small value with max key

		for i := 0; i < 10; i++ {
			key := make([]byte, keySize)
			key[0] = byte(i)
			val := make([]byte, valSize)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put %d failed: %v", i, err)
				return
			}
		}
		t.Logf("Added 10 entries with maxKey successfully")
	})

	t.Run("alternating_sizes", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test3", gdbx.Create)

		// Alternate between very small and very large nodes
		// This tests split point calculation with heterogeneous data
		for i := 0; i < 20; i++ {
			var keySize, valSize int
			if i%2 == 0 {
				keySize = 10
				valSize = 10
			} else {
				keySize = maxKey - 500
				valSize = 500
			}

			key := make([]byte, keySize)
			key[0] = byte(i)
			val := make([]byte, valSize)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put %d (key=%d, val=%d) failed: %v", i, keySize, valSize, err)
				return
			}
		}
		t.Logf("Added 20 alternating-size entries successfully")
	})

	t.Run("insert_at_beginning", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test4", gdbx.Create)

		keySize := maxKey / 2
		valSize := 500

		// First fill the page with keys starting at 'b'
		for i := 0; i < 5; i++ {
			key := make([]byte, keySize)
			key[0] = 'b'
			key[1] = byte(i)
			val := make([]byte, valSize)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put 'b' %d failed: %v", i, err)
				return
			}
		}

		// Now insert at beginning (key 'a') - tests split with insert at idx=0
		key := make([]byte, keySize)
		key[0] = 'a'
		val := make([]byte, valSize)

		if err := txn.Put(dbi, key, val, 0); err != nil {
			t.Errorf("Put 'a' at beginning failed: %v", err)
			return
		}
		t.Logf("Insert at beginning after fill: OK")
	})

	t.Run("insert_in_middle", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test5", gdbx.Create)

		keySize := maxKey / 2
		valSize := 500

		// Insert keys 'a', 'c', 'd', 'e', 'f'
		keys := []byte{'a', 'c', 'd', 'e', 'f'}
		for _, k := range keys {
			key := make([]byte, keySize)
			key[0] = k
			val := make([]byte, valSize)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put '%c' failed: %v", k, err)
				return
			}
		}

		// Now insert 'b' in the middle - tests split with insert in middle
		key := make([]byte, keySize)
		key[0] = 'b'
		val := make([]byte, valSize)

		if err := txn.Put(dbi, key, val, 0); err != nil {
			t.Errorf("Put 'b' in middle failed: %v", err)
			return
		}
		t.Logf("Insert in middle after fill: OK")
	})
}

// TestSplitCompat verifies split behavior matches between gdbx and mdbx
func TestSplitCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Create with gdbx using patterns that trigger splits
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(db.path, 0, 0644)

	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("test", gdbx.Create)

	maxKey := genv.MaxKeySize()
	entries := make(map[string][]byte)

	// Insert enough large entries to cause multiple splits
	for i := 0; i < 50; i++ {
		keySize := maxKey / 2
		valSize := 500

		key := make([]byte, keySize)
		key[0] = byte(i / 26)
		key[1] = byte('a' + (i % 26))
		val := make([]byte, valSize)
		for j := range val {
			val[j] = byte(i)
		}

		if err := gtxn.Put(gdbi, key, val, 0); err != nil {
			t.Errorf("gdbx Put %d failed: %v", i, err)
			gtxn.Abort()
			return
		}
		entries[string(key)] = val
	}

	gtxn.Commit()
	genv.Close()

	// Verify with mdbx
	menv, _ := mdbx.NewEnv(mdbx.Label("test"))
	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	menv.SetOption(mdbx.OptMaxDB, 10)
	menv.Open(db.path, mdbx.Readonly, 0644)
	defer menv.Close()

	mtxn, _ := menv.BeginTxn(nil, mdbx.Readonly)
	defer mtxn.Abort()

	mdbi, _ := mtxn.OpenDBI("test", 0, nil, nil)

	verified := 0
	for keyStr, expectedVal := range entries {
		key := []byte(keyStr)
		gotVal, err := mtxn.Get(mdbi, key)
		if err != nil {
			t.Errorf("mdbx Get key[0]=%d key[1]=%c failed: %v", key[0], key[1], err)
			continue
		}
		if !bytes.Equal(gotVal, expectedVal) {
			t.Errorf("mdbx value mismatch for key[1]=%c", key[1])
			continue
		}
		verified++
	}

	if verified != len(entries) {
		t.Errorf("Only verified %d/%d entries", verified, len(entries))
	} else {
		t.Logf("All %d entries verified after splits", verified)
	}
}

// TestBranchPageSplit tests splits that affect branch pages (tree height > 1)
func TestBranchPageSplit(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-branch-split-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}

	txn, _ := env.BeginTxn(nil, 0)
	dbi, _ := txn.OpenDBISimple("test", gdbx.Create)

	maxKey := env.MaxKeySize()

	// Insert enough entries to create a tree with height > 1
	// With large keys, fewer entries fit per page, so tree grows faster
	keySize := maxKey / 4 // ~500 bytes
	valSize := 100

	for i := 0; i < 500; i++ {
		key := make([]byte, keySize)
		// Spread keys across keyspace to exercise different split scenarios
		key[0] = byte(i % 256)
		key[1] = byte(i / 256)
		val := make([]byte, valSize)
		val[0] = byte(i)

		if err := txn.Put(dbi, key, val, 0); err != nil {
			t.Errorf("Put %d failed: %v", i, err)
			txn.Abort()
			return
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
		return
	}

	t.Logf("Inserted 500 entries with key size %d - tree height should be > 1", keySize)

	// Verify we can read them all back
	txn2, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn2.Abort()

	cursor, _ := txn2.OpenCursor(dbi)
	defer cursor.Close()

	count := 0
	_, _, err = cursor.Get(nil, nil, gdbx.First)
	for err == nil {
		count++
		_, _, err = cursor.Get(nil, nil, gdbx.Next)
	}

	if count != 500 {
		t.Errorf("Read back %d entries, want 500", count)
	} else {
		t.Logf("Read back all 500 entries successfully")
	}
}

// TestSplitWithVeryLargeNodes tests splits when nodes are so large
// that only 2 can fit per page (the minimum for split to work)
func TestSplitWithVeryLargeNodes(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-split-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(dir, 0, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := env.MaxKeySize()

	// Test with maximum size nodes (just under overflow threshold)
	t.Run("max_size_nodes", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test1", gdbx.Create)

		// Calculate max node size that doesn't go to overflow
		// Node = 8 (header) + key + value
		// Must fit on page: nodeSize + 2 (ptr) <= pageCapacity
		// pageCapacity = 4096 - 20 = 4076
		// Max single node = 4076 - 2 = 4074
		// So key + value <= 4074 - 8 = 4066

		keySize := maxKey // 2022
		// Value size that makes node just under overflow threshold
		// nodeSize = 8 + 2022 + valSize
		// For overflow: valSize > maxVal OR nodeSize > 4074
		// To stay inline: nodeSize <= 4074, so valSize <= 4074 - 8 - 2022 = 2044
		// But maxVal = 2037, so valSize is limited to 2037
		valSize := 2037

		t.Logf("Using key=%d, val=%d, nodeSize=%d", keySize, valSize, 8+keySize+valSize)

		// With these sizes, we can fit at most 2 nodes per page
		// Insert enough to cause multiple splits
		for i := 0; i < 10; i++ {
			key := make([]byte, keySize)
			key[0] = byte(i)
			val := make([]byte, valSize)
			val[0] = byte(i)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put %d failed: %v", i, err)
				return
			}
		}
		t.Logf("Added 10 max-size entries successfully")
	})

	// Test split where new entry is larger than existing entries
	t.Run("new_entry_larger", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test2", gdbx.Create)

		// First add several small entries to fill page partially
		smallKeySize := 100
		smallValSize := 100
		for i := 0; i < 15; i++ {
			key := make([]byte, smallKeySize)
			key[0] = 'a'
			key[1] = byte(i)
			val := make([]byte, smallValSize)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put small %d failed: %v", i, err)
				return
			}
		}

		// Now add a large entry that will trigger split
		largeKeySize := maxKey / 2
		largeValSize := 1500
		key := make([]byte, largeKeySize)
		key[0] = 'z'
		val := make([]byte, largeValSize)

		if err := txn.Put(dbi, key, val, 0); err != nil {
			t.Errorf("Put large entry failed: %v", err)
			return
		}
		t.Logf("Added 15 small + 1 large entry successfully")
	})

	// Test alternating between max-key and max-value entries
	t.Run("alternating_max_key_max_val", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test3", gdbx.Create)

		maxVal := env.MaxValSize()

		for i := 0; i < 20; i++ {
			var keySize, valSize int
			if i%2 == 0 {
				// Max key, small value
				keySize = maxKey
				valSize = 100
			} else {
				// Small key, large value (will go to overflow)
				keySize = 100
				valSize = maxVal
			}

			key := make([]byte, keySize)
			key[0] = byte(i)
			val := make([]byte, valSize)

			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Errorf("Put %d (key=%d, val=%d) failed: %v", i, keySize, valSize, err)
				return
			}
		}
		t.Logf("Added 20 alternating max-key/max-val entries successfully")
	})
}
