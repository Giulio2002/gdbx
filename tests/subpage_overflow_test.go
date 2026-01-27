package tests

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestSubpageToSubtreeOverflow tries to trigger ErrPageFull during
// sub-page to sub-tree conversion by filling a sub-page to near capacity
// then adding one more value to trigger the conversion.
func TestSubpageToSubtreeOverflow(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-subpage-overflow-*")
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

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := []byte("testkey")

	// Try to fill the sub-page to near capacity
	// Sub-page is roughly pageSize/2 - some overhead = ~2000 bytes
	// Each value in sub-page: 8 (node header) + 2 (entry ptr) + len(value)
	// When converting to sub-tree: 8 (node header) + 2 (entry ptr) + len(value)
	// So approximately same overhead

	// Test with many small values
	t.Run("many_small_values", func(t *testing.T) {
		// Add values until we likely trigger conversion
		for i := 0; i < 200; i++ {
			value := []byte(fmt.Sprintf("value%03d", i))
			err := txn.Put(dbi, key, value, 0)
			if err != nil {
				t.Errorf("Put value %d failed: %v", i, err)
				return
			}
		}
		t.Logf("Added 200 small values without error")
	})

	txn.Abort()

	// Test with fewer larger values
	t.Run("fewer_large_values", func(t *testing.T) {
		txn2, _ := env.BeginTxn(nil, 0)
		defer txn2.Abort()

		dbi2, _ := txn2.OpenDBISimple("test2", gdbx.Create|gdbx.DupSort)

		key2 := []byte("testkey2")

		// Try values of size 100 bytes each
		// Sub-page capacity ~2000 bytes, each value ~110 bytes overhead
		// Should fit ~18 values before conversion
		for i := 0; i < 50; i++ {
			value := make([]byte, 100)
			value[0] = byte(i)
			err := txn2.Put(dbi2, key2, value, 0)
			if err != nil {
				t.Errorf("Put large value %d failed: %v", i, err)
				return
			}
		}
		t.Logf("Added 50 values of 100 bytes without error")
	})

	// Test with values near maxVal for DupSort
	t.Run("values_near_limit", func(t *testing.T) {
		txn3, _ := env.BeginTxn(nil, 0)
		defer txn3.Abort()

		dbi3, _ := txn3.OpenDBISimple("test3", gdbx.Create|gdbx.DupSort)

		key3 := []byte("testkey3")

		// In DupSort, values become keys in the sub-tree
		// Max value size is limited by what can fit as a key in sub-tree
		maxDupVal := env.MaxKeySize() // Values limited by key size in sub-tree

		t.Logf("Max dup value size (limited by sub-tree key): %d", maxDupVal)

		// Try adding values near the limit
		for i := 0; i < 5; i++ {
			// Use progressively larger values
			valSize := maxDupVal - 100 + i*20
			if valSize > maxDupVal {
				valSize = maxDupVal
			}
			value := make([]byte, valSize)
			value[0] = byte(i)

			err := txn3.Put(dbi3, key3, value, 0)
			if err != nil {
				t.Errorf("Put value size %d failed: %v", valSize, err)
			} else {
				t.Logf("Put value size %d: OK", valSize)
			}
		}
	})
}

// TestSubpageToSubtreeCompat verifies mdbx and gdbx handle sub-page to sub-tree
// conversion the same way
func TestSubpageToSubtreeCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	// Create with mdbx - fill a dupsort key until sub-tree conversion
	menv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	menv.SetOption(mdbx.OptMaxDB, 10)
	if err := menv.Open(db.path, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	mtxn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	mdbi, err := mtxn.OpenDBI("test", mdbx.Create|mdbx.DupSort, nil, nil)
	if err != nil {
		mtxn.Abort()
		t.Fatal(err)
	}

	key := []byte("dupsort_key")
	values := make([][]byte, 100)

	// Add 100 values of 50 bytes each - should trigger sub-tree conversion
	for i := 0; i < 100; i++ {
		values[i] = make([]byte, 50)
		values[i][0] = byte(i)

		if err := mtxn.Put(mdbi, key, values[i], 0); err != nil {
			t.Fatalf("mdbx Put %d failed: %v", i, err)
		}
	}

	mtxn.Commit()
	menv.Close()

	// Read with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()

	genv.SetMaxDBs(10)
	if err := genv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	gtxn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gtxn.Abort()

	gdbi, err := gtxn.OpenDBISimple("test", gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	// Verify all values can be read
	cursor, err := gtxn.OpenCursor(gdbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	k, v, err := cursor.Get(key, nil, gdbx.SetKey)
	if err != nil {
		t.Fatalf("gdbx cursor Set failed: %v", err)
	}

	for err == nil {
		if !bytes.Equal(k, key) {
			break
		}
		// Verify value matches one of our inserted values
		found := false
		for _, expected := range values {
			if bytes.Equal(v, expected) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("gdbx returned unexpected value: %v", v[:min(10, len(v))])
		}
		count++
		k, v, err = cursor.Get(nil, nil, gdbx.NextDup)
	}

	if count != len(values) {
		t.Errorf("gdbx read %d values, want %d", count, len(values))
	} else {
		t.Logf("gdbx read all %d values correctly", count)
	}
}

// TestDupSortLargeValueCompat tests large values in DupSort tables
func TestDupSortLargeValueCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Test gdbx creating, mdbx reading
	t.Run("gdbx_to_mdbx", func(t *testing.T) {
		db := newTestDB(t)
		defer db.cleanup()

		genv, _ := gdbx.NewEnv(gdbx.Default)
		genv.SetMaxDBs(10)
		genv.Open(db.path, 0, 0644)

		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)

		key := []byte("key")
		maxDupVal := genv.MaxKeySize() // Values limited by sub-tree key size

		// Add values of various sizes
		sizes := []int{10, 100, 500, 1000, maxDupVal - 100, maxDupVal}
		values := make(map[int][]byte)

		for _, size := range sizes {
			val := make([]byte, size)
			val[0] = byte(size & 0xFF)
			val[1] = byte((size >> 8) & 0xFF)

			if err := gtxn.Put(gdbi, key, val, 0); err != nil {
				t.Errorf("gdbx Put val size %d failed: %v", size, err)
				continue
			}
			values[size] = val
			t.Logf("gdbx Put val size %d: OK", size)
		}

		gtxn.Commit()
		genv.Close()

		// Read with mdbx
		menv, _ := mdbx.NewEnv(mdbx.Label("test"))
		menv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
		menv.SetOption(mdbx.OptMaxDB, 10)
		menv.Open(db.path, mdbx.Readonly, 0644)
		defer menv.Close()

		mtxn, _ := menv.BeginTxn(nil, mdbx.Readonly)
		defer mtxn.Abort()

		mdbi, _ := mtxn.OpenDBI("test", mdbx.DupSort, nil, nil)

		cursor, _ := mtxn.OpenCursor(mdbi)
		defer cursor.Close()

		count := 0
		_, v, err := cursor.Get(key, nil, mdbx.Set)
		for err == nil {
			count++
			// Check value is one we inserted
			size := int(v[0]) | (int(v[1]) << 8)
			if expected, ok := values[size]; ok {
				if !bytes.Equal(v, expected) {
					t.Errorf("mdbx value size %d mismatch", size)
				}
			}
			_, v, err = cursor.Get(nil, nil, mdbx.NextDup)
		}

		if count != len(values) {
			t.Errorf("mdbx read %d values, want %d", count, len(values))
		} else {
			t.Logf("mdbx read all %d values correctly", count)
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestLargeKeySubpageConversion tests sub-page to sub-tree conversion
// with large keys that force early conversion due to node size limits.
func TestLargeKeySubpageConversion(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-largekey-subpage-*")
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

	// Test with progressively larger keys to trigger early conversion
	t.Run("large_key_few_values", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test1", gdbx.Create|gdbx.DupSort)

		// Large key = 1500 bytes
		// LeafNodeMax = 2038
		// Max subPageLen = 2038 - 8 - 1500 = 530 bytes
		// Sub-page header = 20 bytes, so data area = 510 bytes max
		// Each value: 2 (ptr) + 8 (node) + valueLen = 10 + valueLen
		// With 50-byte values: 60 bytes each, can fit ~8 values before conversion
		keySize := 1500
		key := make([]byte, keySize)
		key[0] = 'k'

		for i := 0; i < 20; i++ {
			value := make([]byte, 50)
			value[0] = byte(i)

			err := txn.Put(dbi, key, value, 0)
			if err != nil {
				t.Errorf("Put value %d failed: %v", i, err)
				return
			}
		}
		t.Logf("Added 20 values with %d-byte key successfully", keySize)
	})

	// Test with key near max size
	t.Run("near_max_key_with_values", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test2", gdbx.Create|gdbx.DupSort)

		// Key just under max to leave some room for sub-page
		keySize := maxKey - 100 // ~1922 bytes
		key := make([]byte, keySize)
		key[0] = 'k'

		// With key=1922, max subPageLen = 2038 - 8 - 1922 = 108 bytes
		// Sub-page header = 20, data area = 88 bytes
		// Each value needs ~12 bytes minimum (2+8+2=12 for 2-byte value)
		// So we can fit maybe 7 tiny values before conversion

		for i := 0; i < 30; i++ {
			value := make([]byte, 10)
			value[0] = byte(i)

			err := txn.Put(dbi, key, value, 0)
			if err != nil {
				t.Errorf("Put value %d failed: %v", i, err)
				return
			}
		}
		t.Logf("Added 30 values with %d-byte key (near max) successfully", keySize)
	})

	// Test where conversion happens with max-size dup values
	t.Run("max_size_dup_values", func(t *testing.T) {
		txn, _ := env.BeginTxn(nil, 0)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple("test3", gdbx.Create|gdbx.DupSort)

		key := []byte("testkey")
		// In DupSort, values are limited by MaxKeySize (they become keys in sub-tree)
		maxDupVal := maxKey

		// Add several max-size values
		for i := 0; i < 10; i++ {
			value := make([]byte, maxDupVal-10+i) // Varying sizes near max
			value[0] = byte(i)

			err := txn.Put(dbi, key, value, 0)
			if err != nil {
				t.Errorf("Put value %d (size=%d) failed: %v", i, len(value), err)
				return
			}
		}
		t.Logf("Added 10 near-max-size dup values successfully")
	})
}
