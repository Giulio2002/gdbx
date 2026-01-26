package tests

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestPageCompaction tests that page compaction correctly reclaims space from holes
// created by deleted entries, allowing new insertions to succeed.
func TestPageCompaction(t *testing.T) {
	t.Run("BasicCompaction", testBasicCompaction)
	t.Run("CompactionAfterMultipleDeletes", testCompactionAfterMultipleDeletes)
	t.Run("CompactionWithDupSort", testCompactionWithDupSort)
	t.Run("CompactionPreservesDataIntegrity", testCompactionPreservesDataIntegrity)
}

// testBasicCompaction tests that deleting an entry and inserting a new one
// works correctly when compaction is needed to reclaim space.
func testBasicCompaction(t *testing.T) {
	path := t.TempDir() + "/compaction.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Fill page with entries
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		t.Fatal(err)
	}

	// Insert entries with moderately sized values to fill page
	numEntries := 50
	valueSize := 60 // Each entry ~68 bytes (8 node header + key + value)
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i)
		}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Delete some entries to create holes
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Delete entries in the middle
	for i := 10; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		if err := txn.Del(dbi, key, nil); err != nil {
			t.Fatalf("Failed to delete entry %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Now insert new entries - should trigger compaction to reclaim space
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Insert new entries that will use the reclaimed space
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("new%03d", i))
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(100 + i)
		}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Fatalf("Failed to insert new entry %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify data integrity
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Check original entries that weren't deleted
	for i := 0; i < numEntries; i++ {
		if i >= 10 && i < 20 {
			continue // These were deleted
		}
		key := []byte(fmt.Sprintf("key%03d", i))
		value, err := txn.Get(dbi, key)
		if err != nil {
			t.Errorf("Failed to get key%03d: %v", i, err)
			continue
		}
		expected := make([]byte, valueSize)
		for j := range expected {
			expected[j] = byte(i)
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("Value mismatch for key%03d", i)
		}
	}

	// Check new entries
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("new%03d", i))
		value, err := txn.Get(dbi, key)
		if err != nil {
			t.Errorf("Failed to get new%03d: %v", i, err)
			continue
		}
		expected := make([]byte, valueSize)
		for j := range expected {
			expected[j] = byte(100 + i)
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("Value mismatch for new%03d", i)
		}
	}
}

// testCompactionAfterMultipleDeletes tests compaction when multiple non-contiguous
// entries are deleted, creating scattered holes.
func testCompactionAfterMultipleDeletes(t *testing.T) {
	path := t.TempDir() + "/compaction_multi.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Insert entries
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		t.Fatal(err)
	}

	numEntries := 100
	valueSize := 30
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Delete every 3rd entry to create scattered holes
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	deletedKeys := make(map[string]bool)
	for i := 0; i < numEntries; i += 3 {
		key := fmt.Sprintf("k%04d", i)
		if err := txn.Del(dbi, []byte(key), nil); err != nil {
			t.Fatalf("Failed to delete entry %d: %v", i, err)
		}
		deletedKeys[key] = true
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Insert new entries to fill the reclaimed space
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("n%04d", i))
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(200 + i%56)
		}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Fatalf("Failed to insert new entry %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify using cursor iteration
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	count := 0
	for k, _, err := cursor.Get(nil, nil, gdbx.First); err == nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
		count++
		key := string(k)
		if deletedKeys[key] {
			t.Errorf("Found deleted key: %s", key)
		}
	}

	expectedCount := numEntries - len(deletedKeys) + 30
	if count != expectedCount {
		t.Errorf("Entry count mismatch: got %d, want %d", count, expectedCount)
	}
}

// testCompactionWithDupSort tests compaction behavior with DUPSORT tables
// where values within a key can be deleted creating holes in sub-pages.
func testCompactionWithDupSort(t *testing.T) {
	path := t.TempDir() + "/compaction_dup.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Insert multiple values per key
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("testkey")
	numValues := 50
	valueSize := 40
	for i := 0; i < numValues; i++ {
		value := make([]byte, valueSize)
		// Use i+1 to avoid zero values
		value[0] = byte(i + 1)
		for j := 1; j < valueSize; j++ {
			value[j] = byte((i + j) % 256)
		}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Fatalf("Failed to insert value %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Delete some values from the middle
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}

	// Position at start of key's values
	_, _, err = cursor.Get(key, nil, gdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	// Delete some values using cursor
	deletedCount := 0
	for i := 0; i < numValues && deletedCount < 15; i++ {
		_, v, err := cursor.Get(nil, nil, gdbx.GetCurrent)
		if err != nil {
			break
		}
		// Delete values where first byte is even
		if v[0]%3 == 0 {
			if err := cursor.Del(0); err != nil {
				t.Fatalf("Failed to delete value: %v", err)
			}
			deletedCount++
		}
		if _, _, err := cursor.Get(nil, nil, gdbx.NextDup); err != nil {
			break
		}
	}
	cursor.Close()
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Insert new values - should work with compacted space
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		value := make([]byte, valueSize)
		value[0] = byte(200 + i)
		for j := 1; j < valueSize; j++ {
			value[j] = byte((200 + i + j) % 256)
		}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			t.Fatalf("Failed to insert new value %d: %v", i, err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify count
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err = txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	_, _, err = cursor.Get(key, nil, gdbx.Set)
	if err != nil {
		t.Fatal(err)
	}

	count, err := cursor.Count()
	if err != nil {
		t.Fatal(err)
	}

	expectedCount := uint64(numValues - deletedCount + 10)
	if count != expectedCount {
		t.Errorf("Value count mismatch: got %d, want %d", count, expectedCount)
	}
}

// testCompactionPreservesDataIntegrity performs an intensive test to ensure
// compaction doesn't corrupt data even under heavy delete/insert cycles.
func testCompactionPreservesDataIntegrity(t *testing.T) {
	path := t.TempDir() + "/compaction_integrity.db"
	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()
	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Track what should be in the database
	expected := make(map[string][]byte)

	// Initial population
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := make([]byte, 20+i%30)
		for j := range value {
			value[j] = byte((i + j) % 256)
		}
		if err := txn.Put(dbi, []byte(key), value, 0); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
		expected[key] = value
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Perform multiple rounds of delete/insert cycles
	for round := 0; round < 5; round++ {
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		dbi, err = txn.OpenDBISimple("test", 0)
		if err != nil {
			t.Fatal(err)
		}

		// Delete some entries
		deleteCount := 0
		for key := range expected {
			if deleteCount >= 30 {
				break
			}
			if err := txn.Del(dbi, []byte(key), nil); err != nil {
				t.Fatalf("Round %d: Failed to delete %s: %v", round, key, err)
			}
			delete(expected, key)
			deleteCount++
		}

		// Insert new entries
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("r%d_key%05d", round, i)
			value := make([]byte, 25+i%20)
			for j := range value {
				value[j] = byte((round*100 + i + j) % 256)
			}
			if err := txn.Put(dbi, []byte(key), value, 0); err != nil {
				t.Fatalf("Round %d: Failed to insert %s: %v", round, key, err)
			}
			expected[key] = value
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all data
	txn, err = env.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err = txn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Check each expected key/value
	for key, expectedValue := range expected {
		value, err := txn.Get(dbi, []byte(key))
		if err != nil {
			t.Errorf("Missing key %s: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Value mismatch for %s: got len=%d, want len=%d", key, len(value), len(expectedValue))
		}
	}

	// Verify no extra keys exist
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	dbCount := 0
	for k, _, err := cursor.Get(nil, nil, gdbx.First); err == nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
		dbCount++
		if _, ok := expected[string(k)]; !ok {
			t.Errorf("Unexpected key in database: %s", k)
		}
	}

	if dbCount != len(expected) {
		t.Errorf("Entry count mismatch: got %d, want %d", dbCount, len(expected))
	}
}
