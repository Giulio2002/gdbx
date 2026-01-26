package tests

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	gdbx "github.com/Giulio2002/gdbx"
)

// TestPruneIterateDelete tests the pattern used by erigon's prune:
// 1. First loop: iterate with NextNoDup to collect keys
// 2. Second loop: iterate with NextNoDup while deleting with Txn.Del
func TestPruneIterateDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-prune-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	// Create a DupSort database (like erigon's KeysTable)
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("keys", gdbx.Create|gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	// Insert keys with duplicate values
	totalKeys := 100
	for i := 0; i < totalKeys; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Put(dbi, key, []byte(fmt.Sprintf("key1_%d", i)), 0); err != nil {
			t.Fatal(err)
		}
		if err := txn.Put(dbi, key, []byte(fmt.Sprintf("key2_%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Multiple prune iterations
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	pruneLimit := uint64(10)
	totalIters := totalKeys / int(pruneLimit)
	totalDeleted := 0

	for iter := 0; iter < totalIters; iter++ {
		keysCursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}

		// First loop: collect keys
		seekKey := make([]byte, 8)
		binary.BigEndian.PutUint64(seekKey, 0)

		minTxNum := uint64(0xFFFFFFFFFFFFFFFF)
		maxTxNum := uint64(0)
		limit := pruneLimit
		collectedCount := 0

		for k, v, err := keysCursor.Get(seekKey, nil, gdbx.SetRange); k != nil; k, v, err = keysCursor.Get(nil, nil, gdbx.NextNoDup) {
			if err != nil && err != gdbx.ErrNotFoundError {
				t.Fatal(err)
			}
			if k == nil {
				break
			}
			txNum := binary.BigEndian.Uint64(k)
			if limit == 0 {
				break
			}
			limit--

			if txNum < minTxNum {
				minTxNum = txNum
			}
			if txNum > maxTxNum {
				maxTxNum = txNum
			}
			collectedCount++

			// Collect all dups (mimicking erigon's ETL collection)
			for ; v != nil; _, v, err = keysCursor.Get(nil, nil, gdbx.NextDup) {
				if err != nil && err != gdbx.ErrNotFoundError {
					t.Fatal(err)
				}
			}
		}

		if minTxNum > maxTxNum {
			keysCursor.Close()
			break
		}

		// Second loop: delete (mimicking erigon's deletion loop)
		binary.BigEndian.PutUint64(seekKey, minTxNum)
		deletedCount := 0

		for txnb, _, err := keysCursor.Get(seekKey, nil, gdbx.SetRange); txnb != nil; txnb, _, err = keysCursor.Get(nil, nil, gdbx.NextNoDup) {
			if err != nil && err != gdbx.ErrNotFoundError {
				t.Fatal(err)
			}
			if txnb == nil {
				break
			}
			if binary.BigEndian.Uint64(txnb) > maxTxNum {
				break
			}
			deletedCount++
			if err = txn.Del(dbi, txnb, nil); err != nil {
				t.Fatal(err)
			}
		}

		keysCursor.Close()
		totalDeleted += deletedCount

		// Each iteration should delete exactly pruneLimit keys
		if deletedCount != collectedCount {
			t.Errorf("Iteration %d: collected %d, deleted %d (should be equal)", iter, collectedCount, deletedCount)
		}
	}

	// Verify all expected keys were deleted
	expectedDeleted := totalIters * int(pruneLimit)
	if totalDeleted != expectedDeleted {
		t.Errorf("Total deleted: %d, expected: %d", totalDeleted, expectedDeleted)
	}

	// Verify remaining count
	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	remaining := 0
	for k, _, err := cursor.Get(nil, nil, gdbx.First); k != nil; k, _, err = cursor.Get(nil, nil, gdbx.NextNoDup) {
		if err != nil && err != gdbx.ErrNotFoundError {
			t.Fatal(err)
		}
		if k == nil {
			break
		}
		remaining++
	}

	expectedRemaining := totalKeys - expectedDeleted
	if remaining != expectedRemaining {
		t.Errorf("Remaining: %d, expected: %d", remaining, expectedRemaining)
	}
}

// TestDeleteIterateSameCursor tests delete-while-iterating with the same cursor
func TestDeleteIterateSameCursor(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-delete-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("vals", gdbx.Create)
	if err != nil {
		t.Fatal(err)
	}

	// Insert entries
	totalEntries := 260
	for i := 0; i < totalEntries; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Put(dbi, key, []byte(fmt.Sprintf("value%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Delete entries using same cursor for iteration and deletion
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	cursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}

	toDelete := 250
	deleted := 0

	k, _, err := cursor.Get(nil, nil, gdbx.First)
	if err != nil {
		t.Fatal(err)
	}

	for k != nil && deleted < toDelete {
		if err := cursor.Del(gdbx.Current); err != nil {
			t.Fatal(err)
		}
		deleted++

		k, _, err = cursor.Get(nil, nil, gdbx.Next)
		if err != nil && err != gdbx.ErrNotFoundError {
			t.Fatal(err)
		}
	}

	cursor.Close()

	if deleted != toDelete {
		t.Errorf("Deleted %d entries, expected %d", deleted, toDelete)
	}

	// Verify remaining
	cursor, err = txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	remaining := 0
	for k, _, err := cursor.Get(nil, nil, gdbx.First); k != nil; k, _, err = cursor.Get(nil, nil, gdbx.Next) {
		if err != nil {
			break
		}
		remaining++
	}

	expectedRemaining := totalEntries - toDelete
	if remaining != expectedRemaining {
		t.Errorf("Remaining %d entries, expected %d", remaining, expectedRemaining)
	}
}

// TestDeleteWithExternalCursor tests the pattern where one cursor iterates
// while Txn.Del (using a separate cached cursor) performs deletions
func TestDeleteWithExternalCursor(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-delete-ext-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := gdbx.NewEnv("")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		t.Fatal(err)
	}
	if err := env.Open(filepath.Join(dir, "test.db"), gdbx.NoSubdir|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("keys", gdbx.Create|gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 60 keys with 2 values each (this triggers the bug at certain boundaries)
	for i := 0; i < 60; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Put(dbi, key, []byte(fmt.Sprintf("val1_%d", i)), 0); err != nil {
			t.Fatal(err)
		}
		if err := txn.Put(dbi, key, []byte(fmt.Sprintf("val2_%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Prune using external cursor pattern
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	keysCursor, err := txn.OpenCursor(dbi)
	if err != nil {
		t.Fatal(err)
	}

	// Collect 10 keys to delete
	seekKey := make([]byte, 8)
	var collected []uint64
	minTxNum := uint64(0xFFFFFFFFFFFFFFFF)
	maxTxNum := uint64(0)
	limit := 10

	for k, v, _ := keysCursor.Get(seekKey, nil, gdbx.SetRange); k != nil && limit > 0; k, v, _ = keysCursor.Get(nil, nil, gdbx.NextNoDup) {
		txNum := binary.BigEndian.Uint64(k)
		collected = append(collected, txNum)
		if txNum < minTxNum {
			minTxNum = txNum
		}
		if txNum > maxTxNum {
			maxTxNum = txNum
		}
		limit--

		for ; v != nil; _, v, _ = keysCursor.Get(nil, nil, gdbx.NextDup) {
		}
	}

	// Delete using Txn.Del
	binary.BigEndian.PutUint64(seekKey, minTxNum)
	var deleted []uint64

	for k, _, _ := keysCursor.Get(seekKey, nil, gdbx.SetRange); k != nil; k, _, _ = keysCursor.Get(nil, nil, gdbx.NextNoDup) {
		txNum := binary.BigEndian.Uint64(k)
		if txNum > maxTxNum {
			break
		}
		deleted = append(deleted, txNum)
		if err := txn.Del(dbi, k, nil); err != nil {
			t.Fatal(err)
		}
	}

	keysCursor.Close()

	// Verify all collected keys were deleted
	if len(deleted) != len(collected) {
		t.Errorf("Collected %v, deleted %v", collected, deleted)
	}

	// Check for skipped keys
	deletedMap := make(map[uint64]bool)
	for _, d := range deleted {
		deletedMap[d] = true
	}
	for _, c := range collected {
		if !deletedMap[c] {
			t.Errorf("Key %d was collected but not deleted", c)
		}
	}
}
