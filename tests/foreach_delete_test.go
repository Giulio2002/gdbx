package tests

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	gdbx "github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// TestForEachDeleteWhileIterating tests the scenario where we iterate
// with one cursor and delete using another (cached) cursor, simulating
// what erigon's TruncateCanonicalHash does.
func TestForEachDeleteWhileIterating(t *testing.T) {
	gdbxPath := t.TempDir() + "/gdbx.db"
	mdbxPath := t.TempDir() + "/mdbx.db"

	// Setup gdbx
	genv, _ := gdbx.NewEnv(gdbx.Default)
	defer genv.Close()
	genv.SetMaxDBs(10)
	genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)

	// Setup mdbx-go
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
	defer menv.Close()
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)

	// Helper to encode uint64 as 8-byte big-endian
	encTs := func(i uint64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, i)
		return b
	}

	// Create table with entries 1-10 in gdbx
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("test", gdbx.Create)
	for i := uint64(1); i <= 10; i++ {
		gtxn.Put(gdbi, encTs(i), []byte(fmt.Sprintf("value%d", i)), 0)
	}
	gtxn.Commit()

	// Create table with entries 1-10 in mdbx
	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBISimple("test", mdbxgo.Create)
	for i := uint64(1); i <= 10; i++ {
		mtxn.Put(mdbi, encTs(i), []byte(fmt.Sprintf("value%d", i)), 0)
	}
	mtxn.Commit()

	// Test gdbx: ForEach with delete from block 7 onwards
	gtxn, _ = genv.BeginTxn(nil, 0)
	gdbi, _ = gtxn.OpenDBISimple("test", 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)

	var gIteratedKeys []uint64
	k, _, err := gcursor.Get(encTs(7), nil, gdbx.SetRange)
	for k != nil && err == nil {
		keyNum := binary.BigEndian.Uint64(k)
		gIteratedKeys = append(gIteratedKeys, keyNum)

		// Delete current entry using txn.Del (uses cached cursor)
		delKey := make([]byte, len(k))
		copy(delKey, k)
		if err := gtxn.Del(gdbi, delKey, nil); err != nil {
			t.Fatalf("gdbx Del failed for key %d: %v", keyNum, err)
		}

		k, _, err = gcursor.Get(nil, nil, gdbx.Next)
	}
	gcursor.Close()
	gtxn.Abort()

	// Test mdbx: ForEach with delete from block 7 onwards
	mtxn, _ = menv.BeginTxn(nil, 0)
	mdbi, _ = mtxn.OpenDBISimple("test", 0)
	mcursor, _ := mtxn.OpenCursor(mdbi)

	var mIteratedKeys []uint64
	k, _, err = mcursor.Get(encTs(7), nil, mdbxgo.SetRange)
	for k != nil && err == nil {
		keyNum := binary.BigEndian.Uint64(k)
		mIteratedKeys = append(mIteratedKeys, keyNum)

		// Delete current entry using txn.Del (uses cached cursor)
		delKey := make([]byte, len(k))
		copy(delKey, k)
		if err := mtxn.Del(mdbi, delKey, nil); err != nil {
			t.Fatalf("mdbx Del failed for key %d: %v", keyNum, err)
		}

		k, _, err = mcursor.Get(nil, nil, mdbxgo.Next)
	}
	mcursor.Close()
	mtxn.Abort()

	t.Logf("gdbx iterated keys: %v", gIteratedKeys)
	t.Logf("mdbx iterated keys: %v", mIteratedKeys)

	// Both should iterate over exactly 4 keys: 7, 8, 9, 10
	expected := []uint64{7, 8, 9, 10}

	if len(gIteratedKeys) != len(expected) {
		t.Errorf("gdbx: expected %d iterated keys, got %d: %v", len(expected), len(gIteratedKeys), gIteratedKeys)
	}
	if len(mIteratedKeys) != len(expected) {
		t.Errorf("mdbx: expected %d iterated keys, got %d: %v", len(expected), len(mIteratedKeys), mIteratedKeys)
	}

	for i, exp := range expected {
		if i < len(gIteratedKeys) && gIteratedKeys[i] != exp {
			t.Errorf("gdbx: expected key %d at position %d, got %d", exp, i, gIteratedKeys[i])
		}
		if i < len(mIteratedKeys) && mIteratedKeys[i] != exp {
			t.Errorf("mdbx: expected key %d at position %d, got %d", exp, i, mIteratedKeys[i])
		}
	}

	// Verify both implementations match
	if len(gIteratedKeys) != len(mIteratedKeys) {
		t.Errorf("gdbx and mdbx have different iteration counts: gdbx=%d, mdbx=%d", len(gIteratedKeys), len(mIteratedKeys))
	} else {
		for i := range gIteratedKeys {
			if gIteratedKeys[i] != mIteratedKeys[i] {
				t.Errorf("gdbx and mdbx differ at position %d: gdbx=%d, mdbx=%d", i, gIteratedKeys[i], mIteratedKeys[i])
			}
		}
	}
}
