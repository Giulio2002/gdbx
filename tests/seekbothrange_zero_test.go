package tests

import (
	"bytes"
	"encoding/binary"
	"testing"

	gdbx "github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestSeekBothRangeZero tests SeekBothRange with a zero value (all zeros).
// This is the specific case that's failing in erigon tests.
func TestSeekBothRangeZero(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create DUPSORT database
	mEnv, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer mEnv.Close()
	mEnv.SetOption(mdbx.OptMaxDB, 10)
	mEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	mEnv.Open(db.path+"/mdbx", mdbx.Create|mdbx.WriteMap, 0644)

	// Create gdbx database
	gEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gEnv.Close()
	gEnv.SetMaxDBs(10)
	gEnv.Open(db.path+"/gdbx", gdbx.NoSubdir|gdbx.WriteMap, 0644)

	// Key is 0x0100000000000001 (like in erigon test)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)
	key[0] = 1 // marker

	// Write entries with txNum 1, 2, 3, ..., 100
	// Value format: txNum (8 bytes) + actualValue (8 bytes)
	mTxn, _ := mEnv.BeginTxn(nil, 0)
	mDBI, _ := mTxn.OpenDBI("test", mdbx.Create|mdbx.DupSort, nil, nil)

	gTxn, _ := gEnv.BeginTxn(nil, 0)
	gDBI, _ := gTxn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)

	for txNum := uint64(1); txNum <= 100; txNum++ {
		val := make([]byte, 16)
		binary.BigEndian.PutUint64(val[:8], txNum)
		binary.BigEndian.PutUint64(val[8:], txNum*100) // some value

		if err := mTxn.Put(mDBI, key, val, 0); err != nil {
			t.Fatalf("mdbx Put: %v", err)
		}
		if err := gTxn.Put(gDBI, key, val, 0); err != nil {
			t.Fatalf("gdbx Put: %v", err)
		}
	}
	mTxn.Commit()
	gTxn.Commit()

	// Read back using SeekBothRange with zero value
	mTxn, _ = mEnv.BeginTxn(nil, mdbx.Readonly)
	mCur, _ := mTxn.OpenCursor(mDBI)
	defer mCur.Close()
	defer mTxn.Abort()

	gTxn, _ = gEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	gCur, _ := gTxn.OpenCursor(gDBI)
	defer gCur.Close()
	defer gTxn.Abort()

	// Search for value >= 0 (all zeros)
	zeroVal := make([]byte, 8)

	mk, mv, mErr := mCur.Get(key, zeroVal, mdbx.GetBothRange)
	gk, gv, gErr := gCur.Get(key, zeroVal, gdbx.GetBothRange)

	t.Logf("mdbx GetBothRange(key, zeros): k=%x, v=%x, err=%v", mk, mv, mErr)
	t.Logf("gdbx GetBothRange(key, zeros): k=%x, v=%x, err=%v", gk, gv, gErr)

	// Both should find txNum=1 (first entry >= 0)
	if (mErr == nil) != (gErr == nil) {
		t.Errorf("Error mismatch: mdbx_err=%v, gdbx_err=%v", mErr, gErr)
	}
	if mErr == nil && gErr == nil {
		if !bytes.Equal(mk, gk) || !bytes.Equal(mv, gv) {
			t.Errorf("Value mismatch: mdbx=(%x, %x), gdbx=(%x, %x)", mk, mv, gk, gv)
		}
	}

	// Also test with specific txNum
	t.Log("\n--- Testing with txNum=50 ---")
	searchVal := make([]byte, 8)
	binary.BigEndian.PutUint64(searchVal, 50)

	mk2, mv2, mErr2 := mCur.Get(key, searchVal, mdbx.GetBothRange)
	gk2, gv2, gErr2 := gCur.Get(key, searchVal, gdbx.GetBothRange)

	t.Logf("mdbx GetBothRange(key, 50): k=%x, v=%x, err=%v", mk2, mv2, mErr2)
	t.Logf("gdbx GetBothRange(key, 50): k=%x, v=%x, err=%v", gk2, gv2, gErr2)

	if (mErr2 == nil) != (gErr2 == nil) {
		t.Errorf("Error mismatch: mdbx_err=%v, gdbx_err=%v", mErr2, gErr2)
	}
	if mErr2 == nil && gErr2 == nil {
		if !bytes.Equal(mk2, gk2) || !bytes.Equal(mv2, gv2) {
			t.Errorf("Value mismatch: mdbx=(%x, %x), gdbx=(%x, %x)", mk2, mv2, gk2, gv2)
		}
	}
}

// TestSeekBothRangeLarge tests SeekBothRange with large dataset (like erigon test).
// This more closely matches the erigon test which has ~1000 entries per key.
func TestSeekBothRangeLarge(t *testing.T) {
	db := newTestDB(t)
	defer db.cleanup()

	// Create gdbx database only (simpler test)
	gEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gEnv.Close()
	gEnv.SetMaxDBs(10)
	gEnv.Open(db.path+"/gdbx", gdbx.NoSubdir|gdbx.WriteMap, 0644)

	// Multiple keys like in erigon test
	numKeys := 10
	numTxNums := 1000

	gTxn, _ := gEnv.BeginTxn(nil, 0)
	gDBI, _ := gTxn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)

	for keyNum := 1; keyNum <= numKeys; keyNum++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(keyNum))
		key[0] = 1 // marker byte like erigon uses

		for txNum := 1; txNum <= numTxNums; txNum++ {
			val := make([]byte, 16)
			binary.BigEndian.PutUint64(val[:8], uint64(txNum))
			binary.BigEndian.PutUint64(val[8:], uint64(keyNum*1000+txNum)) // some value

			if err := gTxn.Put(gDBI, key, val, 0); err != nil {
				t.Fatalf("gdbx Put: %v", err)
			}
		}
	}
	gTxn.Commit()

	// Read back using SeekBothRange with zero value for each key
	gTxn, err = gEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatalf("gdbx BeginTxn: %v", err)
	}
	defer gTxn.Abort()

	// Re-open the DBI in the read transaction
	gDBI, err = gTxn.OpenDBISimple("test", 0)
	if err != nil {
		t.Fatalf("gdbx OpenDBI: %v", err)
	}

	gCur, err := gTxn.OpenCursor(gDBI)
	if err != nil {
		t.Fatalf("gdbx OpenCursor: %v", err)
	}
	defer gCur.Close()

	zeroVal := make([]byte, 8)

	for keyNum := 1; keyNum <= numKeys; keyNum++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(keyNum))
		key[0] = 1 // marker byte

		gk, gv, gErr := gCur.Get(key, zeroVal, gdbx.GetBothRange)

		t.Logf("key=%x: gdbx GetBothRange: k=%x, v=%x, err=%v", key, gk, gv, gErr)

		// Should find txNum=1 (first entry >= 0)
		if gErr != nil {
			t.Errorf("key=%x: unexpected error: %v", key, gErr)
			continue
		}

		// Expected: txNum=1, val=keyNum*1000+1
		expectedTxNum := uint64(1)
		expectedVal := uint64(keyNum*1000 + 1)
		if len(gv) < 16 {
			t.Errorf("key=%x: value too short: %x", key, gv)
			continue
		}
		gotTxNum := binary.BigEndian.Uint64(gv[:8])
		gotVal := binary.BigEndian.Uint64(gv[8:])

		if gotTxNum != expectedTxNum || gotVal != expectedVal {
			t.Errorf("key=%x: expected txNum=%d, val=%d; got txNum=%d, val=%d",
				key, expectedTxNum, expectedVal, gotTxNum, gotVal)
		}
	}
}
