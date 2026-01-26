package tests

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// TestMdbxCreateGdbxEditMdbxVerify tests cross-compatibility:
// 1. Create DB with mdbx (non-dupsorted + dupsorted tables)
// 2. Open with gdbx and modify (add, delete)
// 3. Reopen with mdbx to verify
func TestMdbxCreateGdbxEditMdbxVerify(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	const (
		numKeys    = 100
		numDupVals = 10
		tablePlain = "plain"
		tableDup   = "dupsort"
	)

	// ============================================================
	// STEP 1: Create database with mdbx
	// ============================================================
	t.Log("Step 1: Creating database with mdbx...")

	mdbxEnv, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	mdbxEnv.SetOption(mdbxgo.OptMaxDB, 10)
	mdbxEnv.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)

	if err := mdbxEnv.Open(db.path, mdbxgo.Create, 0644); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Create plain table (non-dupsorted)
	txn, err := mdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	dbiPlain, err := txn.OpenDBI(tablePlain, mdbxgo.Create, nil, nil)
	if err != nil {
		txn.Abort()
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Insert keys 0-99 into plain table
	key := make([]byte, 8)
	val := make([]byte, 32)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100))
		if err := txn.Put(dbiPlain, key, val, 0); err != nil {
			txn.Abort()
			mdbxEnv.Close()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Create dupsorted table
	txn, err = mdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	dbiDup, err := txn.OpenDBI(tableDup, mdbxgo.Create|mdbxgo.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Insert keys 0-99, each with 10 duplicate values
	dupVal := make([]byte, 16)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := 0; j < numDupVals; j++ {
			binary.BigEndian.PutUint64(dupVal, uint64(j))
			binary.BigEndian.PutUint64(dupVal[8:], uint64(i*1000+j))
			if err := txn.Put(dbiDup, key, dupVal, 0); err != nil {
				txn.Abort()
				mdbxEnv.Close()
				t.Fatal(err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	mdbxEnv.Close()
	t.Log("Step 1: Done - created plain table with 100 keys, dupsort table with 100 keys x 10 values")

	// ============================================================
	// STEP 2: Open with gdbx and modify
	// ============================================================
	t.Log("Step 2: Opening with gdbx and modifying...")

	gdbxEnv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}

	gdbxEnv.SetMaxDBs(10)
	gdbxEnv.SetGeometryGeo(gdbx.Geometry{
		SizeLower:  1 << 26,
		SizeNow:    1 << 26,
		SizeUpper:  1 << 30,
		GrowthStep: 1 << 24,
	})

	if err := gdbxEnv.Open(db.path, 0, 0644); err != nil {
		t.Fatal(err)
	}

	// Modify plain table
	gtxn, err := gdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}

	gDbiPlain, err := gtxn.OpenDBISimple(tablePlain, 0)
	if err != nil {
		gtxn.Abort()
		gdbxEnv.Close()
		t.Fatal(err)
	}

	// Add new keys 100-149
	for i := numKeys; i < numKeys+50; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100))
		if err := gtxn.Put(gDbiPlain, key, val, 0); err != nil {
			gtxn.Abort()
			gdbxEnv.Close()
			t.Fatalf("gdbx Put plain key %d: %v", i, err)
		}
	}

	// Delete keys 20-39 from plain table
	for i := 20; i < 40; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := gtxn.Del(gDbiPlain, key, nil); err != nil {
			gtxn.Abort()
			gdbxEnv.Close()
			t.Fatalf("gdbx Del plain key %d: %v", i, err)
		}
	}

	// Update keys 60-79 with new values
	for i := 60; i < 80; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*999))
		if err := gtxn.Put(gDbiPlain, key, val, 0); err != nil {
			gtxn.Abort()
			gdbxEnv.Close()
			t.Fatalf("gdbx Update plain key %d: %v", i, err)
		}
	}

	if _, err := gtxn.Commit(); err != nil {
		gdbxEnv.Close()
		t.Fatalf("gdbx Commit plain: %v", err)
	}

	// Modify dupsorted table
	gtxn, err = gdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		gdbxEnv.Close()
		t.Fatal(err)
	}

	gDbiDup, err := gtxn.OpenDBISimple(tableDup, gdbx.DupSort)
	if err != nil {
		gtxn.Abort()
		gdbxEnv.Close()
		t.Fatal(err)
	}

	// Add new dup values to existing keys 0-9 (values 10-14)
	for i := 0; i < 10; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := numDupVals; j < numDupVals+5; j++ {
			binary.BigEndian.PutUint64(dupVal, uint64(j))
			binary.BigEndian.PutUint64(dupVal[8:], uint64(i*1000+j))
			if err := gtxn.Put(gDbiDup, key, dupVal, 0); err != nil {
				gtxn.Abort()
				gdbxEnv.Close()
				t.Fatalf("gdbx Put dup key %d val %d: %v", i, j, err)
			}
		}
	}

	// Delete specific dup values from keys 50-59 (remove values 3-6)
	for i := 50; i < 60; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := 3; j < 7; j++ {
			binary.BigEndian.PutUint64(dupVal, uint64(j))
			binary.BigEndian.PutUint64(dupVal[8:], uint64(i*1000+j))
			if err := gtxn.Del(gDbiDup, key, dupVal); err != nil {
				gtxn.Abort()
				gdbxEnv.Close()
				t.Fatalf("gdbx Del dup key %d val %d: %v", i, j, err)
			}
		}
	}

	// Delete entire key 90-94 (all dup values)
	for i := 90; i < 95; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := gtxn.Del(gDbiDup, key, nil); err != nil {
			gtxn.Abort()
			gdbxEnv.Close()
			t.Fatalf("gdbx Del all dups key %d: %v", i, err)
		}
	}

	if _, err := gtxn.Commit(); err != nil {
		gdbxEnv.Close()
		t.Fatalf("gdbx Commit dup: %v", err)
	}

	gdbxEnv.Close()
	t.Log("Step 2: Done - modified both tables")

	// ============================================================
	// STEP 3: Reopen with mdbx and make more edits
	// ============================================================
	t.Log("Step 3: Reopening with mdbx and making more edits...")

	mdbxEnv, err = mdbxgo.NewEnv(mdbxgo.Label("test"))
	if err != nil {
		t.Fatal(err)
	}

	mdbxEnv.SetOption(mdbxgo.OptMaxDB, 10)

	if err := mdbxEnv.Open(db.path, 0, 0644); err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// mdbx edits plain table
	txn, err = mdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	dbiPlain, err = txn.OpenDBI(tablePlain, 0, nil, nil)
	if err != nil {
		txn.Abort()
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Add keys 150-174 (mdbx adds after gdbx added 100-149)
	for i := 150; i < 175; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100))
		if err := txn.Put(dbiPlain, key, val, 0); err != nil {
			txn.Abort()
			mdbxEnv.Close()
			t.Fatalf("mdbx Put plain key %d: %v", i, err)
		}
	}

	// Delete keys 40-49 (mdbx deletes some that gdbx left)
	for i := 40; i < 50; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Del(dbiPlain, key, nil); err != nil {
			txn.Abort()
			mdbxEnv.Close()
			t.Fatalf("mdbx Del plain key %d: %v", i, err)
		}
	}

	// Update keys 80-89 with new values
	for i := 80; i < 90; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*777))
		if err := txn.Put(dbiPlain, key, val, 0); err != nil {
			txn.Abort()
			mdbxEnv.Close()
			t.Fatalf("mdbx Update plain key %d: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		mdbxEnv.Close()
		t.Fatalf("mdbx Commit plain: %v", err)
	}

	// mdbx edits dupsorted table
	txn, err = mdbxEnv.BeginTxn(nil, 0)
	if err != nil {
		mdbxEnv.Close()
		t.Fatal(err)
	}

	dbiDup, err = txn.OpenDBI(tableDup, mdbxgo.DupSort, nil, nil)
	if err != nil {
		txn.Abort()
		mdbxEnv.Close()
		t.Fatal(err)
	}

	// Add new dup values to keys 10-19 (values 10-12)
	for i := 10; i < 20; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := numDupVals; j < numDupVals+3; j++ {
			binary.BigEndian.PutUint64(dupVal, uint64(j))
			binary.BigEndian.PutUint64(dupVal[8:], uint64(i*1000+j))
			if err := txn.Put(dbiDup, key, dupVal, 0); err != nil {
				txn.Abort()
				mdbxEnv.Close()
				t.Fatalf("mdbx Put dup key %d val %d: %v", i, j, err)
			}
		}
	}

	// Delete entire key 95-99 (mdbx deletes more after gdbx deleted 90-94)
	for i := 95; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := txn.Del(dbiDup, key, nil); err != nil {
			txn.Abort()
			mdbxEnv.Close()
			t.Fatalf("mdbx Del all dups key %d: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		mdbxEnv.Close()
		t.Fatalf("mdbx Commit dup: %v", err)
	}

	mdbxEnv.Close()
	t.Log("Step 3: Done - mdbx made additional edits")

	// ============================================================
	// STEP 4: Reopen with gdbx and verify all changes
	// ============================================================
	t.Log("Step 4: Reopening with gdbx to verify all changes...")

	gdbxEnv, err = gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer gdbxEnv.Close()

	gdbxEnv.SetMaxDBs(10)

	if err := gdbxEnv.Open(db.path, gdbx.ReadOnly, 0644); err != nil {
		t.Fatal(err)
	}

	// Verify plain table
	gtxn, err = gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	gDbiPlain, err = gtxn.OpenDBISimple(tablePlain, 0)
	if err != nil {
		gtxn.Abort()
		t.Fatal(err)
	}

	// Verify keys 0-19 exist with original value
	for i := 0; i < 20; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 100)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain key %d value = %d, want %d", i, got, expected)
		}
	}

	// Verify keys 20-39 are deleted (by gdbx in step 2)
	for i := 20; i < 40; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, err := gtxn.Get(gDbiPlain, key)
		if !gdbx.IsNotFound(err) {
			t.Errorf("gdbx verify: plain key %d should be deleted", i)
		}
	}

	// Verify keys 40-49 are deleted (by mdbx in step 3)
	for i := 40; i < 50; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, err := gtxn.Get(gDbiPlain, key)
		if !gdbx.IsNotFound(err) {
			t.Errorf("gdbx verify: plain key %d should be deleted (by mdbx)", i)
		}
	}

	// Verify keys 50-59 exist with original value
	for i := 50; i < 60; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 100)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain key %d value = %d, want %d", i, got, expected)
		}
	}

	// Verify keys 60-79 have updated value (by gdbx in step 2)
	for i := 60; i < 80; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 999)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain key %d updated value = %d, want %d", i, got, expected)
		}
	}

	// Verify keys 80-89 have updated value (by mdbx in step 3)
	for i := 80; i < 90; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 777)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain key %d mdbx-updated value = %d, want %d", i, got, expected)
		}
	}

	// Verify keys 90-99 exist with original value
	for i := 90; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 100)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain key %d value = %d, want %d", i, got, expected)
		}
	}

	// Verify keys 100-149 exist (added by gdbx in step 2)
	for i := numKeys; i < numKeys+50; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain gdbx-added key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 100)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain gdbx-added key %d value = %d, want %d", i, got, expected)
		}
	}

	// Verify keys 150-174 exist (added by mdbx in step 3)
	for i := 150; i < 175; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := gtxn.Get(gDbiPlain, key)
		if err != nil {
			t.Errorf("gdbx verify: plain mdbx-added key %d not found: %v", i, err)
			continue
		}
		expected := uint64(i * 100)
		got := binary.BigEndian.Uint64(v)
		if got != expected {
			t.Errorf("gdbx verify: plain mdbx-added key %d value = %d, want %d", i, got, expected)
		}
	}

	gtxn.Abort()

	// Verify dupsorted table
	gtxn, err = gdbxEnv.BeginTxn(nil, gdbx.TxnReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer gtxn.Abort()

	gDbiDup, err = gtxn.OpenDBISimple(tableDup, gdbx.DupSort)
	if err != nil {
		t.Fatal(err)
	}

	cursor, err := gtxn.OpenCursor(gDbiDup)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()

	// Verify keys 0-9 have 15 dup values (original 10 + 5 added by gdbx)
	for i := 0; i < 10; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Errorf("gdbx verify: dup key %d not found: %v", i, err)
			continue
		}
		count, err := cursor.Count()
		if err != nil {
			t.Errorf("gdbx verify: dup key %d count error: %v", i, err)
			continue
		}
		if count != 15 {
			t.Errorf("gdbx verify: dup key %d count = %d, want 15", i, count)
		}
	}

	// Verify keys 10-19 have 13 dup values (original 10 + 3 added by mdbx)
	for i := 10; i < 20; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Errorf("gdbx verify: dup key %d not found: %v", i, err)
			continue
		}
		count, _ := cursor.Count()
		if count != 13 {
			t.Errorf("gdbx verify: dup key %d count = %d, want 13", i, count)
		}
	}

	// Verify keys 20-49 have 10 dup values (unchanged)
	for i := 20; i < 50; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Errorf("gdbx verify: dup key %d not found: %v", i, err)
			continue
		}
		count, _ := cursor.Count()
		if count != 10 {
			t.Errorf("gdbx verify: dup key %d count = %d, want 10", i, count)
		}
	}

	// Verify keys 50-59 have 6 dup values (10 - 4 deleted by gdbx)
	for i := 50; i < 60; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Errorf("gdbx verify: dup key %d not found: %v", i, err)
			continue
		}
		count, _ := cursor.Count()
		if count != 6 {
			t.Errorf("gdbx verify: dup key %d count = %d, want 6", i, count)
		}

		// Verify values 3-6 are deleted
		for j := 3; j < 7; j++ {
			binary.BigEndian.PutUint64(dupVal, uint64(j))
			binary.BigEndian.PutUint64(dupVal[8:], uint64(i*1000+j))
			_, _, err := cursor.Get(key, dupVal, gdbx.GetBoth)
			if err == nil {
				t.Errorf("gdbx verify: dup key %d val %d should be deleted", i, j)
			}
		}
	}

	// Verify keys 60-89 have 10 dup values (unchanged)
	for i := 60; i < 90; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err != nil {
			t.Errorf("gdbx verify: dup key %d not found: %v", i, err)
			continue
		}
		count, _ := cursor.Count()
		if count != 10 {
			t.Errorf("gdbx verify: dup key %d count = %d, want 10", i, count)
		}
	}

	// Verify keys 90-99 are completely deleted (90-94 by gdbx, 95-99 by mdbx)
	for i := 90; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, _, err := cursor.Get(key, nil, gdbx.Set)
		if err == nil {
			t.Errorf("gdbx verify: dup key %d should be completely deleted", i)
		}
	}

	t.Log("Step 4: Done - verification complete")
}

// TestMdbxEditRoundTrip tests multiple edit cycles between mdbx and gdbx
func TestMdbxEditRoundTrip(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	const tableName = "data"

	// Round 1: mdbx creates DB with keys 0-49
	t.Log("Round 1: mdbx creates initial data")
	{
		env, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
		env.SetOption(mdbxgo.OptMaxDB, 10)
		env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
		env.Open(db.path, mdbxgo.Create, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBI(tableName, mdbxgo.Create, nil, nil)

		key := make([]byte, 8)
		val := make([]byte, 8)
		for i := 0; i < 50; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i))
			txn.Put(dbi, key, val, 0)
		}
		txn.Commit()
		env.Close()
	}

	// Round 2: gdbx adds keys 50-99, updates 25-49
	t.Log("Round 2: gdbx modifies data")
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.SetGeometryGeo(gdbx.Geometry{SizeLower: 1 << 26, SizeNow: 1 << 26, SizeUpper: 1 << 30, GrowthStep: 1 << 24})
		env.Open(db.path, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple(tableName, 0)

		key := make([]byte, 8)
		val := make([]byte, 8)

		// Add keys 50-99
		for i := 50; i < 100; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i*2))
			txn.Put(dbi, key, val, 0)
		}

		// Update keys 25-49
		for i := 25; i < 50; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i*3))
			txn.Put(dbi, key, val, 0)
		}

		txn.Commit()
		env.Close()
	}

	// Round 3: mdbx deletes keys 10-19, adds keys 100-109
	t.Log("Round 3: mdbx modifies data")
	{
		env, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
		env.SetOption(mdbxgo.OptMaxDB, 10)
		env.Open(db.path, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)

		key := make([]byte, 8)
		val := make([]byte, 8)

		// Delete keys 10-19
		for i := 10; i < 20; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			txn.Del(dbi, key, nil)
		}

		// Add keys 100-109
		for i := 100; i < 110; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i*4))
			txn.Put(dbi, key, val, 0)
		}

		txn.Commit()
		env.Close()
	}

	// Round 4: gdbx verifies all data
	t.Log("Round 4: gdbx verifies final state")
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.Open(db.path, gdbx.ReadOnly, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, gdbx.TxnReadOnly)
		defer txn.Abort()

		dbi, _ := txn.OpenDBISimple(tableName, 0)

		key := make([]byte, 8)

		// Keys 0-9: value = i (original from round 1)
		for i := 0; i < 10; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i)
			got := binary.BigEndian.Uint64(v)
			if got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Keys 10-19: deleted (round 3)
		for i := 10; i < 20; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			_, err := txn.Get(dbi, key)
			if !gdbx.IsNotFound(err) {
				t.Errorf("key %d should be deleted, got: %v", i, err)
			}
		}

		// Keys 20-24: value = i (original from round 1)
		for i := 20; i < 25; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i)
			got := binary.BigEndian.Uint64(v)
			if got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Keys 25-49: value = i*3 (updated in round 2)
		for i := 25; i < 50; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i * 3)
			got := binary.BigEndian.Uint64(v)
			if got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Keys 50-99: value = i*2 (added in round 2)
		for i := 50; i < 100; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i * 2)
			got := binary.BigEndian.Uint64(v)
			if got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Keys 100-109: value = i*4 (added in round 3)
		for i := 100; i < 110; i++ {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i * 4)
			got := binary.BigEndian.Uint64(v)
			if got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Count total keys
		cursor, _ := txn.OpenCursor(dbi)
		defer cursor.Close()

		count := 0
		_, _, err := cursor.Get(nil, nil, gdbx.First)
		for err == nil {
			count++
			_, _, err = cursor.Get(nil, nil, gdbx.Next)
		}

		// Expected: 0-9 (10) + 20-99 (80) + 100-109 (10) = 100
		if count != 100 {
			t.Errorf("total count = %d, want 100", count)
		}
	}

	t.Log("Round trip test complete")
}

// TestDupSortSubpageToSubtree tests dupsort transitions between subpage and subtree
func TestDupSortSubpageToSubtree(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	const tableName = "dup"

	// Step 1: mdbx creates small dupsort (subpage mode)
	t.Log("Step 1: mdbx creates small dupsort")
	{
		env, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
		env.SetOption(mdbxgo.OptMaxDB, 10)
		env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
		env.Open(db.path, mdbxgo.Create, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBI(tableName, mdbxgo.Create|mdbxgo.DupSort, nil, nil)

		key := []byte("key1")
		// Insert 5 small values (fits in subpage)
		for i := 0; i < 5; i++ {
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(i))
			txn.Put(dbi, key, val, 0)
		}

		txn.Commit()
		env.Close()
	}

	// Step 2: gdbx adds many values (triggers subtree conversion)
	t.Log("Step 2: gdbx adds many values")
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.SetGeometryGeo(gdbx.Geometry{SizeLower: 1 << 26, SizeNow: 1 << 26, SizeUpper: 1 << 30, GrowthStep: 1 << 24})
		env.Open(db.path, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple(tableName, gdbx.DupSort)

		key := []byte("key1")
		// Add 200 more values (should trigger subtree)
		for i := 5; i < 205; i++ {
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(i))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				t.Fatalf("Put val %d: %v", i, err)
			}
		}

		txn.Commit()
		env.Close()
	}

	// Step 3: mdbx verifies all values
	t.Log("Step 3: mdbx verifies all values")
	{
		env, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
		if err != nil {
			t.Fatal(err)
		}
		env.SetOption(mdbxgo.OptMaxDB, 10)
		if err := env.Open(db.path, 0, 0644); err != nil {
			t.Fatal(err)
		}

		txn, err := env.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			env.Close()
			t.Fatal(err)
		}

		dbi, err := txn.OpenDBI(tableName, mdbxgo.DupSort, nil, nil)
		if err != nil {
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}

		key := []byte("key1")
		_, _, err = cursor.Get(key, nil, mdbxgo.Set)
		if err != nil {
			cursor.Close()
			txn.Abort()
			env.Close()
			t.Fatal(err)
		}

		count, _ := cursor.Count()
		if count != 205 {
			t.Errorf("count = %d, want 205", count)
		}

		// Verify all values are present
		vals := make(map[uint64]bool)
		_, v, err := cursor.Get(nil, nil, mdbxgo.FirstDup)
		for err == nil {
			if len(v) == 8 {
				vals[binary.BigEndian.Uint64(v)] = true
			}
			_, v, err = cursor.Get(nil, nil, mdbxgo.NextDup)
		}

		for i := 0; i < 205; i++ {
			if !vals[uint64(i)] {
				t.Errorf("missing value %d", i)
			}
		}

		cursor.Close()
		txn.Abort()
		env.Close()
	}

	// Step 4: gdbx deletes some values
	t.Log("Step 4: gdbx deletes values")
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.SetGeometryGeo(gdbx.Geometry{SizeLower: 1 << 26, SizeNow: 1 << 26, SizeUpper: 1 << 30, GrowthStep: 1 << 24})
		env.Open(db.path, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple(tableName, gdbx.DupSort)

		key := []byte("key1")
		// Delete values 100-149
		for i := 100; i < 150; i++ {
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(i))
			if err := txn.Del(dbi, key, val); err != nil {
				t.Fatalf("Del val %d: %v", i, err)
			}
		}

		txn.Commit()
		env.Close()
	}

	// Step 5: mdbx verifies deletions
	t.Log("Step 5: mdbx verifies deletions")
	{
		env, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
		if err != nil {
			t.Fatal(err)
		}
		env.SetOption(mdbxgo.OptMaxDB, 10)
		if err := env.Open(db.path, 0, 0644); err != nil {
			t.Fatal(err)
		}
		defer env.Close()

		txn, err := env.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			t.Fatal(err)
		}
		defer txn.Abort()

		dbi, err := txn.OpenDBI(tableName, mdbxgo.DupSort, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			t.Fatal(err)
		}
		defer cursor.Close()

		key := []byte("key1")
		_, _, err = cursor.Get(key, nil, mdbxgo.Set)
		if err != nil {
			t.Fatal(err)
		}

		count, err := cursor.Count()
		if err != nil {
			t.Fatal(err)
		}
		expectedCount := uint64(205 - 50) // 205 - 50 deleted = 155
		if count != expectedCount {
			t.Errorf("count = %d, want %d", count, expectedCount)
		}

		// Verify deleted values are gone
		for i := 100; i < 150; i++ {
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(i))
			_, _, err := cursor.Get(key, val, mdbxgo.GetBoth)
			if err == nil {
				t.Errorf("value %d should be deleted", i)
			}
		}

		// Verify remaining values exist
		vals := make(map[uint64]bool)
		_, v, err := cursor.Get(key, nil, mdbxgo.Set)
		_, v, err = cursor.Get(nil, nil, mdbxgo.FirstDup)
		for err == nil {
			if len(v) == 8 {
				vals[binary.BigEndian.Uint64(v)] = true
			}
			_, v, err = cursor.Get(nil, nil, mdbxgo.NextDup)
		}

		for i := 0; i < 100; i++ {
			if !vals[uint64(i)] {
				t.Errorf("value %d should exist", i)
			}
		}
		for i := 150; i < 205; i++ {
			if !vals[uint64(i)] {
				t.Errorf("value %d should exist", i)
			}
		}
	}

	t.Log("Subpage/subtree transition test complete")
}

// TestCursorEditCompat tests cursor-based edits for compatibility
func TestCursorEditCompat(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	db := newTestDB(t)
	defer db.cleanup()

	const tableName = "cursor_test"

	// Step 1: mdbx creates data
	{
		env, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
		env.SetOption(mdbxgo.OptMaxDB, 10)
		env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
		env.Open(db.path, mdbxgo.Create, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBI(tableName, mdbxgo.Create, nil, nil)

		for i := 0; i < 100; i++ {
			key := make([]byte, 8)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(val, uint64(i))
			txn.Put(dbi, key, val, 0)
		}

		txn.Commit()
		env.Close()
	}

	// Step 2: gdbx edits via cursor
	{
		env, _ := gdbx.NewEnv(gdbx.Default)
		env.SetMaxDBs(10)
		env.SetGeometryGeo(gdbx.Geometry{SizeLower: 1 << 26, SizeNow: 1 << 26, SizeUpper: 1 << 30, GrowthStep: 1 << 24})
		env.Open(db.path, 0, 0644)

		txn, _ := env.BeginTxn(nil, 0)
		dbi, _ := txn.OpenDBISimple(tableName, 0)
		cursor, _ := txn.OpenCursor(dbi)

		// Delete every 5th key using cursor
		key := make([]byte, 8)
		for i := 0; i < 100; i += 5 {
			binary.BigEndian.PutUint64(key, uint64(i))
			_, _, err := cursor.Get(key, nil, gdbx.Set)
			if err != nil {
				t.Fatalf("cursor.Get key %d: %v", i, err)
			}
			if err := cursor.Del(0); err != nil {
				t.Fatalf("cursor.Del key %d: %v", i, err)
			}
		}

		// Update via cursor Put
		for i := 1; i < 100; i += 5 {
			binary.BigEndian.PutUint64(key, uint64(i))
			newVal := make([]byte, 8)
			binary.BigEndian.PutUint64(newVal, uint64(i*10))
			if err := cursor.Put(key, newVal, 0); err != nil {
				t.Fatalf("cursor.Put key %d: %v", i, err)
			}
		}

		cursor.Close()
		txn.Commit()
		env.Close()
	}

	// Step 3: mdbx verifies
	{
		env, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
		env.SetOption(mdbxgo.OptMaxDB, 10)
		env.Open(db.path, 0, 0644)
		defer env.Close()

		txn, _ := env.BeginTxn(nil, mdbxgo.Readonly)
		defer txn.Abort()

		dbi, _ := txn.OpenDBI(tableName, 0, nil, nil)

		key := make([]byte, 8)

		// Verify deleted keys (0, 5, 10, ...)
		for i := 0; i < 100; i += 5 {
			binary.BigEndian.PutUint64(key, uint64(i))
			_, err := txn.Get(dbi, key)
			if err == nil {
				t.Errorf("key %d should be deleted", i)
			}
		}

		// Verify updated keys (1, 6, 11, ...) have value i*10
		for i := 1; i < 100; i += 5 {
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i * 10)
			got := binary.BigEndian.Uint64(v)
			if got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Verify unchanged keys have original value
		for i := 2; i < 100; i++ {
			if i%5 == 0 || i%5 == 1 {
				continue // Skip deleted and updated
			}
			binary.BigEndian.PutUint64(key, uint64(i))
			v, err := txn.Get(dbi, key)
			if err != nil {
				t.Errorf("key %d not found: %v", i, err)
				continue
			}
			expected := uint64(i)
			got := binary.BigEndian.Uint64(v)
			if !bytes.Equal(v, key) && got != expected {
				t.Errorf("key %d: got %d, want %d", i, got, expected)
			}
		}

		// Count total: 100 - 20 deleted = 80
		cursor, _ := txn.OpenCursor(dbi)
		defer cursor.Close()

		count := 0
		_, _, err := cursor.Get(nil, nil, mdbxgo.First)
		for err == nil {
			count++
			_, _, err = cursor.Get(nil, nil, mdbxgo.Next)
		}

		if count != 80 {
			t.Errorf("count = %d, want 80", count)
		}
	}
}
