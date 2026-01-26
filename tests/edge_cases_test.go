package tests

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// TestEdgeCases compares gdbx and mdbx-go behavior for edge cases
func TestEdgeCases(t *testing.T) {
	// Create temp directories
	gdbxPath := t.TempDir() + "/gdbx.db"
	mdbxPath := t.TempDir() + "/mdbx.db"

	// Open gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer genv.Close()
	genv.SetMaxDBs(10)
	if err := genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Open mdbx-go
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer menv.Close()
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	if err := menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	t.Run("NilKeyNilValue", func(t *testing.T) {
		testNilKeyNilValue(t, genv, menv)
	})

	t.Run("NilKeyWithValue", func(t *testing.T) {
		testNilKeyWithValue(t, genv, menv)
	})

	t.Run("EmptyKeyEmptyValue", func(t *testing.T) {
		testEmptyKeyEmptyValue(t, genv, menv)
	})

	t.Run("DeleteCurrentThenRead", func(t *testing.T) {
		testDeleteCurrentThenRead(t, genv, menv)
	})

	t.Run("DeleteUntilEmptyAndBeyond", func(t *testing.T) {
		testDeleteUntilEmptyAndBeyond(t, genv, menv)
	})

	t.Run("DeleteCurrentThenNext", func(t *testing.T) {
		testDeleteCurrentThenNext(t, genv, menv)
	})

	t.Run("DeleteCurrentThenPrev", func(t *testing.T) {
		testDeleteCurrentThenPrev(t, genv, menv)
	})

	t.Run("CursorAfterCommit", func(t *testing.T) {
		testCursorAfterCommit(t, genv, menv)
	})

	t.Run("GetOnUninitializedCursor", func(t *testing.T) {
		testGetOnUninitializedCursor(t, genv, menv)
	})

	t.Run("SetNonExistentKey", func(t *testing.T) {
		testSetNonExistentKey(t, genv, menv)
	})
}

func testNilKeyNilValue(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// gdbx
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("nil_test", gdbx.Create)
	gerr := gtxn.Put(gdbi, nil, nil, 0)
	gtxn.Abort()

	// mdbx-go
	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("nil_test", mdbxgo.Create, nil, nil)
	merr := mtxn.Put(mdbi, nil, nil, 0)
	mtxn.Abort()

	compareErrors(t, "Put(nil, nil)", gerr, merr)
}

func testNilKeyWithValue(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// gdbx
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("nil_key_test", gdbx.Create)
	gerr := gtxn.Put(gdbi, nil, []byte("value"), 0)
	gtxn.Abort()

	// mdbx-go
	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("nil_key_test", mdbxgo.Create, nil, nil)
	merr := mtxn.Put(mdbi, nil, []byte("value"), 0)
	mtxn.Abort()

	compareErrors(t, "Put(nil, value)", gerr, merr)
}

func testEmptyKeyEmptyValue(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// gdbx
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("empty_test", gdbx.Create)
	gerr := gtxn.Put(gdbi, []byte{}, []byte{}, 0)
	var gval []byte
	if gerr == nil {
		gval, gerr = gtxn.Get(gdbi, []byte{})
	}
	gtxn.Abort()

	// mdbx-go
	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("empty_test", mdbxgo.Create, nil, nil)
	merr := mtxn.Put(mdbi, []byte{}, []byte{}, 0)
	var mval []byte
	if merr == nil {
		mval, merr = mtxn.Get(mdbi, []byte{})
	}
	mtxn.Abort()

	compareErrors(t, "Put/Get empty key/value", gerr, merr)
	if gerr == nil && merr == nil {
		if !bytes.Equal(gval, mval) {
			t.Errorf("Values differ: gdbx=%v, mdbx=%v", gval, mval)
		}
	}
}

func testDeleteCurrentThenRead(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup: insert some data
	setupData := func() {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple("del_read_test", gdbx.Create)
		gtxn.Put(gdbi, []byte("key1"), []byte("val1"), 0)
		gtxn.Put(gdbi, []byte("key2"), []byte("val2"), 0)
		gtxn.Put(gdbi, []byte("key3"), []byte("val3"), 0)
		gtxn.Commit()

		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI("del_read_test", mdbxgo.Create, nil, nil)
		mtxn.Put(mdbi, []byte("key1"), []byte("val1"), 0)
		mtxn.Put(mdbi, []byte("key2"), []byte("val2"), 0)
		mtxn.Put(mdbi, []byte("key3"), []byte("val3"), 0)
		mtxn.Commit()
	}
	setupData()

	// gdbx: position at key2, delete, then GetCurrent
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("del_read_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gc.Get([]byte("key2"), nil, gdbx.Set)
	gc.Del(0)
	gk, gv, gerr := gc.Get(nil, nil, gdbx.GetCurrent)
	gc.Close()
	gtxn.Abort()

	// mdbx-go: same
	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("del_read_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mc.Get([]byte("key2"), nil, mdbxgo.Set)
	mc.Del(0)
	mk, mv, merr := mc.Get(nil, nil, mdbxgo.GetCurrent)
	mc.Close()
	mtxn.Abort()

	t.Logf("After Delete+GetCurrent:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "DeleteCurrent then GetCurrent", gerr, merr)
}

func testDeleteUntilEmptyAndBeyond(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup: insert some data
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("del_empty_test", gdbx.Create)
	gtxn.Put(gdbi, []byte("a"), []byte("1"), 0)
	gtxn.Put(gdbi, []byte("b"), []byte("2"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("del_empty_test", mdbxgo.Create, nil, nil)
	mtxn.Put(mdbi, []byte("a"), []byte("1"), 0)
	mtxn.Put(mdbi, []byte("b"), []byte("2"), 0)
	mtxn.Commit()

	// gdbx: delete all entries
	gtxn, _ = genv.BeginTxn(nil, 0)
	gdbi, _ = gtxn.OpenDBISimple("del_empty_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)

	var gdelErrors []error
	var gnavErrors []error

	gc.Get(nil, nil, gdbx.First)
	for i := 0; i < 5; i++ { // Try more deletes than entries
		err := gc.Del(0)
		gdelErrors = append(gdelErrors, err)
		if err != nil {
			break
		}
		_, _, err = gc.Get(nil, nil, gdbx.Next)
		gnavErrors = append(gnavErrors, err)
	}
	gc.Close()
	gtxn.Abort()

	// mdbx-go: same
	mtxn, _ = menv.BeginTxn(nil, 0)
	mdbi, _ = mtxn.OpenDBI("del_empty_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)

	var mdelErrors []error
	var mnavErrors []error

	mc.Get(nil, nil, mdbxgo.First)
	for i := 0; i < 5; i++ {
		err := mc.Del(0)
		mdelErrors = append(mdelErrors, err)
		if err != nil {
			break
		}
		_, _, err = mc.Get(nil, nil, mdbxgo.Next)
		mnavErrors = append(mnavErrors, err)
	}
	mc.Close()
	mtxn.Abort()

	t.Logf("Delete errors: gdbx=%v, mdbx=%v", gdelErrors, mdelErrors)
	t.Logf("Nav errors: gdbx=%v, mnavErrors=%v", gnavErrors, mnavErrors)

	if len(gdelErrors) != len(mdelErrors) {
		t.Errorf("Different number of delete iterations: gdbx=%d, mdbx=%d", len(gdelErrors), len(mdelErrors))
	}
}

func testDeleteCurrentThenNext(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("del_next_test", gdbx.Create)
	gtxn.Put(gdbi, []byte("a"), []byte("1"), 0)
	gtxn.Put(gdbi, []byte("b"), []byte("2"), 0)
	gtxn.Put(gdbi, []byte("c"), []byte("3"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("del_next_test", mdbxgo.Create, nil, nil)
	mtxn.Put(mdbi, []byte("a"), []byte("1"), 0)
	mtxn.Put(mdbi, []byte("b"), []byte("2"), 0)
	mtxn.Put(mdbi, []byte("c"), []byte("3"), 0)
	mtxn.Commit()

	// gdbx: position at 'a', delete, then Next
	gtxn, _ = genv.BeginTxn(nil, 0)
	gdbi, _ = gtxn.OpenDBISimple("del_next_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gc.Get(nil, nil, gdbx.First) // at 'a'
	gc.Del(0)
	gk, gv, gerr := gc.Get(nil, nil, gdbx.Next)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, 0)
	mdbi, _ = mtxn.OpenDBI("del_next_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mc.Get(nil, nil, mdbxgo.First)
	mc.Del(0)
	mk, mv, merr := mc.Get(nil, nil, mdbxgo.Next)
	mc.Close()
	mtxn.Abort()

	t.Logf("After Delete+Next:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "Delete then Next", gerr, merr)
	if gerr == nil && merr == nil {
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Errorf("Results differ: gdbx=(%q,%q), mdbx=(%q,%q)", gk, gv, mk, mv)
		}
	}
}

func testDeleteCurrentThenPrev(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("del_prev_test", gdbx.Create)
	gtxn.Put(gdbi, []byte("a"), []byte("1"), 0)
	gtxn.Put(gdbi, []byte("b"), []byte("2"), 0)
	gtxn.Put(gdbi, []byte("c"), []byte("3"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("del_prev_test", mdbxgo.Create, nil, nil)
	mtxn.Put(mdbi, []byte("a"), []byte("1"), 0)
	mtxn.Put(mdbi, []byte("b"), []byte("2"), 0)
	mtxn.Put(mdbi, []byte("c"), []byte("3"), 0)
	mtxn.Commit()

	// gdbx: position at 'c', delete, then Prev
	gtxn, _ = genv.BeginTxn(nil, 0)
	gdbi, _ = gtxn.OpenDBISimple("del_prev_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gc.Get(nil, nil, gdbx.Last) // at 'c'
	gc.Del(0)
	gk, gv, gerr := gc.Get(nil, nil, gdbx.Prev)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, 0)
	mdbi, _ = mtxn.OpenDBI("del_prev_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mc.Get(nil, nil, mdbxgo.Last)
	mc.Del(0)
	mk, mv, merr := mc.Get(nil, nil, mdbxgo.Prev)
	mc.Close()
	mtxn.Abort()

	t.Logf("After Delete+Prev:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "Delete then Prev", gerr, merr)
	if gerr == nil && merr == nil {
		if !bytes.Equal(gk, mk) || !bytes.Equal(gv, mv) {
			t.Errorf("Results differ: gdbx=(%q,%q), mdbx=(%q,%q)", gk, gv, mk, mv)
		}
	}
}

func testCursorAfterCommit(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("cursor_commit_test", gdbx.Create)
	gtxn.Put(gdbi, []byte("key"), []byte("val"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("cursor_commit_test", mdbxgo.Create, nil, nil)
	mtxn.Put(mdbi, []byte("key"), []byte("val"), 0)
	mtxn.Commit()

	// gdbx: open cursor, position, commit txn, try to use cursor
	gtxn, _ = genv.BeginTxn(nil, 0)
	gdbi, _ = gtxn.OpenDBISimple("cursor_commit_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gc.Get(nil, nil, gdbx.First)
	gtxn.Commit()
	_, _, gerr := gc.Get(nil, nil, gdbx.Next)

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, 0)
	mdbi, _ = mtxn.OpenDBI("cursor_commit_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mc.Get(nil, nil, mdbxgo.First)
	mtxn.Commit()
	_, _, merr := mc.Get(nil, nil, mdbxgo.Next)

	t.Logf("Cursor after commit: gdbx_err=%v, mdbx_err=%v", gerr, merr)
	// Both should error (cursor invalid after commit)
	if (gerr == nil) != (merr == nil) {
		t.Errorf("Different behavior: gdbx_err=%v, mdbx_err=%v", gerr, merr)
	}
}

func testGetOnUninitializedCursor(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("uninit_cursor_test", gdbx.Create)
	gtxn.Put(gdbi, []byte("key"), []byte("val"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("uninit_cursor_test", mdbxgo.Create, nil, nil)
	mtxn.Put(mdbi, []byte("key"), []byte("val"), 0)
	mtxn.Commit()

	// gdbx: open cursor, immediately GetCurrent without positioning
	gtxn, _ = genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ = gtxn.OpenDBISimple("uninit_cursor_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gk, gv, gerr := gc.Get(nil, nil, gdbx.GetCurrent)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ = mtxn.OpenDBI("uninit_cursor_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mk, mv, merr := mc.Get(nil, nil, mdbxgo.GetCurrent)
	mc.Close()
	mtxn.Abort()

	t.Logf("GetCurrent on uninitialized cursor:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "GetCurrent uninitialized", gerr, merr)
}

func testSetNonExistentKey(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup with some data
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("set_nonexist_test", gdbx.Create)
	gtxn.Put(gdbi, []byte("aaa"), []byte("1"), 0)
	gtxn.Put(gdbi, []byte("zzz"), []byte("2"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("set_nonexist_test", mdbxgo.Create, nil, nil)
	mtxn.Put(mdbi, []byte("aaa"), []byte("1"), 0)
	mtxn.Put(mdbi, []byte("zzz"), []byte("2"), 0)
	mtxn.Commit()

	// gdbx: Set to nonexistent key
	gtxn, _ = genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ = gtxn.OpenDBISimple("set_nonexist_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gk, gv, gerr := gc.Get([]byte("mmm"), nil, gdbx.Set)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ = mtxn.OpenDBI("set_nonexist_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mk, mv, merr := mc.Get([]byte("mmm"), nil, mdbxgo.Set)
	mc.Close()
	mtxn.Abort()

	t.Logf("Set to nonexistent key:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "Set nonexistent", gerr, merr)
}

func compareErrors(t *testing.T, op string, gerr, merr error) {
	gIsNil := gerr == nil
	mIsNil := merr == nil

	if gIsNil != mIsNil {
		t.Errorf("%s: different error behavior - gdbx=%v, mdbx=%v", op, gerr, merr)
	}
	// Note: We don't compare error types strictly because mdbx-go uses different
	// error codes (ENODATA vs NOTFOUND) for some edge cases. Both returning
	// an error is sufficient for compatibility.
}

// TestDupsortEdgeCases tests DUPSORT-specific edge cases
func TestDupsortEdgeCases(t *testing.T) {
	gdbxPath := t.TempDir() + "/gdbx.db"
	mdbxPath := t.TempDir() + "/mdbx.db"

	genv, _ := gdbx.NewEnv(gdbx.Default)
	defer genv.Close()
	genv.SetMaxDBs(10)
	genv.Open(gdbxPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("test"))
	defer menv.Close()
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(mdbxPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)

	t.Run("DeleteAllDupsOnKey", func(t *testing.T) {
		testDeleteAllDupsOnKey(t, genv, menv)
	})

	t.Run("NextDupAtEnd", func(t *testing.T) {
		testNextDupAtEnd(t, genv, menv)
	})

	t.Run("PrevDupAtStart", func(t *testing.T) {
		testPrevDupAtStart(t, genv, menv)
	})

	t.Run("GetBothNonExistent", func(t *testing.T) {
		testGetBothNonExistent(t, genv, menv)
	})
}

func testDeleteAllDupsOnKey(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup: key with multiple values
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("del_dups_test", gdbx.Create|gdbx.DupSort)
	gtxn.Put(gdbi, []byte("key"), []byte("val1"), 0)
	gtxn.Put(gdbi, []byte("key"), []byte("val2"), 0)
	gtxn.Put(gdbi, []byte("key"), []byte("val3"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("del_dups_test", mdbxgo.Create|mdbxgo.DupSort, nil, nil)
	mtxn.Put(mdbi, []byte("key"), []byte("val1"), 0)
	mtxn.Put(mdbi, []byte("key"), []byte("val2"), 0)
	mtxn.Put(mdbi, []byte("key"), []byte("val3"), 0)
	mtxn.Commit()

	// gdbx: delete all dups one by one
	gtxn, _ = genv.BeginTxn(nil, 0)
	gdbi, _ = gtxn.OpenDBISimple("del_dups_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)

	var gresults []string
	gc.Get(nil, nil, gdbx.First)
	for {
		k, v, err := gc.Get(nil, nil, gdbx.GetCurrent)
		if err != nil {
			gresults = append(gresults, fmt.Sprintf("err:%v", err))
			break
		}
		gresults = append(gresults, fmt.Sprintf("del:%s/%s", k, v))
		if err := gc.Del(0); err != nil {
			gresults = append(gresults, fmt.Sprintf("del_err:%v", err))
			break
		}
		if _, _, err := gc.Get(nil, nil, gdbx.NextDup); err != nil {
			if _, _, err := gc.Get(nil, nil, gdbx.Next); err != nil {
				gresults = append(gresults, "done")
				break
			}
		}
	}
	gc.Close()
	gtxn.Abort()

	// mdbx-go: same
	mtxn, _ = menv.BeginTxn(nil, 0)
	mdbi, _ = mtxn.OpenDBI("del_dups_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)

	var mresults []string
	mc.Get(nil, nil, mdbxgo.First)
	for {
		k, v, err := mc.Get(nil, nil, mdbxgo.GetCurrent)
		if err != nil {
			mresults = append(mresults, fmt.Sprintf("err:%v", err))
			break
		}
		mresults = append(mresults, fmt.Sprintf("del:%s/%s", k, v))
		if err := mc.Del(0); err != nil {
			mresults = append(mresults, fmt.Sprintf("del_err:%v", err))
			break
		}
		if _, _, err := mc.Get(nil, nil, mdbxgo.NextDup); err != nil {
			if _, _, err := mc.Get(nil, nil, mdbxgo.Next); err != nil {
				mresults = append(mresults, "done")
				break
			}
		}
	}
	mc.Close()
	mtxn.Abort()

	t.Logf("gdbx results: %v", gresults)
	t.Logf("mdbx results: %v", mresults)

	if len(gresults) != len(mresults) {
		t.Errorf("Different number of operations: gdbx=%d, mdbx=%d", len(gresults), len(mresults))
	}
}

func testNextDupAtEnd(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("nextdup_end_test", gdbx.Create|gdbx.DupSort)
	gtxn.Put(gdbi, []byte("key"), []byte("val1"), 0)
	gtxn.Put(gdbi, []byte("key"), []byte("val2"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("nextdup_end_test", mdbxgo.Create|mdbxgo.DupSort, nil, nil)
	mtxn.Put(mdbi, []byte("key"), []byte("val1"), 0)
	mtxn.Put(mdbi, []byte("key"), []byte("val2"), 0)
	mtxn.Commit()

	// gdbx: go to last dup, then NextDup
	gtxn, _ = genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ = gtxn.OpenDBISimple("nextdup_end_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gc.Get(nil, nil, gdbx.Last)
	gk, gv, gerr := gc.Get(nil, nil, gdbx.NextDup)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ = mtxn.OpenDBI("nextdup_end_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mc.Get(nil, nil, mdbxgo.Last)
	mk, mv, merr := mc.Get(nil, nil, mdbxgo.NextDup)
	mc.Close()
	mtxn.Abort()

	t.Logf("NextDup at end:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "NextDup at end", gerr, merr)
}

func testPrevDupAtStart(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("prevdup_start_test", gdbx.Create|gdbx.DupSort)
	gtxn.Put(gdbi, []byte("key"), []byte("val1"), 0)
	gtxn.Put(gdbi, []byte("key"), []byte("val2"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("prevdup_start_test", mdbxgo.Create|mdbxgo.DupSort, nil, nil)
	mtxn.Put(mdbi, []byte("key"), []byte("val1"), 0)
	mtxn.Put(mdbi, []byte("key"), []byte("val2"), 0)
	mtxn.Commit()

	// gdbx: go to first, then PrevDup
	gtxn, _ = genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ = gtxn.OpenDBISimple("prevdup_start_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gc.Get(nil, nil, gdbx.First)
	gk, gv, gerr := gc.Get(nil, nil, gdbx.PrevDup)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ = mtxn.OpenDBI("prevdup_start_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mc.Get(nil, nil, mdbxgo.First)
	mk, mv, merr := mc.Get(nil, nil, mdbxgo.PrevDup)
	mc.Close()
	mtxn.Abort()

	t.Logf("PrevDup at start:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "PrevDup at start", gerr, merr)
}

func testGetBothNonExistent(t *testing.T, genv *gdbx.Env, menv *mdbxgo.Env) {
	// Setup
	gtxn, _ := genv.BeginTxn(nil, 0)
	gdbi, _ := gtxn.OpenDBISimple("getboth_nonexist_test", gdbx.Create|gdbx.DupSort)
	gtxn.Put(gdbi, []byte("key"), []byte("val1"), 0)
	gtxn.Put(gdbi, []byte("key"), []byte("val3"), 0)
	gtxn.Commit()

	mtxn, _ := menv.BeginTxn(nil, 0)
	mdbi, _ := mtxn.OpenDBI("getboth_nonexist_test", mdbxgo.Create|mdbxgo.DupSort, nil, nil)
	mtxn.Put(mdbi, []byte("key"), []byte("val1"), 0)
	mtxn.Put(mdbi, []byte("key"), []byte("val3"), 0)
	mtxn.Commit()

	// gdbx: GetBoth with nonexistent value
	gtxn, _ = genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ = gtxn.OpenDBISimple("getboth_nonexist_test", 0)
	gc, _ := gtxn.OpenCursor(gdbi)
	gk, gv, gerr := gc.Get([]byte("key"), []byte("val2"), gdbx.GetBoth)
	gc.Close()
	gtxn.Abort()

	// mdbx-go
	mtxn, _ = menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ = mtxn.OpenDBI("getboth_nonexist_test", 0, nil, nil)
	mc, _ := mtxn.OpenCursor(mdbi)
	mk, mv, merr := mc.Get([]byte("key"), []byte("val2"), mdbxgo.GetBoth)
	mc.Close()
	mtxn.Abort()

	t.Logf("GetBoth nonexistent value:")
	t.Logf("  gdbx: key=%q, val=%q, err=%v", gk, gv, gerr)
	t.Logf("  mdbx: key=%q, val=%q, err=%v", mk, mv, merr)
	compareErrors(t, "GetBoth nonexistent", gerr, merr)
}

// Cleanup helper
func init() {
	// Clean up any leftover test DBs
	os.RemoveAll("/tmp/gdbx_edge_test")
}
