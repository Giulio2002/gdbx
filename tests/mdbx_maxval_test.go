package tests

import (
	"os"
	"runtime"
	"testing"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestMdbxFindMaxVal finds the actual maximum value size that mdbx accepts
func TestMdbxFindMaxVal(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "mdbx-findmax-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := mdbx.NewEnv(mdbx.Label("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(dir, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := env.MaxKeySize()
	t.Logf("mdbx MaxKeySize: %d", maxKey)

	// Test with maxKey and increasingly large values
	testSizes := []int{2030, 2035, 2037, 2040, 2044, 2050, 2060, 2100, 3000, 4000}

	for _, valSize := range testSizes {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		dbi, err := txn.OpenDBI("test", mdbx.Create, nil, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		key := make([]byte, maxKey)
		value := make([]byte, valSize)

		err = txn.Put(dbi, key, value, 0)
		if err != nil {
			t.Logf("mdbx maxKey(%d) + val(%d) = node(%d): FAILED - %v", maxKey, valSize, 8+maxKey+valSize, err)
		} else {
			t.Logf("mdbx maxKey(%d) + val(%d) = node(%d): OK", maxKey, valSize, 8+maxKey+valSize)
		}
		txn.Abort()
	}

	// Also test with small key + large value
	t.Log("\n--- With small key (10 bytes) ---")
	smallKey := 10
	largeSizes := []int{4050, 4060, 4064, 4065, 4066, 4100, 5000, 10000}

	for _, valSize := range largeSizes {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		dbi, err := txn.OpenDBI("test2", mdbx.Create, nil, nil)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}

		key := make([]byte, smallKey)
		value := make([]byte, valSize)

		err = txn.Put(dbi, key, value, 0)
		if err != nil {
			t.Logf("mdbx key(%d) + val(%d) = node(%d): FAILED - %v", smallKey, valSize, 8+smallKey+valSize, err)
		} else {
			t.Logf("mdbx key(%d) + val(%d) = node(%d): OK (likely overflow)", smallKey, valSize, 8+smallKey+valSize)
		}
		txn.Abort()
	}
}
