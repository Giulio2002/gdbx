package tests

import (
	"os"
	"runtime"
	"testing"

	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// TestMdbxMaxKeyMaxValue tests if libmdbx has the same issue with maxKey + maxVal
func TestMdbxMaxKeyMaxValue(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "mdbx-maxkey-*")
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

	// Get the limits from mdbx
	maxKey := env.MaxKeySize()
	// mdbx-go doesn't expose MaxValSize, use same formula as gdbx
	// maxVal = pageSize/2 - nodeSize(8) - minKey(1) - indxSize(2)
	maxVal := 4096/2 - 8 - 1 - 2 // = 2037

	t.Logf("mdbx MaxKeySize: %d, MaxValSize: %d", maxKey, maxVal)
	t.Logf("Node size with max key+val: %d (header + key=%d + val=%d)", 8+maxKey+maxVal, maxKey, maxVal)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	dbi, err := txn.OpenDBI("test", mdbx.Create, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create max-size key and value
	key := make([]byte, maxKey)
	for i := range key {
		key[i] = byte(i % 256)
	}

	value := make([]byte, maxVal)
	for i := range value {
		value[i] = byte((i + 128) % 256)
	}

	// Try to put max key + max value
	err = txn.Put(dbi, key, value, 0)
	if err != nil {
		t.Logf("mdbx Put with maxKey(%d) + maxVal(%d) failed: %v", maxKey, maxVal, err)
	} else {
		t.Logf("mdbx Put succeeded with maxKey(%d) + maxVal(%d)", maxKey, maxVal)

		// Verify we can read it back
		got, err := txn.Get(dbi, key)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		} else if len(got) != len(value) {
			t.Errorf("Got value length %d, want %d", len(got), len(value))
		} else {
			t.Logf("Read back verified: %d bytes", len(got))
		}
	}
}

// TestMdbxKeyValueBoundaries tests various key/value combinations near limits
func TestMdbxKeyValueBoundaries(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dir, err := os.MkdirTemp("", "mdbx-boundary-*")
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
	env.SetOption(mdbx.OptMaxDB, 20)

	if err := env.Open(dir, mdbx.Create, 0644); err != nil {
		t.Fatal(err)
	}

	maxKey := env.MaxKeySize()
	maxVal := 4096/2 - 8 - 1 - 2 // = 2037

	testCases := []struct {
		name    string
		keySize int
		valSize int
	}{
		{"maxKey_smallVal", maxKey, 100},
		{"maxKey_halfMaxVal", maxKey, maxVal / 2},
		{"maxKey_maxVal-100", maxKey, maxVal - 100},
		{"maxKey_maxVal-10", maxKey, maxVal - 10},
		{"maxKey_maxVal-1", maxKey, maxVal - 1},
		{"maxKey_maxVal", maxKey, maxVal},
		{"halfMaxKey_maxVal", maxKey / 2, maxVal},
		{"smallKey_maxVal", 10, maxVal},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			txn, err := env.BeginTxn(nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer txn.Abort()

			dbi, err := txn.OpenDBI(tc.name, mdbx.Create, nil, nil)
			if err != nil {
				t.Fatal(err)
			}

			key := make([]byte, tc.keySize)
			value := make([]byte, tc.valSize)

			err = txn.Put(dbi, key, value, 0)
			if err != nil {
				t.Errorf("mdbx Put(keySize=%d, valSize=%d) failed: %v", tc.keySize, tc.valSize, err)
			} else {
				t.Logf("mdbx Put(keySize=%d, valSize=%d) succeeded", tc.keySize, tc.valSize)
			}
		})
	}
}
