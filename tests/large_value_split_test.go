package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestLargeValueSplit tests inserting a value close to maxVal threshold
// which can cause ErrPageFull during splits.
func TestLargeValueSplit(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-largevalue-*")
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

	// Get thresholds
	maxVal := env.MaxValSize()
	t.Logf("MaxValSize: %d", maxVal)

	// Create a table (not DupSort)
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("CodeVals", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Key similar to Erigon's CodeVals key (28 bytes)
	// Insert some entries to partially fill the page
	for i := 0; i < 50; i++ {
		k := make([]byte, 28)
		k[0] = byte(i)
		v := make([]byte, 50) // Small values
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatalf("Failed to insert initial entry %d: %v", i, err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit initial entries: %v", err)
	}

	// Now try to insert a value close to maxVal
	// This is the problematic case
	valueSize := maxVal // Exactly at threshold
	t.Logf("Inserting value of size %d with key of size 28", valueSize)
	t.Logf("Node size would be: %d", 8+28+valueSize)

	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	k := make([]byte, 28)
	k[0] = 100 // New key
	v := make([]byte, valueSize)
	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Failed to insert large value: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit large value: %v", err)
	}

	// Also test value just over threshold
	t.Log("Testing value just over threshold (should use overflow)...")
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	k = make([]byte, 28)
	k[0] = 101 // New key
	v = make([]byte, maxVal+1)
	err = txn.Put(dbi, k, v, 0)
	if err != nil {
		txn.Abort()
		t.Fatalf("Failed to insert overflow value: %v", err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit overflow value: %v", err)
	}

	t.Log("All inserts succeeded!")
}

// TestLargeValueSplitStress stress tests large value insertions
func TestLargeValueSplitStress(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-largevalue-stress-*")
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

	maxVal := env.MaxValSize()
	t.Logf("MaxValSize: %d", maxVal)

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("CodeVals", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Insert many entries with varying sizes around the threshold
	for i := 0; i < 100; i++ {
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		k := make([]byte, 28)
		k[0] = byte(i)
		k[1] = byte(i >> 8)

		// Vary value size around threshold
		var valueSize int
		switch i % 5 {
		case 0:
			valueSize = maxVal - 100 // Below threshold
		case 1:
			valueSize = maxVal // At threshold
		case 2:
			valueSize = maxVal + 1 // Just over (overflow)
		case 3:
			valueSize = maxVal + 1000 // Well over (overflow)
		case 4:
			valueSize = 100 // Small
		}

		v := make([]byte, valueSize)
		err = txn.Put(dbi, k, v, 0)
		if err != nil {
			txn.Abort()
			t.Fatalf("Failed at iteration %d with value size %d: %v", i, valueSize, err)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Failed to commit at iteration %d: %v", i, err)
		}
	}

	t.Log("Stress test passed!")
}
