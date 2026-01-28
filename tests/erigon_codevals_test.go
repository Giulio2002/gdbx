package tests

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
)

// TestErigonCodeValsScenario reproduces the exact Erigon CodeVals scenario
// where a "page has no space" error occurred.
func TestErigonCodeValsScenario(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-codevals-*")
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

	// Create CodeVals table (NOT DupSort, just a regular table)
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("CodeVals", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// The problematic key from Erigon logs:
	// k=de940573429fa57a3f60b6da3938eb103dd912e5fffffffffffff7d3
	problemKey, _ := hex.DecodeString("de940573429fa57a3f60b6da3938eb103dd912e5fffffffffffff7d3")
	t.Logf("Problem key: %x (len=%d)", problemKey, len(problemKey))

	// First, insert many entries to fill pages
	// Use keys with similar structure (address + version)
	for i := 0; i < 200; i++ {
		k := make([]byte, 28)
		// Simulate address (20 bytes)
		k[0] = byte(i)
		k[1] = byte(i >> 8)
		// Simulate version (8 bytes)
		for j := 20; j < 28; j++ {
			k[j] = 0xff
		}
		k[27] = byte(i)

		// Vary value sizes
		var valueSize int
		switch i % 4 {
		case 0:
			valueSize = 100 // Small
		case 1:
			valueSize = 500 // Medium
		case 2:
			valueSize = maxVal - 50 // Near threshold
		case 3:
			valueSize = maxVal + 100 // Overflow
		}

		v := make([]byte, valueSize)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatalf("Failed to insert entry %d (valueSize=%d): %v", i, valueSize, err)
		}
	}

	// Now insert the problematic key with a large value (contract code)
	// Contract code can be several KB
	for _, codeSize := range []int{1000, 2000, maxVal, maxVal + 1, 5000, 10000, 24576} {
		code := make([]byte, codeSize)
		err := txn.Put(dbi, problemKey, code, 0)
		if err != nil {
			txn.Abort()
			t.Fatalf("Failed to insert code of size %d: %v", codeSize, err)
		}
		t.Logf("Inserted code size %d OK", codeSize)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	t.Log("All inserts succeeded!")
}

// TestErigonCodeValsUpdate tests updating existing code entries
func TestErigonCodeValsUpdate(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-codevals-update-*")
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

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("CodeVals", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Insert initial entries
	problemKey, _ := hex.DecodeString("de940573429fa57a3f60b6da3938eb103dd912e5fffffffffffff7d3")

	// First insert many small entries
	for i := 0; i < 100; i++ {
		k := make([]byte, 28)
		k[0] = byte(i)
		for j := 20; j < 28; j++ {
			k[j] = 0xff
		}
		v := make([]byte, 100)
		if err := txn.Put(dbi, k, v, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	// Insert the problem key with small value first
	smallCode := make([]byte, 100)
	if err := txn.Put(dbi, problemKey, smallCode, 0); err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Now in a new transaction, UPDATE the problem key with a large value
	// This is the pattern that might trigger the bug
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Update with increasingly larger values
	for _, codeSize := range []int{500, 1000, maxVal, maxVal + 1, 5000, 10000} {
		code := make([]byte, codeSize)
		err := txn.Put(dbi, problemKey, code, 0)
		if err != nil {
			txn.Abort()
			t.Fatalf("Failed to update with code size %d: %v", codeSize, err)
		}
		t.Logf("Updated to code size %d OK", codeSize)
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Log("All updates succeeded!")
}

// TestManyLargeInserts tests inserting many large values in sequence
func TestManyLargeInserts(t *testing.T) {
	dir, err := os.MkdirTemp("", "gdbx-manylargeinserts-*")
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

	// Insert many values that are just under the overflow threshold
	// This creates many large nodes that need splitting
	for i := 0; i < 500; i++ {
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		k := make([]byte, 28)
		k[0] = byte(i)
		k[1] = byte(i >> 8)
		for j := 20; j < 28; j++ {
			k[j] = 0xff
		}

		// Value just under threshold (will NOT use overflow)
		v := make([]byte, maxVal)
		err = txn.Put(dbi, k, v, 0)
		if err != nil {
			txn.Abort()
			t.Fatalf("Failed at iteration %d: %v", i, err)
		}

		if _, err := txn.Commit(); err != nil {
			t.Fatalf("Failed to commit at iteration %d: %v", i, err)
		}

		if i%100 == 0 {
			t.Logf("Inserted %d large values", i)
		}
	}

	t.Log("All large inserts succeeded!")
}
