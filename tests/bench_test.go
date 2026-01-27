package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbx "github.com/erigontech/mdbx-go/mdbx"
)

// BenchmarkGdbxWrite benchmarks gdbx write performance
func BenchmarkGdbxWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "gdbx-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	if err := env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096); err != nil {
		b.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		b.Fatal(err)
	}
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		b.Fatal(err)
	}

	// Create DBI
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		b.Fatal(err)
	}
	txn.Commit()

	// Prepare keys and values
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
		values[i] = []byte(fmt.Sprintf("value%08d", i))
	}

	b.ResetTimer()

	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
			txn.Abort()
			b.Fatal(err)
		}
	}

	b.StopTimer()
	txn.Commit()
}

// BenchmarkMdbxWrite benchmarks mdbx write performance
func BenchmarkMdbxWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "mdbx-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	env, err := mdbx.NewEnv(mdbx.Label("bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(dbPath, mdbx.Create, 0644); err != nil {
		b.Fatal(err)
	}

	// Create DBI
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBI("test", mdbx.Create, nil, nil)
	if err != nil {
		txn.Abort()
		b.Fatal(err)
	}
	txn.Commit()

	// Prepare keys and values
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
		values[i] = []byte(fmt.Sprintf("value%08d", i))
	}

	b.ResetTimer()

	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if err := txn.Put(dbi, keys[i], values[i], 0); err != nil {
			txn.Abort()
			b.Fatal(err)
		}
	}

	b.StopTimer()
	txn.Commit()
}

// BenchmarkGdbxWriteBatch benchmarks gdbx batch write with commit
func BenchmarkGdbxWriteBatch1000(b *testing.B) {
	dir, err := os.MkdirTemp("", "gdbx-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	if err := env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096); err != nil {
		b.Fatal(err)
	}
	if err := env.SetMaxDBs(10); err != nil {
		b.Fatal(err)
	}
	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		b.Fatal(err)
	}

	// Create DBI
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		b.Fatal(err)
	}
	txn.Commit()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 1000; j++ {
			key := []byte(fmt.Sprintf("key%08d_%08d", i, j))
			val := []byte(fmt.Sprintf("value%08d_%08d", i, j))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				b.Fatal(err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMdbxWriteBatch1000 benchmarks mdbx batch write with commit
func BenchmarkMdbxWriteBatch1000(b *testing.B) {
	dir, err := os.MkdirTemp("", "mdbx-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	env, err := mdbx.NewEnv(mdbx.Label("bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer env.Close()

	env.SetGeometry(-1, -1, 1<<30, -1, -1, 4096)
	env.SetOption(mdbx.OptMaxDB, 10)

	if err := env.Open(dbPath, mdbx.Create, 0644); err != nil {
		b.Fatal(err)
	}

	// Create DBI
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	dbi, err := txn.OpenDBI("test", mdbx.Create, nil, nil)
	if err != nil {
		txn.Abort()
		b.Fatal(err)
	}
	txn.Commit()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn, err = env.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 1000; j++ {
			key := []byte(fmt.Sprintf("key%08d_%08d", i, j))
			val := []byte(fmt.Sprintf("value%08d_%08d", i, j))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				b.Fatal(err)
			}
		}

		if _, err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
