package benchmarks

import (
	"runtime"
	"testing"

	"github.com/Giulio2002/gdbx"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// BenchmarkDBI benchmarks DBI and transaction operations.
func BenchmarkDBI(b *testing.B) {
	// OpenDBI on existing database
	b.Run("OpenDBI/gdbx", benchOpenDBIGdbx)
	b.Run("OpenDBI/mdbx", benchOpenDBIMdbx)

	// BeginTxn (read-only)
	b.Run("BeginTxnRO/gdbx", benchBeginTxnROGdbx)
	b.Run("BeginTxnRO/mdbx", benchBeginTxnROMdbx)

	// BeginTxn (read-write)
	b.Run("BeginTxnRW/gdbx", benchBeginTxnRWGdbx)
	b.Run("BeginTxnRW/mdbx", benchBeginTxnRWMdbx)

	// Full cycle: BeginTxn + OpenDBI + Abort
	b.Run("TxnCycle/gdbx", benchTxnCycleGdbx)
	b.Run("TxnCycle/mdbx", benchTxnCycleMdbx)
}

// ============ OpenDBI ============

func benchOpenDBIGdbx(b *testing.B) {
	genv, _, _ := getCachedPlainDB(b, 10_000)

	txn, err := genv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	// Open once to ensure it exists
	_, err = txn.OpenDBISimple("bench", 0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = txn.OpenDBISimple("bench", 0)
	}
}

func benchOpenDBIMdbx(b *testing.B) {
	_, menv, _ := getCachedPlainDB(b, 10_000)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := menv.BeginTxn(nil, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer txn.Abort()

	// Open once to ensure it exists
	_, err = txn.OpenDBI("bench", 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = txn.OpenDBI("bench", 0, nil, nil)
	}
}

// ============ BeginTxn Read-Only ============

func benchBeginTxnROGdbx(b *testing.B) {
	genv, _, _ := getCachedPlainDB(b, 10_000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, gdbx.TxnReadOnly)
		if err != nil {
			b.Fatal(err)
		}
		txn.Abort()
	}
}

func benchBeginTxnROMdbx(b *testing.B) {
	_, menv, _ := getCachedPlainDB(b, 10_000)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, mdbxgo.Readonly)
		if err != nil {
			b.Fatal(err)
		}
		txn.Abort()
	}
}

// ============ BeginTxn Read-Write ============

func benchBeginTxnRWGdbx(b *testing.B) {
	genv, _, _ := getCachedPlainDB(b, 10_000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		txn.Abort()
	}
}

func benchBeginTxnRWMdbx(b *testing.B) {
	_, menv, _ := getCachedPlainDB(b, 10_000)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		txn.Abort()
	}
}

// ============ Full Transaction Cycle ============

func benchTxnCycleGdbx(b *testing.B) {
	genv, _, _ := getCachedPlainDB(b, 10_000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := genv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		_, err = txn.OpenDBISimple("bench", 0)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}
		txn.Abort()
	}
}

func benchTxnCycleMdbx(b *testing.B) {
	_, menv, _ := getCachedPlainDB(b, 10_000)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txn, err := menv.BeginTxn(nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		_, err = txn.OpenDBI("bench", 0, nil, nil)
		if err != nil {
			txn.Abort()
			b.Fatal(err)
		}
		txn.Abort()
	}
}
