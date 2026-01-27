package benchmarks

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/Giulio2002/gdbx"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
)

// Large database benchmark configuration
const (
	// Table 1: dupsort-bigdup - 10k keys × 10k values = 100M entries
	bigDupKeys   = 10_000
	bigDupValues = 10_000

	// Table 2: dupsort-smalldup - 1M keys × 10 values = 10M entries
	smallDupKeys   = 1_000_000
	smallDupValues = 10

	// Table 3: plain - 10M keys
	plainKeys = 10_000_000

	// Key/value sizes: random between 16-48 bytes (reduced to avoid page overflow)
	minSize = 16
	maxSize = 48
)

// randomBytes generates random bytes with length between min and max
func randomBytes(min, max int) []byte {
	size := min
	if max > min {
		var b [1]byte
		rand.Read(b[:])
		size = min + int(b[0])%(max-min+1)
	}
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}

// GenerateLargeDB creates a large test database
// Run with: go test -v -run TestGenerateLargeDB -timeout 0
// Database is stored persistently at ~/gdbx_large_bench.db
func TestGenerateLargeDB(t *testing.T) {
	if os.Getenv("GENERATE_LARGE_DB") != "1" {
		t.Skip("Set GENERATE_LARGE_DB=1 to run this test")
	}

	dbPath := os.Getenv("LARGE_DB_PATH")
	if dbPath == "" {
		home, _ := os.UserHomeDir()
		dbPath = home + "/gdbx_large_bench.db"
	}

	// Check if already exists
	if _, err := os.Stat(dbPath); err == nil {
		fmt.Printf("Database already exists at %s\n", dbPath)
		fmt.Println("Delete it manually if you want to regenerate")
		return
	}

	// Remove any partial/corrupt files
	os.Remove(dbPath)
	os.Remove(dbPath + "-lck")

	fmt.Printf("Creating large benchmark database at %s\n", dbPath)
	fmt.Printf("Configuration:\n")
	fmt.Printf("  - bigdup:    %d keys × %d values = %d entries\n", bigDupKeys, bigDupValues, bigDupKeys*bigDupValues)
	fmt.Printf("  - smalldup:  %d keys × %d values = %d entries\n", smallDupKeys, smallDupValues, smallDupKeys*smallDupValues)
	fmt.Printf("  - plain:     %d entries\n", plainKeys)
	fmt.Printf("  - Key/value size: %d-%d bytes (random)\n", minSize, maxSize)
	fmt.Println()

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	// 100GB map size to accommodate large data
	env.SetGeometry(-1, -1, 100<<30, -1, -1, 4096)

	if err := env.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync|gdbx.WriteMap, 0644); err != nil {
		t.Fatal(err)
	}

	// Create tables
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	bigdupDBI, _ := txn.OpenDBISimple("bigdup", gdbx.Create|gdbx.DupSort)
	smalldupDBI, _ := txn.OpenDBISimple("smalldup", gdbx.Create|gdbx.DupSort)
	plainDBI, _ := txn.OpenDBISimple("plain", gdbx.Create)
	_, _ = txn.Commit()

	// Helper to commit in batches
	batchSize := 100_000
	var currentTxn *gdbx.Txn
	var ops int

	startBatch := func() {
		var err error
		currentTxn, err = env.BeginTxn(nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ops = 0
	}

	commitBatch := func() {
		if currentTxn != nil {
			if _, err := currentTxn.Commit(); err != nil {
				t.Fatal(err)
			}
			currentTxn = nil
		}
	}

	maybeCommit := func() {
		ops++
		if ops >= batchSize {
			commitBatch()
			startBatch()
		}
	}

	// Generate bigdup table (10k keys × 10k values)
	fmt.Println("Generating bigdup table...")
	start := time.Now()
	startBatch()

	// Use fixed 32-byte keys and 32-byte values for DUPSORT to avoid page issues
	bigdupKeySize := 32
	bigdupValSize := 32

	for i := 0; i < bigDupKeys; i++ {
		key := make([]byte, bigdupKeySize)
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		rand.Read(key[8:])

		for j := 0; j < bigDupValues; j++ {
			val := make([]byte, bigdupValSize)
			binary.BigEndian.PutUint64(val[:8], uint64(j))
			rand.Read(val[8:])

			if err := currentTxn.Put(bigdupDBI, key, val, 0); err != nil {
				t.Fatalf("bigdup put failed at key %d, val %d: %v", i, j, err)
			}
			maybeCommit()
		}

		if (i+1)%1000 == 0 {
			fmt.Printf("  bigdup: %d/%d keys (%.1f%%)\n", i+1, bigDupKeys, float64(i+1)*100/float64(bigDupKeys))
		}
	}
	commitBatch()
	fmt.Printf("  bigdup completed in %v\n\n", time.Since(start))

	// Generate smalldup table (1M keys × 10 values)
	fmt.Println("Generating smalldup table...")
	start = time.Now()
	startBatch()

	// Use fixed 32-byte keys and 32-byte values
	smalldupKeySize := 32
	smalldupValSize := 32

	for i := 0; i < smallDupKeys; i++ {
		key := make([]byte, smalldupKeySize)
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		rand.Read(key[8:])

		for j := 0; j < smallDupValues; j++ {
			val := make([]byte, smalldupValSize)
			binary.BigEndian.PutUint64(val[:8], uint64(j))
			rand.Read(val[8:])

			if err := currentTxn.Put(smalldupDBI, key, val, 0); err != nil {
				t.Fatalf("smalldup put failed at key %d, val %d: %v", i, j, err)
			}
			maybeCommit()
		}

		if (i+1)%100000 == 0 {
			fmt.Printf("  smalldup: %d/%d keys (%.1f%%)\n", i+1, smallDupKeys, float64(i+1)*100/float64(smallDupKeys))
		}
	}
	commitBatch()
	fmt.Printf("  smalldup completed in %v\n\n", time.Since(start))

	// Generate plain table (10M keys)
	fmt.Println("Generating plain table...")
	start = time.Now()
	startBatch()

	for i := 0; i < plainKeys; i++ {
		key := randomBytes(minSize, maxSize)
		keyWithIdx := make([]byte, 8+len(key))
		binary.BigEndian.PutUint64(keyWithIdx[:8], uint64(i))
		copy(keyWithIdx[8:], key)

		val := randomBytes(minSize, maxSize)

		if err := currentTxn.Put(plainDBI, keyWithIdx, val, 0); err != nil {
			t.Fatalf("plain put failed at key %d: %v", i, err)
		}
		maybeCommit()

		if (i+1)%1000000 == 0 {
			fmt.Printf("  plain: %d/%d keys (%.1f%%)\n", i+1, plainKeys, float64(i+1)*100/float64(plainKeys))
		}
	}
	commitBatch()
	fmt.Printf("  plain completed in %v\n\n", time.Since(start))

	// Sync and report
	env.Sync(true, false)

	// Get file size
	fi, _ := os.Stat(dbPath)
	fmt.Printf("Database created: %s (%.2f GB)\n", dbPath, float64(fi.Size())/(1<<30))

	// Print stats
	txn, _ = env.BeginTxn(nil, gdbx.TxnReadOnly)
	defer txn.Abort()

	stat, _ := txn.Stat(bigdupDBI)
	fmt.Printf("bigdup stats: entries=%d, depth=%d, leafPages=%d, branchPages=%d\n",
		stat.Entries, stat.Depth, stat.LeafPages, stat.BranchPages)

	stat, _ = txn.Stat(smalldupDBI)
	fmt.Printf("smalldup stats: entries=%d, depth=%d, leafPages=%d, branchPages=%d\n",
		stat.Entries, stat.Depth, stat.LeafPages, stat.BranchPages)

	stat, _ = txn.Stat(plainDBI)
	fmt.Printf("plain stats: entries=%d, depth=%d, leafPages=%d, branchPages=%d\n",
		stat.Entries, stat.Depth, stat.LeafPages, stat.BranchPages)
}

// BenchmarkLargeDB runs benchmarks on the pre-generated large database
func BenchmarkLargeDB(b *testing.B) {
	dbPath := os.Getenv("LARGE_DB_PATH")
	if dbPath == "" {
		home, _ := os.UserHomeDir()
		dbPath = home + "/gdbx_large_bench.db"
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		b.Skip("Large DB not found at " + dbPath + ". Run: GENERATE_LARGE_DB=1 go test -v -run TestGenerateLargeDB -timeout 0")
	}

	b.Run("bigdup", func(b *testing.B) {
		benchmarkLargeDBTable(b, dbPath, "bigdup", true, bigDupKeys)
	})

	b.Run("smalldup", func(b *testing.B) {
		benchmarkLargeDBTable(b, dbPath, "smalldup", true, smallDupKeys)
	})

	b.Run("plain", func(b *testing.B) {
		benchmarkLargeDBTable(b, dbPath, "plain", false, plainKeys)
	})
}

// BenchmarkLargeDBWrite runs write operation benchmarks on the large database
// These use transactions that are aborted to avoid modifying the persistent DB
func BenchmarkLargeDBWrite(b *testing.B) {
	dbPath := os.Getenv("LARGE_DB_PATH")
	if dbPath == "" {
		home, _ := os.UserHomeDir()
		dbPath = home + "/gdbx_large_bench.db"
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		b.Skip("Large DB not found at " + dbPath + ". Run: GENERATE_LARGE_DB=1 go test -v -run TestGenerateLargeDB -timeout 0")
	}

	b.Run("bigdup", func(b *testing.B) {
		benchmarkLargeDBWriteTable(b, dbPath, "bigdup", true, bigDupKeys)
	})

	b.Run("smalldup", func(b *testing.B) {
		benchmarkLargeDBWriteTable(b, dbPath, "smalldup", true, smallDupKeys)
	})

	b.Run("plain", func(b *testing.B) {
		benchmarkLargeDBWriteTable(b, dbPath, "plain", false, plainKeys)
	})
}

func benchmarkLargeDBTable(b *testing.B, dbPath, tableName string, isDupsort bool, numKeys int) {
	// Open with gdbx
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	defer genv.Close()
	genv.SetMaxDBs(10)
	if err := genv.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644); err != nil {
		b.Fatal(err)
	}

	// Open with mdbx-go
	runtime.LockOSThread()
	menv, err := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer menv.Close()
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	if err := menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.Readonly, 0644); err != nil {
		b.Fatal(err)
	}
	runtime.UnlockOSThread()

	// Collect sample keys for random access tests
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)

	sampleSize := 10000
	if sampleSize > numKeys {
		sampleSize = numKeys
	}
	sampleKeys := make([][]byte, 0, sampleSize)
	sampleValues := make([][]byte, 0, sampleSize) // For GetBoth tests

	// Collect evenly distributed sample keys
	step := numKeys / sampleSize
	gcursor.Get(nil, nil, gdbx.First)
	for i := 0; i < sampleSize; i++ {
		for j := 0; j < step && i > 0; j++ {
			if isDupsort {
				gcursor.Get(nil, nil, gdbx.NextNoDup)
			} else {
				gcursor.Get(nil, nil, gdbx.Next)
			}
		}
		k, v, err := gcursor.Get(nil, nil, gdbx.GetCurrent)
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		sampleKeys = append(sampleKeys, keyCopy)
		if isDupsort {
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			sampleValues = append(sampleValues, valCopy)
		}
	}
	gcursor.Close()
	gtxn.Abort()

	b.Logf("Collected %d sample keys for %s", len(sampleKeys), tableName)

	// Sub-benchmarks
	b.Run("CursorIterate", func(b *testing.B) {
		benchLargeIterate(b, genv, menv, tableName)
	})

	b.Run("GetRandom", func(b *testing.B) {
		benchLargeGetRandom(b, genv, menv, tableName, sampleKeys)
	})

	b.Run("SetRandom", func(b *testing.B) {
		benchLargeSetRandom(b, genv, menv, tableName, sampleKeys)
	})

	if isDupsort {
		b.Run("NextNoDup", func(b *testing.B) {
			benchLargeNextNoDup(b, genv, menv, tableName)
		})

		b.Run("SetFirstDup", func(b *testing.B) {
			benchLargeSetFirstDup(b, genv, menv, tableName, sampleKeys)
		})

		b.Run("GetBoth", func(b *testing.B) {
			benchLargeGetBoth(b, genv, menv, tableName, sampleKeys, sampleValues)
		})

		b.Run("Count", func(b *testing.B) {
			benchLargeCount(b, genv, menv, tableName, sampleKeys)
		})
	}
}

func benchLargeIterate(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string) {
	// Warm up
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	count := 0
	gcursor.Get(nil, nil, gdbx.First)
	for count < 100000 {
		_, _, err := gcursor.Get(nil, nil, gdbx.Next)
		if err != nil {
			break
		}
		count++
	}
	gcursor.Close()
	gtxn.Abort()

	// Benchmark gdbx
	gtxn, _ = genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ = gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ = gtxn.OpenCursor(gdbi)
	defer gcursor.Close()
	defer gtxn.Abort()

	iterations := 1000000
	start := time.Now()
	gcursor.Get(nil, nil, gdbx.First)
	gcount := 0
	for gcount < iterations {
		_, _, err := gcursor.Get(nil, nil, gdbx.Next)
		if err != nil {
			gcursor.Get(nil, nil, gdbx.First)
		}
		gcount++
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()
	defer mtxn.Abort()

	start = time.Now()
	mcursor.Get(nil, nil, mdbxgo.First)
	mcount := 0
	for mcount < iterations {
		_, _, err := mcursor.Get(nil, nil, mdbxgo.Next)
		if err != nil {
			mcursor.Get(nil, nil, mdbxgo.First)
		}
		mcount++
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(gcount), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(mcount), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(gcount),
		float64(mdbxDur.Nanoseconds())/float64(mcount),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeGetRandom(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 100000

	// Benchmark gdbx
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	defer gtxn.Abort()

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn.Get(gdbi, keys[i%len(keys)])
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	defer mtxn.Abort()

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn.Get(mdbi, keys[i%len(keys)])
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeSetRandom(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 100000

	// Benchmark gdbx
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	defer gcursor.Close()
	defer gtxn.Abort()

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()
	defer mtxn.Abort()

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeNextNoDup(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string) {
	iterations := 100000

	// Benchmark gdbx
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	defer gcursor.Close()
	defer gtxn.Abort()

	// Warm up
	gcursor.Get(nil, nil, gdbx.First)
	for i := 0; i < 10000; i++ {
		_, _, err := gcursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			gcursor.Get(nil, nil, gdbx.First)
		}
	}

	start := time.Now()
	gcursor.Get(nil, nil, gdbx.First)
	gcount := 0
	for gcount < iterations {
		_, _, err := gcursor.Get(nil, nil, gdbx.NextNoDup)
		if err != nil {
			gcursor.Get(nil, nil, gdbx.First)
		}
		gcount++
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()
	defer mtxn.Abort()

	// Warm up
	mcursor.Get(nil, nil, mdbxgo.First)
	for i := 0; i < 10000; i++ {
		_, _, err := mcursor.Get(nil, nil, mdbxgo.NextNoDup)
		if err != nil {
			mcursor.Get(nil, nil, mdbxgo.First)
		}
	}

	start = time.Now()
	mcursor.Get(nil, nil, mdbxgo.First)
	mcount := 0
	for mcount < iterations {
		_, _, err := mcursor.Get(nil, nil, mdbxgo.NextNoDup)
		if err != nil {
			mcursor.Get(nil, nil, mdbxgo.First)
		}
		mcount++
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(gcount), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(mcount), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(gcount),
		float64(mdbxDur.Nanoseconds())/float64(mcount),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeSetFirstDup(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 100000

	// Benchmark gdbx
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	defer gcursor.Close()
	defer gtxn.Abort()

	// Warm up
	for i := 0; i < 1000; i++ {
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Get(nil, nil, gdbx.FirstDup)
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Get(nil, nil, gdbx.FirstDup)
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()
	defer mtxn.Abort()

	// Warm up
	for i := 0; i < 1000; i++ {
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Get(nil, nil, mdbxgo.FirstDup)
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Get(nil, nil, mdbxgo.FirstDup)
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeGetBoth(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string, keys, values [][]byte) {
	if len(keys) == 0 || len(values) == 0 {
		b.Skip("No keys/values")
	}

	iterations := 100000

	// Benchmark gdbx
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	defer gcursor.Close()
	defer gtxn.Abort()

	start := time.Now()
	for i := 0; i < iterations; i++ {
		idx := i % len(keys)
		gcursor.Get(keys[idx], values[idx], gdbx.GetBoth)
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()
	defer mtxn.Abort()

	start = time.Now()
	for i := 0; i < iterations; i++ {
		idx := i % len(keys)
		mcursor.Get(keys[idx], values[idx], mdbxgo.GetBoth)
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeCount(b *testing.B, genv *gdbx.Env, menv *mdbxgo.Env, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Benchmark gdbx
	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)
	defer gcursor.Close()
	defer gtxn.Abort()

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Count()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	mtxn, _ := menv.BeginTxn(nil, mdbxgo.Readonly)
	mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
	mcursor, _ := mtxn.OpenCursor(mdbi)
	defer mcursor.Close()
	defer mtxn.Abort()

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Count()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

// ============================================================================
// WRITE BENCHMARKS
// ============================================================================

func benchmarkLargeDBWriteTable(b *testing.B, dbPath, tableName string, isDupsort bool, numKeys int) {
	// Collect sample keys first using read-only access
	genv, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		b.Fatal(err)
	}
	genv.SetMaxDBs(10)
	if err := genv.Open(dbPath, gdbx.NoSubdir|gdbx.ReadOnly, 0644); err != nil {
		b.Fatal(err)
	}

	gtxn, _ := genv.BeginTxn(nil, gdbx.TxnReadOnly)
	gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
	gcursor, _ := gtxn.OpenCursor(gdbi)

	sampleSize := 10000
	if sampleSize > numKeys {
		sampleSize = numKeys
	}
	sampleKeys := make([][]byte, 0, sampleSize)
	sampleValues := make([][]byte, 0, sampleSize)

	step := numKeys / sampleSize
	gcursor.Get(nil, nil, gdbx.First)
	for i := 0; i < sampleSize; i++ {
		for j := 0; j < step && i > 0; j++ {
			if isDupsort {
				gcursor.Get(nil, nil, gdbx.NextNoDup)
			} else {
				gcursor.Get(nil, nil, gdbx.Next)
			}
		}
		k, v, err := gcursor.Get(nil, nil, gdbx.GetCurrent)
		if err != nil {
			break
		}
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		sampleKeys = append(sampleKeys, keyCopy)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		sampleValues = append(sampleValues, valCopy)
	}
	gcursor.Close()
	gtxn.Abort()
	genv.Close()

	b.Logf("Collected %d sample keys for write tests on %s", len(sampleKeys), tableName)

	// Sub-benchmarks for write operations
	b.Run("Put", func(b *testing.B) {
		benchLargePut(b, dbPath, tableName, sampleKeys)
	})

	b.Run("Del", func(b *testing.B) {
		benchLargeDel(b, dbPath, tableName, sampleKeys)
	})

	b.Run("CursorPut", func(b *testing.B) {
		benchLargeCursorPut(b, dbPath, tableName, sampleKeys)
	})

	b.Run("CursorDel", func(b *testing.B) {
		benchLargeCursorDel(b, dbPath, tableName, sampleKeys)
	})

	if isDupsort {
		b.Run("PutDup", func(b *testing.B) {
			benchLargePutDup(b, dbPath, tableName, sampleKeys)
		})

		b.Run("DelDup", func(b *testing.B) {
			benchLargeDelDup(b, dbPath, tableName, sampleKeys, sampleValues)
		})

		b.Run("CursorPutDup", func(b *testing.B) {
			benchLargeCursorPutDup(b, dbPath, tableName, sampleKeys)
		})

		b.Run("CursorDelDup", func(b *testing.B) {
			benchLargeCursorDelDup(b, dbPath, tableName, sampleKeys)
		})
	}
}

func benchLargePut(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Generate new values for puts
	newValues := make([][]byte, iterations)
	for i := range newValues {
		newValues[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(newValues[i][:8], uint64(i+1000000))
		rand.Read(newValues[i][8:])
	}

	// Benchmark gdbx - use aborted transactions
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gtxn.Put(gdbi, keys[i%len(keys)], newValues[i%len(newValues)], gdbx.Upsert)
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gtxn.Put(gdbi, keys[i%len(keys)], newValues[i%len(newValues)], gdbx.Upsert)
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mtxn.Put(mdbi, keys[i%len(keys)], newValues[i%len(newValues)], mdbxgo.Upsert)
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mtxn.Put(mdbi, keys[i%len(keys)], newValues[i%len(newValues)], mdbxgo.Upsert)
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeDel(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Benchmark gdbx
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gtxn.Del(gdbi, keys[i%len(keys)], nil)
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gtxn.Del(gdbi, keys[i%len(keys)], nil)
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mtxn.Del(mdbi, keys[i%len(keys)], nil)
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mtxn.Del(mdbi, keys[i%len(keys)], nil)
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeCursorPut(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Generate new values
	newValues := make([][]byte, iterations)
	for i := range newValues {
		newValues[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(newValues[i][:8], uint64(i+2000000))
		rand.Read(newValues[i][8:])
	}

	// Benchmark gdbx
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Put(keys[i%len(keys)], newValues[i%len(newValues)], gdbx.Upsert)
		gcursor.Close()
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Put(keys[i%len(keys)], newValues[i%len(newValues)], gdbx.Upsert)
		gcursor.Close()
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Put(keys[i%len(keys)], newValues[i%len(newValues)], mdbxgo.Upsert)
		mcursor.Close()
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Put(keys[i%len(keys)], newValues[i%len(newValues)], mdbxgo.Upsert)
		mcursor.Close()
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeCursorDel(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Benchmark gdbx
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Del(0)
		gcursor.Close()
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Del(0)
		gcursor.Close()
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Del(0)
		mcursor.Close()
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Del(0)
		mcursor.Close()
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

// DUPSORT-specific write benchmarks

func benchLargePutDup(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Generate new duplicate values (add new values to existing keys)
	newDupValues := make([][]byte, iterations)
	for i := range newDupValues {
		newDupValues[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(newDupValues[i][:8], uint64(i+5000000)) // High number to ensure unique
		rand.Read(newDupValues[i][8:])
	}

	// Benchmark gdbx
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gtxn.Put(gdbi, keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gtxn.Put(gdbi, keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mtxn.Put(mdbi, keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mtxn.Put(mdbi, keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeDelDup(b *testing.B, dbPath, tableName string, keys, values [][]byte) {
	if len(keys) == 0 || len(values) == 0 {
		b.Skip("No keys/values")
	}

	iterations := 10000

	// Benchmark gdbx - delete specific duplicate values
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		idx := i % len(keys)
		gtxn.Del(gdbi, keys[idx], values[idx])
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		idx := i % len(keys)
		gtxn.Del(gdbi, keys[idx], values[idx])
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		idx := i % len(keys)
		mtxn.Del(mdbi, keys[idx], values[idx])
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		idx := i % len(keys)
		mtxn.Del(mdbi, keys[idx], values[idx])
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeCursorPutDup(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Generate new duplicate values
	newDupValues := make([][]byte, iterations)
	for i := range newDupValues {
		newDupValues[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(newDupValues[i][:8], uint64(i+6000000))
		rand.Read(newDupValues[i][8:])
	}

	// Benchmark gdbx
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Put(keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		gcursor.Close()
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Put(keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		gcursor.Close()
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Put(keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		mcursor.Close()
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Put(keys[i%len(keys)], newDupValues[i%len(newDupValues)], 0)
		mcursor.Close()
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}

func benchLargeCursorDelDup(b *testing.B, dbPath, tableName string, keys [][]byte) {
	if len(keys) == 0 {
		b.Skip("No keys")
	}

	iterations := 10000

	// Benchmark gdbx - position to key and delete current duplicate
	genv, _ := gdbx.NewEnv(gdbx.Default)
	genv.SetMaxDBs(10)
	genv.Open(dbPath, gdbx.NoSubdir|gdbx.NoMetaSync, 0644)
	defer genv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Del(0) // Delete just the current dup value
		gcursor.Close()
		gtxn.Abort()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		gtxn, _ := genv.BeginTxn(nil, 0)
		gdbi, _ := gtxn.OpenDBISimple(tableName, 0)
		gcursor, _ := gtxn.OpenCursor(gdbi)
		gcursor.Get(keys[i%len(keys)], nil, gdbx.Set)
		gcursor.Del(0)
		gcursor.Close()
		gtxn.Abort()
	}
	gdbxDur := time.Since(start)

	// Benchmark mdbx-go
	runtime.LockOSThread()
	menv, _ := mdbxgo.NewEnv(mdbxgo.Label("bench"))
	menv.SetOption(mdbxgo.OptMaxDB, 10)
	menv.Open(dbPath, mdbxgo.NoSubdir|mdbxgo.NoMetaSync, 0644)
	defer menv.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Del(0)
		mcursor.Close()
		mtxn.Abort()
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		mtxn, _ := menv.BeginTxn(nil, 0)
		mdbi, _ := mtxn.OpenDBI(tableName, 0, nil, nil)
		mcursor, _ := mtxn.OpenCursor(mdbi)
		mcursor.Get(keys[i%len(keys)], nil, mdbxgo.Set)
		mcursor.Del(0)
		mcursor.Close()
		mtxn.Abort()
	}
	mdbxDur := time.Since(start)
	runtime.UnlockOSThread()

	b.ReportMetric(float64(gdbxDur.Nanoseconds())/float64(iterations), "ns/op-gdbx")
	b.ReportMetric(float64(mdbxDur.Nanoseconds())/float64(iterations), "ns/op-mdbx")
	b.ReportMetric(float64(gdbxDur)/float64(mdbxDur), "ratio")
	b.Logf("gdbx=%.0fns, mdbx=%.0fns, ratio=%.2fx",
		float64(gdbxDur.Nanoseconds())/float64(iterations),
		float64(mdbxDur.Nanoseconds())/float64(iterations),
		float64(gdbxDur)/float64(mdbxDur))
}
