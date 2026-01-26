package tests

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Giulio2002/gdbx"
)

// TestConcurrentReadWrite stress tests concurrent read and write transactions.
// This test specifically exercises the race conditions between:
// - Read transactions caching mmap data
// - Write transactions remapping the database file
// - Multiple goroutines accessing the database simultaneously
func TestConcurrentReadWrite(t *testing.T) {
	path := t.TempDir() + "/concurrent_test.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	// Set a small initial size to force remaps during writes
	env.SetGeometry(
		4096*10,        // sizeLower: 40KB min
		4096*10,        // sizeNow: 40KB initial
		1024*1024*1024, // sizeUpper: 1GB max
		4096*10,        // growthStep: grow in small increments
		4096*10,        // shrinkThreshold
		0,              // pageSize: use default
	)

	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Initial setup
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	// Write some initial data
	key := make([]byte, 8)
	val := make([]byte, 64)
	for i := 0; i < 100; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Concurrent test parameters - reduce for race detector
	numReaders := 10
	numWriters := 2
	duration := 3 * time.Second
	if raceEnabled {
		numReaders = 4
		numWriters = 1
		duration = 500 * time.Millisecond
	}

	var wg sync.WaitGroup
	var readOps, writeOps, readErrors, writeErrors atomic.Int64
	done := make(chan struct{})

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			key := make([]byte, 8)

			for {
				select {
				case <-done:
					return
				default:
				}

				// Start read transaction
				rtxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
				if err != nil {
					readErrors.Add(1)
					continue
				}

				rdbi, err := rtxn.OpenDBISimple("test", 0)
				if err != nil {
					rtxn.Abort()
					readErrors.Add(1)
					continue
				}

				// Read multiple keys
				for i := 0; i < 50; i++ {
					binary.BigEndian.PutUint64(key, uint64(i%100))
					_, err := rtxn.Get(rdbi, key)
					if err != nil && err != gdbx.ErrNotFoundError {
						readErrors.Add(1)
					} else {
						readOps.Add(1)
					}
				}

				rtxn.Abort()

				// Small sleep to prevent tight loop
				time.Sleep(time.Microsecond * 10)
			}
		}(r)
	}

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			key := make([]byte, 8)
			val := make([]byte, 256) // Larger value to force page growth

			counter := uint64(100 + writerID*10000)
			for {
				select {
				case <-done:
					return
				default:
				}

				// Start write transaction
				wtxn, err := env.BeginTxn(nil, 0)
				if err != nil {
					writeErrors.Add(1)
					time.Sleep(time.Millisecond)
					continue
				}

				wdbi, err := wtxn.OpenDBISimple("test", 0)
				if err != nil {
					wtxn.Abort()
					writeErrors.Add(1)
					continue
				}

				// Write multiple keys
				for i := 0; i < 20; i++ {
					binary.BigEndian.PutUint64(key, counter)
					binary.BigEndian.PutUint64(val, counter*10)
					if err := wtxn.Put(wdbi, key, val, 0); err != nil {
						writeErrors.Add(1)
					} else {
						writeOps.Add(1)
					}
					counter++
				}

				if _, err := wtxn.Commit(); err != nil {
					writeErrors.Add(1)
				}

				// Small sleep between writes
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Run for duration
	time.Sleep(duration)
	close(done)
	wg.Wait()

	t.Logf("Concurrent test completed: reads=%d writes=%d readErrors=%d writeErrors=%d",
		readOps.Load(), writeOps.Load(), readErrors.Load(), writeErrors.Load())

	// Verify no serious errors occurred
	if readErrors.Load() > readOps.Load()/100 { // Allow 1% error rate
		t.Errorf("Too many read errors: %d", readErrors.Load())
	}
	if writeErrors.Load() > writeOps.Load()/100 {
		t.Errorf("Too many write errors: %d", writeErrors.Load())
	}
}

// TestConcurrentCursorIteration tests multiple cursors iterating concurrently
// while writes are happening.
func TestConcurrentCursorIteration(t *testing.T) {
	path := t.TempDir() + "/concurrent_cursor_test.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Initial setup with DUPSORT
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create|gdbx.DupSort)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	val := make([]byte, 32)
	for i := 0; i < 100; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		for j := 0; j < 10; j++ {
			binary.BigEndian.PutUint64(val, uint64(j))
			if err := txn.Put(dbi, key, val, 0); err != nil {
				txn.Abort()
				t.Fatal(err)
			}
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	numIterators := 8
	numWriters := 2
	duration := 3 * time.Second

	var wg sync.WaitGroup
	var iterOps, writeOps, iterErrors, writeErrors atomic.Int64
	done := make(chan struct{})

	// Start cursor iterators
	for r := 0; r < numIterators; r++ {
		wg.Add(1)
		go func(iterID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				rtxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
				if err != nil {
					iterErrors.Add(1)
					continue
				}

				rdbi, err := rtxn.OpenDBISimple("test", 0)
				if err != nil {
					rtxn.Abort()
					iterErrors.Add(1)
					continue
				}

				cursor, err := rtxn.OpenCursor(rdbi)
				if err != nil {
					rtxn.Abort()
					iterErrors.Add(1)
					continue
				}

				// Iterate through all entries
				count := 0
				k, _, err := cursor.Get(nil, nil, gdbx.First)
				for err == nil {
					count++
					iterOps.Add(1)

					// Try next dup
					_, _, err = cursor.Get(nil, nil, gdbx.NextDup)
					if err == gdbx.ErrNotFoundError {
						// Move to next key
						k, _, err = cursor.Get(nil, nil, gdbx.Next)
						if err == gdbx.ErrNotFoundError {
							break
						}
					}

					if err != nil && err != gdbx.ErrNotFoundError {
						iterErrors.Add(1)
						break
					}

					_ = k // Use k to avoid warning
				}

				cursor.Close()
				rtxn.Abort()

				time.Sleep(time.Microsecond * 100)
			}
		}(r)
	}

	// Start writers that add more duplicates
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			key := make([]byte, 8)
			val := make([]byte, 32)
			counter := uint64(1000 + writerID*100000)

			for {
				select {
				case <-done:
					return
				default:
				}

				wtxn, err := env.BeginTxn(nil, 0)
				if err != nil {
					writeErrors.Add(1)
					time.Sleep(time.Millisecond)
					continue
				}

				wdbi, err := wtxn.OpenDBISimple("test", 0)
				if err != nil {
					wtxn.Abort()
					writeErrors.Add(1)
					continue
				}

				// Add more duplicates to existing keys
				for i := 0; i < 10; i++ {
					binary.BigEndian.PutUint64(key, uint64(i%100))
					binary.BigEndian.PutUint64(val, counter)
					if err := wtxn.Put(wdbi, key, val, 0); err != nil {
						writeErrors.Add(1)
					} else {
						writeOps.Add(1)
					}
					counter++
				}

				if _, err := wtxn.Commit(); err != nil {
					writeErrors.Add(1)
				}

				time.Sleep(time.Millisecond * 5)
			}
		}(w)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	t.Logf("Cursor iteration test completed: iterations=%d writes=%d iterErrors=%d writeErrors=%d",
		iterOps.Load(), writeOps.Load(), iterErrors.Load(), writeErrors.Load())

	if iterErrors.Load() > iterOps.Load()/100 {
		t.Errorf("Too many iteration errors: %d", iterErrors.Load())
	}
}

// TestRapidOpenClose tests rapidly opening and closing read transactions
// while writes are happening to stress test the mmap handling.
func TestRapidOpenClose(t *testing.T) {
	path := t.TempDir() + "/rapid_test.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(10)
	// Small initial size to trigger frequent remaps
	env.SetGeometry(
		4096*4,         // sizeLower
		4096*4,         // sizeNow
		1024*1024*1024, // sizeUpper
		4096*4,         // growthStep
		4096*4,         // shrinkThreshold
		0,              // pageSize
	)

	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Initial data
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	dbi, err := txn.OpenDBISimple("test", gdbx.Create)
	if err != nil {
		txn.Abort()
		t.Fatal(err)
	}

	key := make([]byte, 8)
	val := make([]byte, 32)
	for i := 0; i < 10; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(val, uint64(i*100))
		if err := txn.Put(dbi, key, val, 0); err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	numFastReaders := 20
	numSlowReaders := 5
	numWriters := 2
	duration := 3 * time.Second

	var wg sync.WaitGroup
	var fastReads, slowReads, writes atomic.Int64
	var errors atomic.Int64
	done := make(chan struct{})

	// Fast readers - open, read one key, close immediately
	for r := 0; r < numFastReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := make([]byte, 8)

			for {
				select {
				case <-done:
					return
				default:
				}

				rtxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
				if err != nil {
					errors.Add(1)
					continue
				}

				rdbi, err := rtxn.OpenDBISimple("test", 0)
				if err != nil {
					rtxn.Abort()
					errors.Add(1)
					continue
				}

				binary.BigEndian.PutUint64(key, uint64(0))
				_, err = rtxn.Get(rdbi, key)
				if err != nil && err != gdbx.ErrNotFoundError {
					errors.Add(1)
				} else {
					fastReads.Add(1)
				}

				rtxn.Abort()
			}
		}()
	}

	// Slow readers - hold transaction open for a while
	for r := 0; r < numSlowReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := make([]byte, 8)

			for {
				select {
				case <-done:
					return
				default:
				}

				rtxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
				if err != nil {
					errors.Add(1)
					continue
				}

				rdbi, err := rtxn.OpenDBISimple("test", 0)
				if err != nil {
					rtxn.Abort()
					errors.Add(1)
					continue
				}

				// Read multiple keys while holding transaction
				for i := 0; i < 10; i++ {
					binary.BigEndian.PutUint64(key, uint64(i%10))
					_, err = rtxn.Get(rdbi, key)
					if err != nil && err != gdbx.ErrNotFoundError {
						errors.Add(1)
					} else {
						slowReads.Add(1)
					}
					time.Sleep(time.Millisecond) // Hold txn open
				}

				rtxn.Abort()
			}
		}()
	}

	// Writers that force database growth
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			key := make([]byte, 8)
			val := make([]byte, 512) // Larger values to force growth

			counter := uint64(100 + writerID*100000)
			for {
				select {
				case <-done:
					return
				default:
				}

				wtxn, err := env.BeginTxn(nil, 0)
				if err != nil {
					errors.Add(1)
					time.Sleep(time.Millisecond)
					continue
				}

				wdbi, err := wtxn.OpenDBISimple("test", 0)
				if err != nil {
					wtxn.Abort()
					errors.Add(1)
					continue
				}

				for i := 0; i < 50; i++ {
					binary.BigEndian.PutUint64(key, counter)
					binary.BigEndian.PutUint64(val, counter)
					if err := wtxn.Put(wdbi, key, val, 0); err != nil {
						errors.Add(1)
						break
					}
					writes.Add(1)
					counter++
				}

				if _, err := wtxn.Commit(); err != nil {
					errors.Add(1)
				}

				time.Sleep(time.Millisecond * 2)
			}
		}(w)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	t.Logf("Rapid open/close test: fastReads=%d slowReads=%d writes=%d errors=%d",
		fastReads.Load(), slowReads.Load(), writes.Load(), errors.Load())

	totalOps := fastReads.Load() + slowReads.Load() + writes.Load()
	if errors.Load() > totalOps/100 {
		t.Errorf("Too many errors: %d (total ops: %d)", errors.Load(), totalOps)
	}
}

// TestConcurrentDBIOpen tests concurrent opening of the same DBI from
// multiple transactions.
func TestConcurrentDBIOpen(t *testing.T) {
	path := t.TempDir() + "/concurrent_dbi_test.db"

	env, err := gdbx.NewEnv(gdbx.Default)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	env.SetMaxDBs(50)
	if err := env.Open(path, gdbx.NoSubdir|gdbx.NoMetaSync, 0644); err != nil {
		t.Fatal(err)
	}

	// Create some named databases
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		name := string([]byte{'d', 'b', byte('0' + i)})
		_, err := txn.OpenDBISimple(name, gdbx.Create)
		if err != nil {
			txn.Abort()
			t.Fatal(err)
		}
	}

	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	numGoroutines := 20
	duration := 2 * time.Second

	var wg sync.WaitGroup
	var opens, errors atomic.Int64
	done := make(chan struct{})

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				rtxn, err := env.BeginTxn(nil, gdbx.TxnReadOnly)
				if err != nil {
					errors.Add(1)
					continue
				}

				// Open multiple DBIs
				for i := 0; i < 10; i++ {
					name := string([]byte{'d', 'b', byte('0' + i)})
					_, err := rtxn.OpenDBISimple(name, 0)
					if err != nil {
						errors.Add(1)
					} else {
						opens.Add(1)
					}
				}

				rtxn.Abort()
				time.Sleep(time.Microsecond * 100)
			}
		}(g)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	t.Logf("Concurrent DBI open test: opens=%d errors=%d", opens.Load(), errors.Load())

	if errors.Load() > opens.Load()/100 {
		t.Errorf("Too many errors: %d", errors.Load())
	}
}
