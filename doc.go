// Package gdbx is a pure Go implementation of MDBX, a high-performance
// embedded transactional key-value database.
//
// gdbx is file-format compatible with libmdbx, allowing existing MDBX
// databases to be opened and manipulated by this Go implementation.
//
// Key features:
//   - B+ tree data structure for efficient key-value storage
//   - MVCC (Multi-Version Concurrency Control) for concurrent reads
//   - Single writer, multiple readers concurrency model
//   - Memory-mapped I/O for high performance
//   - ACID transactions with crash recovery
//   - Nested transaction infrastructure (parent page delegation)
//
// Basic usage:
//
//	env, err := gdbx.Create()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer env.Close()
//
//	err = env.Open("/path/to/db", gdbx.NoSubdir, 0644)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Begin a write transaction
//	txn, err := env.BeginTxn(nil, 0)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Open the default database
//	dbi, err := txn.OpenDBI("", gdbx.Create)
//	if err != nil {
//	    txn.Abort()
//	    log.Fatal(err)
//	}
//
//	// Put a key-value pair
//	err = txn.Put(dbi, []byte("key"), []byte("value"), 0)
//	if err != nil {
//	    txn.Abort()
//	    log.Fatal(err)
//	}
//
//	_, _, err = txn.Commit()
//	if err != nil {
//	    log.Fatal(err)
//	}
package gdbx
