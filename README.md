# gdbx

A pure Go implementation of [MDBX](https://gitflic.ru/project/erthink/libmdbx), the high-performance embedded transactional key-value database. File-format compatible with libmdbx.

## Performance

Benchmarks comparing gdbx against [mdbx-go](https://github.com/erigontech/mdbx-go) (CGO wrapper) on AMD Ryzen 5 3600.

### Write Operations (per-operation, single transaction)

**10K entries:**
- SeqPut: gdbx 145ns vs mdbx 240ns (1.7x faster)
- RandPut: gdbx 139ns vs mdbx 241ns (1.7x faster)
- CursorPut: gdbx 131ns vs mdbx 195ns (1.5x faster)

**100K entries:**
- SeqPut: gdbx 192ns vs mdbx 271ns (1.4x faster)
- RandPut: gdbx 173ns vs mdbx 276ns (1.6x faster)
- CursorPut: gdbx 149ns vs mdbx 201ns (1.3x faster)

**1M entries:**
- SeqPut: gdbx 174ns vs mdbx 298ns (1.7x faster)
- RandPut: gdbx 185ns vs mdbx 286ns (1.5x faster)
- CursorPut: gdbx 172ns vs mdbx 198ns (1.2x faster)

### DBI/Transaction Operations

- OpenDBI: gdbx 30ns vs mdbx 250ns (8.3x faster)
- BeginTxn (read-only): gdbx 135ns vs mdbx 321ns (2.4x faster)
- BeginTxn (read-write): gdbx 2494ns vs mdbx 297ns (mdbx faster, file locking overhead)

### Memory

- Zero allocations on Put operations
- Zero allocations on read-only transactions
- Zero allocations on OpenDBI

## Features

- 100% pure Go, no CGO
- File-format compatible with libmdbx
- ACID transactions with MVCC
- Memory-mapped I/O
- B+ tree storage
- DupSort tables
- Nested transactions

## Implementation Differences vs libmdbx

gdbx is file-format compatible with libmdbx but the implementation differs:

### Locking

- **libmdbx**: Configurable via `MDBX_LOCKING` build option. Supports SystemV IPC semaphores (default), POSIX shared mutexes, POSIX-2008 robust mutexes, or Win32 file locking. Lock state stored in shared memory (lock file) with complex handoff protocols.
- **gdbx**: Uses file-based flock() for writer lock. Simpler but higher syscall overhead per write transaction.
- **Rationale**: flock() is available on all Unix systems and Windows (via syscall), requires no platform-specific code paths, and is simple to reason about. The ~2μs overhead per write transaction is acceptable since actual write work dominates. Avoiding IPC semaphores eliminates cleanup issues on process crash.

### Reader Registration

- **libmdbx**: Lock-free reader slot acquisition using atomic CAS with PID/TID tracking. Supports reader "parking" for long transactions.
- **gdbx**: Similar slot-based tracking with atomic operations, but uses LIFO freelist for O(1) slot acquisition. No parking support.
- **Rationale**: LIFO freelist gives O(1) slot acquisition in the common case (reusing recently-freed slots), which is cache-friendly. Parking adds complexity for a rare use case - most applications don't hold read transactions for extended periods.

### Page Management

- **libmdbx**: Complex spill/unspill mechanism to handle dirty pages exceeding RAM. Pages can be temporarily written to disk and reloaded.
- **gdbx**: Simpler dirty page tracking with flat array (65K pages) + overflow map. No spilling - all dirty pages kept in memory until commit.
- **Rationale**: Spilling adds significant complexity for edge cases. Most write transactions modify far fewer than 65K pages (256MB at 4KB page size). The flat array gives O(1) lookup with no allocations. Applications needing huge transactions can increase RAM or batch writes.

### Garbage Collection

- **libmdbx**: LIFO page reclamation with "backlog" management. Tracks retired pages per transaction with complex coalescing.
- **gdbx**: LIFO reclamation via FreeDBI. Freed pages added to transaction's free list, written to GC tree on commit.
- **Rationale**: Both use LIFO for cache efficiency (recently-freed pages are hot). gdbx skips backlog tracking since Go's GC handles memory pressure differently than C. Simpler code with same disk format.

### Copy-on-Write

- **libmdbx**: Pages marked as "frozen" when read transaction references them. Supports "weak" pages for optimization.
- **gdbx**: Simpler COW - dirty pages allocated fresh, old pages freed only when no reader references them. Tracks via reader slot txnid.
- **Rationale**: Frozen/weak page tracking optimizes memory in long-running mixed workloads but adds bookkeeping. gdbx relies on the oldest-reader txnid to know when pages are safe to free - same correctness, less state.

### Memory Mapping

- **libmdbx**: Dynamic geometry adjustment with automatic mmap resize. Supports both read-only and writable mmap modes.
- **gdbx**: Pre-extended mmap with manual geometry. WriteMap mode uses writable mmap, otherwise pages copied on write.
- **Rationale**: Dynamic mmap resize requires careful coordination between processes. Pre-extending to expected size is simpler and avoids remapping during writes. Most deployments know their size requirements upfront.

### Search Optimization

- **libmdbx**: Binary search with various optimizations in C.
- **gdbx**: Assembly-optimized binary search for 8-byte keys (amd64). Full search loop in assembly avoids Go/asm boundary overhead.
- **Rationale**: Go function calls have overhead that C doesn't. For the hot path (key comparison during search), keeping the entire binary search loop in assembly eliminates repeated Go/asm transitions. 8-byte keys are common (uint64 IDs) and can be compared as single BSWAP+CMP.

### Nested Transactions

- **libmdbx**: Full nested transaction support with parent page shadowing and complex abort handling.
- **gdbx**: Nested transaction support with page COW from parent. Simpler implementation.
- **Rationale**: Both support nested transactions for the same use cases (partial rollback). gdbx uses straightforward COW from parent pages rather than shadow tracking - easier to verify correctness.

### What's Identical

- Page format (20-byte header, entry offsets, node layout)
- Meta page triple rotation for atomic commits
- B+ tree structure and algorithms
- DupSort sub-page/sub-tree handling
- Overflow page format for large values
- Lock file format and reader slot layout

## License

MIT
