# gdbx

A pure Go implementation of [MDBX](https://gitflic.ru/project/erthink/libmdbx), the high-performance embedded transactional key-value database. File-format compatible with libmdbx.

## Performance

Benchmarks comparing gdbx against [mdbx-go](https://github.com/erigontech/mdbx-go) (CGO wrapper), BoltDB, and RocksDB on AMD Ryzen 5 3600.

### 8-byte Keys (uint64)

Common case for database IDs. gdbx uses assembly-optimized binary search with BSWAP+CMP.

#### Write Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx |
|-----------|---------|------|------|--------|---------|---------|
| SeqPut | 10K | **134** | 242 | 837 | 2121 | 1.8x faster |
| RandPut | 10K | **130** | 243 | 837 | 1674 | 1.9x faster |
| CursorPut | 10K | **129** | 189 | 848 | 2081 | 1.5x faster |
| SeqPut | 100K | **156** | 275 | 812 | 2370 | 1.8x faster |
| RandPut | 100K | **155** | 267 | 871 | 1895 | 1.7x faster |
| CursorPut | 100K | **148** | 186 | 852 | 1747 | 1.3x faster |
| SeqPut | 1M | **184** | 295 | 745 | 1089 | 1.6x faster |
| RandPut | 1M | **164** | 294 | 770 | 1392 | 1.8x faster |
| CursorPut | 1M | **151** | 189 | 822 | 1307 | 1.3x faster |

#### Read Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx |
|-----------|---------|------|------|--------|---------|---------|
| SeqRead | 10K | **32** | 127 | 13 | 697 | 4.0x faster |
| RandGet | 10K | **96** | 219 | 822 | 2343 | 2.3x faster |
| RandSeek | 10K | **109** | 213 | 503 | 2382 | 2.0x faster |
| SeqRead | 100K | **32** | 123 | 21 | 1194 | 3.8x faster |
| RandGet | 100K | **121** | 261 | 992 | 2337 | 2.2x faster |
| RandSeek | 100K | **138** | 214 | 630 | 3653 | 1.6x faster |
| SeqRead | 1M | **36** | 130 | 22 | 268 | 3.6x faster |
| RandGet | 1M | **148** | 262 | 1103 | 2155 | 1.8x faster |
| RandSeek | 1M | **143** | 220 | 670 | 1231 | 1.5x faster |

### 64-byte Keys

Longer keys use SSE2-optimized binary search comparing 16 bytes at a time.

#### Write Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx |
|-----------|---------|------|------|--------|---------|---------|
| SeqPut | 10K | **215** | 265 | 623 | 2074 | 1.2x faster |
| RandPut | 10K | **198** | 262 | 577 | 2067 | 1.3x faster |
| SeqPut | 100K | **275** | 322 | 706 | 1516 | 1.2x faster |
| RandPut | 100K | **315** | 333 | 685 | 1410 | 1.1x faster |
| SeqPut | 1M | **350** | 360 | 905 | 1071 | 1.0x (equal) |
| RandPut | 1M | **315** | 364 | 923 | 1011 | 1.2x faster |

#### Read Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx |
|-----------|---------|------|------|--------|---------|---------|
| SeqRead | 10K | **32** | 125 | 24 | 174 | 3.9x faster |
| RandGet | 10K | **120** | 256 | 925 | 2587 | 2.1x faster |
| RandSeek | 10K | **140** | 205 | 549 | 1074 | 1.5x faster |
| SeqRead | 100K | **34** | 131 | 19 | 337 | 3.9x faster |
| RandGet | 100K | **162** | 258 | 1042 | 3501 | 1.6x faster |
| RandSeek | 100K | **174** | 230 | 745 | 2065 | 1.3x faster |
| SeqRead | 1M | **41** | 135 | 18 | 478 | 3.3x faster |
| RandGet | 1M | **185** | 290 | 940 | 7338 | 1.6x faster |
| RandSeek | 1M | **233** | 221 | 623 | 4964 | 0.9x (mdbx faster) |

### Big Values (8KB)

Large values use zero-copy reads (direct mmap slice) and in-place overflow page updates.

#### Write Operations (ns/op, MB/s)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB |
|-----------|---------|------|------|--------|---------|
| SeqPut | 100 | 295ns (27.8 GB/s) | 329ns (24.9 GB/s) | 552ns (14.8 GB/s) | 3491ns (2.3 GB/s) |
| RandPut | 100 | 267ns (30.7 GB/s) | 327ns (25.1 GB/s) | 536ns (15.3 GB/s) | 2433ns (3.4 GB/s) |
| SeqPut | 1K | 400ns (20.5 GB/s) | 387ns (21.1 GB/s) | 809ns (10.1 GB/s) | 5098ns (1.6 GB/s) |
| RandPut | 1K | 362ns (22.6 GB/s) | 397ns (20.6 GB/s) | 713ns (11.5 GB/s) | 4770ns (1.7 GB/s) |
| SeqPut | 10K | 907ns (9.0 GB/s) | 759ns (10.8 GB/s) | 799ns (10.3 GB/s) | 27697ns (0.3 GB/s) |
| RandPut | 10K | 828ns (9.9 GB/s) | 715ns (11.5 GB/s) | 824ns (9.9 GB/s) | 26402ns (0.3 GB/s) |

#### Read Operations (ns/op, MB/s)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB |
|-----------|---------|------|------|--------|---------|
| SeqRead | 100 | **44ns (185 GB/s)** | 139ns (59 GB/s) | 22ns (369 GB/s) | 1511ns (5.4 GB/s) |
| RandGet | 100 | **51ns (162 GB/s)** | 194ns (42 GB/s) | 424ns (19 GB/s) | 1733ns (4.7 GB/s) |
| SeqRead | 1K | **43ns (191 GB/s)** | 144ns (57 GB/s) | 74ns (110 GB/s) | 40450ns (0.2 GB/s) |
| RandGet | 1K | **98ns (84 GB/s)** | 221ns (37 GB/s) | 921ns (8.9 GB/s) | 2066ns (4.0 GB/s) |
| SeqRead | 10K | **42ns (196 GB/s)** | 234ns (35 GB/s) | 122ns (67 GB/s) | 8983ns (0.9 GB/s) |
| RandGet | 10K | **102ns (80 GB/s)** | 331ns (25 GB/s) | 1077ns (7.6 GB/s) | 5440ns (1.5 GB/s) |

### DBI/Transaction Operations

| Operation | gdbx | mdbx | vs mdbx |
|-----------|------|------|---------|
| OpenDBI | 30ns | 250ns | 8.3x faster |
| BeginTxn (read-only) | 135ns | 321ns | 2.4x faster |
| BeginTxn (read-write) | 2494ns | 297ns | mdbx faster* |

*gdbx uses file-based flock() which has syscall overhead; mdbx uses shared memory locks.

### Memory

- Zero allocations on all Put operations
- Zero allocations on read-only transactions
- Zero allocations on OpenDBI
- Zero allocations on cursor operations

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
- **gdbx**: Assembly-optimized binary search (amd64). 8-byte keys use BSWAP+CMP for single-instruction comparison. Longer keys use SSE2 SIMD comparing 16 bytes at a time with PCMPEQB+PMOVMSKB. Full search loop in assembly avoids Go/asm boundary overhead.
- **Rationale**: Go function calls have overhead that C doesn't. For the hot path (key comparison during search), keeping the entire binary search loop in assembly eliminates repeated Go/asm transitions. 8-byte keys are common (uint64 IDs) and can be compared in a single operation. SSE2 makes longer keys (64+ bytes) competitive with mdbx.

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
