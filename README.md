# gdbx

A pure Go implementation of [MDBX](https://gitflic.ru/project/erthink/libmdbx), the high-performance embedded transactional key-value database. File-format compatible with libmdbx.

## Performance

Benchmarks comparing gdbx against [mdbx-go](https://github.com/erigontech/mdbx-go) (CGO wrapper), BoltDB, and RocksDB on AMD Ryzen 5 3600.

### 8-byte Keys (uint64)

Common case for database IDs. gdbx uses assembly-optimized binary search with BSWAP+CMP.

#### Write Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqPut | 10K | **142** | 228 | 870 | 2121 | 1.6x | 6.1x | 14.9x |
| RandPut | 10K | **130** | 243 | 837 | 1674 | 1.9x | 6.4x | 12.9x |
| CursorPut | 10K | **129** | 189 | 848 | 2081 | 1.5x | 6.6x | 16.1x |
| SeqPut | 100K | **156** | 275 | 812 | 2370 | 1.8x | 5.2x | 15.2x |
| RandPut | 100K | **155** | 267 | 871 | 1895 | 1.7x | 5.6x | 12.2x |
| CursorPut | 100K | **148** | 186 | 852 | 1747 | 1.3x | 5.8x | 11.8x |
| SeqPut | 1M | **184** | 295 | 745 | 1089 | 1.6x | 4.0x | 5.9x |
| RandPut | 1M | **164** | 294 | 770 | 1392 | 1.8x | 4.7x | 8.5x |
| CursorPut | 1M | **151** | 189 | 822 | 1307 | 1.3x | 5.4x | 8.7x |

#### Read Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqRead | 10K | 32 | 124 | **12** | 618 | 3.9x | 0.4x | 19.3x |
| RandGet | 10K | **95** | 218 | 824 | 2208 | 2.3x | 8.7x | 23.2x |
| RandSeek | 10K | **109** | 213 | 503 | 2382 | 2.0x | 4.6x | 21.9x |
| SeqRead | 100K | 32 | 123 | **21** | 1194 | 3.8x | 0.7x | 37.3x |
| RandGet | 100K | **121** | 261 | 992 | 2337 | 2.2x | 8.2x | 19.3x |
| RandSeek | 100K | **138** | 214 | 630 | 3653 | 1.6x | 4.6x | 26.5x |
| SeqRead | 1M | 36 | 130 | **22** | 268 | 3.6x | 0.6x | 7.4x |
| RandGet | 1M | **148** | 262 | 1103 | 2155 | 1.8x | 7.5x | 14.6x |
| RandSeek | 1M | **143** | 220 | 670 | 1231 | 1.5x | 4.7x | 8.6x |

*Note: BoltDB wins sequential reads due to simpler cursor iteration, but gdbx dominates random access.*

### 64-byte Keys

Longer keys use SSE2-optimized binary search comparing 16 bytes at a time.

#### Write Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqPut | 10K | **215** | 265 | 623 | 2074 | 1.2x | 2.9x | 9.6x |
| RandPut | 10K | **198** | 262 | 577 | 2067 | 1.3x | 2.9x | 10.4x |
| SeqPut | 100K | **275** | 322 | 706 | 1516 | 1.2x | 2.6x | 5.5x |
| RandPut | 100K | **315** | 333 | 685 | 1410 | 1.1x | 2.2x | 4.5x |
| SeqPut | 1M | **350** | 360 | 905 | 1071 | 1.0x | 2.6x | 3.1x |
| RandPut | 1M | **315** | 364 | 923 | 1011 | 1.2x | 2.9x | 3.2x |

#### Read Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqRead | 10K | 32 | 125 | **24** | 174 | 3.9x | 0.8x | 5.4x |
| RandGet | 10K | **120** | 256 | 925 | 2587 | 2.1x | 7.7x | 21.6x |
| RandSeek | 10K | **140** | 205 | 549 | 1074 | 1.5x | 3.9x | 7.7x |
| SeqRead | 100K | 34 | 131 | **19** | 337 | 3.9x | 0.6x | 9.9x |
| RandGet | 100K | **162** | 258 | 1042 | 3501 | 1.6x | 6.4x | 21.6x |
| RandSeek | 100K | **174** | 230 | 745 | 2065 | 1.3x | 4.3x | 11.9x |
| SeqRead | 1M | 41 | 135 | **18** | 478 | 3.3x | 0.4x | 11.7x |
| RandGet | 1M | **185** | 290 | 940 | 7338 | 1.6x | 5.1x | 39.7x |
| RandSeek | 1M | **199** | 220 | 623 | 4964 | 1.1x | 3.1x | 24.9x |

### Big Values (8KB)

Large values use zero-copy reads (direct mmap slice) and in-place overflow page updates.

#### Write Operations (ns/op)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqPut | 100 | **295** | 329 | 552 | 3491 | 1.1x | 1.9x | 11.8x |
| RandPut | 100 | **267** | 327 | 536 | 2433 | 1.2x | 2.0x | 9.1x |
| SeqPut | 1K | **357** | 419 | 809 | 5098 | 1.2x | 2.3x | 14.3x |
| RandPut | 1K | **324** | 456 | 713 | 4770 | 1.4x | 2.2x | 14.7x |
| SeqPut | 10K | 907 | **759** | 799 | 27697 | 0.8x | 0.9x | 30.5x |
| RandPut | 10K | 828 | **715** | 824 | 26402 | 0.9x | 1.0x | 31.9x |

#### Read Operations (ns/op)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqRead | 100 | 44 | 139 | **22** | 1511 | 3.2x | 0.5x | 34.3x |
| RandGet | 100 | **51** | 194 | 424 | 1733 | 3.8x | 8.3x | 34.0x |
| SeqRead | 1K | **43** | 144 | 74 | 40450 | 3.3x | 1.7x | 941x |
| RandGet | 1K | **98** | 221 | 921 | 2066 | 2.3x | 9.4x | 21.1x |
| SeqRead | 10K | **42** | 234 | 122 | 8983 | 5.6x | 2.9x | 214x |
| RandGet | 10K | **102** | 331 | 1077 | 5440 | 3.2x | 10.6x | 53.3x |

*Big value reads use zero-copy (direct mmap slice), achieving 80-196 GB/s throughput.*

### DBI/Transaction Operations

| Operation | gdbx | mdbx | vs mdbx |
|-----------|------|------|---------|
| OpenDBI | 28ns | 264ns | 9.4x faster |
| BeginTxn (read-only) | 136ns | 301ns | 2.2x faster |
| BeginTxn (read-write) | 2281ns | 275ns | mdbx faster* |

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
- Nested transaction infrastructure (parent page delegation)

## Implementation Differences vs libmdbx

gdbx is file-format compatible with libmdbx but the implementation differs:

### Locking

- **libmdbx**: Configurable via `MDBX_LOCKING` build option. Supports SystemV IPC semaphores (default), POSIX shared mutexes, POSIX-2008 robust mutexes, or Win32 file locking. Lock state stored in shared memory (lock file) with complex handoff protocols.
- **gdbx**: Uses file-based flock() for writer lock. Simpler but higher syscall overhead per write transaction.
- **Rationale**: flock() is available on all Unix systems and Windows (via syscall), requires no platform-specific code paths, and is simple to reason about. The ~2us overhead per write transaction is acceptable since actual write work dominates. Avoiding IPC semaphores eliminates cleanup issues on process crash.

### Reader Registration

- **libmdbx**: Lock-free reader slot acquisition using atomic CAS with PID/TID tracking. Supports reader "parking" for long transactions.
- **gdbx**: Similar slot-based tracking with atomic operations, but uses LIFO freelist for O(1) slot acquisition. No parking support.
- **Rationale**: LIFO freelist gives O(1) slot acquisition in the common case (reusing recently-freed slots), which is cache-friendly. Parking adds complexity for a rare use case - most applications don't hold read transactions for extended periods.

### Page Management

- **libmdbx**: Complex spill/unspill mechanism to handle dirty pages exceeding RAM. Pages can be temporarily written to disk and reloaded.
- **gdbx**: Dirty pages tracked via fibonacci hash map (open addressing with linear probing). No spilling - all dirty pages kept in memory until commit.
- **Rationale**: Spilling adds significant complexity for edge cases. The fibonacci hash map gives O(1) average lookup with good cache locality for sequential page numbers (common in B+tree operations). Applications needing huge transactions can increase RAM or batch writes.

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
- **gdbx**: Infrastructure for nested transactions exists (BeginTxn accepts parent, dirty page reads delegate to parent). The `Sub()` method currently runs inline without true nesting.
- **Rationale**: The parent delegation mechanism provides the foundation for nested transaction support. Full implementation would add commit/abort propagation logic.

### What's Identical

- Page format (20-byte header, entry offsets, node layout)
- Meta page triple rotation for atomic commits
- B+ tree structure and algorithms
- DupSort sub-page/sub-tree handling
- Overflow page format for large values
- Lock file format and reader slot layout

## Running Benchmarks

```bash
# Write operations (8-byte keys)
go test -bench="BenchmarkWriteOps" -benchtime=2s -run=^$ ./tests/

# Read operations (8-byte keys)
go test -bench="BenchmarkReadOps" -benchtime=2s -run=^$ ./tests/

# DBI/Transaction operations
go test -bench="BenchmarkDBI" -benchtime=2s -run=^$ ./tests/

# Big values (8KB)
go test -bench="BenchmarkBigVal" -benchtime=2s -run=^$ ./tests/

# 64-byte keys
go test -bench="BenchmarkReadLong|BenchmarkWriteLong" -benchtime=2s -run=^$ ./tests/
```

## License

MIT
