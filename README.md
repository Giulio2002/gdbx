# gdbx

A pure Go implementation of [MDBX](https://gitflic.ru/project/erthink/libmdbx), the high-performance embedded transactional key-value database. File-format compatible with libmdbx.

## Performance

Benchmarks comparing gdbx against [mdbx-go](https://github.com/erigontech/mdbx-go) (CGO wrapper), BoltDB, and RocksDB on AMD Ryzen 5 3600.

### 8-byte Keys (uint64)

Common case for database IDs. gdbx uses assembly-optimized binary search with BSWAP+CMP.

#### Write Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqPut | 10K | **131** | 230 | 859 | 2271 | 1.8x | 6.6x | 17.3x |
| RandPut | 10K | **130** | 227 | 862 | 2106 | 1.7x | 6.6x | 16.2x |
| CursorPut | 10K | **117** | 178 | 867 | 2264 | 1.5x | 7.4x | 19.4x |
| SeqPut | 100K | **156** | 252 | 875 | 1979 | 1.6x | 5.6x | 12.7x |
| RandPut | 100K | **150** | 263 | 876 | 1810 | 1.8x | 5.8x | 12.1x |
| CursorPut | 100K | **157** | 175 | 888 | 1832 | 1.1x | 5.7x | 11.7x |
| SeqPut | 1M | **166** | 284 | 660 | 1136 | 1.7x | 4.0x | 6.8x |
| RandPut | 1M | **165** | 286 | 717 | 1080 | 1.7x | 4.3x | 6.5x |
| CursorPut | 1M | **160** | 189 | 698 | 1114 | 1.2x | 4.4x | 7.0x |

#### Read Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqRead | 10K | 31 | 113 | **12** | 189 | 3.6x | 0.4x | 6.1x |
| RandGet | 10K | **94** | 212 | 860 | 2155 | 2.3x | 9.1x | 22.9x |
| RandSeek | 10K | **102** | 196 | 535 | 1214 | 1.9x | 5.2x | 11.9x |
| SeqRead | 100K | 31 | 114 | **18** | 848 | 3.7x | 0.6x | 27.4x |
| RandGet | 100K | **118** | 259 | 1000 | 2510 | 2.2x | 8.5x | 21.3x |
| RandSeek | 100K | **125** | 200 | 670 | 2335 | 1.6x | 5.4x | 18.7x |
| SeqRead | 1M | 37 | 113 | **22** | 925 | 3.1x | 0.6x | 25.0x |
| RandGet | 1M | **155** | 259 | 1121 | 2258 | 1.7x | 7.2x | 14.6x |
| RandSeek | 1M | **134** | 200 | 744 | 2718 | 1.5x | 5.6x | 20.3x |

*Note: BoltDB wins sequential reads due to simpler cursor iteration, but gdbx dominates random access.*

### 64-byte Keys

Longer keys use SSE2-optimized binary search comparing 16 bytes at a time.

#### Write Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqPut | 10K | **202** | 238 | 565 | 2316 | 1.2x | 2.8x | 11.5x |
| RandPut | 10K | **192** | 242 | 568 | 2083 | 1.3x | 3.0x | 10.8x |
| SeqPut | 100K | **227** | 261 | 610 | 1723 | 1.2x | 2.7x | 7.6x |
| RandPut | 100K | **231** | 261 | 652 | 1575 | 1.1x | 2.8x | 6.8x |
| SeqPut | 1M | **268** | 312 | 800 | 1022 | 1.2x | 3.0x | 3.8x |
| RandPut | 1M | **269** | 284 | 901 | 1032 | 1.1x | 3.3x | 3.8x |

#### Read Operations (ns/op, lower is better)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqRead | 10K | 34 | 113 | **23** | 158 | 3.3x | 0.7x | 4.6x |
| RandGet | 10K | **110** | 221 | 975 | 1481 | 2.0x | 8.9x | 13.5x |
| RandSeek | 10K | **155** | 210 | 610 | 674 | 1.4x | 3.9x | 4.3x |
| SeqRead | 100K | 35 | 116 | **20** | 158 | 3.3x | 0.6x | 4.5x |
| RandGet | 100K | **145** | 244 | 1074 | 1265 | 1.7x | 7.4x | 8.7x |
| RandSeek | 100K | **199** | 216 | 651 | 803 | 1.1x | 3.3x | 4.0x |
| SeqRead | 1M | 42 | 121 | **19** | 283 | 2.9x | 0.5x | 6.7x |
| RandGet | 1M | **182** | 274 | 1082 | 2311 | 1.5x | 5.9x | 12.7x |
| RandSeek | 1M | 225 | **215** | 665 | 1588 | 1.0x | 3.0x | 7.1x |

### Big Values (8KB)

Large values use zero-copy reads (direct mmap slice) and in-place overflow page updates.

#### Write Operations (ns/op)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqPut | 100 | **268** | 315 | 525 | 2810 | 1.2x | 2.0x | 10.5x |
| RandPut | 100 | **290** | 308 | 535 | 3378 | 1.1x | 1.8x | 11.6x |
| SeqPut | 1K | **340** | 420 | 768 | 4274 | 1.2x | 2.3x | 12.6x |
| RandPut | 1K | **346** | 438 | 776 | 4061 | 1.3x | 2.2x | 11.7x |
| SeqPut | 10K | 793 | **688** | 832 | 27834 | 0.9x | 1.0x | 35.1x |
| RandPut | 10K | 896 | **690** | 843 | 28356 | 0.8x | 0.9x | 31.6x |

#### Read Operations (ns/op)

| Operation | Entries | gdbx | mdbx | BoltDB | RocksDB | vs mdbx | vs Bolt | vs Rocks |
|-----------|---------|------|------|--------|---------|---------|---------|----------|
| SeqRead | 100 | 41 | 126 | **21** | 2682 | 3.1x | 0.5x | 65.4x |
| RandGet | 100 | **44** | 185 | 473 | 1533 | 4.2x | 10.7x | 34.8x |
| SeqRead | 1K | **42** | 129 | 77 | 28679 | 3.1x | 1.8x | 683x |
| RandGet | 1K | **97** | 209 | 921 | 2310 | 2.2x | 9.5x | 23.8x |
| SeqRead | 10K | **43** | 221 | 112 | 8605 | 5.1x | 2.6x | 200x |
| RandGet | 10K | **104** | 323 | 1067 | 10713 | 3.1x | 10.3x | 103x |

*Big value reads use zero-copy (direct mmap slice), achieving 78-200 GB/s throughput.*

### DBI/Transaction Operations

| Operation | gdbx | mdbx | vs mdbx |
|-----------|------|------|---------|
| OpenDBI | 27ns | 244ns | 9.0x faster |
| BeginTxn (read-only) | 139ns | 303ns | 2.2x faster |
| BeginTxn (read-write) | 2108ns | 281ns | mdbx faster* |

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
- Zero heap allocations on writes (dirty pages in mmap'd spill buffer)

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
- **gdbx**: Dirty pages stored in a memory-mapped spill buffer (`spill/` package) rather than Go heap. The spill buffer uses a segmented design with multiple mmap regions to allow growth without invalidating existing page slices. Each segment has its own bitmap for O(1) slot allocation. Page lookups use a fibonacci hash map (open addressing with linear probing).
- **Rationale**: Storing dirty pages in mmap'd memory eliminates GC pressure entirely - the OS manages paging while Go sees only small slot metadata. The segmented design avoids remapping (which would invalidate existing slices) by simply adding new segments when capacity is exhausted. This achieves zero heap allocations on all write operations while supporting large transactions.

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
- **gdbx**: Assembly-optimized binary search (amd64/arm64). 8-byte keys use BSWAP+CMP for single-instruction comparison. Longer keys use SSE2 SIMD comparing 16 bytes at a time with PCMPEQB+PMOVMSKB. Full search loop in assembly avoids Go/asm boundary overhead.
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
go test -bench="BenchmarkWriteOps" -benchtime=2s -run=^$ ./benchmarks/

# Read operations (8-byte keys)
go test -bench="BenchmarkReadOps" -benchtime=2s -run=^$ ./benchmarks/

# DBI/Transaction operations
go test -bench="BenchmarkDBI" -benchtime=2s -run=^$ ./benchmarks/

# Big values (8KB)
go test -bench="BenchmarkBigVal" -benchtime=2s -run=^$ ./benchmarks/

# 64-byte keys
go test -bench="BenchmarkReadLong|BenchmarkWriteLong" -benchtime=2s -run=^$ ./benchmarks/
```

## License

MIT
