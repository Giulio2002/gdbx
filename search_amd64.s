//go:build amd64

#include "textflag.h"

// Constants matching page.go
#define PAGE_HEADER_SIZE 20
#define NODE_SIZE 8

// func prefetchPage(data []byte)
// Prefetches page data into L1 cache for faster subsequent access.
// Uses non-temporal prefetch (PREFETCHNTA) to avoid cache pollution.
TEXT ·prefetchPage(SB), NOSPLIT, $0-24
    MOVQ    data_base+0(FP), AX      // AX = data.ptr
    MOVQ    data_len+8(FP), CX       // CX = data.len

    // Prefetch first cache line (64 bytes) - page header
    PREFETCHT0 0(AX)

    // Prefetch entry pointer area (typically starts at offset 20)
    PREFETCHT0 64(AX)

    // If page is larger, prefetch more
    CMPQ    CX, $256
    JL      prefetch_done
    PREFETCHT0 128(AX)
    PREFETCHT0 192(AX)

prefetch_done:
    RET

// func getKeyAndCompareAsm(pageData []byte, idx int, searchKey []byte) int
// Extracts key at idx from page and compares with searchKey.
// Returns -1 if searchKey < nodeKey, 0 if equal, 1 if searchKey > nodeKey.
//
// Arguments:
//   pageData: slice at FP+0 (ptr, len, cap = 24 bytes)
//   idx: int at FP+24 (8 bytes)
//   searchKey: slice at FP+32 (ptr, len, cap = 24 bytes)
// Return: int at FP+56 (8 bytes)
TEXT ·getKeyAndCompareAsm(SB), NOSPLIT, $0-64
    // Load pageData pointer and length
    MOVQ    pageData_base+0(FP), SI      // SI = pageData.ptr
    MOVQ    pageData_len+8(FP), R8       // R8 = pageData.len (for bounds if needed)

    // Load idx
    MOVQ    idx+24(FP), AX               // AX = idx

    // Calculate entry offset: offset = data[20 + idx*2] | data[21 + idx*2]<<8 + 20
    LEAQ    (SI)(AX*2), DI               // DI = pageData + idx*2
    ADDQ    $PAGE_HEADER_SIZE, DI        // DI = pageData + 20 + idx*2
    MOVWLZX 0(DI), BX                    // BX = uint16 at entry offset location
    ADDQ    $PAGE_HEADER_SIZE, BX        // BX = actual offset (storedOffset + 20)

    // Get key size: keySize = data[offset+6] | data[offset+7]<<8
    LEAQ    (SI)(BX*1), DI               // DI = pageData + offset
    MOVWLZX 6(DI), CX                    // CX = keySize (little endian uint16)

    // Now: nodeKey starts at DI+8, length CX
    ADDQ    $NODE_SIZE, DI               // DI = nodeKey.ptr

    // Load searchKey
    MOVQ    searchKey_base+32(FP), R9    // R9 = searchKey.ptr
    MOVQ    searchKey_len+40(FP), R10    // R10 = searchKey.len

    // Compare lengths first for fast path
    // minLen = min(searchKey.len, nodeKey.len)
    MOVQ    CX, R11                      // R11 = nodeKey.len
    CMPQ    R10, R11
    CMOVQLT R10, R11                     // R11 = min(R10, CX)

    // If minLen == 0, compare by length only
    TESTQ   R11, R11
    JZ      compare_lengths

    // Compare bytes: DI = nodeKey, R9 = searchKey, R11 = minLen
    MOVQ    R11, R12                     // R12 = bytes remaining

    // SSE2 loop: compare 16 bytes at a time
compare_sse_loop:
    CMPQ    R12, $16
    JL      compare_loop                 // Less than 16 bytes, use scalar

    MOVOU   0(R9), X0                    // Load 16 bytes from searchKey
    MOVOU   0(DI), X1                    // Load 16 bytes from nodeKey
    PCMPEQB X1, X0                       // Compare bytes
    PMOVMSKB X0, AX                      // Move byte mask to AX

    CMPQ    AX, $0xFFFF                  // All 16 bytes equal?
    JNE     compare_sse_diff             // Found a difference

    ADDQ    $16, R9
    ADDQ    $16, DI
    SUBQ    $16, R12
    JMP     compare_sse_loop

compare_sse_diff:
    // AX contains bitmask: bit i is 1 if byte i is equal
    XORQ    $0xFFFF, AX                  // Invert: bit i is 1 if byte i differs
    BSFQ    AX, AX                       // AX = index of first differing byte
    MOVBLZX 0(R9)(AX*1), BX              // Load byte from searchKey
    MOVBLZX 0(DI)(AX*1), R13             // Load byte from nodeKey
    CMPB    BL, R13B
    JB      return_neg1
    JMP     return_pos1

compare_loop:
    // Try to compare 8 bytes at a time
    CMPQ    R12, $8
    JL      compare_small

    MOVQ    0(R9), AX                    // Load 8 bytes from searchKey
    MOVQ    0(DI), BX                    // Load 8 bytes from nodeKey
    BSWAPQ  AX                           // Convert to big-endian for proper comparison
    BSWAPQ  BX
    CMPQ    AX, BX
    JNE     found_diff_8

    ADDQ    $8, R9
    ADDQ    $8, DI
    SUBQ    $8, R12
    JMP     compare_loop

compare_small:
    // Compare remaining bytes one at a time
    TESTQ   R12, R12
    JZ      compare_lengths

compare_byte:
    MOVBLZX 0(R9), AX
    MOVBLZX 0(DI), BX
    CMPB    AL, BL
    JNE     found_diff_1
    INCQ    R9
    INCQ    DI
    DECQ    R12
    JNZ     compare_byte

compare_lengths:
    // Bytes are equal up to minLen, compare by length
    CMPQ    R10, CX                      // Compare searchKey.len vs nodeKey.len
    JL      return_neg1
    JG      return_pos1

    // Equal
    MOVQ    $0, ret+56(FP)
    RET

found_diff_8:
    // Found difference in 8-byte comparison (after BSWAP)
    // If AX < BX, searchKey < nodeKey -> return -1
    // If AX > BX, searchKey > nodeKey -> return 1
    JB      return_neg1
    JMP     return_pos1

found_diff_1:
    // Found difference in byte comparison
    JB      return_neg1
    JMP     return_pos1

return_neg1:
    MOVQ    $-1, ret+56(FP)
    RET

return_pos1:
    MOVQ    $1, ret+56(FP)
    RET

// func compareKeysAsm(a, b []byte) int
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Uses SSE2 SIMD for keys >= 16 bytes, scalar for smaller.
TEXT ·compareKeysAsm(SB), NOSPLIT, $0-56
    MOVQ    a_base+0(FP), SI             // SI = a.ptr
    MOVQ    a_len+8(FP), AX              // AX = a.len
    MOVQ    b_base+24(FP), DI            // DI = b.ptr
    MOVQ    b_len+32(FP), BX             // BX = b.len

    // minLen = min(a.len, b.len)
    MOVQ    AX, CX
    CMPQ    BX, CX
    CMOVQLT BX, CX                       // CX = min(AX, BX)

    // If minLen == 0, compare by length only
    TESTQ   CX, CX
    JZ      cmp_lengths

    MOVQ    CX, R8                       // R8 = bytes remaining

    // SSE2 loop: compare 16 bytes at a time
cmp_sse_loop:
    CMPQ    R8, $16
    JL      cmp_loop                     // Less than 16 bytes, use scalar

    MOVOU   0(SI), X0                    // Load 16 bytes from a
    MOVOU   0(DI), X1                    // Load 16 bytes from b
    PCMPEQB X1, X0                       // Compare bytes: 0xFF if equal, 0x00 if not
    PMOVMSKB X0, R9                      // Move byte mask to R9

    CMPQ    R9, $0xFFFF                  // All 16 bytes equal?
    JNE     cmp_sse_diff                 // Found a difference

    ADDQ    $16, SI
    ADDQ    $16, DI
    SUBQ    $16, R8
    JMP     cmp_sse_loop

cmp_sse_diff:
    // R9 contains bitmask: bit i is 1 if byte i is equal
    // Find first differing byte using BSF (bit scan forward)
    XORQ    $0xFFFF, R9                  // Invert lower 16 bits: bit i is 1 if byte i differs
    BSFQ    R9, R9                       // R9 = index of first differing byte
    MOVBLZX 0(SI)(R9*1), R10             // Load byte from a
    MOVBLZX 0(DI)(R9*1), R11             // Load byte from b
    CMPB    R10B, R11B
    JB      cmp_neg1
    JMP     cmp_pos1

    // Scalar loop: compare 8 bytes at a time
cmp_loop:
    CMPQ    R8, $8
    JL      cmp_small

    MOVQ    0(SI), R9
    MOVQ    0(DI), R10
    BSWAPQ  R9
    BSWAPQ  R10
    CMPQ    R9, R10
    JNE     cmp_diff_8

    ADDQ    $8, SI
    ADDQ    $8, DI
    SUBQ    $8, R8
    JMP     cmp_loop

cmp_small:
    TESTQ   R8, R8
    JZ      cmp_lengths

cmp_byte:
    MOVBLZX 0(SI), R9
    MOVBLZX 0(DI), R10
    CMPB    R9B, R10B
    JNE     cmp_diff_1
    INCQ    SI
    INCQ    DI
    DECQ    R8
    JNZ     cmp_byte

cmp_lengths:
    CMPQ    AX, BX
    JL      cmp_neg1
    JG      cmp_pos1
    MOVQ    $0, ret+48(FP)
    RET

cmp_diff_8:
    JB      cmp_neg1
    JMP     cmp_pos1

cmp_diff_1:
    JB      cmp_neg1
    JMP     cmp_pos1

cmp_neg1:
    MOVQ    $-1, ret+48(FP)
    RET

cmp_pos1:
    MOVQ    $1, ret+48(FP)
    RET

// func searchPageAsm(pageData []byte, key []byte, isBranch bool) int
// Binary search within a page using assembly-optimized comparison.
// For now, this is a stub that calls back to Go - the main optimization is getKeyAndCompareAsm.
TEXT ·searchPageAsm(SB), NOSPLIT, $0-56
    // This is more complex to implement in pure assembly due to the loop structure.
    // The main benefit comes from getKeyAndCompareAsm which is called from Go code.
    // Return -1 to indicate "use Go implementation"
    MOVQ    $-1, ret+48(FP)
    RET

// func binarySearchLeaf8(pageData []byte, key uint64, n int) int
// Binary search on leaf page for 8-byte keys.
// key is already byte-swapped (big-endian as uint64).
// Page layout: header(20) + entries[n*2] + nodes
// Node layout: dataSize(4) + flags(1) + pad(1) + keySize(2) + key(8)
TEXT ·binarySearchLeaf8(SB), NOSPLIT, $0-48
    MOVQ    pageData_base+0(FP), SI      // SI = page data pointer
    MOVQ    key+24(FP), R8               // R8 = search key (big-endian uint64)
    MOVQ    n+32(FP), CX                 // CX = number of entries

    // If n <= 0, return 0
    TESTQ   CX, CX
    JLE     leaf8_return_zero

    // Fast path: check last entry first (append optimization)
    MOVQ    CX, R9
    DECQ    R9                           // R9 = n-1 (last index)

    // Get entry offset for last entry: offset = data[20 + (n-1)*2]
    LEAQ    20(SI)(R9*2), DI             // DI = &data[20 + (n-1)*2]
    MOVWLZX 0(DI), AX                    // AX = stored offset
    ADDQ    $20, AX                      // AX = actual node offset

    // Get key at last entry: key is at node+8, size is at node+6
    LEAQ    (SI)(AX*1), DI               // DI = node pointer
    MOVWLZX 6(DI), BX                    // BX = keySize
    CMPQ    BX, $8                       // Must be 8-byte key
    JNE     leaf8_fallback

    // Compare: load node key as big-endian uint64
    MOVQ    8(DI), R10                   // R10 = node key (little-endian in memory)
    BSWAPQ  R10                          // R10 = node key (big-endian)

    CMPQ    R8, R10
    JA      leaf8_return_n               // key > last: return n (insert after)
    JE      leaf8_return_last            // key == last: return n-1

    // Binary search from 0 to n-2
    XORQ    AX, AX                       // low = 0
    MOVQ    R9, BX
    DECQ    BX                           // high = n-2

leaf8_loop:
    CMPQ    AX, BX                       // low <= high?
    JG      leaf8_done

    // mid = (low + high) / 2
    MOVQ    AX, R9
    ADDQ    BX, R9
    SHRQ    $1, R9                       // R9 = mid

    // Get entry offset for mid
    LEAQ    20(SI)(R9*2), DI             // DI = &data[20 + mid*2]
    MOVWLZX 0(DI), R10                   // R10 = stored offset
    ADDQ    $20, R10                     // R10 = actual node offset

    // Get key at mid: key is at node+8
    LEAQ    (SI)(R10*1), DI              // DI = node pointer
    MOVWLZX 6(DI), R11                   // R11 = keySize
    CMPQ    R11, $8
    JNE     leaf8_fallback

    MOVQ    8(DI), R10                   // R10 = node key
    BSWAPQ  R10                          // big-endian

    CMPQ    R8, R10
    JB      leaf8_go_left
    JA      leaf8_go_right

    // Equal: return mid
    MOVQ    R9, ret+40(FP)
    RET

leaf8_go_left:
    MOVQ    R9, BX
    DECQ    BX                           // high = mid - 1
    JMP     leaf8_loop

leaf8_go_right:
    MOVQ    R9, AX
    INCQ    AX                           // low = mid + 1
    JMP     leaf8_loop

leaf8_done:
    // Return low
    MOVQ    AX, ret+40(FP)
    RET

leaf8_return_zero:
    MOVQ    $0, ret+40(FP)
    RET

leaf8_return_n:
    MOVQ    CX, ret+40(FP)
    RET

leaf8_return_last:
    MOVQ    CX, AX
    DECQ    AX
    MOVQ    AX, ret+40(FP)
    RET

leaf8_fallback:
    // Key size != 8, return -1 to signal fallback to Go
    MOVQ    $-1, ret+40(FP)
    RET

// func binarySearchBranch8(pageData []byte, key uint64, n int) int
// Binary search on branch page for 8-byte keys.
// Branch pages: entry 0 has no key (leftmost child), search entries 1 to n-1.
TEXT ·binarySearchBranch8(SB), NOSPLIT, $0-48
    MOVQ    pageData_base+0(FP), SI      // SI = page data pointer
    MOVQ    key+24(FP), R8               // R8 = search key (big-endian uint64)
    MOVQ    n+32(FP), CX                 // CX = number of entries

    // If n <= 1, return 0 (only leftmost child)
    CMPQ    CX, $1
    JLE     branch8_return_zero

    // Fast path: check last entry first
    MOVQ    CX, R9
    DECQ    R9                           // R9 = n-1 (last index)

    // Get entry offset for last entry
    LEAQ    20(SI)(R9*2), DI
    MOVWLZX 0(DI), AX
    ADDQ    $20, AX

    // Get key at last entry
    LEAQ    (SI)(AX*1), DI
    MOVWLZX 6(DI), BX
    CMPQ    BX, $8
    JNE     branch8_fallback

    MOVQ    8(DI), R10
    BSWAPQ  R10

    CMPQ    R8, R10
    JAE     branch8_return_last          // key >= last separator: use rightmost child

    // Binary search from 1 to n-2
    MOVQ    $1, AX                       // low = 1
    MOVQ    R9, BX
    DECQ    BX                           // high = n-2

branch8_loop:
    CMPQ    AX, BX
    JG      branch8_done

    // mid = (low + high) / 2
    MOVQ    AX, R9
    ADDQ    BX, R9
    SHRQ    $1, R9                       // R9 = mid

    // Get entry offset for mid
    LEAQ    20(SI)(R9*2), DI
    MOVWLZX 0(DI), R10
    ADDQ    $20, R10

    // Get key at mid
    LEAQ    (SI)(R10*1), DI
    MOVWLZX 6(DI), R11
    CMPQ    R11, $8
    JNE     branch8_fallback

    MOVQ    8(DI), R10
    BSWAPQ  R10

    CMPQ    R8, R10
    JB      branch8_go_left
    JA      branch8_go_right

    // Equal: return mid
    MOVQ    R9, ret+40(FP)
    RET

branch8_go_left:
    MOVQ    R9, BX
    DECQ    BX
    JMP     branch8_loop

branch8_go_right:
    MOVQ    R9, AX
    INCQ    AX
    JMP     branch8_loop

branch8_done:
    // Return low - 1 (for branch pages)
    MOVQ    AX, R9
    DECQ    R9
    MOVQ    R9, ret+40(FP)
    RET

branch8_return_zero:
    MOVQ    $0, ret+40(FP)
    RET

branch8_return_last:
    MOVQ    CX, AX
    DECQ    AX
    MOVQ    AX, ret+40(FP)
    RET

branch8_fallback:
    MOVQ    $-1, ret+40(FP)
    RET
