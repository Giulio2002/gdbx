//go:build arm64

#include "textflag.h"

// Constants matching page.go
#define PAGE_HEADER_SIZE 20
#define NODE_SIZE 8

// func prefetchPage(data []byte)
// Prefetches page data into L1 cache for faster subsequent access.
// Uses PRFM instruction for prefetch.
TEXT ·prefetchPage(SB), NOSPLIT, $0-24
    MOVD    data_base+0(FP), R0      // R0 = data.ptr
    MOVD    data_len+8(FP), R1       // R1 = data.len

    // Prefetch first cache line (64 bytes) - page header
    PRFM    (R0), PLDL1KEEP

    // Prefetch entry pointer area (typically starts at offset 20)
    ADD     $64, R0, R2
    PRFM    (R2), PLDL1KEEP

    // If page is larger, prefetch more
    CMP     $256, R1
    BLT     prefetch_done
    ADD     $128, R0, R2
    PRFM    (R2), PLDL1KEEP
    ADD     $192, R0, R2
    PRFM    (R2), PLDL1KEEP

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
    // Load pageData pointer
    MOVD    pageData_base+0(FP), R0      // R0 = pageData.ptr

    // Load idx
    MOVD    idx+24(FP), R1               // R1 = idx

    // Calculate entry offset: offset = data[20 + idx*2] | data[21 + idx*2]<<8 + 20
    ADD     R1, R1, R2                   // R2 = idx*2
    ADD     $PAGE_HEADER_SIZE, R2        // R2 = 20 + idx*2
    ADD     R0, R2                       // R2 = pageData + 20 + idx*2
    MOVHU   (R2), R3                     // R3 = uint16 at entry offset location
    ADD     $PAGE_HEADER_SIZE, R3        // R3 = actual offset (storedOffset + 20)

    // Get key size: keySize = data[offset+6] | data[offset+7]<<8
    ADD     R0, R3, R4                   // R4 = pageData + offset
    ADD     $6, R4, R5                   // R5 = &data[offset+6]
    MOVHU   (R5), R6                     // R6 = keySize (little endian uint16)

    // Now: nodeKey starts at R4+8, length R6
    ADD     $NODE_SIZE, R4               // R4 = nodeKey.ptr

    // Load searchKey
    MOVD    searchKey_base+32(FP), R7    // R7 = searchKey.ptr
    MOVD    searchKey_len+40(FP), R8     // R8 = searchKey.len

    // Compare lengths first for fast path
    // minLen = min(searchKey.len, nodeKey.len)
    CMP     R6, R8
    CSEL    LT, R8, R6, R9               // R9 = min(R8, R6)

    // If minLen == 0, compare by length only
    CBZ     R9, compare_lengths

    // Compare bytes: R4 = nodeKey, R7 = searchKey, R9 = minLen
    MOVD    R9, R10                      // R10 = bytes remaining

    // NEON loop: compare 16 bytes at a time
compare_neon_loop:
    CMP     $16, R10
    BLT     compare_loop                 // Less than 16 bytes, use scalar

    VLD1    (R7), [V0.B16]               // Load 16 bytes from searchKey
    VLD1    (R4), [V1.B16]               // Load 16 bytes from nodeKey
    VCMEQ   V1.B16, V0.B16, V2.B16       // Compare bytes: 0xFF if equal, 0x00 if not

    // Check if all bytes are equal (all 0xFF)
    VMOV    V2.D[0], R11
    VMOV    V2.D[1], R12
    AND     R11, R12, R11
    CMP     $-1, R11                     // All 0xFF = -1 as signed
    BNE     compare_neon_diff

    ADD     $16, R7
    ADD     $16, R4
    SUB     $16, R10
    B       compare_neon_loop

compare_neon_diff:
    // Find first differing byte
    // V2 has 0xFF for equal, 0x00 for different
    // We need to find the first 0x00 byte
    VMOV    V2.D[0], R11
    VMOV    V2.D[1], R12

    // Check lower 8 bytes first
    CMP     $-1, R11
    BNE     diff_in_lower

    // Difference is in upper 8 bytes
    ADD     $8, R7
    ADD     $8, R4
    MOVD    R12, R11
    B       find_diff_byte

diff_in_lower:
    // Difference is in lower 8 bytes
find_diff_byte:
    // R11 has the 8-byte comparison result, find first 0x00 byte
    // Invert: 0x00 becomes 0xFF, 0xFF becomes 0x00
    MVN     R11, R11
    // Find position of first non-zero byte (first diff)
    RBIT    R11, R11                     // Reverse bits
    CLZ     R11, R11                     // Count leading zeros
    LSR     $3, R11                      // Divide by 8 to get byte index

    // Load bytes at differing position
    MOVBU   (R7)(R11), R12               // searchKey byte
    MOVBU   (R4)(R11), R13               // nodeKey byte
    CMP     R13, R12
    BLO     return_neg1
    B       return_pos1

compare_loop:
    // Compare 8 bytes at a time
    CMP     $8, R10
    BLT     compare_small

    MOVD    (R7), R11                    // Load 8 bytes from searchKey
    MOVD    (R4), R12                    // Load 8 bytes from nodeKey
    REV     R11, R11                     // Convert to big-endian for proper comparison
    REV     R12, R12
    CMP     R12, R11
    BNE     found_diff_8

    ADD     $8, R7
    ADD     $8, R4
    SUB     $8, R10
    B       compare_loop

compare_small:
    // Compare remaining bytes one at a time
    CBZ     R10, compare_lengths

compare_byte:
    MOVBU   (R7), R11
    MOVBU   (R4), R12
    CMP     R12, R11
    BNE     found_diff_1
    ADD     $1, R7
    ADD     $1, R4
    SUB     $1, R10
    CBNZ    R10, compare_byte

compare_lengths:
    // Bytes are equal up to minLen, compare by length
    CMP     R6, R8                       // Compare searchKey.len vs nodeKey.len
    BLT     return_neg1
    BGT     return_pos1

    // Equal
    MOVD    $0, R0
    MOVD    R0, ret+56(FP)
    RET

found_diff_8:
    // Found difference in 8-byte comparison (after REV)
    BLO     return_neg1
    B       return_pos1

found_diff_1:
    // Found difference in byte comparison
    BLO     return_neg1
    B       return_pos1

return_neg1:
    MOVD    $-1, R0
    MOVD    R0, ret+56(FP)
    RET

return_pos1:
    MOVD    $1, R0
    MOVD    R0, ret+56(FP)
    RET

// func compareKeysAsm(a, b []byte) int
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Uses NEON SIMD for keys >= 16 bytes, scalar for smaller.
TEXT ·compareKeysAsm(SB), NOSPLIT, $0-56
    MOVD    a_base+0(FP), R0             // R0 = a.ptr
    MOVD    a_len+8(FP), R1              // R1 = a.len
    MOVD    b_base+24(FP), R2            // R2 = b.ptr
    MOVD    b_len+32(FP), R3             // R3 = b.len

    // minLen = min(a.len, b.len)
    CMP     R3, R1
    CSEL    LT, R1, R3, R4               // R4 = min(R1, R3)

    // If minLen == 0, compare by length only
    CBZ     R4, cmp_lengths

    MOVD    R4, R5                       // R5 = bytes remaining

    // NEON loop: compare 16 bytes at a time
cmp_neon_loop:
    CMP     $16, R5
    BLT     cmp_loop

    VLD1    (R0), [V0.B16]               // Load 16 bytes from a
    VLD1    (R2), [V1.B16]               // Load 16 bytes from b
    VCMEQ   V1.B16, V0.B16, V2.B16       // Compare bytes

    // Check if all bytes are equal
    VMOV    V2.D[0], R6
    VMOV    V2.D[1], R7
    AND     R6, R7, R6
    CMP     $-1, R6
    BNE     cmp_neon_diff

    ADD     $16, R0
    ADD     $16, R2
    SUB     $16, R5
    B       cmp_neon_loop

cmp_neon_diff:
    // Find first differing byte
    VMOV    V2.D[0], R6
    VMOV    V2.D[1], R7

    CMP     $-1, R6
    BNE     cmp_diff_in_lower

    ADD     $8, R0
    ADD     $8, R2
    MOVD    R7, R6
    B       cmp_find_diff

cmp_diff_in_lower:
cmp_find_diff:
    MVN     R6, R6
    RBIT    R6, R6
    CLZ     R6, R6
    LSR     $3, R6

    MOVBU   (R0)(R6), R7                 // byte from a
    MOVBU   (R2)(R6), R8                 // byte from b
    CMP     R8, R7
    BLO     cmp_neg1
    B       cmp_pos1

cmp_loop:
    // Compare 8 bytes at a time
    CMP     $8, R5
    BLT     cmp_small

    MOVD    (R0), R6
    MOVD    (R2), R7
    REV     R6, R6
    REV     R7, R7
    CMP     R7, R6
    BNE     cmp_diff_8

    ADD     $8, R0
    ADD     $8, R2
    SUB     $8, R5
    B       cmp_loop

cmp_small:
    CBZ     R5, cmp_lengths

cmp_byte:
    MOVBU   (R0), R6
    MOVBU   (R2), R7
    CMP     R7, R6
    BNE     cmp_diff_1
    ADD     $1, R0
    ADD     $1, R2
    SUB     $1, R5
    CBNZ    R5, cmp_byte

cmp_lengths:
    CMP     R3, R1
    BLT     cmp_neg1
    BGT     cmp_pos1
    MOVD    $0, R0
    MOVD    R0, ret+48(FP)
    RET

cmp_diff_8:
    BLO     cmp_neg1
    B       cmp_pos1

cmp_diff_1:
    BLO     cmp_neg1
    B       cmp_pos1

cmp_neg1:
    MOVD    $-1, R0
    MOVD    R0, ret+48(FP)
    RET

cmp_pos1:
    MOVD    $1, R0
    MOVD    R0, ret+48(FP)
    RET

// func searchPageAsm(pageData []byte, key []byte, isBranch bool) int
// Binary search within a page using assembly-optimized comparison.
// For now, this is a stub that calls back to Go - the main optimization is getKeyAndCompareAsm.
TEXT ·searchPageAsm(SB), NOSPLIT, $0-56
    // Return -1 to indicate "use Go implementation"
    MOVD    $-1, R0
    MOVD    R0, ret+48(FP)
    RET

// func binarySearchLeaf8(pageData []byte, key uint64, n int) int
// Binary search on leaf page for 8-byte keys.
// key is already byte-swapped (big-endian as uint64).
// Page layout: header(20) + entries[n*2] + nodes
// Node layout: dataSize(4) + flags(1) + pad(1) + keySize(2) + key(8)
TEXT ·binarySearchLeaf8(SB), NOSPLIT, $0-48
    MOVD    pageData_base+0(FP), R0      // R0 = page data pointer
    MOVD    key+24(FP), R1               // R1 = search key (big-endian uint64)
    MOVD    n+32(FP), R2                 // R2 = number of entries

    // If n <= 0, return 0
    CMP     $0, R2
    BLE     leaf8_return_zero

    // Fast path: check last entry first (append optimization)
    SUB     $1, R2, R3                   // R3 = n-1 (last index)

    // Get entry offset for last entry: offset = data[20 + (n-1)*2]
    ADD     R3, R3, R4                   // R4 = (n-1)*2
    ADD     $PAGE_HEADER_SIZE, R4        // R4 = 20 + (n-1)*2
    ADD     R0, R4                       // R4 = &data[20 + (n-1)*2]
    MOVHU   (R4), R5                     // R5 = stored offset
    ADD     $PAGE_HEADER_SIZE, R5        // R5 = actual node offset

    // Get key at last entry: key is at node+8, size is at node+6
    ADD     R0, R5, R6                   // R6 = node pointer
    ADD     $6, R6, R7
    MOVHU   (R7), R7                     // R7 = keySize
    CMP     $8, R7                       // Must be 8-byte key
    BNE     leaf8_fallback

    // Compare: load node key as big-endian uint64
    ADD     $8, R6, R6                   // R6 = &node.key
    MOVD    (R6), R7                     // R7 = node key (little-endian in memory)
    REV     R7, R7                       // R7 = node key (big-endian)

    CMP     R7, R1
    BHI     leaf8_return_n               // key > last: return n (insert after)
    BEQ     leaf8_return_last            // key == last: return n-1

    // Binary search from 0 to n-2
    MOVD    $0, R4                       // low = 0
    SUB     $1, R3, R5                   // high = n-2

leaf8_loop:
    CMP     R5, R4                       // low <= high?
    BGT     leaf8_done

    // mid = (low + high) / 2
    ADD     R5, R4, R6
    LSR     $1, R6                       // R6 = mid

    // Get entry offset for mid
    ADD     R6, R6, R7                   // R7 = mid*2
    ADD     $PAGE_HEADER_SIZE, R7        // R7 = 20 + mid*2
    ADD     R0, R7                       // R7 = &data[20 + mid*2]
    MOVHU   (R7), R8                     // R8 = stored offset
    ADD     $PAGE_HEADER_SIZE, R8        // R8 = actual node offset

    // Get key at mid: key is at node+8
    ADD     R0, R8, R9                   // R9 = node pointer
    ADD     $6, R9, R10
    MOVHU   (R10), R10                   // R10 = keySize
    CMP     $8, R10
    BNE     leaf8_fallback

    ADD     $8, R9, R9
    MOVD    (R9), R10                    // R10 = node key
    REV     R10, R10                     // big-endian

    CMP     R10, R1
    BLO     leaf8_go_left
    BHI     leaf8_go_right

    // Equal: return mid
    MOVD    R6, ret+40(FP)
    RET

leaf8_go_left:
    SUB     $1, R6, R5                   // high = mid - 1
    B       leaf8_loop

leaf8_go_right:
    ADD     $1, R6, R4                   // low = mid + 1
    B       leaf8_loop

leaf8_done:
    // Return low
    MOVD    R4, ret+40(FP)
    RET

leaf8_return_zero:
    MOVD    $0, R0
    MOVD    R0, ret+40(FP)
    RET

leaf8_return_n:
    MOVD    R2, ret+40(FP)
    RET

leaf8_return_last:
    SUB     $1, R2, R0
    MOVD    R0, ret+40(FP)
    RET

leaf8_fallback:
    // Key size != 8, return -1 to signal fallback to Go
    MOVD    $-1, R0
    MOVD    R0, ret+40(FP)
    RET

// func binarySearchBranch8(pageData []byte, key uint64, n int) int
// Binary search on branch page for 8-byte keys.
// Branch pages: entry 0 has no key (leftmost child), search entries 1 to n-1.
TEXT ·binarySearchBranch8(SB), NOSPLIT, $0-48
    MOVD    pageData_base+0(FP), R0      // R0 = page data pointer
    MOVD    key+24(FP), R1               // R1 = search key (big-endian uint64)
    MOVD    n+32(FP), R2                 // R2 = number of entries

    // If n <= 1, return 0 (only leftmost child)
    CMP     $1, R2
    BLE     branch8_return_zero

    // Fast path: check last entry first
    SUB     $1, R2, R3                   // R3 = n-1 (last index)

    // Get entry offset for last entry
    ADD     R3, R3, R4
    ADD     $PAGE_HEADER_SIZE, R4
    ADD     R0, R4
    MOVHU   (R4), R5
    ADD     $PAGE_HEADER_SIZE, R5

    // Get key at last entry
    ADD     R0, R5, R6
    ADD     $6, R6, R7
    MOVHU   (R7), R7
    CMP     $8, R7
    BNE     branch8_fallback

    ADD     $8, R6, R6
    MOVD    (R6), R7
    REV     R7, R7

    CMP     R7, R1
    BHS     branch8_return_last          // key >= last separator: use rightmost child

    // Binary search from 1 to n-2
    MOVD    $1, R4                       // low = 1
    SUB     $1, R3, R5                   // high = n-2

branch8_loop:
    CMP     R5, R4
    BGT     branch8_done

    // mid = (low + high) / 2
    ADD     R5, R4, R6
    LSR     $1, R6

    // Get entry offset for mid
    ADD     R6, R6, R7
    ADD     $PAGE_HEADER_SIZE, R7
    ADD     R0, R7
    MOVHU   (R7), R8
    ADD     $PAGE_HEADER_SIZE, R8

    // Get key at mid
    ADD     R0, R8, R9
    ADD     $6, R9, R10
    MOVHU   (R10), R10
    CMP     $8, R10
    BNE     branch8_fallback

    ADD     $8, R9, R9
    MOVD    (R9), R10
    REV     R10, R10

    CMP     R10, R1
    BLO     branch8_go_left
    BHI     branch8_go_right

    // Equal: return mid
    MOVD    R6, ret+40(FP)
    RET

branch8_go_left:
    SUB     $1, R6, R5
    B       branch8_loop

branch8_go_right:
    ADD     $1, R6, R4
    B       branch8_loop

branch8_done:
    // Return low - 1 (for branch pages)
    SUB     $1, R4, R0
    MOVD    R0, ret+40(FP)
    RET

branch8_return_zero:
    MOVD    $0, R0
    MOVD    R0, ret+40(FP)
    RET

branch8_return_last:
    SUB     $1, R2, R0
    MOVD    R0, ret+40(FP)
    RET

branch8_fallback:
    MOVD    $-1, R0
    MOVD    R0, ret+40(FP)
    RET

// func binarySearchLeafN(pageData []byte, key []byte, n int) int
// Binary search on leaf page for variable-length keys using NEON.
// Page layout: header(20) + entries[n*2] + nodes
// Node layout: dataSize(4) + flags(1) + pad(1) + keySize(2) + key(keyLen)
//
// Arguments:
//   pageData: slice at FP+0 (ptr, len, cap = 24 bytes)
//   key: slice at FP+24 (ptr, len, cap = 24 bytes)
//   n: int at FP+48 (8 bytes)
// Return: int at FP+56 (8 bytes)
TEXT ·binarySearchLeafN(SB), NOSPLIT, $0-64
    MOVD    pageData_base+0(FP), R0      // R0 = page data pointer
    MOVD    key_base+24(FP), R1          // R1 = search key pointer
    MOVD    key_len+32(FP), R2           // R2 = search key length
    MOVD    n+48(FP), R3                 // R3 = number of entries

    // If n <= 0, return 0
    CMP     $0, R3
    BLE     leafN_return_zero

    // Fast path: check last entry first (append optimization)
    SUB     $1, R3, R4                   // R4 = n-1 (last index)

    // Get entry offset for last entry
    ADD     R4, R4, R5
    ADD     $PAGE_HEADER_SIZE, R5
    ADD     R0, R5
    MOVHU   (R5), R6
    ADD     $PAGE_HEADER_SIZE, R6

    // Get key info: keySize, key starts at offset+8
    ADD     R0, R6, R7                   // R7 = node pointer
    ADD     $6, R7, R8
    MOVHU   (R8), R8                     // R8 = nodeKeySize
    ADD     $NODE_SIZE, R7               // R7 = nodeKey pointer

    // Compare search key with last node key
    // R1 = searchKey, R2 = searchKeyLen, R7 = nodeKey, R8 = nodeKeyLen
    CMP     R8, R2
    CSEL    LT, R2, R8, R9               // R9 = min(searchKeyLen, nodeKeyLen)

    // Save pointers for loop
    MOVD    R1, R10                      // R10 = searchKey ptr
    MOVD    R9, R11                      // R11 = bytes remaining

leafN_last_cmp_loop:
    CMP     $16, R11
    BLT     leafN_last_cmp_small

    VLD1    (R10), [V0.B16]
    VLD1    (R7), [V1.B16]
    VCMEQ   V1.B16, V0.B16, V2.B16

    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    AND     R12, R13, R12
    CMP     $-1, R12
    BNE     leafN_last_found_diff

    ADD     $16, R10
    ADD     $16, R7
    SUB     $16, R11
    B       leafN_last_cmp_loop

leafN_last_cmp_small:
    CBZ     R11, leafN_last_cmp_lengths

leafN_last_cmp_byte:
    MOVBU   (R10), R12
    MOVBU   (R7), R13
    CMP     R13, R12
    BNE     leafN_last_diff_byte
    ADD     $1, R10
    ADD     $1, R7
    SUB     $1, R11
    CBNZ    R11, leafN_last_cmp_byte

leafN_last_cmp_lengths:
    CMP     R8, R2                       // searchKeyLen vs nodeKeyLen
    BLT     leafN_last_less
    BGT     leafN_last_greater
    // Equal: return n-1
    MOVD    R4, ret+56(FP)
    RET

leafN_last_found_diff:
    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    CMP     $-1, R12
    BNE     leafN_last_diff_lower
    ADD     $8, R10
    ADD     $8, R7
    MOVD    R13, R12
    B       leafN_last_find_byte

leafN_last_diff_lower:
leafN_last_find_byte:
    MVN     R12, R12
    RBIT    R12, R12
    CLZ     R12, R12
    LSR     $3, R12
    MOVBU   (R10)(R12), R13
    MOVBU   (R7)(R12), R14
    CMP     R14, R13
    BLO     leafN_last_less
    B       leafN_last_greater

leafN_last_diff_byte:
    BLO     leafN_last_less
    B       leafN_last_greater

leafN_last_greater:
    // searchKey > last: return n (insert after)
    MOVD    R3, ret+56(FP)
    RET

leafN_last_less:
    // searchKey < last: do binary search from 0 to n-2
    MOVD    $0, R5                       // low = 0
    SUB     $1, R4, R6                   // high = n-2

leafN_loop:
    CMP     R6, R5                       // low <= high?
    BGT     leafN_done

    // mid = (low + high) / 2
    ADD     R6, R5, R4
    LSR     $1, R4                       // R4 = mid

    // Get entry offset for mid
    ADD     R4, R4, R7
    ADD     $PAGE_HEADER_SIZE, R7
    ADD     R0, R7
    MOVHU   (R7), R8
    ADD     $PAGE_HEADER_SIZE, R8

    // Get key info
    ADD     R0, R8, R7                   // R7 = node pointer
    ADD     $6, R7, R9
    MOVHU   (R9), R8                     // R8 = nodeKeyLen
    ADD     $NODE_SIZE, R7               // R7 = nodeKey pointer

    // Compare: R1 = searchKey, R2 = searchKeyLen, R7 = nodeKey, R8 = nodeKeyLen
    CMP     R8, R2
    CSEL    LT, R2, R8, R9               // R9 = min

    MOVD    R1, R10                      // R10 = searchKey ptr
    MOVD    R9, R11                      // R11 = bytes remaining

leafN_mid_cmp_loop:
    CMP     $16, R11
    BLT     leafN_mid_cmp_small

    VLD1    (R10), [V0.B16]
    VLD1    (R7), [V1.B16]
    VCMEQ   V1.B16, V0.B16, V2.B16

    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    AND     R12, R13, R12
    CMP     $-1, R12
    BNE     leafN_mid_found_diff

    ADD     $16, R10
    ADD     $16, R7
    SUB     $16, R11
    B       leafN_mid_cmp_loop

leafN_mid_cmp_small:
    CBZ     R11, leafN_mid_cmp_lengths

leafN_mid_cmp_byte:
    MOVBU   (R10), R12
    MOVBU   (R7), R13
    CMP     R13, R12
    BNE     leafN_mid_diff_byte
    ADD     $1, R10
    ADD     $1, R7
    SUB     $1, R11
    CBNZ    R11, leafN_mid_cmp_byte

leafN_mid_cmp_lengths:
    CMP     R8, R2
    BLT     leafN_go_left
    BGT     leafN_go_right
    // Equal: return mid
    MOVD    R4, ret+56(FP)
    RET

leafN_mid_found_diff:
    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    CMP     $-1, R12
    BNE     leafN_mid_diff_lower
    ADD     $8, R10
    ADD     $8, R7
    MOVD    R13, R12
    B       leafN_mid_find_byte

leafN_mid_diff_lower:
leafN_mid_find_byte:
    MVN     R12, R12
    RBIT    R12, R12
    CLZ     R12, R12
    LSR     $3, R12
    MOVBU   (R10)(R12), R13
    MOVBU   (R7)(R12), R14
    CMP     R14, R13
    BLO     leafN_go_left
    B       leafN_go_right

leafN_mid_diff_byte:
    BLO     leafN_go_left
    B       leafN_go_right

leafN_go_left:
    SUB     $1, R4, R6                   // high = mid - 1
    B       leafN_loop

leafN_go_right:
    ADD     $1, R4, R5                   // low = mid + 1
    B       leafN_loop

leafN_done:
    MOVD    R5, ret+56(FP)
    RET

leafN_return_zero:
    MOVD    $0, R0
    MOVD    R0, ret+56(FP)
    RET

// func binarySearchBranchN(pageData []byte, key []byte, n int) int
// Binary search on branch page for variable-length keys using NEON.
// Branch pages: entry 0 has no key (leftmost child), search entries 1 to n-1.
TEXT ·binarySearchBranchN(SB), NOSPLIT, $0-64
    MOVD    pageData_base+0(FP), R0      // R0 = page data pointer
    MOVD    key_base+24(FP), R1          // R1 = search key pointer
    MOVD    key_len+32(FP), R2           // R2 = search key length
    MOVD    n+48(FP), R3                 // R3 = number of entries

    // If n <= 1, return 0 (only leftmost child)
    CMP     $1, R3
    BLE     branchN_return_zero

    // Fast path: check last entry first
    SUB     $1, R3, R4                   // R4 = n-1 (last index)

    // Get entry offset for last entry
    ADD     R4, R4, R5
    ADD     $PAGE_HEADER_SIZE, R5
    ADD     R0, R5
    MOVHU   (R5), R6
    ADD     $PAGE_HEADER_SIZE, R6

    // Get key info
    ADD     R0, R6, R7                   // R7 = node pointer
    ADD     $6, R7, R8
    MOVHU   (R8), R8                     // R8 = nodeKeyLen
    ADD     $NODE_SIZE, R7               // R7 = nodeKey pointer

    // Compare search key with last node key
    CMP     R8, R2
    CSEL    LT, R2, R8, R9               // R9 = min

    MOVD    R1, R10
    MOVD    R9, R11

branchN_last_cmp_loop:
    CMP     $16, R11
    BLT     branchN_last_cmp_small

    VLD1    (R10), [V0.B16]
    VLD1    (R7), [V1.B16]
    VCMEQ   V1.B16, V0.B16, V2.B16

    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    AND     R12, R13, R12
    CMP     $-1, R12
    BNE     branchN_last_found_diff

    ADD     $16, R10
    ADD     $16, R7
    SUB     $16, R11
    B       branchN_last_cmp_loop

branchN_last_cmp_small:
    CBZ     R11, branchN_last_cmp_lengths

branchN_last_cmp_byte:
    MOVBU   (R10), R12
    MOVBU   (R7), R13
    CMP     R13, R12
    BNE     branchN_last_diff_byte
    ADD     $1, R10
    ADD     $1, R7
    SUB     $1, R11
    CBNZ    R11, branchN_last_cmp_byte

branchN_last_cmp_lengths:
    CMP     R8, R2
    BLT     branchN_last_less
    // searchKey >= last: return n-1 (rightmost child)
    MOVD    R4, ret+56(FP)
    RET

branchN_last_found_diff:
    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    CMP     $-1, R12
    BNE     branchN_last_diff_lower
    ADD     $8, R10
    ADD     $8, R7
    MOVD    R13, R12
    B       branchN_last_find_byte

branchN_last_diff_lower:
branchN_last_find_byte:
    MVN     R12, R12
    RBIT    R12, R12
    CLZ     R12, R12
    LSR     $3, R12
    MOVBU   (R10)(R12), R13
    MOVBU   (R7)(R12), R14
    CMP     R14, R13
    BLO     branchN_last_less
    // searchKey > last: return n-1
    MOVD    R4, ret+56(FP)
    RET

branchN_last_diff_byte:
    BLO     branchN_last_less
    MOVD    R4, ret+56(FP)
    RET

branchN_last_less:
    // searchKey < last: binary search from 1 to n-2
    MOVD    $1, R5                       // low = 1
    SUB     $1, R4, R6                   // high = n-2

branchN_loop:
    CMP     R6, R5
    BGT     branchN_done

    // mid = (low + high) / 2
    ADD     R6, R5, R4
    LSR     $1, R4

    // Get entry offset for mid
    ADD     R4, R4, R7
    ADD     $PAGE_HEADER_SIZE, R7
    ADD     R0, R7
    MOVHU   (R7), R8
    ADD     $PAGE_HEADER_SIZE, R8

    // Get key info
    ADD     R0, R8, R7
    ADD     $6, R7, R9
    MOVHU   (R9), R8                     // R8 = nodeKeyLen
    ADD     $NODE_SIZE, R7               // R7 = nodeKey pointer

    // Compare
    CMP     R8, R2
    CSEL    LT, R2, R8, R9

    MOVD    R1, R10
    MOVD    R9, R11

branchN_mid_cmp_loop:
    CMP     $16, R11
    BLT     branchN_mid_cmp_small

    VLD1    (R10), [V0.B16]
    VLD1    (R7), [V1.B16]
    VCMEQ   V1.B16, V0.B16, V2.B16

    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    AND     R12, R13, R12
    CMP     $-1, R12
    BNE     branchN_mid_found_diff

    ADD     $16, R10
    ADD     $16, R7
    SUB     $16, R11
    B       branchN_mid_cmp_loop

branchN_mid_cmp_small:
    CBZ     R11, branchN_mid_cmp_lengths

branchN_mid_cmp_byte:
    MOVBU   (R10), R12
    MOVBU   (R7), R13
    CMP     R13, R12
    BNE     branchN_mid_diff_byte
    ADD     $1, R10
    ADD     $1, R7
    SUB     $1, R11
    CBNZ    R11, branchN_mid_cmp_byte

branchN_mid_cmp_lengths:
    CMP     R8, R2
    BLT     branchN_go_left
    BGT     branchN_go_right
    // Equal: return mid
    MOVD    R4, ret+56(FP)
    RET

branchN_mid_found_diff:
    VMOV    V2.D[0], R12
    VMOV    V2.D[1], R13
    CMP     $-1, R12
    BNE     branchN_mid_diff_lower
    ADD     $8, R10
    ADD     $8, R7
    MOVD    R13, R12
    B       branchN_mid_find_byte

branchN_mid_diff_lower:
branchN_mid_find_byte:
    MVN     R12, R12
    RBIT    R12, R12
    CLZ     R12, R12
    LSR     $3, R12
    MOVBU   (R10)(R12), R13
    MOVBU   (R7)(R12), R14
    CMP     R14, R13
    BLO     branchN_go_left
    B       branchN_go_right

branchN_mid_diff_byte:
    BLO     branchN_go_left
    B       branchN_go_right

branchN_go_left:
    SUB     $1, R4, R6
    B       branchN_loop

branchN_go_right:
    ADD     $1, R4, R5
    B       branchN_loop

branchN_done:
    // Return low - 1 (for branch pages)
    SUB     $1, R5, R0
    MOVD    R0, ret+56(FP)
    RET

branchN_return_zero:
    MOVD    $0, R0
    MOVD    R0, ret+56(FP)
    RET
