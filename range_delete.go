package gdbx

import (
	"encoding/binary"
	"unsafe"

	"github.com/Giulio2002/gdbx/spill"
)

// DeleteRange deletes all entries in the key range [from, to).
// For DupSort databases, all duplicate values for each key in the range are deleted.
// If to is nil, deletes from `from` to the end of the database.
// If from is nil, deletes from the beginning of the database to `to`.
// If both from and to are nil, deletes all entries (equivalent to Drop without delete).
// Returns the number of entries deleted (for DupSort, counts each duplicate value).
//
// This method uses B-tree-aware optimization: when entire subtrees fall within
// the deletion range, their pages are freed in bulk without visiting individual entries.
func (txn *Txn) DeleteRange(dbi DBI, from, to []byte) (int64, error) {
	if !txn.valid() {
		return 0, NewError(ErrBadTxn)
	}

	if txn.IsReadOnly() {
		return 0, NewError(ErrPermissionDenied)
	}

	if int(dbi) >= len(txn.trees) {
		return 0, NewError(ErrBadDBI)
	}

	// FreeDBI (0) is the GC database, not accessible for normal operations
	if dbi == FreeDBI {
		return 0, NewError(ErrBadDBI)
	}

	tree := &txn.trees[dbi]
	if tree.isEmpty() {
		return 0, nil
	}

	isDupSort := tree.Flags&uint16(DupSort) != 0

	// Special case: delete all entries (both from and to are nil)
	if from == nil && to == nil {
		deleted := int64(tree.Items)
		txn.drFreeTreeRecursive(tree.Root, int(tree.Height), isDupSort, tree)
		tree.Root = invalidPgno
		tree.Height = 0
		tree.Items = 0
		tree.LeafPages = 0
		tree.BranchPages = 0
		tree.LargePages = 0
		tree.ModTxnid = txnid(txn.txnID)
		txn.drMarkDbiDirty(dbi)
		return deleted, nil
	}

	// Cache comparator
	txn.cacheComparator(dbi)
	cmp := txn.dbiComparators[dbi]

	// Validate from < to when both are set
	if from != nil && to != nil && cmp(from, to) >= 0 {
		return 0, nil
	}

	newRoot, deleted, err := txn.drDeleteInPage(tree.Root, int(tree.Height), from, to, cmp, isDupSort, tree)
	if err != nil {
		return deleted, err
	}

	if newRoot == tree.Root && deleted == 0 {
		return 0, nil
	}

	tree.Root = newRoot
	tree.Items -= uint64(deleted)
	tree.ModTxnid = txnid(txn.txnID)
	txn.drMarkDbiDirty(dbi)

	// Collapse tree height: if root branch has a single child, make child the new root
	for tree.Height > 1 && tree.Root != invalidPgno {
		rootPage, err := txn.getPage(tree.Root)
		if err != nil || rootPage.isLeaf() || rootPage.numEntries() != 1 {
			break
		}
		childPgno := nodeGetChildPgnoDirect(rootPage, 0)
		txn.freePages = append(txn.freePages, tree.Root)
		if tree.BranchPages > 0 {
			tree.BranchPages--
		}
		tree.Root = childPgno
		tree.Height--
	}

	// Handle empty root page
	if tree.Root != invalidPgno {
		rootPage, err := txn.getPage(tree.Root)
		if err == nil && rootPage.numEntries() == 0 {
			txn.freePages = append(txn.freePages, tree.Root)
			if rootPage.isLeaf() && tree.LeafPages > 0 {
				tree.LeafPages--
			} else if rootPage.isBranch() && tree.BranchPages > 0 {
				tree.BranchPages--
			}
			tree.Root = invalidPgno
			tree.Height = 0
		}
	}

	return deleted, nil
}

// drDeleteInPage dispatches to leaf or branch handler based on tree height.
func (txn *Txn) drDeleteInPage(pg pgno, height int, from, to []byte, cmp func([]byte, []byte) int, isDupSort bool, tree *tree) (pgno, int64, error) {
	if height <= 0 || pg == invalidPgno {
		return pg, 0, nil
	}
	if height == 1 {
		return txn.drDeleteInLeaf(pg, from, to, cmp, isDupSort, tree)
	}
	return txn.drDeleteInBranch(pg, height, from, to, cmp, isDupSort, tree)
}

// drDeleteInLeaf deletes entries in [from, to) from a leaf page using binary search.
func (txn *Txn) drDeleteInLeaf(pg pgno, from, to []byte, cmp func([]byte, []byte) int, isDupSort bool, tree *tree) (pgno, int64, error) {
	p, err := txn.getPage(pg)
	if err != nil {
		return pg, 0, err
	}

	numEntries := p.numEntries()
	if numEntries == 0 {
		return pg, 0, nil
	}

	// Binary search for startIdx: first entry with key >= from
	startIdx := 0
	if from != nil {
		lo, hi := 0, numEntries
		for lo < hi {
			mid := (lo + hi) / 2
			if cmp(nodeGetKeyDirect(p, mid), from) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		startIdx = lo
	}

	// Binary search for endIdx: first entry with key >= to
	endIdx := numEntries
	if to != nil {
		lo, hi := startIdx, numEntries
		for lo < hi {
			mid := (lo + hi) / 2
			if cmp(nodeGetKeyDirect(p, mid), to) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		endIdx = lo
	}

	if startIdx >= endIdx {
		return pg, 0, nil // Nothing to delete in this page
	}

	// COW the leaf page
	newPg, dirtyPage, err := txn.drCowPage(pg)
	if err != nil {
		return pg, 0, err
	}

	// Free resources for each entry being deleted and count items
	var deleted int64
	for i := startIdx; i < endIdx; i++ {
		flags := nodeGetFlagsDirect(dirtyPage, i)

		// Free overflow pages for large values
		if flags&nodeBig != 0 {
			overflowPg := nodeGetOverflowPgnoDirect(dirtyPage, i)
			dataSize := nodeGetDataSizeDirect(dirtyPage, i)
			txn.drFreeOverflowPages(overflowPg, dataSize, tree)
		}

		if isDupSort {
			deleted += txn.drCountAndFreeDupItems(dirtyPage, i, flags, tree)
		} else {
			deleted++
		}
	}

	// If ALL entries are being deleted, free the entire page
	if startIdx == 0 && endIdx == numEntries {
		txn.freePages = append(txn.freePages, newPg)
		if tree.LeafPages > 0 {
			tree.LeafPages--
		}
		return invalidPgno, deleted, nil
	}

	// Remove entries in reverse order for correct index handling
	for i := endIdx - 1; i >= startIdx; i-- {
		dirtyPage.removeEntry(i)
	}

	return newPg, deleted, nil
}

// drDeleteInBranch handles deletion in a branch page.
// It identifies fully-contained children (freeing their subtrees in bulk)
// and recursively processes boundary children.
func (txn *Txn) drDeleteInBranch(pg pgno, height int, from, to []byte, cmp func([]byte, []byte) int, isDupSort bool, tree *tree) (pgno, int64, error) {
	p, err := txn.getPage(pg)
	if err != nil {
		return pg, 0, err
	}

	numEntries := p.numEntries()
	if numEntries == 0 {
		return pg, 0, nil
	}

	// In a branch page, entry i has key K[i] and child C[i].
	// C[i]'s subtree contains keys in [K[i], K[i+1]) where K[N] = +infinity.

	// Find startChild: rightmost child where K[i] <= from
	startChild := 0
	if from != nil {
		lo, hi := 0, numEntries-1
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if cmp(nodeGetKeyDirect(p, mid), from) <= 0 {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		startChild = lo
	}

	// Find endChild: rightmost child where K[i] < to
	endChild := numEntries - 1
	if to != nil {
		lo, hi := 0, numEntries-1
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if cmp(nodeGetKeyDirect(p, mid), to) < 0 {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		endChild = lo

		// Edge case: if K[endChild] >= to, no children have keys < to
		if cmp(nodeGetKeyDirect(p, endChild), to) >= 0 {
			if endChild == 0 {
				return pg, 0, nil
			}
			endChild--
		}
	}

	if startChild > endChild {
		return pg, 0, nil
	}

	// Phase 1: Determine which children are fully contained vs boundary,
	// and recurse into boundary children. Defer subtree freeing to Phase 2.
	type childResult struct {
		origPgno pgno
		newPgno  pgno
		deleted  int64
		fully    bool // fully contained — subtree to be freed in Phase 2
	}
	rangeSize := endChild - startChild + 1
	results := make([]childResult, rangeSize)
	var totalDeleted int64
	hasModifications := false

	for i := startChild; i <= endChild; i++ {
		childPgno := nodeGetChildPgnoDirect(p, i)
		ri := i - startChild

		if txn.drIsChildFullyContained(p, i, numEntries, from, to, cmp) {
			// Mark for subtree freeing in Phase 2
			results[ri] = childResult{origPgno: childPgno, newPgno: invalidPgno, fully: true}
			hasModifications = true
		} else {
			// Boundary child — determine sub-range and recurse
			var subFrom, subTo []byte
			if i == startChild && i == endChild {
				subFrom, subTo = from, to
			} else if i == startChild {
				subFrom = from // to=nil: delete from `from` to end of subtree
			} else if i == endChild {
				subTo = to // from=nil: delete from start of subtree to `to`
			}
			// Middle children (startChild < i < endChild) are always fully contained,
			// so this else-branch only handles startChild and endChild.

			newChild, deleted, err := txn.drDeleteInPage(childPgno, height-1, subFrom, subTo, cmp, isDupSort, tree)
			if err != nil {
				return pg, totalDeleted + deleted, err
			}

			results[ri] = childResult{origPgno: childPgno, newPgno: newChild, deleted: deleted}
			totalDeleted += deleted

			if deleted > 0 || newChild != childPgno {
				hasModifications = true
			}
		}
	}

	if !hasModifications {
		return pg, totalDeleted, nil
	}

	// Phase 2: COW the branch page, free fully-contained subtrees, and apply changes.
	newPg, dirtyPage, err := txn.drCowPage(pg)
	if err != nil {
		return pg, totalDeleted, err
	}

	// Free subtrees of fully-contained children
	for i := 0; i < rangeSize; i++ {
		if !results[i].fully {
			continue
		}
		items, err := txn.drFreeSubtreeRecursive(results[i].origPgno, height-1, isDupSort, tree)
		if err != nil {
			return newPg, totalDeleted + items, err
		}
		results[i].deleted = items
		totalDeleted += items
	}

	// Apply changes in reverse order for correct index handling
	for i := endChild; i >= startChild; i-- {
		r := results[i-startChild]
		if r.fully || r.newPgno == invalidPgno {
			// Remove entry for freed/empty child
			dirtyPage.removeEntry(i)
		} else if r.newPgno != r.origPgno {
			// Update child pointer (child was COW'd)
			drUpdateBranchChildPgno(dirtyPage, i, r.newPgno)
		}
	}

	// Check if branch page is now empty
	if dirtyPage.numEntries() == 0 {
		txn.freePages = append(txn.freePages, newPg)
		if tree.BranchPages > 0 {
			tree.BranchPages--
		}
		return invalidPgno, totalDeleted, nil
	}

	return newPg, totalDeleted, nil
}

// drIsChildFullyContained checks whether child i's entire subtree is within [from, to).
// Child i's subtree covers keys in [K[i], K[i+1]) where K[numEntries] = +infinity.
func (txn *Txn) drIsChildFullyContained(p *page, i, numEntries int, from, to []byte, cmp func([]byte, []byte) int) bool {
	// Lower bound check: K[i] >= from
	if from != nil {
		if cmp(nodeGetKeyDirect(p, i), from) < 0 {
			return false
		}
	}

	// Upper bound check: K[i+1] <= to (or last child and to is nil)
	if i < numEntries-1 {
		nextKey := nodeGetKeyDirect(p, i+1)
		if to == nil {
			// to=nil means delete to end; this non-last child is bounded by K[i+1]
			return true
		}
		if cmp(nextKey, to) > 0 {
			return false
		}
	} else {
		// Last child: subtree extends to +infinity, only fully contained if to is nil
		if to != nil {
			return false
		}
	}

	return true
}

// drFreeSubtreeRecursive walks all pages in a subtree, frees them, and returns the item count.
func (txn *Txn) drFreeSubtreeRecursive(pg pgno, height int, isDupSort bool, tree *tree) (int64, error) {
	if pg == invalidPgno || height <= 0 {
		return 0, nil
	}

	p, err := txn.getPage(pg)
	if err != nil {
		return 0, err
	}

	numEntries := p.numEntries()
	var items int64

	if height == 1 {
		// Leaf page: count items and free associated resources
		for i := 0; i < numEntries; i++ {
			flags := nodeGetFlagsDirect(p, i)

			if flags&nodeBig != 0 {
				overflowPg := nodeGetOverflowPgnoDirect(p, i)
				dataSize := nodeGetDataSizeDirect(p, i)
				txn.drFreeOverflowPages(overflowPg, dataSize, tree)
			}

			if isDupSort {
				items += txn.drCountAndFreeDupItems(p, i, flags, tree)
			} else {
				items++
			}
		}

		txn.freePages = append(txn.freePages, pg)
		if tree.LeafPages > 0 {
			tree.LeafPages--
		}
	} else {
		// Branch page: recurse into all children, then free self
		for i := 0; i < numEntries; i++ {
			childPgno := nodeGetChildPgnoDirect(p, i)
			childItems, err := txn.drFreeSubtreeRecursive(childPgno, height-1, isDupSort, tree)
			if err != nil {
				return items + childItems, err
			}
			items += childItems
		}

		txn.freePages = append(txn.freePages, pg)
		if tree.BranchPages > 0 {
			tree.BranchPages--
		}
	}

	return items, nil
}

// drCountAndFreeDupItems counts duplicate items for a DupSort entry and frees sub-tree pages if applicable.
func (txn *Txn) drCountAndFreeDupItems(p *page, idx int, flags nodeFlags, tree *tree) int64 {
	if flags&nodeTree != 0 {
		// N_TREE: sub-tree with separate pages
		// Tree struct layout: Flags(2) Height(2) DupfixSize(4) Root(4) BranchPages(4)
		//   LeafPages(4) LargePages(4) Sequence(8) Items(8) ModTxnid(8) = 48 bytes
		data := nodeGetDataDirect(p, idx)
		if data != nil && len(data) >= 48 {
			dupItems := int64(binary.LittleEndian.Uint64(data[32:40]))
			// Free all pages in the sub-tree
			subRoot := pgno(binary.LittleEndian.Uint32(data[8:12]))
			subHeight := int(binary.LittleEndian.Uint16(data[2:4]))
			txn.drFreeAllSubTreePages(subRoot, subHeight)
			return dupItems
		}
	} else if flags&nodeDup != 0 {
		// N_DUP: inline sub-page
		// Sub-page header has lower field at offset 12-14; numEntries = lower >> 1
		data := nodeGetDataDirect(p, idx)
		if data != nil && len(data) >= 14 {
			lower := binary.LittleEndian.Uint16(data[12:14])
			return int64(lower >> 1)
		}
	}
	return 1
}

// drFreeAllSubTreePages recursively frees all pages in a DupSort sub-tree.
func (txn *Txn) drFreeAllSubTreePages(root pgno, height int) {
	if root == invalidPgno || height <= 0 {
		return
	}

	p, err := txn.getPage(root)
	if err != nil {
		return
	}

	if height > 1 {
		numEntries := p.numEntries()
		for i := 0; i < numEntries; i++ {
			childPgno := nodeGetChildPgnoDirect(p, i)
			txn.drFreeAllSubTreePages(childPgno, height-1)
		}
	}

	txn.freePages = append(txn.freePages, root)
}

// drFreeTreeRecursive frees all pages in a tree, including overflow and DupSort sub-tree resources.
// Used for the delete-all optimization (from=nil, to=nil).
func (txn *Txn) drFreeTreeRecursive(pg pgno, height int, isDupSort bool, tree *tree) {
	if pg == invalidPgno || height <= 0 {
		return
	}

	p, err := txn.getPage(pg)
	if err != nil {
		return
	}

	numEntries := p.numEntries()

	if height == 1 {
		// Leaf: free overflow and sub-tree resources
		for i := 0; i < numEntries; i++ {
			flags := nodeGetFlagsDirect(p, i)
			if flags&nodeBig != 0 {
				overflowPg := nodeGetOverflowPgnoDirect(p, i)
				dataSize := nodeGetDataSizeDirect(p, i)
				txn.drFreeOverflowPages(overflowPg, dataSize, tree)
			}
			if isDupSort && flags&nodeTree != 0 {
				data := nodeGetDataDirect(p, i)
				if data != nil && len(data) >= 12 {
					subRoot := pgno(binary.LittleEndian.Uint32(data[8:12]))
					subHeight := int(binary.LittleEndian.Uint16(data[2:4]))
					txn.drFreeAllSubTreePages(subRoot, subHeight)
				}
			}
		}
	} else {
		// Branch: recurse into all children
		for i := 0; i < numEntries; i++ {
			childPgno := nodeGetChildPgnoDirect(p, i)
			txn.drFreeTreeRecursive(childPgno, height-1, isDupSort, tree)
		}
	}

	txn.freePages = append(txn.freePages, pg)
}

// drFreeOverflowPages frees overflow pages for a large value.
func (txn *Txn) drFreeOverflowPages(overflowPg pgno, dataSize uint32, tree *tree) {
	pageSize := int(txn.env.pageSize)
	firstPageData := pageSize - pageHeaderSize

	remaining := int(dataSize) - firstPageData
	numPages := 1
	if remaining > 0 {
		numPages += (remaining + pageSize - 1) / pageSize
	}

	for i := 0; i < numPages; i++ {
		txn.freePages = append(txn.freePages, overflowPg+pgno(i))
	}
	if tree.LargePages >= pgno(numPages) {
		tree.LargePages -= pgno(numPages)
	}
}

// drCowPage creates a writable copy of a page (copy-on-write).
// Returns the (possibly new) page number, the writable page, and an error.
func (txn *Txn) drCowPage(oldPg pgno) (pgno, *page, error) {
	// Already dirty — return as-is
	if p := txn.dirtyTracker.get(oldPg); p != nil {
		return oldPg, p, nil
	}

	oldPage, err := txn.getPage(oldPg)
	if err != nil {
		return 0, nil, err
	}

	// WriteMap optimization: modify in-place if page belongs to current txn
	if txn.env.isWriteMap() && oldPage.header().Txnid == txnid(txn.txnID) {
		txn.dirtyTracker.set(oldPg, oldPage)
		return oldPg, oldPage, nil
	}

	// Allocate a new page number
	newPgno := txn.allocatedPg
	txn.allocatedPg++

	var newData []byte
	var usedMmap bool

	if txn.env.isWriteMap() {
		newData = txn.env.getMmapPageData(newPgno)
		if newData != nil {
			copy(newData, oldPage.Data)
			usedMmap = true
		}
	}
	if !usedMmap {
		var spillSlot *spill.Slot
		newData, spillSlot, err = txn.env.spillBuf.Allocate()
		if err != nil {
			panic("gdbx: spill buffer allocation failed: " + err.Error())
		}
		copy(newData, oldPage.Data)
		txn.spillSlots.Set(uint32(newPgno), unsafe.Pointer(spillSlot))
	}

	newPage := getPooledPageStruct(newData)
	txn.pooledPageStructs = append(txn.pooledPageStructs, newPage)
	newPage.header().PageNo = newPgno
	newPage.header().Txnid = txnid(txn.txnID)

	txn.dirtyTracker.set(newPgno, newPage)

	return newPgno, newPage, nil
}

// drUpdateBranchChildPgno updates a branch page entry's child pointer.
func drUpdateBranchChildPgno(p *page, idx int, newChildPgno pgno) {
	offset := p.entryOffset(idx)
	if offset == 0 && idx > 0 {
		return
	}
	// The child pgno is stored in the first 4 bytes of the node (DataSize field for branch nodes)
	binary.LittleEndian.PutUint32(p.Data[offset:], uint32(newChildPgno))
}

// drMarkDbiDirty marks a DBI as modified in this transaction.
func (txn *Txn) drMarkDbiDirty(dbi DBI) {
	if txn.dbiDirty == nil {
		txn.dbiDirty = make([]bool, len(txn.trees))
	}
	if int(dbi) < len(txn.dbiDirty) {
		txn.dbiDirty[dbi] = true
	}
}
