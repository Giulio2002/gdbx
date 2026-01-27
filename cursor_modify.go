package gdbx

import (
	"encoding/binary"
)

// put inserts or updates a key-value pair.
func (c *Cursor) put(key, value []byte, flags uint) error {
	// Validate key size
	maxKey := c.txn.env.MaxKeySize()
	if len(key) > maxKey {
		return NewError(ErrBadValSize)
	}

	// Check if this is a DUPSORT database
	isDupSort := c.tree.Flags&uint16(DupSort) != 0

	// OPTIMIZATION: Append flag - position at end without binary search
	if flags&Append != 0 {
		c.reset()
		exact, err := c.positionForAppend(key)
		if err != nil {
			return err
		}
		// For append, we never have an exact match (we're appending new key)
		// But if exact is true, it means key equals last key
		if exact && !isDupSort {
			// Update existing key
			return c.putAfterPosition(key, value, flags, true, isDupSort)
		}
		return c.putAfterPosition(key, value, flags, exact, isDupSort)
	}

	// Normal path: Search for the key position
	c.reset()
	exact, err := c.searchForInsert(key)
	if err != nil && !IsNotFound(err) {
		return err
	}

	// Handle NoOverwrite flag
	if exact && flags&NoOverwrite != 0 {
		// For DUPSORT, NoOverwrite means don't add duplicate value
		if isDupSort {
			// Check if exact value already exists
			if c.hasDupValue(value) {
				return NewError(ErrKeyExist)
			}
		} else {
			return NewError(ErrKeyExist)
		}
	}

	// Handle NoDupData flag (DUPSORT only)
	if exact && isDupSort && flags&NoDupData != 0 {
		// NoDupData returns error if exact key+value pair already exists
		if c.hasDupValue(value) {
			return NewError(ErrKeyExist)
		}
	}

	// For DUPSORT databases, handle duplicate values
	if isDupSort && exact {
		return c.putDupSort(key, value, flags)
	}

	// Determine if value is too large for inline storage
	// Must check both: value exceeds maxVal OR combined node exceeds page capacity
	maxVal := c.txn.env.MaxValSize()
	pageCapacity := int(c.txn.env.pageSize) - 20 - 2 // pageSize - header - entry pointer
	nodeSize := 8 + len(key) + len(value)            // header + key + value
	isBig := len(value) > maxVal || nodeSize > pageCapacity

	// Fast path: if updating a big value with another big value, try in-place update
	if exact && isBig {
		p := c.pages[c.top]
		idx := int(c.indices[c.top])
		oldFlags := nodeGetFlagsDirect(p, idx)
		if oldFlags&nodeBig != 0 {
			// Both old and new are big - try in-place overflow update
			oldPgno := nodeGetOverflowPgnoDirect(p, idx)
			oldSize := nodeGetDataSizeDirect(p, idx)
			if c.updateOverflowInPlace(oldPgno, oldSize, value) {
				// Update succeeded in place
				// Only update node header if size changed
				if uint32(len(value)) != oldSize {
					return c.updateBigNodeSize(uint32(len(value)))
				}
				// Same size - just mark tree dirty and we're done
				c.markTreeDirty()
				return nil
			}
		}
	}

	// Build the node (allocates new overflow pages if big)
	nodeData, overflowPgno, err := c.buildNode(key, value, isBig)
	if err != nil {
		return err
	}

	// If key exists, update it (non-DUPSORT case)
	if exact {
		return c.updateNode(nodeData, overflowPgno)
	}

	// Insert new node
	return c.insertNode(nodeData, overflowPgno)
}

// PutTree inserts or updates a sub-database entry in the main database.
// This sets the N_TREE flag on the node, which is required for libmdbx compatibility.
// The value should be a 48-byte serialized Tree structure.
func (c *Cursor) PutTree(key, treeData []byte, flags uint) error {
	// Validate key size
	maxKey := c.txn.env.MaxKeySize()
	if len(key) > maxKey {
		return NewError(ErrBadValSize)
	}

	// Tree data should never be "big" (it's always 48 bytes)
	if len(treeData) != 48 {
		return NewError(ErrBadValSize)
	}

	// Search for the key position
	c.reset()
	exact, err := c.searchForInsert(key)
	if err != nil && !IsNotFound(err) {
		return err
	}

	// Handle NoOverwrite flag
	if exact && flags&NoOverwrite != 0 {
		return NewError(ErrKeyExist)
	}

	// Build the node with N_TREE flag
	nodeData, _, err := c.buildNodeWithFlags(key, treeData, false, nodeTree)
	if err != nil {
		return err
	}

	// If key exists, update it
	if exact {
		return c.updateNode(nodeData, 0)
	}

	// Insert new node
	return c.insertNode(nodeData, 0)
}

// hasDupValue checks if the current key already has the given value (for DUPSORT).
func (c *Cursor) hasDupValue(value []byte) bool {
	if c.top < 0 || c.state != cursorPointing {
		return false
	}

	p := c.pages[c.top]
	idx := int(c.indices[c.top])
	nodeFlags := nodeGetFlagsDirect(p, idx)

	if nodeFlags&nodeDup == 0 && nodeFlags&nodeTree == 0 {
		// Single value - compare directly
		existingVal := nodeGetDataDirect(p, idx)
		return c.txn.compareDupValues(c.dbi, existingVal, value) == 0
	}

	data := nodeGetDataDirect(p, idx)
	if data == nil {
		return false
	}

	if nodeFlags&nodeTree != 0 {
		// Sub-tree: search in nested B+tree
		return c.hasDupValueInSubTree(data, value)
	}

	// Sub-page: search in inline sub-page
	return c.hasDupValueInSubPage(data, value)
}

// hasDupValueInSubPage searches for a value in an inline sub-page.
func (c *Cursor) hasDupValueInSubPage(subPageData []byte, value []byte) bool {
	if len(subPageData) < 20 {
		return false
	}

	// Parse sub-page header
	dupfixKsize := int(binary.LittleEndian.Uint16(subPageData[8:]))
	flags := binary.LittleEndian.Uint16(subPageData[10:])
	lower := int(binary.LittleEndian.Uint16(subPageData[12:]))

	// Check for DUPFIX format (P_DUPFIX = 0x20)
	if (flags&uint16(pageDupfix) != 0) && dupfixKsize > 0 && dupfixKsize < 65535 {
		// DUPFIX: fixed-size values after 20-byte header
		numEntries := (len(subPageData) - pageHeaderSize) / dupfixKsize

		// Binary search
		low, high := 0, numEntries-1
		for low <= high {
			mid := (low + high) / 2
			start := pageHeaderSize + mid*dupfixKsize
			end := start + dupfixKsize
			if end > len(subPageData) {
				return false
			}
			midVal := subPageData[start:end]
			cmp := c.txn.compareDupValues(c.dbi, midVal, value)
			if cmp < 0 {
				low = mid + 1
			} else if cmp > 0 {
				high = mid - 1
			} else {
				return true
			}
		}
		return false
	}

	// Variable-size: lower = numEntries * 2
	if lower <= 0 {
		return false
	}
	numEntries := lower / 2

	// Binary search through entry pointers
	low, high := 0, numEntries-1
	for low <= high {
		mid := (low + high) / 2

		// Get entry offset (stored at pageHeaderSize + mid*2)
		if pageHeaderSize+mid*2+2 > len(subPageData) {
			return false
		}
		storedOffset := int(binary.LittleEndian.Uint16(subPageData[pageHeaderSize+mid*2:]))
		nodeOffset := storedOffset + pageHeaderSize

		// Read node header (nodeSize bytes: 4 dataSize + 1 flags + 1 extra + 2 keySize)
		if nodeOffset+nodeSize > len(subPageData) {
			return false
		}
		keySize := int(binary.LittleEndian.Uint16(subPageData[nodeOffset+6:]))

		// In sub-pages, the "key" is actually the duplicate value
		valStart := nodeOffset + nodeSize
		valEnd := valStart + keySize
		if valEnd > len(subPageData) {
			return false
		}
		midVal := subPageData[valStart:valEnd]

		cmp := c.txn.compareDupValues(c.dbi, midVal, value)
		if cmp < 0 {
			low = mid + 1
		} else if cmp > 0 {
			high = mid - 1
		} else {
			return true
		}
	}
	return false
}

// hasDupValueInSubTree searches for a value in a sub-tree.
func (c *Cursor) hasDupValueInSubTree(treeData []byte, value []byte) bool {
	if len(treeData) < 48 {
		return false
	}

	// Parse tree root
	rootPgno := pgno(binary.LittleEndian.Uint32(treeData[8:]))
	if rootPgno == invalidPgno {
		return false
	}

	// Search in sub-tree (values are stored as keys in sub-tree)
	// Use cached mmap data to avoid race with write txn remap
	mmapData := c.txn.mmapData
	pageSize := int(c.txn.pageSize)

	currentPgno := rootPgno
	for {
		offset := int(currentPgno) * pageSize
		if offset+pageSize > len(mmapData) {
			return false
		}
		pageData := mmapData[offset : offset+pageSize]

		flags := binary.LittleEndian.Uint16(pageData[10:])
		lower := int(binary.LittleEndian.Uint16(pageData[12:]))
		n := lower >> 1

		isLeaf := flags&0x02 != 0

		if !isLeaf {
			// Branch page: binary search entries 1 to n-1
			idx := 0
			if n > 1 {
				low, high := 1, n-1
				for low <= high {
					mid := (low + high) / 2
					storedOffset := int(binary.LittleEndian.Uint16(pageData[20+mid*2:]))
					nodeOffset := storedOffset + 20
					keySize := int(binary.LittleEndian.Uint16(pageData[nodeOffset+6:]))
					keyStart := nodeOffset + 8
					if keyStart+keySize > len(pageData) {
						return false
					}
					midKey := pageData[keyStart : keyStart+keySize]

					cmp := c.txn.compareDupValues(c.dbi, value, midKey)
					if cmp < 0 {
						high = mid - 1
					} else if cmp > 0 {
						low = mid + 1
					} else {
						idx = mid
						goto descend
					}
				}
				idx = low - 1
			}
		descend:
			// Get child pgno
			storedOffset := int(binary.LittleEndian.Uint16(pageData[20+idx*2:]))
			nodeOffset := storedOffset + 20
			currentPgno = pgno(binary.LittleEndian.Uint32(pageData[nodeOffset:]))
			continue
		}

		// Leaf page: binary search
		if n == 0 {
			return false
		}

		low, high := 0, n-1
		for low <= high {
			mid := (low + high) / 2
			storedOffset := int(binary.LittleEndian.Uint16(pageData[20+mid*2:]))
			nodeOffset := storedOffset + 20
			keySize := int(binary.LittleEndian.Uint16(pageData[nodeOffset+6:]))
			keyStart := nodeOffset + 8
			if keyStart+keySize > len(pageData) {
				return false
			}
			midKey := pageData[keyStart : keyStart+keySize]

			cmp := c.txn.compareDupValues(c.dbi, value, midKey)
			if cmp < 0 {
				high = mid - 1
			} else if cmp > 0 {
				low = mid + 1
			} else {
				return true
			}
		}
		return false
	}
}

// positionForAppend positions the cursor at the end of the tree for append operations.
// This is an optimization that avoids binary search when we know we're appending.
// Returns (exact, error) where exact=true if key equals last key (for update).
func (c *Cursor) positionForAppend(key []byte) (bool, error) {
	if c.tree.isEmpty() {
		// Empty tree - nothing to position
		return false, nil
	}

	// Navigate to the rightmost leaf, building cursor stack
	currentPgno := c.tree.Root
	for {
		p, err := c.txn.getPage(currentPgno)
		if err != nil {
			return false, err
		}

		if err := c.pushPage(p, 0); err != nil {
			return false, err
		}

		numEntries := p.numEntries()
		if numEntries == 0 {
			return false, nil
		}

		if p.isLeaf() {
			// At leaf - position at last entry
			lastIdx := numEntries - 1
			c.indices[c.top] = uint16(lastIdx)

			// Get last key and compare
			lastKey := nodeGetKeyDirect(p, lastIdx)
			if lastKey == nil {
				return false, ErrCorruptedError
			}

			cmp := c.txn.compareKeys(c.dbi, key, lastKey)
			if cmp < 0 {
				return false, NewError(ErrKeyMismatch)
			}
			if cmp == 0 {
				// Exact match - cursor at existing key
				c.state = cursorPointing
				return true, nil
			}

			// key > lastKey - position after last entry for insert
			c.indices[c.top] = uint16(numEntries)
			c.state = cursorPointing
			return false, nil
		}

		// Branch page - go to rightmost child
		lastIdx := numEntries - 1
		c.indices[c.top] = uint16(lastIdx)
		childPgno := c.getChildPgno(p, lastIdx)
		currentPgno = childPgno
	}
}

// putAfterPosition completes the put operation after cursor is positioned.
// This is used by the append optimization path.
func (c *Cursor) putAfterPosition(key, value []byte, flags uint, exact, isDupSort bool) error {
	// For DUPSORT databases, handle duplicate values
	if isDupSort && exact {
		return c.putDupSort(key, value, flags)
	}

	// Determine if value is too large for inline storage
	// Must check both: value exceeds maxVal OR combined node exceeds page capacity
	maxVal := c.txn.env.MaxValSize()
	pageCapacity := int(c.txn.env.pageSize) - 20 - 2 // pageSize - header - entry pointer
	nodeSize := 8 + len(key) + len(value)            // header + key + value
	isBig := len(value) > maxVal || nodeSize > pageCapacity

	// Fast path: if updating a big value with another big value, try in-place update
	if exact && isBig {
		p := c.pages[c.top]
		idx := int(c.indices[c.top])
		oldFlags := nodeGetFlagsDirect(p, idx)
		if oldFlags&nodeBig != 0 {
			// Both old and new are big - try in-place overflow update
			oldPgno := nodeGetOverflowPgnoDirect(p, idx)
			oldSize := nodeGetDataSizeDirect(p, idx)
			if c.updateOverflowInPlace(oldPgno, oldSize, value) {
				// Update succeeded in place
				// Only update node header if size changed
				if uint32(len(value)) != oldSize {
					return c.updateBigNodeSize(uint32(len(value)))
				}
				// Same size - just mark tree dirty and we're done
				c.markTreeDirty()
				return nil
			}
		}
	}

	// Build the node (allocates new overflow pages if big)
	nodeData, overflowPgno, err := c.buildNode(key, value, isBig)
	if err != nil {
		return err
	}

	// If key exists, update it (non-DUPSORT case)
	if exact {
		return c.updateNode(nodeData, overflowPgno)
	}

	// Insert new node
	return c.insertNode(nodeData, overflowPgno)
}

// checkAppendDup verifies the AppendDup constraint - value must be >= last value for this key.
func (c *Cursor) checkAppendDup(value []byte) error {
	if c.top < 0 {
		return nil
	}

	p := c.pages[c.top]
	idx := int(c.indices[c.top])
	nodeFlags := nodeGetFlagsDirect(p, idx)

	// Get the last duplicate value for this key
	var lastValue []byte

	if nodeFlags&nodeTree != 0 {
		// Sub-tree: find last value in nested B+tree
		lastValue = c.getLastDupValueSubTree(p, idx)
	} else if nodeFlags&nodeDup != 0 {
		// Sub-page: get last value from inline sub-page
		lastValue = c.getLastDupValueSubPage(p, idx)
	} else {
		// Single value - get it directly
		lastValue = nodeGetDataDirect(p, idx)
	}

	if lastValue == nil {
		// No existing values - append is always valid
		return nil
	}

	// Compare value with last value
	cmp := c.txn.compareDupValues(c.dbi, value, lastValue)
	if cmp < 0 {
		return NewError(ErrKeyMismatch)
	}
	return nil
}

// getLastDupValueSubPage returns the last duplicate value from an inline sub-page.
func (c *Cursor) getLastDupValueSubPage(p *page, idx int) []byte {
	data := nodeGetDataDirect(p, idx)
	if len(data) < pageHeaderSize {
		return nil
	}

	// Parse sub-page header
	dupfixKsize := binary.LittleEndian.Uint16(data[8:10])
	flags := binary.LittleEndian.Uint16(data[10:12])
	lower := binary.LittleEndian.Uint16(data[12:14])

	// Check for DUPFIX format
	if flags&uint16(pageDupfix) != 0 && dupfixKsize > 0 && dupfixKsize < 65535 {
		numValues := (len(data) - pageHeaderSize) / int(dupfixKsize)
		if numValues == 0 {
			return nil
		}
		// Last value is at the end
		start := pageHeaderSize + (numValues-1)*int(dupfixKsize)
		return data[start : start+int(dupfixKsize)]
	}

	// Variable-size values
	numEntries := int(lower) / 2
	if numEntries == 0 {
		return nil
	}

	// Get last entry (highest index)
	lastIdx := numEntries - 1
	offsetPos := pageHeaderSize + lastIdx*2
	if offsetPos+2 > len(data) {
		return nil
	}
	storedOffset := int(binary.LittleEndian.Uint16(data[offsetPos:]))
	nodePos := storedOffset + pageHeaderSize
	if nodePos+nodeSize > len(data) {
		return nil
	}
	keySize := int(binary.LittleEndian.Uint16(data[nodePos+6:]))
	valueStart := nodePos + nodeSize
	valueEnd := valueStart + keySize
	if valueEnd > len(data) {
		return nil
	}
	return data[valueStart:valueEnd]
}

// getLastDupValueSubTree returns the last duplicate value from a sub-tree.
func (c *Cursor) getLastDupValueSubTree(p *page, idx int) []byte {
	treeData := nodeGetDataDirect(p, idx)
	if treeData == nil || len(treeData) < 48 {
		return nil
	}

	// Parse tree root
	rootPgno := pgno(binary.LittleEndian.Uint32(treeData[8:12]))
	if rootPgno == invalidPgno {
		return nil
	}

	// Navigate to rightmost leaf
	// Use cached mmap data to avoid race with write txn remap
	mmapData := c.txn.mmapData
	pageSize := int(c.txn.pageSize)

	currentPgno := rootPgno
	for {
		offset := int(currentPgno) * pageSize
		if offset+pageSize > len(mmapData) {
			return nil
		}
		pageData := mmapData[offset : offset+pageSize]

		flags := binary.LittleEndian.Uint16(pageData[10:])
		lower := int(binary.LittleEndian.Uint16(pageData[12:]))
		n := lower >> 1

		isLeaf := flags&0x02 != 0

		if !isLeaf {
			// Branch page: go to rightmost child
			if n == 0 {
				return nil
			}
			lastIdx := n - 1
			storedOffset := int(binary.LittleEndian.Uint16(pageData[20+lastIdx*2:]))
			nodeOffset := storedOffset + 20
			currentPgno = pgno(binary.LittleEndian.Uint32(pageData[nodeOffset:]))
			continue
		}

		// Leaf page: get last entry
		if n == 0 {
			return nil
		}
		lastIdx := n - 1
		storedOffset := int(binary.LittleEndian.Uint16(pageData[20+lastIdx*2:]))
		nodeOffset := storedOffset + 20
		keySize := int(binary.LittleEndian.Uint16(pageData[nodeOffset+6:]))
		keyStart := nodeOffset + 8
		keyEnd := keyStart + keySize
		if keyEnd > len(pageData) {
			return nil
		}
		// Use three-index slice to cap capacity at length
		return pageData[keyStart:keyEnd:keyEnd]
	}
}

// putDupSort handles insertion of a duplicate value for DUPSORT databases.
func (c *Cursor) putDupSort(key, value []byte, flags uint) error {
	if c.top < 0 {
		return ErrCorruptedError
	}

	// Handle AppendDup flag - value must be >= last value for this key
	if flags&AppendDup != 0 {
		if err := c.checkAppendDup(value); err != nil {
			return err
		}
	}

	// Get dirty copy of current page
	p, err := c.touchPage()
	if err != nil {
		return err
	}

	idx := int(c.indices[c.top])
	nodeFlags := nodeGetFlagsDirect(p, idx)

	// Check current node type
	if nodeFlags&nodeTree != 0 {
		// Sub-tree: insert into nested B+tree
		return c.putDupSubTree(key, value)
	}

	if nodeFlags&nodeDup != 0 {
		// Inline sub-page: add value to sub-page
		return c.putDupSubPage(p, idx, key, value)
	}

	// Single value: convert to sub-page with two values
	return c.convertToDupSubPage(p, idx, key, value)
}

// convertToDupSubPage converts a single-value node to a sub-page with two values.
func (c *Cursor) convertToDupSubPage(p *page, idx int, key, newValue []byte) error {
	// Get existing value
	existingValue := nodeGetDataDirect(p, idx)
	if existingValue == nil {
		return ErrCorruptedError
	}

	// Make a copy since we're going to modify the page
	existingValue = append([]byte(nil), existingValue...)

	// Compare values to determine order
	cmp := c.txn.compareDupValues(c.dbi, existingValue, newValue)
	if cmp == 0 {
		// Same value - no change needed
		return nil
	}

	var val1, val2 []byte
	if cmp < 0 {
		val1 = existingValue
		val2 = newValue
	} else {
		val1 = newValue
		val2 = existingValue
	}

	// Build sub-page with two values
	values := [][]byte{val1, val2}
	subPageData := c.buildDupSubPage(values)

	// Check total node size (header + key + subpage data) against leaf_nodemax
	// This ensures the node will actually fit on a page
	totalNodeSize := nodeSize + len(key) + len(subPageData)
	if totalNodeSize > c.txn.env.LeafNodeMax() {
		// Node too large for inline sub-page, convert directly to sub-tree
		return c.convertToSubTree(p, idx, key, values)
	}

	// Build new node with Dup flag
	nodeData := c.buildDupNode(key, subPageData)

	// Update the node
	if p.updateEntry(idx, nodeData) {
		c.pages[c.top] = p
		// Increment Items for the new duplicate value (for mdbx compatibility,
		// Items counts total data items, not just unique keys)
		c.tree.Items++
		c.markTreeDirty()
		return nil
	}

	// Not enough space - need to delete and reinsert
	p.removeEntry(idx)
	err := c.insertNodeAt(p, idx, nodeData, 0, true)
	if err == nil {
		// Increment Items for the new duplicate value
		c.tree.Items++
	}
	return err
}

// putDupSubPage adds a value to an existing inline sub-page.
func (c *Cursor) putDupSubPage(p *page, idx int, key, value []byte) error {
	// Get existing sub-page data
	existingData := nodeGetDataDirect(p, idx)
	if existingData == nil || len(existingData) < 16 {
		return ErrCorruptedError
	}

	// Parse sub-page to get existing values
	values, err := c.parseSubPageValues(existingData)
	if err != nil {
		return err
	}

	// Find insert position (maintain sorted order)
	insertPos := 0
	for i, v := range values {
		cmp := c.txn.compareDupValues(c.dbi, value, v)
		if cmp == 0 {
			// Value already exists
			return nil
		}
		if cmp < 0 {
			break
		}
		insertPos = i + 1
	}

	// Insert new value at position
	newValues := make([][]byte, len(values)+1)
	copy(newValues[:insertPos], values[:insertPos])
	newValues[insertPos] = value
	copy(newValues[insertPos+1:], values[insertPos:])

	// Check if we need to convert to sub-tree
	subPageData := c.buildDupSubPage(newValues)
	// Check total node size (header + key + subpage data) against leaf_nodemax
	// This ensures the node will actually fit on a page
	totalNodeSize := nodeSize + len(key) + len(subPageData)
	if totalNodeSize > c.txn.env.LeafNodeMax() {
		// Convert to sub-tree
		return c.convertToSubTree(p, idx, key, newValues)
	}

	// Build new node
	nodeData := c.buildDupNode(key, subPageData)

	// Update the node
	if p.updateEntry(idx, nodeData) {
		c.pages[c.top] = p
		// Increment Items for the new duplicate value (for mdbx compatibility,
		// Items counts total data items, not just unique keys)
		c.tree.Items++
		c.markTreeDirty()
		return nil
	}

	// UpdateEntry failed - might be due to fragmentation (holes from previous updates).
	// Try compacting the page and retrying.
	pageSize := uint16(c.txn.env.pageSize)
	tempData := make([]byte, pageSize)
	tempPage := &page{Data: tempData}
	p.compactTo(tempPage, pageSize)

	// Try update on compacted page
	if tempPage.updateEntry(idx, nodeData) {
		// Copy compacted page back
		copy(p.Data, tempPage.Data)
		c.pages[c.top] = p
		// Increment Items for the new duplicate value
		c.tree.Items++
		c.markTreeDirty()
		return nil
	}

	// Still not enough space on page - need to split
	p.removeEntry(idx)
	err = c.insertNodeAt(p, idx, nodeData, 0, true)
	if err == nil {
		// Increment Items for the new duplicate value
		c.tree.Items++
	}
	return err
}

// convertToSubTree converts an inline sub-page to a sub-tree (nested B+tree).
// This is called when the sub-page becomes too large to fit inline.
func (c *Cursor) convertToSubTree(p *page, idx int, key []byte, values [][]byte) error {
	// Allocate a new leaf page for the sub-tree root
	subRootPgno, subRoot, err := c.allocatePage()
	if err != nil {
		return err
	}

	// Initialize as a leaf page
	pageSize := uint16(c.txn.env.pageSize)
	subRoot.init(subRootPgno, pageLeaf, pageSize)
	subRoot.header().Txnid = txnid(c.txn.txnID)

	// Check if this is DUPFIXED (fixed-size values)
	isDupFixed := c.tree.Flags&uint16(DupFixed) != 0
	var dupfixSize uint32 = 0
	if isDupFixed && len(values) > 0 {
		dupfixSize = uint32(len(values[0]))
		// Set DUPFIX flag on the page
		subRoot.header().Flags |= pageDupfix
		subRoot.header().DupfixKsize = uint16(dupfixSize)
	}

	// Insert all values into the sub-tree root page
	// In a DUPSORT sub-tree, each value becomes a key with empty data
	for i, val := range values {
		if isDupFixed {
			// For DUPFIX pages, values are stored contiguously after header
			// without node headers or entry indices
			offset := pageHeaderSize + i*int(dupfixSize)
			copy(subRoot.Data[offset:], val)
		} else {
			// Build a leaf node for the sub-tree
			// In sub-trees: the "key" is the duplicate value, data is empty
			subNode := c.buildSubTreeNode(val)
			if !subRoot.insertEntry(i, subNode) {
				// Page is full - this shouldn't happen for a fresh page
				// with the same data that fit in a sub-page
				return NewError(ErrPageFull)
			}
		}
	}

	// For DUPFIX pages, manually set lower (no entries pointer) and upper
	if isDupFixed {
		subRoot.header().Lower = 0 // No entry pointers for DUPFIX
		subRoot.header().Upper = pageSize - pageHeaderSize - uint16(len(values))*uint16(dupfixSize)
	}

	// Create the tree_t structure for the sub-tree
	subTree := tree{
		Flags:       flags_db2sub(c.tree.Flags),
		Height:      1,
		DupfixSize:  dupfixSize,
		Root:        subRootPgno,
		BranchPages: 0,
		LeafPages:   1,
		LargePages:  0,
		Sequence:    0,
		Items:       uint64(len(values)),
		ModTxnid:    txnid(c.txn.txnID),
	}

	// Build new node with N_DUP | N_TREE flags (serializes tree directly, no allocation)
	nodeData := c.buildNodeWithDupTree(key, &subTree)

	// Replace the existing node
	if p.updateEntry(idx, nodeData) {
		c.pages[c.top] = p
		c.tree.LeafPages++ // Sub-tree adds a leaf page
		c.markTreeDirty()
		return nil
	}

	// Not enough space - need to split the main tree page
	// This is an update (key already exists), so don't increment Items
	p.removeEntry(idx)
	return c.insertNodeAt(p, idx, nodeData, 0, true)
}

// flags_db2sub converts main database flags to sub-database flags
// (mirrors libmdbx's flags_db2sub function)
func flags_db2sub(flags uint16) uint16 {
	// For DUPSORT sub-trees, the flags relate to how values are stored
	// DUPSORT becomes the sub-tree's key handling (values are sorted)
	// REVERSEDUP becomes the sub-tree's REVERSEKEY
	// DUPFIXED becomes the sub-tree's DUPFIXED equivalent
	subFlags := uint16(0)
	if flags&uint16(ReverseDup) != 0 {
		subFlags |= uint16(ReverseKey) // REVERSEDUP → REVERSEKEY in sub-tree
	}
	if flags&uint16(DupFixed) != 0 {
		subFlags |= uint16(DupFixed)
	}
	if flags&uint16(IntegerDup) != 0 {
		subFlags |= uint16(IntegerKey) // INTEGERDUP → INTEGERKEY in sub-tree
	}
	return subFlags
}

// buildSubTreeNode builds a node for a DUPSORT sub-tree.
// In sub-trees, the "key" is the duplicate value and data is empty.
func (c *Cursor) buildSubTreeNode(value []byte) []byte {
	keyLen := len(value)
	totalSize := nodeSize + keyLen // 8-byte header + key, no data

	// Use nodeBuf if it fits, otherwise allocate
	var nodeData []byte
	if totalSize <= len(c.nodeBuf) {
		nodeData = c.nodeBuf[:totalSize]
	} else {
		nodeData = make([]byte, totalSize)
	}

	// Node header: dataSize=0, flags=0, extra=0, keySize
	binary.LittleEndian.PutUint32(nodeData[0:4], 0) // dataSize = 0
	nodeData[4] = 0                                 // flags = 0
	nodeData[5] = 0                                 // extra
	binary.LittleEndian.PutUint16(nodeData[6:8], uint16(keyLen))

	// Copy key (the duplicate value)
	copy(nodeData[nodeSize:], value)

	return nodeData
}

// buildNodeWithDupTree builds a node with N_DUP | N_TREE flags for a sub-tree reference.
// Uses cursor's nodeBuf to avoid allocation when possible.
func (c *Cursor) buildNodeWithDupTree(key []byte, tree *tree) []byte {
	keyLen := len(key)
	dataLen := treeSize // 48 bytes
	totalSize := nodeSize + keyLen + dataLen

	// Use nodeBuf if it fits, otherwise allocate
	var nodeData []byte
	if totalSize <= len(c.nodeBuf) {
		nodeData = c.nodeBuf[:totalSize]
	} else {
		nodeData = make([]byte, totalSize)
	}

	// Node header
	binary.LittleEndian.PutUint32(nodeData[0:4], uint32(dataLen))
	nodeData[4] = uint8(nodeDup | nodeTree) // N_DUP | N_TREE
	nodeData[5] = 0                         // extra
	binary.LittleEndian.PutUint16(nodeData[6:8], uint16(keyLen))

	// Copy key
	copy(nodeData[nodeSize:], key)

	// Serialize tree directly to nodeData (avoids separate allocation)
	serializeTreeToBuf(tree, nodeData[nodeSize+keyLen:])

	return nodeData
}

// putDupSubTree inserts a value into a sub-tree (nested B+tree).
func (c *Cursor) putDupSubTree(key, value []byte) error {
	// Get the sub-tree metadata from the current node
	if c.top < 0 {
		return NewError(ErrInvalid)
	}

	p := c.currentPage()
	idx := int(c.currentIndex())

	// Get node data
	n := nodeFromPage(p, idx)
	if n == nil {
		return ErrCorruptedError
	}

	// Verify it's a sub-tree node
	if n.header().Flags&nodeTree == 0 {
		return NewError(ErrIncompatible)
	}

	// Parse the tree_t structure from node data
	treeData := n.nodeData()
	if len(treeData) < treeSize {
		return ErrCorruptedError
	}

	subTree := &tree{
		Flags:       binary.LittleEndian.Uint16(treeData[0:2]),
		Height:      binary.LittleEndian.Uint16(treeData[2:4]),
		DupfixSize:  binary.LittleEndian.Uint32(treeData[4:8]),
		Root:        pgno(binary.LittleEndian.Uint32(treeData[8:12])),
		BranchPages: pgno(binary.LittleEndian.Uint32(treeData[12:16])),
		LeafPages:   pgno(binary.LittleEndian.Uint32(treeData[16:20])),
		LargePages:  pgno(binary.LittleEndian.Uint32(treeData[20:24])),
		Sequence:    binary.LittleEndian.Uint64(treeData[24:32]),
		Items:       binary.LittleEndian.Uint64(treeData[32:40]),
		ModTxnid:    txnid(binary.LittleEndian.Uint64(treeData[40:48])),
	}

	// Create a sub-cursor for the nested tree
	subCursor := &Cursor{
		signature: cursorSignature,
		state:     cursorUninitialized,
		top:       -1,
		dbi:       c.dbi,
		txn:       c.txn,
		tree:      subTree,
	}

	// Search for the insert position in the sub-tree
	// In sub-trees, the "key" is the duplicate value
	found, err := subCursor.searchForInsert(value)
	if err != nil {
		return err
	}

	if found {
		// Value already exists in sub-tree
		return nil
	}

	// Build a node for the sub-tree (key=value, data=empty)
	subNode := c.buildSubTreeNode(value)

	// Insert into sub-tree
	// Note: insertNode will increment subTree.Items via insertNodeAt
	if err := subCursor.insertNode(subNode, 0); err != nil {
		return err
	}

	// Update sub-tree metadata (Items already incremented by insertNode)
	subTree.ModTxnid = txnid(c.txn.txnID)

	// Touch the main page and update the node
	mainPage, err := c.touchPage()
	if err != nil {
		return err
	}

	// Build updated node with new tree data (serializes directly, no allocation)
	mainKey := n.key()
	nodeData := c.buildNodeWithDupTree(mainKey, subTree)

	// Update the node in place
	if mainPage.updateEntry(idx, nodeData) {
		c.pages[c.top] = mainPage
		// Increment main tree's Items for the new duplicate value
		// (for mdbx compatibility, Items counts total data items)
		c.tree.Items++
		c.markTreeDirty()
		return nil
	}

	// Shouldn't happen - tree data is same size
	return NewError(ErrPageFull)
}

// parseSubPageValues extracts values from an inline sub-page.
// libmdbx format: 20-byte page header, entry pointers at offset 20, 8-byte node headers.
// Uses scratch buffer (c.valuesBuf) for small numbers of values to avoid allocation.
func (c *Cursor) parseSubPageValues(data []byte) ([][]byte, error) {
	if len(data) < 20 {
		return nil, ErrCorruptedError
	}

	// Sub-page header (20 bytes - full page_t)
	dupfixKsize := binary.LittleEndian.Uint16(data[8:10])
	flags := binary.LittleEndian.Uint16(data[10:12])
	lower := binary.LittleEndian.Uint16(data[12:14])

	// Check for DUPFIX format (fixed-size values)
	if flags&uint16(pageDupfix) != 0 && dupfixKsize > 0 && dupfixKsize < 65535 {
		// Fixed-size values stored contiguously after header
		numValues := (len(data) - 20) / int(dupfixKsize)
		// Use scratch buffer if possible
		var values [][]byte
		if numValues <= len(c.valuesBuf) {
			values = c.valuesBuf[:numValues]
		} else {
			values = make([][]byte, numValues)
		}
		for i := 0; i < numValues; i++ {
			start := 20 + i*int(dupfixKsize)
			values[i] = data[start : start+int(dupfixKsize)]
		}
		return values, nil
	}

	// Variable-size values with pointer table
	// libmdbx format:
	// - entry pointers start at offset 20 (after page header)
	// - stored offset + 20 (PAGEHDRSZ) = actual node position
	// - nodes have 8-byte headers (dataSize:4, flags:1, extra:1, keySize:2)
	// - keySize contains the value length (values are stored as "keys" in sub-pages)
	numEntries := int(lower) / 2
	if numEntries == 0 {
		return nil, nil
	}

	// Use scratch buffer if possible
	var values [][]byte
	if numEntries <= len(c.valuesBuf) {
		values = c.valuesBuf[:numEntries]
	} else {
		values = make([][]byte, numEntries)
	}
	for i := 0; i < numEntries; i++ {
		// Entry offset is at position 20 + i*2
		offsetPos := 20 + i*2
		if offsetPos+2 > len(data) {
			return nil, ErrCorruptedError
		}
		storedOffset := int(binary.LittleEndian.Uint16(data[offsetPos:]))

		// Actual node position = stored offset + 20 (PAGEHDRSZ)
		nodePos := storedOffset + 20
		if nodePos+8 > len(data) {
			return nil, ErrCorruptedError
		}

		// 8-byte node header: dataSize(4) + flags(1) + extra(1) + keySize(2)
		// In sub-pages, keySize is the value length
		keySize := int(binary.LittleEndian.Uint16(data[nodePos+6:]))
		valueStart := nodePos + 8
		valueEnd := valueStart + keySize
		if valueEnd > len(data) {
			return nil, ErrCorruptedError
		}

		values[i] = data[valueStart:valueEnd]
	}

	return values, nil
}

// buildDupSubPage builds an inline sub-page containing the given values.
// In DUPSORT sub-pages, VALUES are stored as KEYS (in the keySize field).
// libmdbx format: 20-byte page header, entry pointers at offset 20, 8-byte node headers.
// Uses scratch buffer (c.subPageBuf) for small sub-pages to avoid allocation.
func (c *Cursor) buildDupSubPage(values [][]byte) []byte {
	// Build sub-page in libmdbx format for compatibility.
	// Header: 20 bytes (full page_t)
	// Entry pointers: numValues * 2 bytes (at offset 20)
	// Nodes: each is 8-byte header (dataSize:4, flags:1, extra:1, keySize:2) + value
	ptrTableSize := len(values) * 2
	nodeDataSize := 0
	for _, v := range values {
		nodeDataSize += nodeSize + len(v)
	}

	totalSize := pageHeaderSize + ptrTableSize + nodeDataSize
	// Use scratch buffer if possible (result is copied by caller)
	var data []byte
	if totalSize <= len(c.subPageBuf) {
		data = c.subPageBuf[:totalSize]
		// Clear the buffer area we'll use
		clear(data)
	} else {
		data = make([]byte, totalSize)
	}

	// Write page header (20 bytes)
	// txnid = 0 (will be set by commit or inherited)
	binary.LittleEndian.PutUint64(data[0:], 0)
	// dupfix_ksize = 0xFFFF (sentinel for non-DUPFIX, matching libmdbx)
	binary.LittleEndian.PutUint16(data[8:], 0xFFFF)
	// flags = Leaf | SubP
	binary.LittleEndian.PutUint16(data[10:], uint16(pageLeaf|pageSubP))
	// lower = numEntries * 2 (size of entry pointer table)
	binary.LittleEndian.PutUint16(data[12:], uint16(ptrTableSize))
	// upper = free space marker (unused for sub-pages, set to match libmdbx)
	binary.LittleEndian.PutUint16(data[14:], uint16(ptrTableSize))
	// pgno = 0 (not meaningful for sub-pages)
	binary.LittleEndian.PutUint32(data[16:], 0)

	// Write nodes from end of sub-page going backwards.
	// Nodes are stored in reverse order (largest value first in memory).
	// Entry pointers are stored offsets (relative to start), actual node = stored + 20.
	nodePos := totalSize
	nodePositions := make([]int, len(values))

	// Calculate node positions (stored end-to-start)
	for i := len(values) - 1; i >= 0; i-- {
		v := values[i]
		entrySize := nodeSize + len(v)
		nodePos -= entrySize
		nodePositions[i] = nodePos
	}

	// Write nodes and entry pointers
	for i, v := range values {
		pos := nodePositions[i]

		// Entry pointer = stored offset (pos - 20, since actual node = stored + 20)
		storedOffset := pos - pageHeaderSize
		ptrPos := pageHeaderSize + i*2
		binary.LittleEndian.PutUint16(data[ptrPos:], uint16(storedOffset))

		// Write 8-byte node header
		// dataSize = 0 (for sub-page leaf nodes)
		binary.LittleEndian.PutUint32(data[pos:], 0)
		// flags = 0
		data[pos+4] = 0
		// extra = 0
		data[pos+5] = 0
		// keySize = value length (values are stored as "keys" in sub-pages)
		binary.LittleEndian.PutUint16(data[pos+6:], uint16(len(v)))

		// Write value
		copy(data[pos+nodeSize:], v)
	}

	return data
}

// buildDupNode builds a node with the Dup flag set, containing a sub-page.
func (c *Cursor) buildDupNode(key, subPageData []byte) []byte {
	totalSize := nodeSize + len(key) + len(subPageData)
	nodeData := make([]byte, totalSize)

	// Write header
	binary.LittleEndian.PutUint32(nodeData[0:], uint32(len(subPageData))) // dataSize
	nodeData[4] = byte(nodeDup)                                           // flags
	nodeData[5] = 0                                                       // extra
	binary.LittleEndian.PutUint16(nodeData[6:], uint16(len(key)))         // keySize

	// Write key
	copy(nodeData[nodeSize:], key)

	// Write sub-page data
	copy(nodeData[nodeSize+len(key):], subPageData)

	return nodeData
}

// searchForInsert searches for the insert position and returns if exact match found.
// Uses cursor's embedded page buffers to avoid allocation during tree traversal.
func (c *Cursor) searchForInsert(key []byte) (bool, error) {
	if c.tree.isEmpty() {
		// Empty tree - need to create root
		c.state = cursorUninitialized
		return false, ErrNotFoundError
	}

	// Reset cursor stack
	c.top = -1
	c.dirtyMask = 0

	// Start at root using embedded buffer
	currentPgno := c.tree.Root

	// Search down the tree using embedded buffers
	for {
		// Push page using embedded buffer (no allocation)
		if c.top >= CursorStackSize-1 {
			return false, ErrCursorFullError
		}
		c.top++
		level := c.top

		// Get page into embedded buffer or use dirty page
		buf := &c.pagesBuf[level]
		p := c.txn.fillPageHotPath(currentPgno, buf)
		c.pages[level] = p
		if p != buf {
			// Page is dirty, mark it
			c.dirtyMask |= uint32(1) << level
		}

		idx := c.searchPage(p, key)
		c.indices[level] = uint16(idx)

		if p.isLeaf() {
			if idx >= p.numEntries() {
				c.state = cursorPointing
				return false, nil
			}

			// Check for exact match using allocation-free method
			foundKey := nodeGetKeyDirect(p, idx)
			if foundKey == nil {
				return false, ErrCorruptedError
			}
			cmp := c.txn.compareKeys(c.dbi, key, foundKey)

			c.state = cursorPointing
			return cmp == 0, nil
		}

		// Branch page: get child pgno and continue
		currentPgno = c.getChildPgno(p, idx)
	}
}

// buildNode constructs the node data for insertion.
// Uses cursor's scratch buffer when possible to avoid allocation.
func (c *Cursor) buildNode(key, value []byte, isBig bool) ([]byte, pgno, error) {
	return c.buildNodeWithFlags(key, value, isBig, 0)
}

// buildNodeWithFlags constructs the node data for insertion with additional node flags.
// The extraFlags parameter allows setting node type flags like nodeTree for sub-database entries.
func (c *Cursor) buildNodeWithFlags(key, value []byte, isBig bool, extraFlags nodeFlags) ([]byte, pgno, error) {
	var overflowPgno pgno
	var nodeFlags nodeFlags
	var dataSize uint32
	var nodeDataSize int

	if isBig {
		// Allocate overflow pages
		var err error
		overflowPgno, err = c.allocateOverflow(value)
		if err != nil {
			return nil, 0, err
		}
		nodeFlags = nodeBig
		dataSize = uint32(len(value)) // Store original size in header
		nodeDataSize = nodeSize + len(key) + 4
	} else {
		dataSize = uint32(len(value))
		nodeDataSize = nodeSize + len(key) + len(value)
	}

	// Add extra flags (e.g., nodeTree for sub-database entries)
	nodeFlags |= extraFlags

	// Use scratch buffer if node fits, otherwise allocate
	var nodeData []byte
	if nodeDataSize <= len(c.nodeBuf) {
		nodeData = c.nodeBuf[:nodeDataSize]
	} else {
		nodeData = make([]byte, nodeDataSize)
	}

	// Write header (use fast path on little-endian machines)
	putUint32LE(nodeData[0:], dataSize)
	nodeData[4] = byte(nodeFlags)
	nodeData[5] = 0 // extra
	putUint16LE(nodeData[6:], uint16(len(key)))

	// Write key
	copy(nodeData[nodeSize:], key)

	// Write data or overflow pgno
	if isBig {
		putUint32LE(nodeData[nodeSize+len(key):], uint32(overflowPgno))
	} else {
		copy(nodeData[nodeSize+len(key):], value)
	}

	return nodeData, overflowPgno, nil
}

// updateNode updates an existing node at the cursor position.
func (c *Cursor) updateNode(nodeData []byte, overflowPgno pgno) error {
	if c.top < 0 {
		return ErrCorruptedError
	}

	// Get dirty copy of the page
	p, err := c.touchPage()
	if err != nil {
		return err
	}

	idx := int(c.indices[c.top])

	// Check if old node is big and needs overflow pages freed (allocation-free)
	oldFlags := nodeGetFlagsDirect(p, idx)
	if oldFlags&nodeBig != 0 {
		// Free old overflow pages
		oldOverflowPgno := nodeGetOverflowPgnoDirect(p, idx)
		oldDataSize := nodeGetDataSizeDirect(p, idx)
		c.freeOverflow(oldOverflowPgno, oldDataSize)
	}

	// Try to update in place
	if p.updateEntry(idx, nodeData) {
		c.pages[c.top] = p
		return nil
	}

	// Not enough space - need to delete and reinsert
	// This is an update (key already exists), so don't increment Items
	p.removeEntry(idx)
	return c.insertNodeAt(p, idx, nodeData, overflowPgno, true)
}

// insertNode inserts a new node at the cursor position.
func (c *Cursor) insertNode(nodeData []byte, overflowPgno pgno) error {
	// Handle empty tree
	if c.tree.isEmpty() {
		return c.createRoot(nodeData, overflowPgno)
	}

	if c.top < 0 {
		return ErrCorruptedError
	}

	// Get dirty copy of the page
	p, err := c.touchPage()
	if err != nil {
		return err
	}

	idx := int(c.indices[c.top])
	return c.insertNodeAt(p, idx, nodeData, overflowPgno, false)
}

// insertNodeAt inserts a node at a specific position on a page.
// If isUpdate is true, Items count is not incremented (the key already exists).
func (c *Cursor) insertNodeAt(p *page, idx int, nodeData []byte, overflowPgno pgno, isUpdate bool) error {
	// Try to insert on current page using transaction's scratch buffer
	if p.insertEntryWithBuf(idx, nodeData, c.txn.compactBuf[:]) {
		c.pages[c.top] = p
		c.indices[c.top] = uint16(idx)
		if !isUpdate {
			c.tree.Items++
		}
		// Note: LeafPages counts NUMBER of leaf pages, not entries.
		// It's only incremented when new pages are created (createRoot, splitAndInsert).
		return nil
	}

	// Page is full - need to split
	return c.splitAndInsert(p, idx, nodeData, overflowPgno, isUpdate)
}

// createRoot creates a new root page for an empty tree.
func (c *Cursor) createRoot(nodeData []byte, overflowPgno pgno) error {
	// Allocate a new page
	pgno, p, err := c.allocatePage()
	if err != nil {
		return err
	}

	pageSize := c.txn.env.pageSize

	// Initialize as leaf page
	p.init(pgno, pageLeaf, uint16(pageSize))
	p.header().Txnid = txnid(c.txn.txnID)

	// Insert the node
	if !p.insertEntry(0, nodeData) {
		return NewError(ErrPageFull)
	}

	// Update tree metadata
	c.tree.Root = pgno
	c.tree.Height = 1
	c.tree.LeafPages = 1
	c.tree.Items = 1
	c.tree.ModTxnid = txnid(c.txn.txnID)

	// Mark tree as dirty
	c.markTreeDirty()

	// Set cursor position
	c.reset()
	if err := c.pushPage(p, 0); err != nil {
		return err
	}
	c.state = cursorPointing

	return nil
}

// splitAndInsert splits a page and inserts the node.
// If isUpdate is true, Items count is not incremented (the key already exists).
func (c *Cursor) splitAndInsert(p *page, idx int, nodeData []byte, overflowPgno pgno, isUpdate bool) error {
	pageSize := c.txn.env.pageSize

	// Find split point
	splitIdx := p.splitPoint(len(nodeData), idx)

	// Allocate new page for the split
	newPgno, newPage, err := c.allocatePage()
	if err != nil {
		return err
	}

	// Initialize new page with same type
	newPage.init(newPgno, p.pageType(), uint16(pageSize))
	newPage.header().Txnid = txnid(c.txn.txnID)

	// Move entries after split point to new page
	numEntries := p.numEntries()
	newIdx := 0
	for i := splitIdx; i < numEntries; i++ {
		offset := p.entryOffset(i)
		nodeSize := p.calcNodeSize(i)
		if nodeSize > 0 {
			// Bounds check to prevent panic
			end := int(offset) + nodeSize
			if end > len(p.Data) {
				return ErrCorruptedError
			}
			newPage.insertEntry(newIdx, p.Data[offset:end])
			newIdx++
		}
	}

	// Remove moved entries from original page using O(1) bulk removal
	// This is much faster than the O(n) sequential removal loop
	p.removeEntriesFrom(splitIdx)

	// Compact the page to reclaim space and fix the upper pointer.
	// After removeEntriesFrom, upper may still point to removed entry data,
	// causing free space calculation to be incorrect.
	// Use transaction's scratch buffer to avoid pool allocations.
	p.compactWithBuf(c.txn.compactBuf[:])

	// Insert the new node in the appropriate page
	// Special case: when splitIdx=0, new node goes to left page alone (all existing went right)
	if splitIdx == 0 || idx < splitIdx {
		if !p.insertEntryWithBuf(idx, nodeData, c.txn.compactBuf[:]) {
			// Shouldn't happen after split
			return NewError(ErrPageFull)
		}
	} else {
		newIdx := idx - splitIdx
		if !newPage.insertEntryWithBuf(newIdx, nodeData, c.txn.compactBuf[:]) {
			return NewError(ErrPageFull)
		}
	}

	// Update page counts
	if p.isLeaf() {
		c.tree.LeafPages++
	} else {
		c.tree.BranchPages++
	}
	if !isUpdate {
		c.tree.Items++
	}
	c.tree.ModTxnid = txnid(c.txn.txnID)

	// Get the separator key (first key of new page) - allocation-free
	sepKey := nodeGetKeyDirect(newPage, 0)
	if sepKey == nil {
		return ErrCorruptedError
	}

	// Insert separator into parent
	return c.insertIntoParent(p.pageNo(), newPgno, sepKey)
}

// insertIntoParent inserts a separator key into the parent branch page.
func (c *Cursor) insertIntoParent(leftPgno, rightPgno pgno, sepKey []byte) error {
	// If we're at the root, create a new root
	if c.top == 0 {
		return c.createNewRoot(leftPgno, rightPgno, sepKey)
	}

	// Pop the leaf and work with parent
	c.popPage()

	// Get dirty copy of parent
	parentPage, err := c.touchPage()
	if err != nil {
		return err
	}

	// Build branch node for the new child
	branchNode := c.buildBranchNode(sepKey, rightPgno)

	// Insert after current position
	idx := int(c.indices[c.top]) + 1

	if parentPage.insertEntry(idx, branchNode) {
		c.pages[c.top] = parentPage
		c.tree.BranchPages++ // Approximate
		return nil
	}

	// Parent is full - need to split it too (branch node is always new, not update)
	return c.splitAndInsert(parentPage, idx, branchNode, rightPgno, false)
}

// createNewRoot creates a new root with two children.
func (c *Cursor) createNewRoot(leftPgno, rightPgno pgno, sepKey []byte) error {
	pageSize := c.txn.env.pageSize

	// Allocate new root page
	rootPgno, rootPage, err := c.allocatePage()
	if err != nil {
		return err
	}

	// Initialize as branch page
	rootPage.init(rootPgno, pageBranch, uint16(pageSize))
	rootPage.header().Txnid = txnid(c.txn.txnID)

	// Build branch nodes
	// First entry: empty key pointing to left child
	leftNode := c.buildBranchNode(nil, leftPgno)
	rootPage.insertEntry(0, leftNode)

	// Second entry: separator key pointing to right child
	rightNode := c.buildBranchNode(sepKey, rightPgno)
	rootPage.insertEntry(1, rightNode)

	// Update tree
	c.tree.Root = rootPgno
	c.tree.Height++
	c.tree.BranchPages++
	c.tree.ModTxnid = txnid(c.txn.txnID)

	c.markTreeDirty()

	return nil
}

// buildBranchNode builds a branch node pointing to a child page.
// Uses cursor's branch scratch buffer when possible to avoid allocation.
func (c *Cursor) buildBranchNode(key []byte, childPgno pgno) []byte {
	totalSize := nodeSize + len(key)

	// Use scratch buffer if node fits, otherwise allocate
	var nodeData []byte
	if totalSize <= len(c.branchBuf) {
		nodeData = c.branchBuf[:totalSize]
	} else {
		nodeData = make([]byte, totalSize)
	}

	// For branch nodes, DataSize field holds the child page number
	binary.LittleEndian.PutUint32(nodeData[0:], uint32(childPgno))
	nodeData[4] = 0 // flags
	nodeData[5] = 0 // extra
	binary.LittleEndian.PutUint16(nodeData[6:], uint16(len(key)))

	// Write key
	copy(nodeData[nodeSize:], key)

	return nodeData
}

// del deletes the current key-value pair.
func (c *Cursor) del(flags uint) error {
	if c.top < 0 || c.state != cursorPointing {
		return ErrNotFoundError
	}

	// Check if this is a DUPSORT database with duplicate values
	isDupSort := c.tree.Flags&uint16(DupSort) != 0

	// NoDupData or AllDups flag means delete all values for the key (the entire node)
	if flags&NoDupData != 0 || flags&AllDups != 0 {
		return c.delNode()
	}

	if isDupSort && c.dup.initialized && (c.dup.subPageNum > 0 || c.dup.isSubTree) {
		// For DUPSORT, delete just the current duplicate value (not the whole key)
		return c.delDupValue(flags)
	}

	// Non-DUPSORT or single value: delete the entire node
	return c.delNode()
}

// delNode deletes the entire node at the current cursor position.
func (c *Cursor) delNode() error {
	// Get dirty copy of the page
	p, err := c.touchPage()
	if err != nil {
		return err
	}

	idx := int(c.indices[c.top])

	// Check for overflow pages (allocation-free)
	oldFlags := nodeGetFlagsDirect(p, idx)
	if oldFlags&nodeBig != 0 {
		c.freeOverflow(nodeGetOverflowPgnoDirect(p, idx), nodeGetDataSizeDirect(p, idx))
	}

	// For DupSort tables, count the number of duplicates to decrement Items correctly.
	// Each duplicate was counted when inserted, so we need to decrement by the total count.
	itemsToDecrement := uint64(1)
	isDupSort := c.tree.Flags&uint16(DupSort) != 0
	if isDupSort {
		if oldFlags&nodeTree != 0 {
			// N_TREE: get Items from sub-tree structure
			dataSize := nodeGetDataSizeDirect(p, idx)
			if dataSize >= 48 {
				data := nodeGetDataDirect(p, idx)
				if len(data) >= 40 {
					itemsToDecrement = binary.LittleEndian.Uint64(data[32:40])
				}
			}
		} else if oldFlags&nodeDup != 0 {
			// N_DUP: count from sub-page header (lower field / 2)
			data := nodeGetDataDirect(p, idx)
			if len(data) >= 14 {
				lower := binary.LittleEndian.Uint16(data[12:14])
				itemsToDecrement = uint64(lower >> 1)
			}
		}
	}

	// Remove the entry
	if !p.removeEntry(idx) {
		return ErrCorruptedError
	}

	c.pages[c.top] = p
	c.tree.Items -= itemsToDecrement
	c.tree.ModTxnid = txnid(c.txn.txnID)

	// Handle underflow (if page becomes too empty)
	// For now, we don't implement merging - pages stay half-empty
	// This is valid behavior (LMDB/MDBX also delay merging)

	// Reset dup state
	c.dup.initialized = false

	// Check if page is empty
	if p.numEntries() == 0 {
		if c.top == 0 {
			// Root is empty - tree becomes empty
			c.tree.Root = invalidPgno
			c.tree.Height = 0
			c.tree.LeafPages = 0
			// Add root page to free list
			c.txn.freePages = append(c.txn.freePages, p.pageNo())
			c.state = cursorEOF
			c.afterDelete = false
		} else {
			// Non-root page is empty - remove from tree and free
			if err := c.freeEmptyPage(p); err != nil {
				return err
			}
			// After freeEmptyPage:
			// - If tree collapsed, cursor is at the new root (which may be a leaf)
			// - Otherwise, cursor is at the parent (branch) level
			curPage := c.pages[c.top]
			if curPage.numEntries() > 0 && c.indices[c.top] < uint16(curPage.numEntries()) {
				if curPage.isLeaf() {
					// After tree collapse, we're directly at a leaf page
					// The entry at current index is the "next" entry
					c.afterDelete = true
				} else {
					// We're at a branch page, descend to the leaf at current position
					_, _, err := c.descendLeft()
					if err != nil {
						c.state = cursorEOF
						c.afterDelete = false
					} else {
						// Now we're at a leaf, set afterDelete so Next returns current
						c.afterDelete = true
					}
				}
			} else {
				// No more children, set to EOF
				c.state = cursorEOF
				c.afterDelete = false
			}
		}
	} else if idx >= p.numEntries() {
		// We're past the end of this page but page is not empty.
		// Keep idx at the deleted position (out of bounds) and set afterDelete=false.
		// This way:
		// - Next: idx+1 > numEntries, so navigate forward correctly
		// - Prev: idx > 0, so decrement to idx-1 which is the last remaining entry
		c.afterDelete = false
	} else {
		// idx is still valid - the entry at idx is the "next" entry
		c.afterDelete = true
	}

	c.markTreeDirty()

	return nil
}

// freeEmptyPage removes an empty non-root page from the tree and adds it to the free list.
func (c *Cursor) freeEmptyPage(emptyPage *page) error {
	if c.top <= 0 {
		// Shouldn't be called for root
		return nil
	}

	// Add page to free list
	c.txn.freePages = append(c.txn.freePages, emptyPage.pageNo())

	// Update page counts
	if emptyPage.isLeaf() {
		if c.tree.LeafPages > 0 {
			c.tree.LeafPages--
		}
	} else {
		if c.tree.BranchPages > 0 {
			c.tree.BranchPages--
		}
	}

	// Pop this level and go to parent
	c.popPage()

	// Get dirty copy of parent
	parentPage, err := c.touchPage()
	if err != nil {
		return err
	}

	parentIdx := int(c.indices[c.top])

	// Remove the entry from parent that pointed to the empty page
	if !parentPage.removeEntry(parentIdx) {
		return ErrCorruptedError
	}

	c.pages[c.top] = parentPage

	// If parent is now empty too and it's the root, tree shrinks
	if parentPage.numEntries() == 0 {
		if c.top == 0 {
			// Root is now empty - tree is empty
			c.tree.Root = invalidPgno
			c.tree.Height = 0
			// Add the root to free list
			c.txn.freePages = append(c.txn.freePages, parentPage.pageNo())
			if c.tree.BranchPages > 0 {
				c.tree.BranchPages--
			}
		}
		// Note: If non-root parent is empty, we should recursively handle it,
		// but that's a complex rebalancing operation. For now, leave it empty
		// and let compaction handle it later.
	} else if parentPage.numEntries() == 1 && c.top == 0 && !parentPage.isLeaf() {
		// Root branch with only one child - can collapse tree height
		// Get the only remaining child and make it the new root
		childPgno := c.getChildPgno(parentPage, 0)
		c.tree.Root = childPgno
		c.tree.Height--
		// Add old root to free list
		c.txn.freePages = append(c.txn.freePages, parentPage.pageNo())
		if c.tree.BranchPages > 0 {
			c.tree.BranchPages--
		}

		// CRITICAL: Re-initialize cursor to point at the new root.
		// The old root (branch page) is now freed, cursor stack is invalid.
		// Push the new root (the remaining child) onto the cursor stack.
		c.top = -1 // Reset stack
		if err := c.pushPageByPgno(childPgno, 0); err != nil {
			return err
		}
		// After collapse, the cursor is at the new root (which is a leaf or branch).
		// If it's a leaf, index 0 is the first entry.
		// If it's still a branch (multi-level collapse), caller will handle descent.
		return nil
	}

	// Adjust cursor position
	if parentIdx >= parentPage.numEntries() && parentPage.numEntries() > 0 {
		c.indices[c.top] = uint16(parentPage.numEntries() - 1)
	}

	return nil
}

// delDupValue deletes a single duplicate value from a DUPSORT database.
func (c *Cursor) delDupValue(flags uint) error {
	// Handle sub-tree case (N_TREE flag - many duplicates)
	if c.dup.isSubTree {
		return c.delDupSubTreeValue()
	}

	// Handle inline sub-page (N_DUP flag - few duplicates)
	if c.dup.subPageNum <= 1 {
		// Only one value - delete the entire node
		return c.delNode()
	}

	// Multiple values in sub-page - remove just the current value
	return c.delDupSubPageValue()
}

// delDupSubTreeValue removes a single value from a sub-tree (nested B+tree).
func (c *Cursor) delDupSubTreeValue() error {
	if c.dup.subTop < 0 {
		return ErrCorruptedError
	}

	// Touch the main page first (for updating tree metadata)
	mainPage, err := c.touchPage()
	if err != nil {
		return err
	}
	mainIdx := int(c.indices[c.top])

	// Get main key for potential node rebuilding
	mainKey := nodeGetKeyDirect(mainPage, mainIdx)
	if mainKey == nil {
		return ErrCorruptedError
	}
	// Make a copy since we'll be modifying the page
	mainKey = append([]byte(nil), mainKey...)

	// Touch sub-tree pages from root to leaf (COW for sub-tree)
	// This allocates new pages for the entire path
	subLeafPage, err := c.touchSubTreePath()
	if err != nil {
		return err
	}

	subIdx := int(c.dup.subIndices[c.dup.subTop])

	// Remove the entry from the sub-tree leaf page
	if !subLeafPage.removeEntry(subIdx) {
		return ErrCorruptedError
	}

	// Update sub-tree metadata
	c.dup.subTree.Items--
	c.dup.subTree.ModTxnid = txnid(c.txn.txnID)

	// Check if sub-tree is now empty
	if c.dup.subTree.Items == 0 {
		// Sub-tree is empty - delete the entire node
		// Free sub-tree pages
		c.freeSubTreePages()
		return c.delNode()
	}

	// Check if we should convert back to sub-page (optional optimization)
	// If sub-tree has very few items and they would fit in a sub-page
	// For now, we keep it as a sub-tree - conversion is optional

	// Check if leaf page is empty (but tree not empty - need rebalancing)
	if subLeafPage.numEntries() == 0 && c.dup.subTop > 0 {
		// Leaf is empty but tree has other entries - need to handle underflow
		// For now, just free the empty page
		// Full rebalancing (merge/borrow) is deferred
		c.dup.subTree.LeafPages--
	}

	// Update the main node with new sub-tree metadata (serializes directly, no allocation)
	nodeData := c.buildNodeWithDupTree(mainKey, &c.dup.subTree)

	// Replace the node in the main page
	if err := c.replaceNodeAt(mainPage, mainIdx, nodeData); err != nil {
		return err
	}

	c.pages[c.top] = mainPage
	// Decrement tree.Items since we're deleting a dup value (Items tracks all values including dups)
	c.tree.Items--
	c.tree.ModTxnid = txnid(c.txn.txnID)
	c.markTreeDirty()

	// Adjust sub-tree cursor position
	if subIdx >= subLeafPage.numEntries() {
		if subLeafPage.numEntries() > 0 {
			c.dup.subIndices[c.dup.subTop] = uint16(subLeafPage.numEntries() - 1)
		}
	}

	// Reset position tracking
	c.dup.atFirst = false
	c.dup.atLast = false

	// Mark that next move should return current position
	c.afterDelete = true

	return nil
}

// touchSubTreePath touches all pages in the sub-tree path from root to leaf.
// Returns the touched leaf page.
func (c *Cursor) touchSubTreePath() (*page, error) {
	if c.dup.subTop < 0 {
		return nil, ErrCorruptedError
	}

	// Touch pages from root (level 0) to leaf (subTop)
	for level := int8(0); level <= c.dup.subTop; level++ {
		origPage := c.dup.subPages[level]
		if origPage == nil {
			return nil, ErrCorruptedError
		}
		oldPgno := origPage.pageNo()

		// Check if already dirty
		if dirty := c.txn.dirtyTracker.get(oldPgno); dirty != nil {
			c.dup.subPages[level] = dirty
			continue
		}

		// OPTIMIZATION: In-place modification for WriteMap mode
		// If the page's txnid equals current transaction, we already own it.
		if c.txn.env.isWriteMap() && origPage.header().Txnid == txnid(c.txn.txnID) {
			c.txn.dirtyTracker.set(oldPgno, origPage)
			continue
		}

		// Allocate new page (COW)
		newPgno := c.txn.allocatedPg
		c.txn.allocatedPg++

		var newData []byte
		var usedMmap bool
		if c.txn.env.isWriteMap() {
			// WriteMap mode: try mmap directly (no remap during transaction)
			newData = c.txn.env.getMmapPageData(newPgno)
			if newData != nil {
				copy(newData, origPage.Data)
				usedMmap = true
			}
		}
		if !usedMmap {
			// Normal mode or mmap out of bounds
			newData = c.txn.env.getPageDataFromCache()
			copy(newData, origPage.Data)
			c.txn.pooledPageData = append(c.txn.pooledPageData, newData)
		}

		newPage := getPooledPageStruct(newData)
		c.txn.pooledPageStructs = append(c.txn.pooledPageStructs, newPage)
		newPage.header().PageNo = newPgno
		newPage.header().Txnid = txnid(c.txn.txnID)

		// Mark as dirty
		c.txn.dirtyTracker.set(newPgno, newPage)
		c.dup.subPages[level] = newPage

		// Update parent's child pointer or tree root
		if level == 0 {
			// Update sub-tree root
			c.dup.subTree.Root = newPgno
		} else {
			// Update parent's child pointer
			parentPage := c.dup.subPages[level-1]
			parentIdx := int(c.dup.subIndices[level-1])
			c.updateChildPointer(parentPage, parentIdx, newPgno)
		}
	}

	return c.dup.subPages[c.dup.subTop], nil
}

// freeSubTreePages adds all sub-tree pages to the free list.
func (c *Cursor) freeSubTreePages() {
	// Simple implementation: just decrement page counts
	// The pages will be reclaimed during garbage collection
	// or reused after commit
	if c.dup.subTree.LeafPages > 0 {
		c.tree.LeafPages--
	}
	// Branch pages are already tracked in c.dup.subTree.BranchPages
	// but we don't maintain per-tree page counts precisely
}

// delDupSubPageValue removes a single value from an inline sub-page.
func (c *Cursor) delDupSubPageValue() error {
	// Get dirty copy of the main page
	p, err := c.touchPage()
	if err != nil {
		return err
	}

	idx := int(c.indices[c.top])

	// Parse current values from sub-page
	currentData := nodeGetDataDirect(p, idx)
	if currentData == nil {
		return ErrCorruptedError
	}

	values, err := c.parseSubPageValues(currentData)
	if err != nil {
		return err
	}

	if c.dup.subPageIdx >= len(values) {
		return ErrCorruptedError
	}

	// Remove the value at current index
	newValues := make([][]byte, 0, len(values)-1)
	for i, v := range values {
		if i != c.dup.subPageIdx {
			newValues = append(newValues, v)
		}
	}

	// Get the key for rebuilding the node
	key := nodeGetKeyDirect(p, idx)
	if key == nil {
		return ErrCorruptedError
	}

	if len(newValues) == 0 {
		// No values left - delete the entire node
		return c.delNode()
	}

	if len(newValues) == 1 {
		// Only one value left - convert to regular node (no sub-page)
		return c.convertDupToSingle(p, idx, key, newValues[0])
	}

	// Multiple values remain - rebuild the sub-page
	newSubPage := c.buildDupSubPage(newValues)

	// Build new node with updated sub-page
	nodeData := c.buildDupNode(key, newSubPage)

	// Update the node in place
	if err := c.replaceNodeAt(p, idx, nodeData); err != nil {
		return err
	}

	// Re-parse node positions (this resets subPageIdx to 0)
	c.initDupSubPage(newSubPage)

	// Restore cursor position after re-initialization
	if c.dup.subPageIdx >= len(newValues) {
		c.dup.subPageIdx = len(newValues) - 1
	}

	c.pages[c.top] = p
	// Decrement tree.Items since we're deleting a dup value (Items tracks all values including dups)
	c.tree.Items--
	c.tree.ModTxnid = txnid(c.txn.txnID)
	c.markTreeDirty()

	// Mark that next move should return current position
	c.afterDelete = true

	return nil
}

// convertDupToSingle converts a DUPSORT node from sub-page format to a regular single-value node.
func (c *Cursor) convertDupToSingle(p *page, idx int, key, value []byte) error {
	// Build a regular node (no Dup flag)
	nodeDataSize := nodeSize + len(key) + len(value)
	var nodeData []byte
	if nodeDataSize <= len(c.nodeBuf) {
		nodeData = c.nodeBuf[:nodeDataSize]
	} else {
		nodeData = make([]byte, nodeDataSize)
	}

	// Write header (no Dup flag)
	binary.LittleEndian.PutUint32(nodeData[0:], uint32(len(value)))
	nodeData[4] = 0 // no flags
	nodeData[5] = 0 // extra
	binary.LittleEndian.PutUint16(nodeData[6:], uint16(len(key)))

	// Write key and value
	copy(nodeData[nodeSize:], key)
	copy(nodeData[nodeSize+len(key):], value)

	// Replace the node
	if err := c.replaceNodeAt(p, idx, nodeData); err != nil {
		return err
	}

	// Reset dup state since we're now a single value
	c.dup.initialized = false
	c.dup.subPageNum = 0
	c.dup.subPageData = nil
	c.dup.nodePositions = nil

	// Mark that next move should return current position
	c.afterDelete = true

	c.pages[c.top] = p
	// Note: Do NOT decrement tree.Items here - we're converting a dup to single, not deleting a key
	// Items counts the number of distinct keys in the tree
	c.tree.ModTxnid = txnid(c.txn.txnID)
	c.markTreeDirty()

	return nil
}

// replaceNodeAt replaces the node at the given index with new node data.
// If the page doesn't have space after removal, triggers a page split.
func (c *Cursor) replaceNodeAt(p *page, idx int, newNodeData []byte) error {
	// Get old node size
	oldOffset := p.entryOffset(idx)
	if oldOffset == 0 {
		return ErrCorruptedError
	}

	oldDataSize := nodeGetDataSizeDirect(p, idx)
	oldKeySize := int(binary.LittleEndian.Uint16(p.Data[oldOffset+6:]))
	oldNodeSize := nodeSize + oldKeySize + int(oldDataSize)
	newNodeSize := len(newNodeData)

	// Check if new node fits in the same space
	sizeDiff := newNodeSize - oldNodeSize

	if sizeDiff == 0 {
		// Same size - just copy in place
		copy(p.Data[oldOffset:], newNodeData)
		return nil
	}

	// Different size - need to remove and reinsert
	// First remove the old entry
	if !p.removeEntry(idx) {
		return ErrCorruptedError
	}

	// Now insert the new entry
	if p.insertEntry(idx, newNodeData) {
		return nil
	}

	// Page is full after removal - need to split
	// This can happen if the node grew (e.g., sub-tree metadata expanded)
	// Use insertNodeAt which handles splitting properly
	// Pass isUpdate=true since we're replacing an existing key
	return c.insertNodeAt(p, idx, newNodeData, 0, true)
}

// touchPage returns a dirty (writable) copy of the current page.
// This implements proper copy-on-write: a new page is allocated and
// parent pointers are updated to point to the new page.
func (c *Cursor) touchPage() (*page, error) {
	if c.top < 0 {
		return nil, ErrCorruptedError
	}

	// Touch pages from root to current, allocating new pages for each
	return c.touchPageAt(int(c.top))
}

// touchPageAt touches the page at the given stack level and all parents.
// Uses iterative approach to avoid recursion overhead.
// Uses stackDirty array and dirtyMask bitmask to avoid tracker lookups.
func (c *Cursor) touchPageAt(level int) (*page, error) {
	if level < 0 || level > int(c.top) {
		return nil, ErrCorruptedError
	}

	levelBit := uint32(1) << level

	// Ultra-fast path 1: cursor-local dirty page (verify it matches current page)
	// Must verify pages[level] == stackDirty[level] since cursor may have navigated
	if c.stackDirty[level] != nil && c.pages[level] == c.stackDirty[level] {
		return c.stackDirty[level], nil
	}

	// Fast path 2: dirtyMask says dirty but stackDirty not set (from parent txn or another cursor)
	if c.dirtyMask&levelBit != 0 {
		return c.pages[level], nil
	}

	// Find highest dirty level below target using bitmask
	// Create mask for levels 0 to level-1
	belowMask := levelBit - 1
	dirtyBelow := c.dirtyMask & belowMask

	// Find highest set bit in dirtyBelow (that's our firstDirty)
	startLevel := 0
	if dirtyBelow != 0 {
		// Find highest bit position - simple loop for small values
		for bit := level - 1; bit >= 0; bit-- {
			if dirtyBelow&(1<<bit) != 0 {
				startLevel = bit + 1
				break
			}
		}
	}

	var resultPage *page
	for i := startLevel; i <= level; i++ {
		origPage := c.pages[i]
		oldPgno := origPage.pageNo()

		// Check if already dirty (might be dirty from another cursor or previous op)
		if dirty := c.txn.dirtyTracker.get(oldPgno); dirty != nil {
			c.pages[i] = dirty
			c.stackDirty[i] = dirty // Cache in cursor for next access
			c.dirtyMask |= uint32(1) << i
			resultPage = dirty
			continue
		}

		// OPTIMIZATION: In-place modification for WriteMap mode
		// If the page's txnid equals current transaction, we already own it:
		// - Created this txn: no readers can see it (didn't exist before)
		// - COW'd this txn: readers see old version at old pgno
		// So we can modify in-place without allocating a new page number.
		if c.txn.env.isWriteMap() && origPage.header().Txnid == txnid(c.txn.txnID) {
			// Page already belongs to this transaction, modify in-place
			c.txn.dirtyTracker.set(oldPgno, origPage)
			c.stackDirty[i] = origPage
			c.dirtyMask |= uint32(1) << i
			resultPage = origPage
			continue
		}

		// Allocate a NEW page number (proper COW)
		newPgno := c.txn.allocatedPg
		c.txn.allocatedPg++

		var newData []byte
		var usedMmap bool
		if c.txn.env.isWriteMap() {
			// WriteMap mode: try mmap directly (zero allocation)
			// Note: We intentionally do NOT call extendMmap here. Remapping during a
			// transaction would invalidate all cached page references, causing crashes.
			// If the page is beyond mmap bounds, fall back to heap allocation.
			newData = c.txn.env.getMmapPageData(newPgno)
			if newData != nil {
				copy(newData, origPage.Data)
				usedMmap = true
			}
		}
		if !usedMmap {
			// Normal mode or mmap out of bounds
			newData = c.txn.env.getPageDataFromCache()
			copy(newData, origPage.Data)
			c.txn.pooledPageData = append(c.txn.pooledPageData, newData)
		}

		newPage := getPooledPageStruct(newData)
		c.txn.pooledPageStructs = append(c.txn.pooledPageStructs, newPage)
		newPage.header().PageNo = newPgno
		newPage.header().Txnid = txnid(c.txn.txnID)

		// Store in both cursor AND tracker
		c.txn.dirtyTracker.set(newPgno, newPage)
		c.pages[i] = newPage
		c.stackDirty[i] = newPage // Cache in cursor for next access
		c.dirtyMask |= uint32(1) << i

		// If this is the root, update tree.Root
		if i == 0 {
			c.tree.Root = newPgno
			c.tree.ModTxnid = txnid(c.txn.txnID)
			c.markTreeDirty()
		} else {
			// Update parent's child pointer (parent already touched in previous iteration)
			parentIdx := int(c.indices[i-1])
			c.updateChildPointer(c.pages[i-1], parentIdx, newPgno)
		}

		resultPage = newPage
	}

	return resultPage, nil
}

// updateChildPointer updates a branch page entry's child pointer.
func (c *Cursor) updateChildPointer(parentPage *page, idx int, newChildPgno pgno) {
	// Get the entry offset
	offset := parentPage.entryOffset(idx)
	if offset == 0 && idx > 0 {
		return // Invalid offset
	}

	// The child pgno is stored in the first 4 bytes of the node (DataSize field for branch nodes)
	binary.LittleEndian.PutUint32(parentPage.Data[offset:], uint32(newChildPgno))
}

// allocatePage allocates a new page.
func (c *Cursor) allocatePage() (pgno, *page, error) {
	var newPgno pgno

	// Check free list first
	if len(c.txn.freePages) > 0 {
		// Pop a page from the free list
		newPgno = c.txn.freePages[len(c.txn.freePages)-1]
		c.txn.freePages = c.txn.freePages[:len(c.txn.freePages)-1]
	} else {
		// Allocate from end of file
		newPgno = c.txn.allocatedPg
		c.txn.allocatedPg++
	}

	var data []byte
	var usedMmap bool
	if c.txn.env.isWriteMap() {
		// WriteMap mode: try to use mmap directly (zero allocation)
		// Note: No remap during transaction to avoid invalidating page references
		data = c.txn.env.getMmapPageData(newPgno)
		if data != nil {
			usedMmap = true
		}
	}
	if !usedMmap {
		// Normal mode or mmap out of bounds: allocate from cache
		data = c.txn.env.getPageDataFromCache()
		c.txn.pooledPageData = append(c.txn.pooledPageData, data)
	}
	// Note: No need to clear page - page.init() sets header, and
	// lower/upper bounds define valid data region. Unwritten areas
	// are never read. This matches libmdbx behavior.

	p := getPooledPageStruct(data)
	c.txn.pooledPageStructs = append(c.txn.pooledPageStructs, p)

	// Mark as dirty
	c.txn.dirtyTracker.set(newPgno, p)

	return newPgno, p, nil
}

// allocateOverflow allocates overflow pages for large values.
// MDBX format: first page has header, subsequent pages are raw data with no header.
func (c *Cursor) allocateOverflow(data []byte) (pgno, error) {
	pageSize := int(c.txn.env.pageSize)
	firstPageData := pageSize - pageHeaderSize // First page has header

	// Calculate number of pages needed
	// First page holds (pageSize - headerSize) bytes, subsequent pages hold pageSize bytes each
	remaining := len(data) - firstPageData
	numPages := 1
	if remaining > 0 {
		numPages += (remaining + pageSize - 1) / pageSize
	}

	// Allocate consecutive pages
	firstPgno := c.txn.allocatedPg
	c.txn.allocatedPg += pgno(numPages)

	// Write data to overflow pages
	offset := 0
	isWriteMap := c.txn.env.isWriteMap()
	for i := 0; i < numPages; i++ {
		pgno := firstPgno + pgno(i)

		var pdata []byte
		var usedMmap bool
		if isWriteMap {
			// WriteMap mode: try mmap directly
			pdata = c.txn.env.getMmapPageData(pgno)
			if pdata != nil {
				clear(pdata)
				usedMmap = true
			}
		}
		if !usedMmap {
			// Normal mode or mmap out of bounds
			pdata = c.txn.env.getPageDataFromCache()
			clear(pdata)
			c.txn.pooledPageData = append(c.txn.pooledPageData, pdata)
		}
		p := getPooledPageStruct(pdata)
		c.txn.pooledPageStructs = append(c.txn.pooledPageStructs, p)

		if i == 0 {
			// First page has header
			p.init(pgno, pageLarge, uint16(pageSize))
			p.header().Txnid = txnid(c.txn.txnID)
			p.setOverflowPages(uint32(numPages))

			// Copy data after header
			end := min(offset+firstPageData, len(data))
			copy(p.Data[pageHeaderSize:], data[offset:end])
			offset = end
		} else {
			// Subsequent pages are raw data with no header
			end := min(offset+pageSize, len(data))
			copy(p.Data, data[offset:end])
			offset = end
		}

		// Mark as dirty
		c.txn.dirtyTracker.set(pgno, p)
	}

	c.tree.LargePages += pgno(numPages)

	return firstPgno, nil
}

// freeOverflow frees overflow pages.
// MDBX format: first page has header, subsequent pages are raw data with no header.
func (c *Cursor) freeOverflow(overflowPgno pgno, dataSize uint32) {
	pageSize := int(c.txn.env.pageSize)
	firstPageData := pageSize - pageHeaderSize

	// Calculate number of pages (same logic as allocateOverflow)
	remaining := int(dataSize) - firstPageData
	numPages := 1
	if remaining > 0 {
		numPages += (remaining + pageSize - 1) / pageSize
	}

	// Add pages to free list
	for i := 0; i < numPages; i++ {
		c.txn.freePages = append(c.txn.freePages, overflowPgno+pgno(i))
	}

	if c.tree.LargePages >= pgno(numPages) {
		c.tree.LargePages -= pgno(numPages)
	}
}

// markTreeDirty marks the tree as modified in this transaction.
func (c *Cursor) markTreeDirty() {
	if c.txn.dbiDirty == nil {
		c.txn.dbiDirty = make([]bool, len(c.txn.trees))
	}
	if int(c.dbi) < len(c.txn.dbiDirty) {
		c.txn.dbiDirty[c.dbi] = true
	}
}

// updateOverflowInPlace attempts to update overflow data in place when the new value
// fits within the same number of pages as the old value. Returns true on success.
func (c *Cursor) updateOverflowInPlace(oldPgno pgno, oldSize uint32, newData []byte) bool {
	pageSize := int(c.txn.env.pageSize)
	firstPageData := pageSize - pageHeaderSize
	newSize := len(newData)

	// Calculate number of pages for old and new data
	oldRemaining := int(oldSize) - firstPageData
	oldNumPages := 1
	if oldRemaining > 0 {
		oldNumPages += (oldRemaining + pageSize - 1) / pageSize
	}

	newRemaining := newSize - firstPageData
	newNumPages := 1
	if newRemaining > 0 {
		newNumPages += (newRemaining + pageSize - 1) / pageSize
	}

	// Can only update in place if new data fits in same or fewer pages
	if newNumPages > oldNumPages {
		return false
	}

	// Fast path for WriteMap mode - write directly to mmap
	if c.txn.env.isWriteMap() {
		// Get direct pointer to overflow data in mmap
		mmapData := c.txn.env.getMmapPageData(oldPgno)
		if mmapData == nil {
			return false // Page not in mmap bounds
		}

		// For multi-page overflow, all pages are contiguous in mmap
		// First page: [header 20 bytes][data]
		// Subsequent pages: [data only]
		// Since pages are contiguous, we can write in one copy for same-size updates
		if newNumPages == oldNumPages && newSize == int(oldSize) {
			// Same size - just copy the data directly (no header update needed)
			copy(mmapData[pageHeaderSize:pageHeaderSize+newSize], newData)
			return true
		}

		// Different size or page count - need to update header and possibly clear
		// Update header on first page
		p := &page{Data: mmapData}
		p.header().Txnid = txnid(c.txn.txnID)
		p.setOverflowPages(uint32(newNumPages))

		// Copy new data
		copy(mmapData[pageHeaderSize:pageHeaderSize+newSize], newData)

		// Clear any remaining space if new data is smaller
		totalOldData := int(oldSize)
		if newSize < totalOldData {
			clear(mmapData[pageHeaderSize+newSize : pageHeaderSize+totalOldData])
		}

		// If we used fewer pages, free the extra ones
		for i := newNumPages; i < oldNumPages; i++ {
			c.txn.freePages = append(c.txn.freePages, oldPgno+pgno(i))
			if c.tree.LargePages > 0 {
				c.tree.LargePages--
			}
		}

		return true
	}

	// Non-WriteMap mode: need to track dirty pages
	offset := 0
	for i := 0; i < newNumPages; i++ {
		currentPgno := oldPgno + pgno(i)

		// Get or create dirty page
		p := c.txn.dirtyTracker.get(currentPgno)
		var pdata []byte
		if p != nil {
			pdata = p.Data
		} else {
			// Page not dirty yet - need to make a dirty copy
			pdata = c.txn.env.getPageDataFromCache()
			srcData := c.txn.getPageDataFast(currentPgno)
			copy(pdata, srcData)
			c.txn.pooledPageData = append(c.txn.pooledPageData, pdata)
			p = getPooledPageStruct(pdata)
			c.txn.pooledPageStructs = append(c.txn.pooledPageStructs, p)
			c.txn.dirtyTracker.set(currentPgno, p)
		}

		if i == 0 {
			// First page has header
			p.header().Txnid = txnid(c.txn.txnID)
			p.setOverflowPages(uint32(newNumPages))
			// Copy data after header
			end := min(offset+firstPageData, newSize)
			copy(pdata[pageHeaderSize:], newData[offset:end])
			if end-offset < firstPageData {
				clear(pdata[pageHeaderSize+end-offset:])
			}
			offset = end
		} else {
			// Subsequent pages are raw data
			end := min(offset+pageSize, newSize)
			copy(pdata, newData[offset:end])
			if end-offset < pageSize {
				clear(pdata[end-offset:])
			}
			offset = end
		}
	}

	// If we used fewer pages, free the extra ones
	for i := newNumPages; i < oldNumPages; i++ {
		c.txn.freePages = append(c.txn.freePages, oldPgno+pgno(i))
		if c.tree.LargePages > 0 {
			c.tree.LargePages--
		}
	}

	return true
}

// updateBigNodeSize updates the node header for an in-place big value update.
// This is called after updateOverflowInPlace succeeds when the size changed.
func (c *Cursor) updateBigNodeSize(newSize uint32) error {
	// Get dirty copy of the page
	dirtyPage, err := c.touchPage()
	if err != nil {
		return err
	}

	idx := int(c.indices[c.top])

	// Update the node's data size field directly
	nodeOffset := int(dirtyPage.entryOffset(idx))
	if nodeOffset < pageHeaderSize || nodeOffset >= len(dirtyPage.Data)-nodeSize {
		return ErrCorruptedError
	}

	// Node header: [dataSize:4][flags:1][extra:1][keySize:2]
	// Update dataSize (first 4 bytes of node header)
	putUint32LE(dirtyPage.Data[nodeOffset:], newSize)

	c.pages[c.top] = dirtyPage
	c.markTreeDirty()
	return nil
}
