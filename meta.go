package gdbx

import (
	"crypto/rand"
	"sync/atomic"
	"unsafe"
)

// Meta page constants
const (
	// metaSize is the size of the meta page structure
	metaSize = 256 // Approximate, actual structure is smaller but aligned

	// numMetas is the number of meta pages (rotating)
	numMetas = 3

	// metaMagic is the MDBX magic number (56-bit prime)
	metaMagic uint64 = 0x59659DBDEF4C11

	// metaDataVersion is the current data format version
	metaDataVersion = 3

	// metaDataMagic combines magic and version
	metaDataMagic = (metaMagic << 8) + metaDataVersion
)

// canary holds user-defined marker values for detecting partial updates.
type canary struct {
	X, Y, Z, V uint64
}

// canarySize is the size of the canary structure
const canarySize = 32

// meta represents a meta page structure.
// This must match the libmdbx meta_t layout exactly.
//
// Memory layout:
//
//	Offset  Size  Field
//	0       8     magic_and_version
//	8       8     txnid_a (two-phase update)
//	16      2     reserve16
//	18      1     validator_id
//	19      1     extra_pagehdr
//	20      20    geometry
//	40      48    gc tree
//	88      48    main tree
//	136     32    canary
//	168     8     sign
//	176     8     txnid_b (two-phase update)
//	184     8     pages_retired
//	192     16    bootid
//	208     16    dxbid
type meta struct {
	// Magic and version for file format identification
	MagicAndVersion [2]uint32

	// Transaction ID - first part of two-phase update
	TxnidA [2]uint32

	// Reserved and extra fields
	Reserve16    uint16
	ValidatorID  uint8
	ExtraPageHdr int8

	// Database geometry
	Geometry geo

	// Core database trees
	GCTree   tree // Garbage collection tree
	MainTree tree // Main database tree

	// User canary for detecting partial updates
	Canary canary

	// Data signature
	Sign [2]uint32

	// Transaction ID - second part of two-phase update
	TxnidB [2]uint32

	// Pages retired after COW
	PagesRetired [2]uint32

	// Boot ID for detecting system reboots
	BootID [16]byte

	// Database GUID
	DXBID [16]byte
}

// readMeta reads a meta page from raw bytes.
func readMeta(data []byte) (*meta, error) {
	if len(data) < 220 { // Minimum meta size
		return nil, errMetaTooSmall
	}
	return (*meta)(unsafe.Pointer(&data[0])), nil
}

// magicValid returns true if the magic number is valid.
func (m *meta) magicValid() bool {
	magic := uint64(m.MagicAndVersion[0]) | (uint64(m.MagicAndVersion[1]) << 32)
	return (magic >> 8) == metaMagic
}

// version returns the data format version.
func (m *meta) version() uint8 {
	return uint8(m.MagicAndVersion[0])
}

// txnidASafe atomically reads the first txnid.
func (m *meta) txnidASafe() txnid {
	lo := atomic.LoadUint32(&m.TxnidA[0])
	hi := atomic.LoadUint32(&m.TxnidA[1])
	return txnid(uint64(lo) | (uint64(hi) << 32))
}

// txnidBSafe atomically reads the second txnid.
func (m *meta) txnidBSafe() txnid {
	lo := atomic.LoadUint32(&m.TxnidB[0])
	hi := atomic.LoadUint32(&m.TxnidB[1])
	return txnid(uint64(lo) | (uint64(hi) << 32))
}

// txnID returns the transaction ID (must have txnid_a == txnid_b).
func (m *meta) txnID() txnid {
	return txnid(uint64(m.TxnidA[0]) | (uint64(m.TxnidA[1]) << 32))
}

// setTxnid sets both txnid fields.
func (m *meta) setTxnid(tid txnid) {
	m.TxnidA[0] = uint32(tid)
	m.TxnidA[1] = uint32(tid >> 32)
	m.TxnidB[0] = uint32(tid)
	m.TxnidB[1] = uint32(tid >> 32)
}

// isConsistent returns true if txnid_a == txnid_b (complete write).
func (m *meta) isConsistent() bool {
	return m.txnidASafe() == m.txnidBSafe()
}

// isWeak returns true if this is a weak (non-synced) meta.
func (m *meta) isWeak() bool {
	sign := uint64(m.Sign[0]) | (uint64(m.Sign[1]) << 32)
	return sign <= 1 // DATASIGN_NONE=0 or DATASIGN_WEAK=1
}

// isSteady returns true if this is a steady (synced) meta.
func (m *meta) isSteady() bool {
	return !m.isWeak()
}

// Data signature constants matching libmdbx
const (
	datasignWeak   = 1                    // DATASIGN_WEAK: not synced to disk
	datasignSteady = 0xFFFFFFFFFFFFFFFF   // ~DATASIGN_NONE: synced to disk
)

// setSignWeak marks the meta as weak (not synced).
func (m *meta) setSignWeak() {
	m.Sign[0] = uint32(datasignWeak)
	m.Sign[1] = uint32(datasignWeak >> 32)
}

// setSignSteady marks the meta as steady (synced).
func (m *meta) setSignSteady() {
	m.Sign[0] = 0xFFFFFFFF
	m.Sign[1] = 0xFFFFFFFF
}

// pageSize returns the database page size.
// In MDBX v3, the page size is stored in the GC tree's DupfixSize field
// (which serves as a pagesize field in this context).
func (m *meta) pageSize() uint32 {
	return m.GCTree.DupfixSize
}

// validate checks if the meta page is valid.
func (m *meta) validate() error {
	if !m.magicValid() {
		return errMetaInvalidMagic
	}

	version := m.version()
	if version < 2 || version > metaDataVersion {
		return errMetaInvalidVersion
	}

	if !m.isConsistent() {
		return errMetaInconsistent
	}

	return nil
}

// clone creates a copy of the meta page.
func (m *meta) clone() *meta {
	clone := *m
	return &clone
}

// metaTriple holds references to all three meta pages with their state.
type metaTriple struct {
	metas  [numMetas]*meta
	txnids [numMetas]txnid
	recent int // Index of most recent valid meta
	steady int // Index of most recent steady (synced) meta
}

// newMetaTriple creates a metaTriple from page data.
func newMetaTriple(pages [numMetas][]byte) (*metaTriple, error) {
	mt := &metaTriple{
		recent: -1,
		steady: -1,
	}

	var maxTxnid, maxSteadyTxnid txnid

	for i := 0; i < numMetas; i++ {
		m, err := readMeta(pages[i])
		if err != nil {
			continue
		}

		if err := m.validate(); err != nil {
			continue
		}

		mt.metas[i] = m
		mt.txnids[i] = m.txnID()

		if mt.txnids[i] > maxTxnid {
			maxTxnid = mt.txnids[i]
			mt.recent = i
		}

		if m.isSteady() && mt.txnids[i] > maxSteadyTxnid {
			maxSteadyTxnid = mt.txnids[i]
			mt.steady = i
		}
	}

	if mt.recent < 0 {
		return nil, errMetaNoValid
	}

	// If no steady meta, use recent
	if mt.steady < 0 {
		mt.steady = mt.recent
	}

	return mt, nil
}

// updateFromPages updates the metaTriple in place from page data without allocation.
func (mt *metaTriple) updateFromPages(pages [numMetas][]byte) error {
	mt.recent = -1
	mt.steady = -1

	var maxTxnid, maxSteadyTxnid txnid

	for i := 0; i < numMetas; i++ {
		m, err := readMeta(pages[i])
		if err != nil {
			mt.metas[i] = nil
			mt.txnids[i] = 0
			continue
		}

		if err := m.validate(); err != nil {
			mt.metas[i] = nil
			mt.txnids[i] = 0
			continue
		}

		mt.metas[i] = m
		mt.txnids[i] = m.txnID()

		if mt.txnids[i] > maxTxnid {
			maxTxnid = mt.txnids[i]
			mt.recent = i
		}

		if m.isSteady() && mt.txnids[i] > maxSteadyTxnid {
			maxSteadyTxnid = mt.txnids[i]
			mt.steady = i
		}
	}

	if mt.recent < 0 {
		return errMetaNoValid
	}

	// If no steady meta, use recent
	if mt.steady < 0 {
		mt.steady = mt.recent
	}

	return nil
}

// recentMeta returns the most recently committed meta page.
func (mt *metaTriple) recentMeta() *meta {
	if mt.recent < 0 {
		return nil
	}
	return mt.metas[mt.recent]
}

// steadyMeta returns the most recently synced meta page.
func (mt *metaTriple) steadyMeta() *meta {
	if mt.steady < 0 {
		return nil
	}
	return mt.metas[mt.steady]
}

// nextMetaIndex returns the index to use for the next meta page update.
// Uses rotating scheme: picks the oldest valid meta.
func (mt *metaTriple) nextMetaIndex() int {
	// Find meta with lowest txnid (oldest)
	minIdx := 0
	minTxnid := mt.txnids[0]

	for i := 1; i < numMetas; i++ {
		if mt.txnids[i] < minTxnid {
			minTxnid = mt.txnids[i]
			minIdx = i
		}
	}

	return minIdx
}

// Meta page errors
var (
	errMetaTooSmall       = &pageError{"meta page too small"}
	errMetaInvalidMagic   = &pageError{"invalid magic number"}
	errMetaInvalidVersion = &pageError{"invalid format version"}
	errMetaInconsistent   = &pageError{"meta page inconsistent (incomplete write)"}
	errMetaNoValid        = &pageError{"no valid meta page found"}
)

// beginMetaUpdate starts a two-phase meta update by setting txnid_b to 0.
func (m *meta) beginMetaUpdate(newTxnid txnid) {
	// Set txnid_a to new value
	atomic.StoreUint32(&m.TxnidA[0], uint32(newTxnid))
	atomic.StoreUint32(&m.TxnidA[1], uint32(newTxnid>>32))

	// Set txnid_b to 0 to mark update in progress
	atomic.StoreUint32(&m.TxnidB[0], 0)
	atomic.StoreUint32(&m.TxnidB[1], 0)
}

// endMetaUpdate completes a two-phase meta update by setting txnid_b.
func (m *meta) endMetaUpdate(tid txnid) {
	atomic.StoreUint32(&m.TxnidB[0], uint32(tid))
	atomic.StoreUint32(&m.TxnidB[1], uint32(tid>>32))
}

// initMeta initializes a meta page for a new database.
func initMeta(m *meta, pageSize uint32, tid txnid) {
	// Set magic and version
	magic := metaDataMagic
	m.MagicAndVersion[0] = uint32(magic)
	m.MagicAndVersion[1] = uint32(magic >> 32)

	// Set transaction ID
	m.setTxnid(tid)

	// Initialize geometry
	// Geo fields: GrowPV, ShrinkPV, Lower, Upper (DBPgsize), Now, Next
	// - GrowPV/ShrinkPV: packed exponential values for grow/shrink thresholds
	// - Lower: minimum datafile size in pages
	// - Upper (DBPgsize): maximum datafile size in pages
	// - Now: current allocated pages
	// - Next: next page number to allocate
	//
	// Default values match libmdbx's default initialization:
	// GrowPV=0x180, ShrinkPV=0x300, Upper=0x1800000 (about 100GB at 4KB pages)
	m.Geometry = geo{
		GrowPV:   0x0180, // Default grow step (matches libmdbx)
		ShrinkPV: 0x0300, // Default shrink threshold (matches libmdbx)
		Lower:    numMetas,
		DBPgsize: 0x1800000, // "Upper" - max pages (~100GB at 4KB)
		Now:      numMetas,
		Next:     numMetas, // Next allocatable page (after meta pages)
	}

	// Initialize trees as empty
	// GC tree uses INTEGERKEY (txnid-based keys) and stores page size in DupfixSize
	m.GCTree.Flags = treeFlagIntegerKey
	m.GCTree.DupfixSize = pageSize
	m.GCTree.Root = invalidPgno
	m.MainTree.Root = invalidPgno

	// Set sign to steady (0xFFFFFFFFFFFFFFFF).
	// In libmdbx: DATASIGN_NONE=0, DATASIGN_WEAK=1, anything > 1 is steady.
	// New databases are synced on init, so they start as steady.
	m.setSignSteady()

	// Generate random boot ID
	rand.Read(m.BootID[:])
}

// geo represents database geometry/size parameters.
// This structure must match the libmdbx geo_t layout exactly (20 bytes).
// In MDBX, this is a union where some fields have dual meanings.
type geo struct {
	GrowPV   uint16 // Growth step as packed exponential value
	ShrinkPV uint16 // Shrink threshold as packed exponential value
	Lower    pgno   // Minimal size of datafile in pages (or unused)
	DBPgsize pgno   // Page size in bytes (or Upper in some contexts)
	Now      pgno   // Current size / first unallocated page (or end_pgno)
	Next     pgno   // Next page to allocate
}

// geoSize is the size of the geo structure in bytes
const geoSize = 20

// tree represents database/table metadata.
// This structure must match the libmdbx tree_t layout exactly (48 bytes).
type tree struct {
	Flags       uint16 // Database flags (REVERSEKEY, DUPSORT, etc)
	Height      uint16 // Height of this B+ tree
	DupfixSize  uint32 // Key size for DUPFIXED pages
	Root        pgno   // Root page number
	BranchPages pgno   // Number of branch pages
	LeafPages   pgno   // Number of leaf pages
	LargePages  pgno   // Number of large/overflow pages
	Sequence    uint64 // Table sequence counter
	Items       uint64 // Number of data items
	ModTxnid    txnid  // Transaction ID of last modification
}

// treeSize is the size of the tree structure in bytes
const treeSize = 48

// Database flags
const (
	// treeFlagReverseKey uses reverse string comparison for keys
	treeFlagReverseKey uint16 = 0x02

	// treeFlagDupSort allows multiple values per key (sorted)
	treeFlagDupSort uint16 = 0x04

	// treeFlagIntegerKey uses native-endian uint32/uint64 keys
	treeFlagIntegerKey uint16 = 0x08

	// treeFlagDupFixed uses fixed-size values in DUPSORT tables
	treeFlagDupFixed uint16 = 0x10

	// treeFlagIntegerDup uses fixed-size integer values in DUPSORT
	treeFlagIntegerDup uint16 = 0x20

	// treeFlagReverseDup uses reverse comparison for values
	treeFlagReverseDup uint16 = 0x40
)

// isEmpty returns true if the tree has no items.
func (t *tree) isEmpty() bool {
	return t.Root == invalidPgno || t.Items == 0
}

// isDupSort returns true if the database allows duplicate keys.
func (t *tree) isDupSort() bool {
	return t.Flags&treeFlagDupSort != 0
}

// isDupFixed returns true if duplicate values are fixed-size.
func (t *tree) isDupFixed() bool {
	return t.Flags&treeFlagDupFixed != 0
}

// isIntegerKey returns true if keys are native-endian integers.
func (t *tree) isIntegerKey() bool {
	return t.Flags&treeFlagIntegerKey != 0
}

// isReverseKey returns true if keys use reverse comparison.
func (t *tree) isReverseKey() bool {
	return t.Flags&treeFlagReverseKey != 0
}

// totalPages returns the total number of pages used by this tree.
func (t *tree) totalPages() uint64 {
	return uint64(t.BranchPages) + uint64(t.LeafPages) + uint64(t.LargePages)
}

// clone creates a copy of the tree metadata.
func (t *tree) clone() *tree {
	clone := *t
	return &clone
}

// reset resets the tree to empty state.
func (t *tree) reset() {
	t.Root = invalidPgno
	t.Height = 0
	t.BranchPages = 0
	t.LeafPages = 0
	t.LargePages = 0
	t.Items = 0
	// Keep Flags, DupfixSize, Sequence, and ModTxnid
}

// sizeBytes returns the current database size in bytes.
func (g *geo) sizeBytes(pageSize uint) uint64 {
	return uint64(g.Now) * uint64(pageSize)
}

// minSizeBytes returns the minimum database size in bytes.
func (g *geo) minSizeBytes(pageSize uint) uint64 {
	return uint64(g.Lower) * uint64(pageSize)
}

// maxSizeBytes returns the maximum database size in bytes.
func (g *geo) maxSizeBytes(pageSize uint) uint64 {
	return uint64(g.Next) * uint64(pageSize)
}

// clone creates a copy of the geometry.
func (g *geo) clone() *geo {
	clone := *g
	return &clone
}
