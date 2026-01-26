package gdbx

// Database format constants - must match libmdbx for file compatibility
const (
	// Magic is a 56-bit prime that identifies MDBX files
	Magic uint64 = 0x59659DBDEF4C11

	// DataVersion is the database file format version
	DataVersion = 3

	// LockVersion is the lock file format version
	LockVersion = 6

	// DataMagic combines magic and version for validation
	DataMagic = (Magic << 8) + DataVersion

	// LockMagic combines magic and lock version
	LockMagic = (Magic << 8) + LockVersion
)

// Page size constraints
const (
	// MinPageSize is the minimum allowed page size (256 bytes)
	MinPageSize = 256

	// MaxPageSize is the maximum allowed page size (64KB)
	MaxPageSize = 65536

	// DefaultPageSize is the default page size (4KB)
	DefaultPageSize = 4096
)

// Page header and node sizes
const (
	// PageHeaderSize is the fixed page header size (20 bytes)
	PageHeaderSize = 20

	// NodeHeaderSize is the fixed node header size (8 bytes)
	NodeHeaderSize = 8
)

// Database limits
const (
	// MaxDBI is the maximum number of named databases
	MaxDBI = 32765

	// MaxDataSize is the maximum size of a data item
	MaxDataSize = 0x7fff0000

	// MaxPageNo is the maximum page number
	MaxPageNo uint32 = 0x7FFFffff

	// NumMetas is the number of meta pages (rotating)
	NumMetas = 3

	// MinPageNo is the first non-meta page number
	MinPageNo = NumMetas

	// CoreDBs is the number of core databases (GC and Main)
	CoreDBs = 2

	// FreeDBI is the handle for the GC/free page database
	FreeDBI = 0

	// MainDBI is the handle for the main database
	MainDBI = 1
)

// Transaction ID constants
const (
	// MinTxnID is the minimum valid transaction ID
	MinTxnID uint64 = 1

	// InitialTxnID is the initial transaction ID for new databases
	InitialTxnID uint64 = MinTxnID + NumMetas - 1

	// InvalidTxnID represents an invalid transaction ID
	InvalidTxnID uint64 = 0xFFFFFFFFFFFFFFFF
)

// InvalidPageNo represents an invalid page number (empty tree marker)
const InvalidPageNo uint32 = 0xFFFFFFFF

// PageFlags define page types
type PageFlags uint16

const (
	// PageBranch indicates a branch (internal) page
	PageBranch PageFlags = 0x01

	// PageLeaf indicates a leaf page
	PageLeaf PageFlags = 0x02

	// PageLarge indicates a large/overflow page
	PageLarge PageFlags = 0x04

	// PageMeta indicates a meta page
	PageMeta PageFlags = 0x08

	// PageLegacyDirty is a legacy dirty flag (pre v0.10)
	PageLegacyDirty PageFlags = 0x10

	// PageBad is an explicit flag for invalid pages
	PageBad = PageLegacyDirty

	// PageDupfix indicates a DUPFIXED page
	PageDupfix PageFlags = 0x20

	// PageSubP indicates a sub-page for DUPSORT
	PageSubP PageFlags = 0x40

	// PageSpilled indicates a page spilled in parent txn
	PageSpilled PageFlags = 0x2000

	// PageLoose indicates a freed page available for reuse
	PageLoose PageFlags = 0x4000

	// PageFrozen indicates a retired page with known status
	PageFrozen PageFlags = 0x8000
)

// NodeFlags define node types within pages
type NodeFlags uint8

const (
	// NodeBig indicates data is on a large/overflow page
	NodeBig NodeFlags = 0x01

	// NodeTree indicates data is a B-tree (sub-database)
	NodeTree NodeFlags = 0x02

	// NodeDup indicates data has duplicates
	NodeDup NodeFlags = 0x04
)

// Label is a label for an environment (mdbx-go compatibility)
type Label string

// Default is the default environment label (mdbx-go compatibility)
const Default Label = "default"

// Environment flags (untyped uint constants for mdbx-go compatibility)
const (
	// EnvDefaults is the default (durable) mode
	EnvDefaults uint = 0

	// Validation enables extra validation of DB structure
	Validation uint = 0x00002000

	// NoSubdir means the path is a filename, not a directory
	NoSubdir uint = 0x00004000

	// ReadOnly opens the environment in read-only mode
	ReadOnly uint = 0x00020000

	// Exclusive opens in exclusive/monopolistic mode
	Exclusive uint = 0x00400000

	// Accede uses existing mode if opened by other processes
	Accede uint = 0x40000000

	// WriteMap maps data with write permission (faster, riskier)
	WriteMap uint = 0x00080000

	// NoStickyThreads allows transactions to move between threads
	NoStickyThreads uint = 0x00200000

	// NoReadAhead disables OS readahead
	NoReadAhead uint = 0x00800000

	// NoMemInit skips zeroing malloc'd memory
	NoMemInit uint = 0x01000000

	// LifoReclaim uses LIFO policy for GC reclamation
	LifoReclaim uint = 0x04000000

	// PagePerturb fills released pages with garbage (debug)
	PagePerturb uint = 0x08000000

	// NoMetaSync skips meta page sync after commit
	NoMetaSync uint = 0x00040000

	// SafeNoSync skips sync but keeps steady commits
	SafeNoSync uint = 0x00010000

	// UtterlyNoSync skips all syncs (dangerous)
	UtterlyNoSync = SafeNoSync | NoMetaSync

	// Durable is an alias for EnvDefaults (mdbx-go compatibility)
	Durable = EnvDefaults

	// Readonly is an alias for ReadOnly (mdbx-go compatibility)
	Readonly = ReadOnly

	// NoTLS is an alias for NoStickyThreads (mdbx-go compatibility)
	NoTLS = NoStickyThreads

	// NoReadahead is an alias for NoReadAhead (mdbx-go compatibility)
	NoReadahead = NoReadAhead
)

// Transaction flags (untyped uint constants for mdbx-go compatibility)
const (
	// TxnReadWrite is the default read-write transaction
	TxnReadWrite uint = 0

	// TxnReadOnly creates a read-only transaction
	TxnReadOnly uint = 0x20000

	// TxnReadOnlyPrepare prepares a read-only transaction
	TxnReadOnlyPrepare = TxnReadOnly | 0x01000000

	// TxnTry attempts a non-blocking write transaction
	TxnTry uint = 0x10000000

	// TxnNoMetaSync skips meta sync for this transaction
	TxnNoMetaSync uint = 0x00040000

	// TxnNoSync skips sync for this transaction
	TxnNoSync uint = 0x00010000
)

// Transaction flag aliases (mdbx-go short naming convention)
const (
	TxRW         = TxnReadWrite
	TxRO         = TxnReadOnly
	TxNoSync     = TxnNoSync
	TxNoMetaSync = TxnNoMetaSync
)

// Database flags (untyped uint constants for mdbx-go compatibility)
const (
	// DBDefaults uses default comparison and features
	DBDefaults uint = 0

	// ReverseKey uses reverse string comparison for keys
	ReverseKey uint = 0x02

	// DupSort allows multiple values per key (sorted)
	DupSort uint = 0x04

	// IntegerKey uses uint32/uint64 keys in native byte order
	IntegerKey uint = 0x08

	// DupFixed uses fixed-size values in DUPSORT tables
	DupFixed uint = 0x10

	// IntegerDup uses fixed-size integer values in DUPSORT
	IntegerDup uint = 0x20

	// ReverseDup uses reverse comparison for values
	ReverseDup uint = 0x40

	// Create creates the database if it doesn't exist
	Create uint = 0x40000

	// DBAccede opens with unknown flags
	DBAccede uint = 0x40000000
)

// Put flags (untyped uint constants for mdbx-go compatibility)
const (
	// Upsert is the default insert-or-update mode
	Upsert uint = 0

	// NoOverwrite returns error if key exists
	NoOverwrite uint = 0x10

	// NoDupData returns error if key-value pair exists (DUPSORT)
	NoDupData uint = 0x20

	// Current overwrites current item (cursor put)
	Current uint = 0x40

	// AllDups replaces all duplicates for key
	AllDups uint = 0x80

	// Reserve reserves space without copying data
	Reserve uint = 0x10000

	// Append assumes data is being appended
	Append uint = 0x20000

	// AppendDup assumes duplicate data is being appended
	AppendDup uint = 0x40000

	// Multiple stores multiple data items (DUPFIXED)
	Multiple uint = 0x80000
)

// Copy flags
const (
	// CopyDefaults performs a standard copy
	CopyDefaults uint = 0

	// CopyCompact compacts the database during copy
	CopyCompact uint = 0x01
)

// Warmup flags (mdbx-go compatibility)
const (
	// WarmupDefault is the default warmup behavior
	WarmupDefault uint = 0

	// WarmupForce forces warmup even if already done
	WarmupForce uint = 0x01

	// WarmupOomSafe uses OOM-safe warmup (slower but safer)
	WarmupOomSafe uint = 0x02

	// WarmupLock holds lock during warmup
	WarmupLock uint = 0x04

	// WarmupTouchLimit limits pages touched during warmup
	WarmupTouchLimit uint = 0x08

	// WarmupRelease releases pages after warmup
	WarmupRelease uint = 0x10
)

// File names
const (
	// DataFileName is the data file name in an environment directory
	DataFileName = "mdbx.dat"

	// LockFileName is the lock file name in an environment directory
	LockFileName = "mdbx.lck"

	// LockSuffix is appended when NoSubdir is used
	LockSuffix = "-lck"
)

// Log level constants (mdbx-go compatibility)
type LogLvl int

const (
	LogLvlFatal       LogLvl = 0
	LogLvlError       LogLvl = 1
	LogLvlWarn        LogLvl = 2
	LogLvlNotice      LogLvl = 3
	LogLvlVerbose     LogLvl = 4
	LogLvlDebug       LogLvl = 5
	LogLvlTrace       LogLvl = 6
	LogLvlExtra       LogLvl = 7
	LogLvlDoNotChange LogLvl = -1
)

// LoggerDoNotChange is an alias for LogLvlDoNotChange (mdbx-go compatibility)
const LoggerDoNotChange = LogLvlDoNotChange

// Debug constants (mdbx-go compatibility)
const (
	DbgAssert          uint = 1
	DbgAudit           uint = 2
	DbgJitter          uint = 4
	DbgDump            uint = 8
	DbgLegacyMultiOpen uint = 16
	DbgLegacyTxOverlap uint = 32
	DbgDoNotChange     uint = 0xFFFFFFFF
)

// AllowTxOverlap allows overlapping transactions (mdbx-go compatibility)
const AllowTxOverlap = DbgLegacyTxOverlap

// MaxDbi is the maximum number of named databases (mdbx-go compatibility alias)
const MaxDbi = MaxDBI
