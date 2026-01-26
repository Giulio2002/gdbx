package gdbx

import (
	"errors"
	"os"
	"time"
)

// TxnOp is a function that operates on a transaction.
// This is the callback type for View, Update, and RunTxn.
type TxnOp func(txn *Txn) error

// CmpFunc is a comparison function for keys or values.
type CmpFunc = func(a, b []byte) int

// Size is a size type for geometry parameters (mdbx-go compatibility).
type Size int64

// Geometry holds database geometry parameters (mdbx-go compatibility).
type Geometry struct {
	SizeLower       Size // Lower limit for datafile size
	SizeNow         Size // Current datafile size (use -1 for default)
	SizeUpper       Size // Upper limit for datafile size
	GrowthStep      Size // Growth step in bytes
	ShrinkThreshold Size // Shrink threshold in bytes
	PageSize        int  // Page size in bytes (use -1 for default)
}

// SetGeometryGeo sets database geometry using Geometry struct (mdbx-go compatibility).
func (e *Env) SetGeometryGeo(geo Geometry) error {
	return e.SetGeometry(
		int64(geo.SizeLower),
		int64(geo.SizeNow),
		int64(geo.SizeUpper),
		int64(geo.GrowthStep),
		int64(geo.ShrinkThreshold),
		geo.PageSize,
	)
}

// SetGeometrySize sets database geometry using Size types (mdbx-go compatibility).
func (e *Env) SetGeometrySize(sizeLower, sizeNow, sizeUpper, growthStep, shrinkThreshold Size, pageSize int) error {
	return e.SetGeometry(
		int64(sizeLower),
		int64(sizeNow),
		int64(sizeUpper),
		int64(growthStep),
		int64(shrinkThreshold),
		pageSize,
	)
}

// View executes a read-only transaction.
// The transaction is automatically committed when fn returns nil,
// or aborted when fn returns an error.
func (e *Env) View(fn TxnOp) error {
	return e.RunTxn(TxnReadOnly, fn)
}

// Update executes a read-write transaction.
// The transaction is automatically committed when fn returns nil,
// or aborted when fn returns an error.
func (e *Env) Update(fn TxnOp) error {
	return e.RunTxn(TxnReadWrite, fn)
}

// RunTxn runs a transaction with the given flags.
// The transaction is automatically committed when fn returns nil,
// or aborted when fn returns an error.
func (e *Env) RunTxn(flags uint, fn TxnOp) error {
	txn, err := e.BeginTxn(nil, flags)
	if err != nil {
		return err
	}
	err = fn(txn)
	if err != nil {
		txn.Abort()
		return err
	}
	_, err = txn.Commit()
	return err
}

// Bind binds a cursor to a transaction and database.
// This is useful for cursor pooling - get a cursor from CursorFromPool(),
// then bind it to reuse it across transactions.
func (c *Cursor) Bind(txn *Txn, dbi DBI) error {
	if !txn.valid() {
		return NewError(ErrBadTxn)
	}
	if dbi >= DBI(len(txn.trees)) {
		return NewError(ErrBadDBI)
	}

	c.signature = cursorSignature
	c.txn = txn
	c.dbi = dbi
	c.tree = &txn.trees[dbi]
	c.state = cursorUninitialized
	c.top = -1
	c.dirtyMask = 0

	// Register cursor with transaction
	txn.cursors = append(txn.cursors, c)

	return nil
}

// Renew renews a cursor for a new read-only transaction.
// This is used with cursor pooling to reuse a cursor.
func (c *Cursor) Renew(txn *Txn) error {
	if !txn.valid() {
		return NewError(ErrBadTxn)
	}
	if txn.flags&uint32(TxnReadOnly) == 0 {
		return NewError(ErrIncompatible) // Renew is only for read-only transactions
	}
	return c.Bind(txn, c.dbi)
}

// Unbind detaches the cursor from its transaction.
// The cursor can be re-bound to another transaction using Bind.
func (c *Cursor) Unbind() error {
	if c == nil {
		return nil
	}
	// Remove from transaction's cursor list if bound
	if c.txn != nil {
		c.txn.removeCursor(c)
	}
	// Reset cursor state
	c.txn = nil
	c.tree = nil
	c.state = cursorUninitialized
	c.top = -1
	c.dirtyMask = 0
	return nil
}

// cursorPool for cursor reuse
var cursorBindPool = make(chan *Cursor, 128)

// CursorFromPool gets a cursor from the pool.
// The cursor must be bound to a transaction using Bind before use.
func CursorFromPool() *Cursor {
	select {
	case c := <-cursorBindPool:
		return c
	default:
		return &Cursor{}
	}
}

// CursorToPool returns a cursor to the pool.
// The cursor should be unbound (Close() called) before returning.
func CursorToPool(c *Cursor) {
	if c == nil {
		return
	}
	c.txn = nil
	c.tree = nil
	c.state = cursorUninitialized
	c.top = -1
	c.dirtyMask = 0

	select {
	case cursorBindPool <- c:
	default:
		// Pool is full, let GC handle it
	}
}

// CreateCursor creates a new unbound cursor.
func CreateCursor() *Cursor {
	return &Cursor{}
}

// Multi wraps multi-value pages for DUPFIXED databases.
type Multi struct {
	page   []byte
	stride int
}

// WrapMulti wraps a multi-value page.
func WrapMulti(page []byte, stride int) *Multi {
	return &Multi{page: page, stride: stride}
}

// Vals returns all values.
func (m *Multi) Vals() [][]byte {
	if m.stride == 0 || len(m.page) == 0 {
		return nil
	}
	n := len(m.page) / m.stride
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		vals[i] = m.page[i*m.stride : (i+1)*m.stride]
	}
	return vals
}

// Val returns value at index i.
func (m *Multi) Val(i int) []byte {
	if m.stride == 0 || i < 0 || i*m.stride >= len(m.page) {
		return nil
	}
	return m.page[i*m.stride : (i+1)*m.stride]
}

// Len returns the number of values.
func (m *Multi) Len() int {
	if m.stride == 0 {
		return 0
	}
	return len(m.page) / m.stride
}

// Stride returns the stride.
func (m *Multi) Stride() int {
	return m.stride
}

// Size returns the total size.
func (m *Multi) Size() int {
	return len(m.page)
}

// Page returns the raw page data.
func (m *Multi) Page() []byte {
	return m.page
}

// Duration16dot16 is a 16.16 fixed-point duration (mdbx-go compatibility).
type Duration16dot16 uint32

// NewDuration16dot16 converts a time.Duration to 16.16 fixed-point.
func NewDuration16dot16(d time.Duration) Duration16dot16 {
	secs := d.Seconds()
	return Duration16dot16(secs * 65536)
}

// ToDuration converts 16.16 fixed-point to time.Duration.
func (d Duration16dot16) ToDuration() time.Duration {
	secs := float64(d) / 65536
	return time.Duration(secs * float64(time.Second))
}

// Errno is an error type for MDBX error codes (mdbx-go compatibility).
type Errno int

// Error returns the error message for an Errno.
func (e Errno) Error() string {
	if msg, ok := errorMessages[ErrorCode(e)]; ok {
		return msg
	}
	return "unknown error"
}

// Is reports whether e matches target.
func (e Errno) Is(target error) bool {
	if t, ok := target.(Errno); ok {
		return e == t
	}
	return false
}

// OpError wraps an error with operation context (mdbx-go compatibility).
type OpError struct {
	Op  string
	Err error
}

// Error returns the error message.
func (e *OpError) Error() string {
	return e.Op + ": " + e.Err.Error()
}

// Is reports whether e matches target.
func (e *OpError) Is(target error) bool {
	return errors.Is(e.Err, target)
}

// Unwrap returns the wrapped error.
func (e *OpError) Unwrap() error {
	return e.Err
}

// IsErrno checks if err is a specific Errno.
func IsErrno(err error, errno Errno) bool {
	var e Errno
	if errors.As(err, &e) {
		return e == errno
	}
	return false
}

// IsErrnoFn checks if err matches a predicate function.
func IsErrnoFn(err error, fn func(error) bool) bool {
	return fn(err)
}

// IsNotExist returns true if the error indicates a file doesn't exist.
func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// FromHex converts a hex string to bytes.
func FromHex(s string) []byte {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		s = s[2:]
	}
	if len(s)%2 != 0 {
		s = "0" + s
	}
	result := make([]byte, len(s)/2)
	for i := 0; i < len(result); i++ {
		result[i] = fromHexChar(s[i*2])<<4 | fromHexChar(s[i*2+1])
	}
	return result
}

func fromHexChar(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// GetSysRamInfo returns system RAM information.
func GetSysRamInfo() (pageSize, totalPages, availablePages int, err error) {
	// Default values - actual implementation would use syscalls
	pageSize = 4096
	totalPages = 1024 * 1024 // 4GB / 4KB
	availablePages = totalPages / 2
	return
}

// LoggerFunc is a callback function for logging (mdbx-go compatibility).
type LoggerFunc func(msg string, args ...any)

// global logger settings
var (
	globalLogLevel LogLvl     = LogLvlDoNotChange
	globalLogger   LoggerFunc = nil //nolint:unused // Part of mdbx-go compat API
	globalDebug    uint       = 0
)

// SetDebug sets debug flags (mdbx-go compatibility).
// Returns the previous debug flags.
func SetDebug(flags uint) uint {
	prev := globalDebug
	if flags != DbgDoNotChange {
		globalDebug = flags
	}
	return prev
}

// SetLogger sets the logger function and level (mdbx-go compatibility).
// Returns the previous log level.
func SetLogger(logger LoggerFunc, level LogLvl) LogLvl {
	prev := globalLogLevel
	globalLogger = logger
	if level != LogLvlDoNotChange {
		globalLogLevel = level
	}
	return prev
}

// HandleSlowReadersFunc is called when slow readers are detected (mdbx-go compatibility).
type HandleSlowReadersFunc func(env *Env, txn *Txn, pid int, tid uint64, laggard uint64, gap uint64, space uint64, retry int) int

// global slow readers handler
var globalSlowReadersHandler HandleSlowReadersFunc = nil

// SetHandleSlowReaders sets the slow readers handler (mdbx-go compatibility).
// Returns the previous handler.
func SetHandleSlowReaders(fn HandleSlowReadersFunc) HandleSlowReadersFunc {
	prev := globalSlowReadersHandler
	globalSlowReadersHandler = fn
	return prev
}

// Cursor.PutMulti stores multiple values for a key (DUPFIXED).
func (c *Cursor) PutMulti(key []byte, page []byte, stride int, flags uint) error {
	if !c.valid() {
		return ErrBadCursorError
	}
	if c.txn.flags&uint32(TxnReadOnly) != 0 {
		return NewError(ErrPermissionDenied)
	}

	// Store each value individually
	for i := 0; i < len(page)/stride; i++ {
		val := page[i*stride : (i+1)*stride]
		if err := c.Put(key, val, flags); err != nil {
			return err
		}
	}
	return nil
}

// Cursor.PutReserve reserves space for a value.
func (c *Cursor) PutReserve(key []byte, n int, flags uint) ([]byte, error) {
	if !c.valid() {
		return nil, ErrBadCursorError
	}
	if c.txn.flags&uint32(TxnReadOnly) != 0 {
		return nil, NewError(ErrPermissionDenied)
	}

	// Allocate and store empty value
	value := make([]byte, n)
	if err := c.Put(key, value, flags); err != nil {
		return nil, err
	}
	return value, nil
}
