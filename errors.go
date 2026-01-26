package gdbx

import (
	"errors"
	"fmt"
)

// Error represents a gdbx error with an error code
type Error struct {
	Code    ErrorCode
	Message string
	Err     error // wrapped error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("gdbx: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("gdbx: %s", e.Message)
}

func (e *Error) Unwrap() error {
	return e.Err
}

// ErrorCode represents MDBX-compatible error codes
type ErrorCode int

// Error codes - matching MDBX for compatibility
const (
	// Success indicates the operation completed successfully
	Success ErrorCode = 0

	// ErrSuccess is an alias for Success (mdbx-go compatibility)
	ErrSuccess = Success

	// ResultFalse is an alias for Success
	ResultFalse = Success

	// ResultTrue indicates success with special meaning
	ResultTrue ErrorCode = -1

	// ErrKeyExist indicates the key/data pair already exists
	ErrKeyExist ErrorCode = -30799

	// ErrNotFound indicates the key/data pair was not found (EOF)
	ErrNotFound ErrorCode = -30798

	// ErrPageNotFound indicates a requested page was not found (corruption)
	ErrPageNotFound ErrorCode = -30797

	// ErrCorrupted indicates the database is corrupted
	ErrCorrupted ErrorCode = -30796

	// ErrPanic indicates a fatal environment error
	ErrPanic ErrorCode = -30795

	// ErrVersionMismatch indicates DB version doesn't match library
	ErrVersionMismatch ErrorCode = -30794

	// ErrInvalid indicates the file is not a valid MDBX file
	ErrInvalid ErrorCode = -30793

	// ErrMapFull indicates the environment mapsize was reached
	ErrMapFull ErrorCode = -30792

	// ErrDBsFull indicates the environment maxdbs was reached
	ErrDBsFull ErrorCode = -30791

	// ErrReadersFull indicates the environment maxreaders was reached
	ErrReadersFull ErrorCode = -30790

	// ErrTxnFull indicates the transaction has too many dirty pages
	ErrTxnFull ErrorCode = -30788

	// ErrCursorFull indicates cursor stack overflow (corruption)
	ErrCursorFull ErrorCode = -30787

	// ErrPageFull indicates a page has no space (internal error)
	ErrPageFull ErrorCode = -30786

	// ErrUnableExtendMapsize indicates mapping couldn't be extended
	ErrUnableExtendMapsize ErrorCode = -30785

	// ErrIncompatible indicates incompatible operation or flags
	ErrIncompatible ErrorCode = -30784

	// ErrBadRSlot indicates reader slot was corrupted or reused
	ErrBadRSlot ErrorCode = -30783

	// ErrBadTxn indicates the transaction is invalid
	ErrBadTxn ErrorCode = -30782

	// ErrBadValSize indicates invalid key or data size
	ErrBadValSize ErrorCode = -30781

	// ErrBadDBI indicates the DBI handle is invalid
	ErrBadDBI ErrorCode = -30780

	// ErrProblem indicates an unexpected internal error
	ErrProblem ErrorCode = -30779

	// ErrBusy indicates another write transaction is running
	ErrBusy ErrorCode = -30778

	// ErrMultiVal indicates the key has multiple associated values
	ErrMultiVal ErrorCode = -30421

	// ErrMultival is an alias for ErrMultiVal (mdbx-go compatibility)
	ErrMultival = ErrMultiVal

	// ErrBadSign indicates bad signature (memory corruption or ABI mismatch)
	ErrBadSign ErrorCode = -30420

	// ErrWannaRecovery indicates recovery is needed but DB is read-only
	ErrWannaRecovery ErrorCode = -30419

	// ErrKeyMismatch indicates key mismatch with cursor position
	ErrKeyMismatch ErrorCode = -30418

	// ErrTooLarge indicates database is too large for system
	ErrTooLarge ErrorCode = -30417

	// ErrThreadMismatch indicates thread attempted to use unowned object
	ErrThreadMismatch ErrorCode = -30416

	// ErrTxnOverlapping indicates overlapping read/write transactions
	ErrTxnOverlapping ErrorCode = -30415

	// ErrBacklogDepleted indicates GC ran out of free pages
	ErrBacklogDepleted ErrorCode = -30414

	// ErrDuplicatedCLK indicates duplicate lock file exists
	ErrDuplicatedCLK ErrorCode = -30413

	// ErrDanglingDBI indicates resources need closing before DBI can be reused
	ErrDanglingDBI ErrorCode = -30412

	// ErrOusted indicates parked transaction was evicted for GC
	ErrOusted ErrorCode = -30411

	// ErrMVCCRetarded indicates parked transaction's snapshot is too old
	ErrMVCCRetarded ErrorCode = -30410
)

// Error descriptions
var errorMessages = map[ErrorCode]string{
	Success:                "success",
	ResultTrue:             "operation result true",
	ErrKeyExist:            "key/data pair already exists",
	ErrNotFound:            "key/data pair not found",
	ErrPageNotFound:        "requested page not found",
	ErrCorrupted:           "database is corrupted",
	ErrPanic:               "fatal environment error",
	ErrVersionMismatch:     "database version mismatch",
	ErrInvalid:             "file is not a valid MDBX database",
	ErrMapFull:             "environment mapsize limit reached",
	ErrDBsFull:             "environment maxdbs limit reached",
	ErrReadersFull:         "environment maxreaders limit reached",
	ErrTxnFull:             "transaction has too many dirty pages",
	ErrCursorFull:          "cursor stack overflow",
	ErrPageFull:            "page has no space",
	ErrUnableExtendMapsize: "unable to extend memory mapping",
	ErrIncompatible:        "incompatible operation or flags",
	ErrBadRSlot:            "reader slot corrupted",
	ErrBadTxn:              "transaction is invalid",
	ErrBadValSize:          "invalid key or value size",
	ErrBadDBI:              "invalid DBI handle",
	ErrProblem:             "unexpected internal error",
	ErrBusy:                "another write transaction is running",
	ErrMultiVal:            "key has multiple values",
	ErrBadSign:             "bad signature",
	ErrWannaRecovery:       "recovery needed but database is read-only",
	ErrKeyMismatch:         "key mismatch with cursor position",
	ErrTooLarge:            "database too large for system",
	ErrThreadMismatch:      "thread attempted to use unowned object",
	ErrTxnOverlapping:      "overlapping transactions",
	ErrBacklogDepleted:     "GC backlog depleted",
	ErrDuplicatedCLK:       "duplicate lock file exists",
	ErrDanglingDBI:         "dangling DBI handle",
	ErrOusted:              "parked transaction was evicted",
	ErrMVCCRetarded:        "MVCC snapshot is too old",
}

// NewError creates a new Error with the given code
func NewError(code ErrorCode) *Error {
	msg, ok := errorMessages[code]
	if !ok {
		msg = fmt.Sprintf("unknown error code %d", code)
	}
	return &Error{Code: code, Message: msg}
}

// WrapError creates a new Error wrapping another error
func WrapError(code ErrorCode, err error) *Error {
	e := NewError(code)
	e.Err = err
	return e
}

// Common error variables for convenience
var (
	ErrKeyExistError            = NewError(ErrKeyExist)
	ErrNotFoundError            = NewError(ErrNotFound)
	ErrPageNotFoundError        = NewError(ErrPageNotFound)
	ErrCorruptedError           = NewError(ErrCorrupted)
	ErrPanicError               = NewError(ErrPanic)
	ErrVersionMismatchError     = NewError(ErrVersionMismatch)
	ErrInvalidError             = NewError(ErrInvalid)
	ErrMapFullError             = NewError(ErrMapFull)
	ErrDBsFullError             = NewError(ErrDBsFull)
	ErrReadersFullError         = NewError(ErrReadersFull)
	ErrTxnFullError             = NewError(ErrTxnFull)
	ErrCursorFullError          = NewError(ErrCursorFull)
	ErrPageFullError            = NewError(ErrPageFull)
	ErrUnableExtendMapsizeError = NewError(ErrUnableExtendMapsize)
	ErrIncompatibleError        = NewError(ErrIncompatible)
	ErrBadRSlotError            = NewError(ErrBadRSlot)
	ErrBadTxnError              = NewError(ErrBadTxn)
	ErrBadValSizeError          = NewError(ErrBadValSize)
	ErrBadDBIError              = NewError(ErrBadDBI)
	ErrProblemError             = NewError(ErrProblem)
	ErrBusyError                = NewError(ErrBusy)
)

// NotFound is a sentinel error for "key not found" (mdbx-go compatibility).
// Use IsNotFound() to check for this error.
var NotFound = errors.New("key not found")

// IsNotFound returns true if the error is ErrNotFound
func IsNotFound(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrNotFound
	}
	return false
}

// IsKeyExist returns true if the error is ErrKeyExist
func IsKeyExist(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrKeyExist
	}
	return false
}

// IsKeyExists is an alias for IsKeyExist (mdbx-go compatibility)
func IsKeyExists(err error) bool {
	return IsKeyExist(err)
}

// IsCorrupted returns true if the error indicates database corruption
func IsCorrupted(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrCorrupted || e.Code == ErrPageNotFound
	}
	return false
}

// IsMapFull returns true if the error is ErrMapFull (mdbx-go compatibility)
func IsMapFull(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Code == ErrMapFull
	}
	return false
}

// MapFull is an alias for ErrMapFull (mdbx-go compatibility)
const MapFull = ErrMapFull

// MapFullErrorMessage is the error message for MapFull (mdbx-go compatibility)
var MapFullErrorMessage = "The allocated database storage size limit has been reached."

// CorruptErrorHardwareRecommendations is the hardware recommendation for corruption errors (mdbx-go compatibility)
var CorruptErrorHardwareRecommendations = "Maybe free space is over on disk. Otherwise it's hardware failure. Before creating issue please use tools like https://www.memtest86.com to test RAM and tools like https://www.smartmontools.org to test Disk. To handle hardware risks: use ECC RAM, use RAID of disks, run multiple application instances (or do backups). If hardware checks passed - check FS settings - 'fsync' and 'flock' must be enabled. "

// CorruptErrorBacktraceRecommendations is the backtrace recommendation for corruption errors (mdbx-go compatibility)
var CorruptErrorBacktraceRecommendations = "Otherwise - please create issue in Application repo."

// CorruptErrorRecoveryRecommendations is the recovery recommendation for corruption errors (mdbx-go compatibility)
var CorruptErrorRecoveryRecommendations = "On default DURABLE mode, power outage can't cause this error. On other modes - power outage may break last transaction and mdbx_chk can recover db in this case, see '-t' and '-0|1|2' options."

// CorruptErrorMessage is the combined error message for corruption errors (mdbx-go compatibility)
var CorruptErrorMessage = CorruptErrorHardwareRecommendations + " " + CorruptErrorBacktraceRecommendations + " " + CorruptErrorRecoveryRecommendations

// Code returns the error code from an error, or ErrProblem if not a gdbx error
func Code(err error) ErrorCode {
	if err == nil {
		return Success
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ErrProblem
}
