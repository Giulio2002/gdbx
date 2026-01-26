package gdbx

// DBI is a database handle (index into environment's database array).
type DBI uint32

// Drop deletes all data in a database, or deletes the database entirely.
// If del is true, the database is deleted; otherwise it is emptied.
func (txn *Txn) Drop(dbi DBI, del bool) error {
	if !txn.valid() {
		return NewError(ErrBadTxn)
	}

	if txn.IsReadOnly() {
		return NewError(ErrPermissionDenied)
	}

	if dbi < CoreDBs {
		return NewError(ErrInvalid) // Can't drop core DBs
	}

	if int(dbi) >= len(txn.trees) {
		return NewError(ErrBadDBI)
	}

	// TODO: Implement tree deletion
	// 1. Walk the tree and add all pages to free list
	// 2. Reset tree to empty state
	// 3. If del, remove from main database

	txn.trees[dbi].reset()

	// Mark the tree as dirty so it gets persisted
	if txn.dbiDirty == nil {
		txn.dbiDirty = make([]bool, len(txn.trees))
	}
	if int(dbi) < len(txn.dbiDirty) {
		txn.dbiDirty[dbi] = true
	}

	if del {
		// Remove from environment's DBI list
		txn.env.dbisMu.Lock()
		txn.env.dbis[dbi] = nil
		txn.env.dbisMu.Unlock()
	}

	return nil
}

// DBIFlags returns the flags for a database.
func (txn *Txn) DBIFlags(dbi DBI) (uint, error) {
	if !txn.valid() {
		return 0, NewError(ErrBadTxn)
	}

	if int(dbi) >= len(txn.trees) {
		return 0, NewError(ErrBadDBI)
	}

	return uint(txn.trees[dbi].Flags), nil
}

// Sequence gets or updates the sequence number for a database.
// If increment > 0, adds to the sequence and returns the new value.
// If increment == 0, returns the current value without changing it.
func (txn *Txn) Sequence(dbi DBI, increment uint64) (uint64, error) {
	if !txn.valid() {
		return 0, NewError(ErrBadTxn)
	}

	if int(dbi) >= len(txn.trees) {
		return 0, NewError(ErrBadDBI)
	}

	if increment > 0 && txn.IsReadOnly() {
		return 0, NewError(ErrPermissionDenied)
	}

	t := &txn.trees[dbi]
	result := t.Sequence

	if increment > 0 {
		t.Sequence += increment
	}

	return result, nil
}

// SetCompare sets a custom key comparison function for a database.
// Must be called before any data operations on the database.
func (e *Env) SetCompare(dbi DBI, cmp func(a, b []byte) int) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.dbisMu.Lock()
	defer e.dbisMu.Unlock()

	if int(dbi) >= len(e.dbis) {
		return NewError(ErrBadDBI)
	}

	if e.dbis[dbi] == nil {
		e.dbis[dbi] = &dbiInfo{}
	}
	e.dbis[dbi].cmp = cmp

	return nil
}

// SetDupCompare sets a custom data comparison function for DUPSORT databases.
// Must be called before any data operations on the database.
func (e *Env) SetDupCompare(dbi DBI, cmp func(a, b []byte) int) error {
	if !e.valid() {
		return NewError(ErrInvalid)
	}

	e.dbisMu.Lock()
	defer e.dbisMu.Unlock()

	if int(dbi) >= len(e.dbis) {
		return NewError(ErrBadDBI)
	}

	if e.dbis[dbi] == nil {
		e.dbis[dbi] = &dbiInfo{}
	}
	e.dbis[dbi].dcmp = cmp

	return nil
}

// DBIStat is an alias for the Stat method for compatibility.
func (txn *Txn) DBIStat(dbi DBI) (*Stat, error) {
	return txn.Stat(dbi)
}
