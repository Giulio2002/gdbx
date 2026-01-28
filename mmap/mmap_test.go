package mmap

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	// Create temp file
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data
	data := []byte("hello world test data for mmap")
	if _, err := f.Write(data); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Create read-only mmap
	m, err := New(int(f.Fd()), 0, len(data), false)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	defer m.Close()
	f.Close()

	// Verify data
	if !bytes.Equal(m.Data(), data) {
		t.Errorf("mmap data mismatch: got %q, want %q", m.Data(), data)
	}

	// Verify size
	if m.Size() != int64(len(data)) {
		t.Errorf("size mismatch: got %d, want %d", m.Size(), len(data))
	}
}

func TestMapFile(t *testing.T) {
	// Create temp file with data
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	data := []byte("MapFile test data content")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Map the file
	m, err := MapFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Verify data
	if !bytes.Equal(m.Data(), data) {
		t.Errorf("data mismatch: got %q, want %q", m.Data(), data)
	}
}

func TestWritable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	// Create file
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write initial data
	initial := make([]byte, 4096)
	copy(initial, []byte("initial"))
	if _, err := f.Write(initial); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Create writable mmap
	m, err := New(int(f.Fd()), 0, len(initial), true)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write through mmap
	copy(m.Data(), []byte("modified"))

	// Sync
	if err := m.Sync(); err != nil {
		m.Close()
		f.Close()
		t.Fatal(err)
	}

	m.Close()
	f.Close()

	// Read back
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.HasPrefix(data, []byte("modified")) {
		t.Errorf("expected modified data, got %q", data[:20])
	}
}

func TestRemap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	// Create file
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Initial size
	initialSize := 4096
	if err := f.Truncate(int64(initialSize)); err != nil {
		t.Fatal(err)
	}

	// Create mmap
	m, err := New(int(f.Fd()), 0, initialSize, true)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Write initial data
	copy(m.Data(), []byte("test data"))

	// Extend file
	newSize := 8192
	if err := f.Truncate(int64(newSize)); err != nil {
		t.Fatal(err)
	}

	// Remap
	if err := m.Remap(int64(newSize)); err != nil {
		t.Fatal(err)
	}

	// Verify new size
	if m.Size() != int64(newSize) {
		t.Errorf("size after remap: got %d, want %d", m.Size(), newSize)
	}

	// Verify original data intact
	if !bytes.HasPrefix(m.Data(), []byte("test data")) {
		t.Errorf("data corrupted after remap")
	}

	// Write to new region
	copy(m.Data()[initialSize:], []byte("new region"))
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size := 4096
	if err := f.Truncate(int64(size)); err != nil {
		t.Fatal(err)
	}

	m, err := New(int(f.Fd()), 0, size, true)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Write data
	copy(m.Data()[100:], []byte("test"))

	// Sync range
	if err := m.SyncRange(0, int64(size)); err != nil {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	data := []byte("close test")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	m, err := MapFile(path, false)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify nil data
	if m.Data() != nil {
		t.Error("data should be nil after close")
	}

	// Double close should be safe
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.dat")

	// Create empty file
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}

	// Should fail with ErrEmptyFile
	_, err := MapFile(path, false)
	if err != ErrEmptyFile {
		t.Errorf("expected ErrEmptyFile, got %v", err)
	}
}

func TestInvalidSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Zero size should fail
	_, err = New(int(f.Fd()), 0, 0, false)
	if err != ErrInvalidSize {
		t.Errorf("expected ErrInvalidSize for size 0, got %v", err)
	}

	// Negative size should fail
	_, err = New(int(f.Fd()), 0, -1, false)
	if err != ErrInvalidSize {
		t.Errorf("expected ErrInvalidSize for size -1, got %v", err)
	}
}

func TestAdvise(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	data := make([]byte, 4096)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	m, err := MapFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// These may be no-ops on some platforms but shouldn't error
	if err := m.AdviseSequential(); err != nil {
		t.Errorf("AdviseSequential failed: %v", err)
	}
	if err := m.AdviseRandom(); err != nil {
		t.Errorf("AdviseRandom failed: %v", err)
	}
	if err := m.AdviseWillNeed(); err != nil {
		t.Errorf("AdviseWillNeed failed: %v", err)
	}
	if err := m.AdviseDontNeed(); err != nil {
		t.Errorf("AdviseDontNeed failed: %v", err)
	}
}
