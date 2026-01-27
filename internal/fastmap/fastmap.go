// Package fastmap provides a fast hash map for integer keys.
// Uses fibonacci hashing for better distribution of sequential keys.
package fastmap

import "unsafe"

// Uint32Map is a fast hash map from uint32 to unsafe.Pointer.
// Uses open addressing with linear probing and fibonacci hashing.
type Uint32Map struct {
	buckets []bucket
	count   int
	mask    uint32
}

type bucket struct {
	key   uint32
	value unsafe.Pointer
	used  bool // Needed because key=0 might be valid
}

// Fibonacci hash constant: 2^32 / golden ratio
const fibHash32 = 2654435769

// hash computes a fast hash using fibonacci hashing
func (m *Uint32Map) hash(key uint32) uint32 {
	return key * fibHash32
}

// Get returns the value for the given key, or nil if not found.
func (m *Uint32Map) Get(key uint32) unsafe.Pointer {
	if len(m.buckets) == 0 {
		return nil
	}
	h := m.hash(key)
	idx := h & m.mask
	for {
		b := &m.buckets[idx]
		if !b.used {
			return nil
		}
		if b.key == key {
			return b.value
		}
		idx = (idx + 1) & m.mask
	}
}

// Set stores a key-value pair.
func (m *Uint32Map) Set(key uint32, value unsafe.Pointer) {
	if len(m.buckets) == 0 {
		m.buckets = make([]bucket, 16)
		m.mask = 15
	} else if m.count >= len(m.buckets)*3/4 {
		m.grow()
	}

	h := m.hash(key)
	idx := h & m.mask
	for {
		b := &m.buckets[idx]
		if !b.used {
			b.key = key
			b.value = value
			b.used = true
			m.count++
			return
		}
		if b.key == key {
			b.value = value
			return
		}
		idx = (idx + 1) & m.mask
	}
}

// grow doubles the hash table size
func (m *Uint32Map) grow() {
	oldBuckets := m.buckets
	newSize := len(oldBuckets) * 2
	m.buckets = make([]bucket, newSize)
	m.mask = uint32(newSize - 1)
	m.count = 0

	for i := range oldBuckets {
		if oldBuckets[i].used {
			m.Set(oldBuckets[i].key, oldBuckets[i].value)
		}
	}
}

// ForEach iterates over all key-value pairs.
func (m *Uint32Map) ForEach(fn func(uint32, unsafe.Pointer)) {
	for i := range m.buckets {
		if m.buckets[i].used {
			fn(m.buckets[i].key, m.buckets[i].value)
		}
	}
}

// Clear removes all entries but keeps the backing array.
func (m *Uint32Map) Clear() {
	clear(m.buckets)
	m.count = 0
}

// Len returns the number of entries.
func (m *Uint32Map) Len() int {
	return m.count
}
