package fastmap

import (
	"math/rand"
	"testing"
	"unsafe"
)

// dummy is a placeholder struct for creating real pointers
type dummy struct {
	x int
}

// Test basic functionality
func TestUint32Map(t *testing.T) {
	m := &Uint32Map{}

	// Test empty map
	if m.Get(1) != nil {
		t.Error("Expected nil for empty map")
	}

	// Test set and get with real pointers
	d1 := &dummy{100}
	d2 := &dummy{200}
	val1 := unsafe.Pointer(d1)
	val2 := unsafe.Pointer(d2)

	m.Set(1, val1)
	m.Set(2, val2)

	if m.Get(1) != val1 {
		t.Error("Get(1) failed")
	}
	if m.Get(2) != val2 {
		t.Error("Get(2) failed")
	}
	if m.Get(3) != nil {
		t.Error("Get(3) should be nil")
	}

	// Test update
	d3 := &dummy{300}
	val3 := unsafe.Pointer(d3)
	m.Set(1, val3)
	if m.Get(1) != val3 {
		t.Error("Update failed")
	}

	// Test len
	if m.Len() != 2 {
		t.Errorf("Expected len=2, got %d", m.Len())
	}

	// Test clear
	m.Clear()
	if m.Len() != 0 {
		t.Error("Clear failed")
	}
	if m.Get(1) != nil {
		t.Error("Get after clear should be nil")
	}
}

// Test with many entries to trigger growth
func TestUint32MapGrowth(t *testing.T) {
	m := &Uint32Map{}

	n := 10000
	dummies := make([]*dummy, n)
	for i := 0; i < n; i++ {
		dummies[i] = &dummy{i * 10}
		m.Set(uint32(i), unsafe.Pointer(dummies[i]))
	}

	if m.Len() != n {
		t.Errorf("Expected len=%d, got %d", n, m.Len())
	}

	// Verify all values
	for i := 0; i < n; i++ {
		v := m.Get(uint32(i))
		if v != unsafe.Pointer(dummies[i]) {
			t.Errorf("Get(%d) failed", i)
		}
	}
}

// Test with key=0
func TestUint32MapZeroKey(t *testing.T) {
	m := &Uint32Map{}

	d := &dummy{999}
	val := unsafe.Pointer(d)
	m.Set(0, val)

	if m.Get(0) != val {
		t.Error("Zero key failed")
	}
	if m.Len() != 1 {
		t.Error("Len should be 1")
	}
}

// Pre-allocate dummies for benchmarks
var benchDummies []*dummy

func init() {
	benchDummies = make([]*dummy, 200000)
	for i := range benchDummies {
		benchDummies[i] = &dummy{i}
	}
}

// Benchmark: Sequential writes - FastMap
func BenchmarkFastMapSeqWrite(b *testing.B) {
	m := &Uint32Map{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(uint32(i), unsafe.Pointer(benchDummies[i%len(benchDummies)]))
	}
}

// Benchmark: Sequential writes - Go map
func BenchmarkGoMapSeqWrite(b *testing.B) {
	m := make(map[uint32]unsafe.Pointer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m[uint32(i)] = unsafe.Pointer(benchDummies[i%len(benchDummies)])
	}
}

// Benchmark: Random writes - FastMap
func BenchmarkFastMapRandWrite(b *testing.B) {
	m := &Uint32Map{}
	keys := make([]uint32, b.N)
	for i := range keys {
		keys[i] = rand.Uint32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(keys[i], unsafe.Pointer(benchDummies[i%len(benchDummies)]))
	}
}

// Benchmark: Random writes - Go map
func BenchmarkGoMapRandWrite(b *testing.B) {
	m := make(map[uint32]unsafe.Pointer)
	keys := make([]uint32, b.N)
	for i := range keys {
		keys[i] = rand.Uint32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m[keys[i]] = unsafe.Pointer(benchDummies[i%len(benchDummies)])
	}
}

// Benchmark: Sequential reads (after populating) - FastMap
func BenchmarkFastMapSeqRead(b *testing.B) {
	m := &Uint32Map{}
	for i := 0; i < 100000; i++ {
		m.Set(uint32(i), unsafe.Pointer(benchDummies[i]))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get(uint32(i % 100000))
	}
}

// Benchmark: Sequential reads - Go map
func BenchmarkGoMapSeqRead(b *testing.B) {
	m := make(map[uint32]unsafe.Pointer)
	for i := 0; i < 100000; i++ {
		m[uint32(i)] = unsafe.Pointer(benchDummies[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m[uint32(i%100000)]
	}
}

// Benchmark: Random reads - FastMap
func BenchmarkFastMapRandRead(b *testing.B) {
	m := &Uint32Map{}
	keys := make([]uint32, 100000)
	for i := range keys {
		keys[i] = rand.Uint32()
		m.Set(keys[i], unsafe.Pointer(benchDummies[i]))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get(keys[i%100000])
	}
}

// Benchmark: Random reads - Go map
func BenchmarkGoMapRandRead(b *testing.B) {
	m := make(map[uint32]unsafe.Pointer)
	keys := make([]uint32, 100000)
	for i := range keys {
		keys[i] = rand.Uint32()
		m[keys[i]] = unsafe.Pointer(benchDummies[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m[keys[i%100000]]
	}
}

// Benchmark: Miss reads (keys not in map) - FastMap
func BenchmarkFastMapMissRead(b *testing.B) {
	m := &Uint32Map{}
	for i := 0; i < 100000; i++ {
		m.Set(uint32(i), unsafe.Pointer(benchDummies[i]))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get(uint32(i + 1000000)) // Keys that don't exist
	}
}

// Benchmark: Miss reads - Go map
func BenchmarkGoMapMissRead(b *testing.B) {
	m := make(map[uint32]unsafe.Pointer)
	for i := 0; i < 100000; i++ {
		m[uint32(i)] = unsafe.Pointer(benchDummies[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m[uint32(i+1000000)]
	}
}

// Benchmark: Mixed read/write - FastMap
func BenchmarkFastMapMixed(b *testing.B) {
	m := &Uint32Map{}
	// Pre-populate
	for i := 0; i < 10000; i++ {
		m.Set(uint32(i), unsafe.Pointer(benchDummies[i]))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			m.Set(uint32(i), unsafe.Pointer(benchDummies[i%len(benchDummies)]))
		} else {
			_ = m.Get(uint32(i % 10000))
		}
	}
}

// Benchmark: Mixed read/write - Go map
func BenchmarkGoMapMixed(b *testing.B) {
	m := make(map[uint32]unsafe.Pointer)
	for i := 0; i < 10000; i++ {
		m[uint32(i)] = unsafe.Pointer(benchDummies[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			m[uint32(i)] = unsafe.Pointer(benchDummies[i%len(benchDummies)])
		} else {
			_ = m[uint32(i%10000)]
		}
	}
}
