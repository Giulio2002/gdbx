//go:build darwin

package mmap

import "errors"

// tryMremap is not available on macOS, always returns error to trigger fallback.
func (m *Map) tryMremap(newSize int) ([]byte, error) {
	return nil, errors.New("mremap not available on darwin")
}
