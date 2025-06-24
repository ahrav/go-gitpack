//go:build !go1.22
// +build !go1.22

package objstore

import "unsafe"

// copyMemory falls back to copy() when linkname is not available.
func copyMemory(to, from unsafe.Pointer, n int) {
	if n <= 0 {
		return
	}

	toSlice := (*[1 << 30]byte)(to)[:n:n]
	fromSlice := (*[1 << 30]byte)(from)[:n:n]
	copy(toSlice, fromSlice)
}
