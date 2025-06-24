//go:build go1.22
// +build go1.22

package objstore

import "unsafe"

// copyMemory is a fast memory copy using memmove.
//
//go:linkname copyMemory runtime.memmove
//go:noescape
func copyMemory(to, from unsafe.Pointer, n int)
