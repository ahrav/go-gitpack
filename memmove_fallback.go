// memmove_fallback.go
//
// Build-tag pair: this file and memmove_go122.go form a mutually exclusive pair
// that together provide the copyMemory function on all supported Go versions.
//
//   - memmove_fallback.go (this file): compiled on Go < 1.22 ("!go1.22"), where
//     //go:linkname to runtime.memmove is either unavailable or unreliable.
//     Falls back to a pure-Go copy via the built-in copy().
//   - memmove_go122.go: compiled on Go >= 1.22 ("go1.22"). Uses //go:linkname
//     to call the runtime's memmove directly, avoiding an intermediate slice
//     header allocation.
//
// The [1<<30]byte cast pattern: Go does not allow direct slicing of an
// unsafe.Pointer. The idiom (*[1 << 30]byte)(ptr)[:n:n] reinterprets the
// pointer as a pointer to a very large fixed-size array and then immediately
// slices it to exactly n bytes with both length and capacity set to n. The
// array size (1 GiB) is never actually allocated; it merely tells the compiler
// the upper bound for bounds-check elimination. This is a well-known Go idiom
// for constructing a []byte from an unsafe.Pointer without allocation.

//go:build !go1.22
// +build !go1.22

package objstore

import "unsafe"

// copyMemory copies n bytes from the memory address 'from' to the memory
// address 'to'. This is the fallback implementation for Go < 1.22 that
// uses the built-in copy() on synthesized byte slices.
//
// Preconditions:
//   - 'to' and 'from' must point to at least n bytes of valid, accessible memory.
//   - n must be non-negative. If n <= 0, the function is a no-op.
//   - The memory regions [to, to+n) and [from, from+n) may overlap; the built-in
//     copy() handles overlapping regions correctly (it behaves like C's memmove,
//     not memcpy).
func copyMemory(to, from unsafe.Pointer, n int) {
	if n <= 0 {
		return
	}

	toSlice := (*[1 << 30]byte)(to)[:n:n]
	fromSlice := (*[1 << 30]byte)(from)[:n:n]
	copy(toSlice, fromSlice)
}
