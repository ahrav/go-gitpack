// memmove_go122.go
//
// Build-tag pair: this file and memmove_fallback.go form a mutually exclusive
// pair that together provide the copyMemory function on all supported Go
// versions.
//
//   - memmove_go122.go (this file): compiled on Go >= 1.22 ("go1.22"). Links
//     directly to the runtime's memmove for zero-overhead memory copying.
//   - memmove_fallback.go: compiled on Go < 1.22. Uses a pure-Go copy()
//     fallback.
//
// Fragility warning: this file relies on the internal symbol runtime.memmove,
// which is not part of Go's public API and may be renamed, removed, or have
// its signature changed in any future Go release without notice. If a future
// Go version breaks this linkage, the build will fail at link time with an
// "undefined: runtime.memmove" error. In that case, the fix is to update the
// build tag or fall back to the pure-Go implementation. The existence of
// memmove_fallback.go ensures the package still compiles on older Go versions.
//
// Directive reference:
//   - //go:linkname copyMemory runtime.memmove — instructs the linker to
//     resolve the symbol "copyMemory" in this package to "runtime.memmove"
//     in the runtime package, effectively aliasing our function to the
//     runtime's assembly-optimized memmove.
//   - //go:noescape — tells the compiler that none of the pointer arguments
//     escape to the heap through this function call. This is safe because
//     runtime.memmove only reads from 'from' and writes to 'to' without
//     retaining either pointer. Without this directive, the compiler would
//     conservatively assume the pointers escape, causing unnecessary heap
//     allocations at every call site.

//go:build go1.22
// +build go1.22

package objstore

import "unsafe"

// copyMemory copies n bytes from the memory address 'from' to the memory
// address 'to' using the runtime's assembly-optimized memmove.
//
// This is functionally identical to the fallback in memmove_fallback.go but
// avoids the overhead of constructing intermediate slice headers. The runtime's
// memmove correctly handles overlapping memory regions.
//
// Preconditions (same as memmove_fallback.go):
//   - 'to' and 'from' must point to at least n bytes of valid, accessible memory.
//   - n must be non-negative.
//
//go:linkname copyMemory runtime.memmove
//go:noescape
func copyMemory(to, from unsafe.Pointer, n int)
