// unsafe.go
//
// Zero-copy conversion utilities that bypass Go's normal copy semantics
// using the unsafe package. Every function here trades safety for
// performance on hot paths (e.g., pack-name parsing, header string
// comparisons) where the allocation cost of a normal []byte-to-string
// copy is measurable.
//
// Audit isolation: all unsafe usage in the package is confined to this
// file (and the pointer arithmetic in object.go's parseObjectHeaderUnsafe).
// Restricting unsafe to a small number of files makes it easier to review
// and to locate during security audits.

package objstore

import (
	"unsafe"

	"golang.org/x/exp/mmap"
)

// btostr converts a byte slice to a string without copying the underlying
// data. The returned string shares the same backing memory as b.
//
// SAFETY / MUTATION INVARIANT: neither the original byte slice nor the
// returned string may be mutated after this call. Mutating b after
// conversion would silently corrupt the string, violating Go's immutable
// string guarantee. Callers must ensure that b is either:
//   - owned solely by the caller and never written to again, or
//   - backed by read-only memory (e.g., an mmap region opened read-only).
//
// This is safe under the Go memory model because unsafe.String is
// defined to produce a string header that aliases the provided pointer
// and length, and the garbage collector treats the resulting string as
// keeping the backing array alive.
func btostr(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// strtob converts a string to a byte slice without copying. The returned
// slice aliases the string's backing memory and MUST NOT be mutated.
func strtob(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// mmapData exposes the underlying byte slice of an mmap.ReaderAt without
// copying.
//
// SAFETY: mmap.ReaderAt is a struct with exactly one field, `data []byte`
// (see golang.org/x/exp/mmap). The cast below relies on that layout; the
// dependency version is pinned in go.mod, and any layout change would be
// caught immediately by the package tests since every pack read flows
// through this helper. The returned slice aliases read-only mapped memory:
// callers MUST NOT mutate it and MUST NOT retain it past the store's Close
// (munmap invalidates the pages).
//
// Motivation: handing zlib a *bytes.Reader over this slice lets the flate
// decompressor use its specialized bytes.Reader decode loop and reads pack
// bytes straight from the page cache, eliminating both the io.SectionReader
// indirection and a bufio copy per object.
func mmapData(r *mmap.ReaderAt) []byte {
	return (*struct{ data []byte })(unsafe.Pointer(r)).data
}
