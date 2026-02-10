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

import "unsafe"

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
