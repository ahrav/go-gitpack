//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

// newlineMask64 returns a bit per byte of the 64 bytes at p that equals
// '\n'. The caller guarantees 64 readable bytes. Implemented with SSE2
// compares (tokenize_amd64.s), which every amd64 Go target provides. With
// gitpack_libdeflate && cgo, newlineMask64 uses the SWAR fallback to satisfy
// Go's restriction on assembly in cgo packages.
//
//go:noescape
func newlineMask64(p *byte) uint64
