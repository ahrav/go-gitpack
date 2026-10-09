//go:build !amd64 || purego

package objstore

import "unsafe"

// newlineMask64 returns a bit per byte of the 64 bytes at p that equals
// '\n'. The caller guarantees 64 readable bytes. Eight word-sized SWAR tests
// fold into the 64-bit mask.
func newlineMask64(p *byte) uint64 {
	q := unsafe.Pointer(p)
	return newlineMask8(*(*uint64)(q)) |
		newlineMask8(*(*uint64)(unsafe.Add(q, 8)))<<8 |
		newlineMask8(*(*uint64)(unsafe.Add(q, 16)))<<16 |
		newlineMask8(*(*uint64)(unsafe.Add(q, 24)))<<24 |
		newlineMask8(*(*uint64)(unsafe.Add(q, 32)))<<32 |
		newlineMask8(*(*uint64)(unsafe.Add(q, 40)))<<40 |
		newlineMask8(*(*uint64)(unsafe.Add(q, 48)))<<48 |
		newlineMask8(*(*uint64)(unsafe.Add(q, 56)))<<56
}
