//go:build !amd64 || purego || (gitpack_libdeflate && cgo)

package objstore

import (
	"encoding/binary"
	"unsafe"
)

// newlineMask64 returns a bit per byte of the 64 bytes at p that equals
// '\n'. The caller guarantees 64 readable bytes. Eight word-sized SWAR tests
// fold into the 64-bit mask.
//
// binary.LittleEndian gives newlineMask8 the same byte order on every
// target, so mask bit i corresponds to input byte i on big-endian hosts too.
// The fixed-size array view keeps the eight loads free of bounds checks.
func newlineMask64(p *byte) uint64 {
	b := (*[64]byte)(unsafe.Pointer(p))
	return newlineMask8(binary.LittleEndian.Uint64(b[0:8])) |
		newlineMask8(binary.LittleEndian.Uint64(b[8:16]))<<8 |
		newlineMask8(binary.LittleEndian.Uint64(b[16:24]))<<16 |
		newlineMask8(binary.LittleEndian.Uint64(b[24:32]))<<24 |
		newlineMask8(binary.LittleEndian.Uint64(b[32:40]))<<32 |
		newlineMask8(binary.LittleEndian.Uint64(b[40:48]))<<40 |
		newlineMask8(binary.LittleEndian.Uint64(b[48:56]))<<48 |
		newlineMask8(binary.LittleEndian.Uint64(b[56:64]))<<56
}
