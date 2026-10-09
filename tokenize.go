package objstore

import (
	"bytes"
	"math/bits"
	"unsafe"
)

// tokenize splits src into lines at '\n' without copying the bytes: each
// returned string aliases src (see btostr), so src must stay immutable for
// as long as the strings are used. The newline is excluded from each line
// and a trailing line without a newline is included. Empty input returns
// nil.
//
// Newlines are located 64 bytes at a time: eight word-sized SWAR tests fold
// into one 64-bit mask with a bit per newline, and the lines are then cut at
// the set bits. Source lines average a few dozen bytes, where a per-line
// IndexByte call spends most of its time in call overhead and loop-exit
// mispredictions; the block mask pays those costs once per 64 bytes instead
// of once per line and stays portable across architectures.
func tokenize(src []byte) []string {
	if len(src) == 0 {
		return nil
	}

	lines := make([]string, bytes.Count(src, nlByte)+1)
	n := 0
	start := 0 // start of the current line
	i := 0
	for ; i+64 <= len(src); i += 64 {
		p := unsafe.Pointer(unsafe.SliceData(src[i : i+64]))
		m := newlineMask8(*(*uint64)(p)) |
			newlineMask8(*(*uint64)(unsafe.Add(p, 8)))<<8 |
			newlineMask8(*(*uint64)(unsafe.Add(p, 16)))<<16 |
			newlineMask8(*(*uint64)(unsafe.Add(p, 24)))<<24 |
			newlineMask8(*(*uint64)(unsafe.Add(p, 32)))<<32 |
			newlineMask8(*(*uint64)(unsafe.Add(p, 40)))<<40 |
			newlineMask8(*(*uint64)(unsafe.Add(p, 48)))<<48 |
			newlineMask8(*(*uint64)(unsafe.Add(p, 56)))<<56
		for m != 0 {
			nl := i + bits.TrailingZeros64(m)
			lines[n] = btostr(src[start:nl])
			n++
			start = nl + 1
			m &= m - 1
		}
	}
	for ; i < len(src); i++ {
		if src[i] == '\n' {
			lines[n] = btostr(src[start:i])
			n++
			start = i + 1
		}
	}
	if start < len(src) {
		lines[n] = btostr(src[start:])
		n++
	}
	return lines[:n]
}

// newlineMask8 returns, in its low eight bits, one bit per byte of the
// little-endian word w that equals '\n'.
func newlineMask8(w uint64) uint64 {
	const (
		nlWord = 0x0a0a0a0a0a0a0a0a
		low7   = 0x7f7f7f7f7f7f7f7f
	)
	x := w ^ nlWord
	// A byte of x is zero exactly where w had '\n'. Adding 0x7f to the low
	// seven bits of a byte sets its high bit when any of those bits is set,
	// without carrying into the next byte; or-ing x covers bytes whose own
	// high bit is set. The complement then has a high bit at exactly the
	// zero bytes, and the multiply gathers those eight bits into one byte.
	zero := ^(((x & low7) + low7) | x | low7)
	return ((zero >> 7) * 0x0102040810204080) >> 56
}
