//go:build arm && !arm64
// +build arm,!arm64

package objstore

import "encoding/binary"

// Uint64 returns the first eight bytes of h as an implementation-native uint64.
// This version uses safe byte operations for ARMv6 compatibility.
func (h Hash) Uint64() uint64 {
	// Use native byte order to match the behavior of the unsafe version.
	if hostLittle {
		return binary.LittleEndian.Uint64(h[:8])
	}
	return binary.BigEndian.Uint64(h[:8])
}
