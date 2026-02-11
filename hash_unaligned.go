// hash_unaligned.go
//
// Build-tag pair: this file and hash_aligned.go form a mutually exclusive pair
// that together provide the Hash.Uint64() method on all platforms.
//
//   - hash_unaligned.go (this file): compiled only on 32-bit ARM ("arm,!arm64").
//     Uses encoding/binary with an explicit endianness branch to avoid unaligned
//     memory access, which can cause a SIGBUS on ARMv6 (e.g., Raspberry Pi 1).
//   - hash_aligned.go: compiled on all other architectures. Uses an unsafe
//     pointer cast for maximum performance.
//
// Agreement invariant: both files MUST produce identical Uint64() results for
// the same Hash value on any given machine. This file achieves agreement by
// detecting the host byte order at init time via hostLittle (defined in
// store.go) and selecting the matching encoding/binary decoder.
//
// Cross-file dependency: hostLittle is a package-level bool computed once in
// store.go by inspecting the byte representation of a uint16. If hostLittle is
// ever removed or relocated, this file will fail to compile.

//go:build arm && !arm64
// +build arm,!arm64

package objstore

import "encoding/binary"

// Uint64 returns the first eight bytes of h as a host-native uint64.
//
// This version uses encoding/binary for safe, alignment-correct decoding on
// 32-bit ARM. It branches on hostLittle (see store.go) to match the byte
// order that the unsafe cast in hash_aligned.go would produce on the same
// machine. See the file-level comment for the agreement invariant.
func (h Hash) Uint64() uint64 {
	// Use native byte order to match the behavior of the unsafe version.
	if hostLittle {
		return binary.LittleEndian.Uint64(h[:8])
	}
	return binary.BigEndian.Uint64(h[:8])
}
