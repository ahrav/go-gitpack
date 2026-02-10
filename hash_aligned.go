// hash_aligned.go
//
// Build-tag pair: this file and hash_unaligned.go form a mutually exclusive
// pair that together provide the Hash.Uint64() method on all platforms.
//
//   - hash_aligned.go   (this file): compiled on all architectures EXCEPT
//     32-bit ARM (i.e., "!arm || arm64"). Uses an unsafe pointer cast for
//     maximum performance.
//   - hash_unaligned.go: compiled only on 32-bit ARM ("arm,!arm64"). Uses
//     encoding/binary for safe, alignment-correct decoding.
//
// Agreement invariant: both files MUST produce identical Uint64() results for
// the same Hash value on any given machine. Because both interpret the first
// eight bytes in host-native byte order, this invariant holds as long as neither
// file applies byte swapping.
//
// Unsafe cast safety: the cast *(*uint64)(unsafe.Pointer(&h[0])) is safe here
// because Hash is a [20]byte array, which is always at least 8 bytes long, and
// on the architectures selected by this build tag, uint64 loads from arbitrary
// byte-aligned addresses are supported by the hardware (or transparently fixed
// up by the OS). The Go specification guarantees that array elements are
// contiguous in memory.
//
// Endianness: the returned uint64 uses host-native byte order. On little-endian
// machines (x86, arm64, most modern hardware), h[0] occupies the least-
// significant byte. On big-endian machines, h[0] occupies the most-significant
// byte. This is intentional: the value is used only for hash-map bucketing and
// sharding, not for serialization, so byte order does not matter as long as it
// is consistent within a single process.

//go:build !arm || arm64
// +build !arm arm64

package objstore

import "unsafe"

// Uint64 returns the first eight bytes of h as a host-native uint64.
//
// This version uses an unsafe pointer cast, which compiles to a single load
// instruction on aligned architectures. See the file-level comment for safety
// analysis and the build-tag pairing with hash_unaligned.go.
func (h Hash) Uint64() uint64 { return *(*uint64)(unsafe.Pointer(&h[0])) }
