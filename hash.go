package objstore

import (
	"encoding/hex"
	"fmt"
	"unsafe"
)

// Hash represents a raw Git object identifier.
//
// It is the 20-byte binary form of a SHA-1 digest as used by Git internally.
// The zero value is the all-zero hash, which never resolves to a real object.
type Hash [20]byte

// ParseHash converts a 40-char hex string to Hash.
//
// ParseHash converts the canonical, 40-character hexadecimal SHA-1 string
// produced by Git into its raw 20-byte representation.
//
// An error is returned when the input • is not exactly 40 runes long or • cannot
// be decoded as hexadecimal.
// The zero Hash value (all zero bytes) never corresponds to a real Git object
// and is therefore safe to use as a sentinel in maps.
func ParseHash(s string) (Hash, error) {
	var h Hash
	if len(s) != 40 {
		return h, fmt.Errorf("invalid hash length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return h, err
	}
	copy(h[:], b)
	return h, nil
}

// Uint64 returns the first eight bytes of h as an implementation-native
// uint64.
//
// The value is taken verbatim from the underlying byte slice; no byte-order
// conversion is performed.
// The conversion is implemented via an unsafe cast which is safe because
// Hash' backing array is 20 bytes and therefore long-word aligned on every
// platform that Go supports.
// The numeric representation is only meant for in-memory shortcuts such as
// hash-table look-ups and must not be persisted or used as a portable
// identifier.
func (h Hash) Uint64() uint64 { return *(*uint64)(unsafe.Pointer(&h[0])) }
