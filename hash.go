// hash.go
//
// Core type for Git object identifiers (SHA-1 hashes).
//
// This file defines the Hash type and its basic operations (formatting,
// parsing, zero-checking). The Hash type is a fixed-size [20]byte array
// that represents the raw binary form of a SHA-1 digest.
//
// Thread safety: Hash is a value type (fixed-size array) and is safe to copy,
// compare, and read concurrently without synchronization. There is no shared
// mutable state. Functions in this file (String, IsZero, ParseHash) are all
// pure and safe for concurrent use.
//
// Related files:
//   - hash_aligned.go:   provides Hash.Uint64() for architectures with relaxed
//     alignment (all except 32-bit ARM).
//   - hash_unaligned.go: provides Hash.Uint64() for 32-bit ARM using safe
//     byte-order-aware decoding.
package objstore

import (
	"encoding/hex"
	"fmt"
)

// Hash represents a raw Git object identifier.
//
// It is the 20-byte binary form of a SHA-1 digest as used by Git internally.
// The zero value is the all-zero hash, which never resolves to a real object.
//
// Hash also provides a Uint64() method (defined in hash_aligned.go or
// hash_unaligned.go depending on the target architecture) that returns the
// first eight bytes as a uint64, useful for hash-map bucketing and sharding.
type Hash [20]byte

// String returns the hexadecimal string representation of the hash.
// This is the canonical 40-character format used by Git.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// IsZero returns true if the hash is the zero value.
func (h Hash) IsZero() bool { return h == (Hash{}) }

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
