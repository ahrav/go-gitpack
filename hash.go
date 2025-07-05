package objstore

import (
	"encoding/hex"
	"fmt"
)

// Hash represents a raw Git object identifier.
//
// It is the 20-byte binary form of a SHA-1 digest as used by Git internally.
// The zero value is the all-zero hash, which never resolves to a real object.
type Hash [20]byte

// String returns the hexadecimal string representation of the hash.
// This is the canonical 40-character format used by Git.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// IsZero returns true if the hash is the zero value.
func (h Hash) IsZero() bool { return h == (Hash{}) }

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
