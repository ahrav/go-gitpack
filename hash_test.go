// hash_test.go tests the Hash type, including parsing from hex strings via
// ParseHash and the String() round-trip representation.

package objstore

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHash validates ParseHash for valid 40-character hex strings, invalid
// (non-hex) input, and wrong-length input. It ensures that valid hashes
// decode to the expected byte representation and that error cases are rejected.
func TestHash(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		checkResult func(t *testing.T, hash Hash, err error)
	}{
		{
			name:        "valid hash",
			input:       "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0",
			expectError: false,
			checkResult: func(t *testing.T, hash Hash, err error) {
				require.NoError(t, err)
				expected := "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0"
				assert.Equal(t, expected, hex.EncodeToString(hash[:]))
			},
		},
		{
			name:        "invalid hash",
			input:       "invalid",
			expectError: true,
			checkResult: func(t *testing.T, hash Hash, err error) {
				assert.Error(t, err)
			},
		},
		{
			name:        "wrong length",
			input:       "abcd",
			expectError: true,
			checkResult: func(t *testing.T, hash Hash, err error) {
				assert.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := ParseHash(tt.input)
			tt.checkResult(t, hash, err)
		})
	}
}

// TestHashString verifies that Hash.String() produces the canonical lowercase
// 40-character hex representation, covering a normal hash, all-zeros, and
// all-ones boundary values.
func TestHashString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid hash",
			input:    "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0",
			expected: "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0",
		},
		{
			name:     "all zeros",
			input:    "0000000000000000000000000000000000000000",
			expected: "0000000000000000000000000000000000000000",
		},
		{
			name:     "all ones",
			input:    "ffffffffffffffffffffffffffffffffffffffff",
			expected: "ffffffffffffffffffffffffffffffffffffffff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := ParseHash(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, hash.String())
		})
	}
}
