package objstore

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
