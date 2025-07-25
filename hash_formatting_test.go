package objstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHashFormattingInErrors verifies that Hash objects are consistently
// formatted in error messages to prevent double hex encoding issues.
func TestHashFormattingInErrors(t *testing.T) {
	// Create a test hash
	testHash := Hash{0xac, 0xe1, 0x2c, 0xa7, 0xb9, 0x81, 0x46, 0xaf, 0x23, 0xd6,
		0xc0, 0xdb, 0x3f, 0xf0, 0x4b, 0x36, 0x9b, 0x32, 0xd3, 0x06}
	expectedHex := "ace12ca7b98146af23d6c0db3ff04b369b32d306"

	tests := []struct {
		name   string
		errGen func() error
		check  func(t *testing.T, err error)
	}{
		{
			name: "object not found error",
			errGen: func() error {
				return fmt.Errorf("object %s not found", testHash)
			},
			check: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), expectedHex)
				assert.NotContains(t, err.Error(), "616365") // Should not contain double-encoded hex
			},
		},
		{
			name: "wrapped error with hash",
			errGen: func() error {
				innerErr := fmt.Errorf("object %s not found", testHash)
				return fmt.Errorf("failed to load: %w", innerErr)
			},
			check: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), expectedHex)
				assert.NotContains(t, err.Error(), "616365") // Should not contain double-encoded hex
			},
		},
		{
			name: "multiple hashes in error",
			errGen: func() error {
				hash2 := Hash{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
				return fmt.Errorf("failed processing commit %s (tree: %s)", testHash, hash2)
			},
			check: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), expectedHex)
				assert.Contains(t, err.Error(), "123456789abcdef0")
				// Verify no double encoding
				assert.Equal(t, 1, strings.Count(err.Error(), expectedHex))
			},
		},
		{
			name: "circular reference error",
			errGen: func() error {
				return fmt.Errorf("circular delta reference detected for %s", testHash)
			},
			check: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), expectedHex)
				assert.NotContains(t, err.Error(), "616365")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.errGen()
			require.Error(t, err)
			tt.check(t, err)

			if strings.Contains(err.Error(), "object") && strings.Contains(err.Error(), "not found") {
				expected := fmt.Sprintf("object %s not found", expectedHex)
				if !strings.Contains(err.Error(), expected) {
					t.Errorf("Error message format incorrect.\nGot: %s\nExpected to contain: %s", err.Error(), expected)
				}
			}
		})
	}
}

// TestDoubleHexEncodingRegression tests for regression of the double hex encoding issue
// where a hash like "ace12ca7..." would be encoded again as "616365313263613..." in error messages.
func TestDoubleHexEncodingRegression(t *testing.T) {
	// This is the hash from the actual error we encountered.
	testHash := Hash{0xac, 0xe1, 0x2c, 0xa7, 0xb9, 0x81, 0x46, 0xaf, 0x23, 0xd6,
		0xc0, 0xdb, 0x3f, 0xf0, 0x4b, 0x36, 0x9b, 0x32, 0xd3, 0x06}
	expectedHex := "ace12ca7b98146af23d6c0db3ff04b369b32d306"

	doubleEncodedStart := "61636531326361" // hex encoding of "ace12ca"

	err1 := fmt.Errorf("failed to get tree %s: %w", testHash, fmt.Errorf("object %s not found", testHash))
	assert.Contains(t, err1.Error(), expectedHex, "Error should contain the correct hex representation")
	assert.NotContains(t, err1.Error(), doubleEncodedStart, "Error should NOT contain double-encoded hex")

	err2 := fmt.Errorf("failed to create tree iterator for %s: %w", testHash, err1)
	assert.Contains(t, err2.Error(), expectedHex, "Wrapped error should contain the correct hex representation")
	assert.NotContains(t, err2.Error(), doubleEncodedStart, "Wrapped error should NOT contain double-encoded hex")
}
