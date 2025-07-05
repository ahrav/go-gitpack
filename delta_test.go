package objstore

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadVarIntFromReader(t *testing.T) {
	tests := []struct {
		data        []byte
		expected    uint64
		consumed    int
		expectError bool
	}{
		{[]byte{0x00}, 0, 1, false},
		{[]byte{0x7f}, 127, 1, false},
		{[]byte{0x80, 0x01}, 128, 2, false},
		{[]byte{0xff, 0x7f}, 16383, 2, false},
		{[]byte{0x80, 0x80, 0x01}, 16384, 3, false},
		{[]byte{}, 0, -1, true}, // empty buffer now returns error
	}

	for _, test := range tests {
		reader := bufio.NewReader(bytes.NewReader(test.data))
		value, consumed, err := readVarIntFromReader(reader)

		if test.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, test.expected, value)
			assert.Equal(t, test.consumed, consumed)
		}
	}
}

func TestDeltaCycleDetection(t *testing.T) {
	ctx := newDeltaContext(10)

	hash1, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
	hash2, _ := ParseHash("abcdef1234567890abcdef1234567890abcdef12")

	assert.NoError(t, ctx.checkRefDelta(hash1))
	ctx.enterRefDelta(hash1)

	assert.Error(t, ctx.checkRefDelta(hash1), "Should detect circular reference")

	ctx2 := newDeltaContext(2)
	ctx2.enterRefDelta(hash1)
	ctx2.enterRefDelta(hash2)

	hash3, _ := ParseHash("fedcba0987654321fedcba0987654321fedcba09")
	assert.Error(t, ctx2.checkRefDelta(hash3), "Should hit depth limit")
}
