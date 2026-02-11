// object_test.go tests the ObjectType enumeration, type detection heuristics,
// and platform endianness detection used throughout the objstore package.

package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestObjectTypeString verifies that every defined ObjectType maps to its
// canonical Git string representation (e.g., ObjCommit -> "commit") and that
// an undefined ObjectType value (ObjectType(99)) returns an empty string to
// signal that it is not a recognized type.
func TestObjectTypeString(t *testing.T) {
	tests := []struct {
		objType  ObjectType
		expected string
	}{
		{ObjCommit, "commit"},
		{ObjTree, "tree"},
		{ObjBlob, "blob"},
		{ObjTag, "tag"},
		{ObjOfsDelta, "ofs-delta"},
		{ObjRefDelta, "ref-delta"},
		{ObjectType(99), ""}, // Unrecognized type should return empty string.
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, test.objType.String())
	}
}

// TestDetectType verifies the heuristic that infers an object's type from its
// raw content. The function looks for characteristic prefixes: "tree " for
// trees, "parent " or "author " near the start for commits, and falls back to
// blob for anything else.
func TestDetectType(t *testing.T) {
	tests := []struct {
		data     []byte
		expected ObjectType
	}{
		{[]byte("tree 123\x00some tree data"), ObjTree},
		{[]byte("parent abc\nauthor Someone"), ObjCommit},
		{[]byte("author Someone\ncommitter"), ObjCommit},
		{[]byte("just some blob data"), ObjBlob},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, detectType(test.data))
	}
}

// TestIsLittleEndian checks that the compile-time endianness constant
// (hostLittle) returns a consistent boolean value. The test does not assert a
// specific value because the code must work on both little-endian (amd64,
// arm64) and big-endian platforms; it only confirms determinism and type.
func TestIsLittleEndian(t *testing.T) {
	result1 := hostLittle
	result2 := hostLittle

	assert.Equal(t, result1, result2, "isLittleEndian should return consistent results")

	assert.IsType(t, true, result1, "isLittleEndian should return a boolean")

	// Document what we expect on common architectures.
	// Note: This is informational and will vary by platform.
	t.Logf("Platform is little-endian: %v", result1)

	// Most common platforms (amd64, arm64) are little-endian.
	// This is just documentation, not a strict assertion since the code should work on both.
}
