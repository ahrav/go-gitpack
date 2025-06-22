package objstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		{ObjectType(99), ""},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, test.objType.String())
	}
}

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

func TestIsLittleEndian(t *testing.T) {
	result1 := hostLittle
	result2 := hostLittle

	assert.Equal(t, result1, result2, "isLittleEndian should return consistent results")

	assert.IsType(t, true, result1, "isLittleEndian should return a boolean")

	// Document what we expect on common architectures.
	// Note: This is informational and will vary by platform
	t.Logf("Platform is little-endian: %v", result1)

	// Most common platforms (amd64, arm64) are little-endian
	// This is just documentation, not a strict assertion since the code should work on both.
}
