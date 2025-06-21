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
