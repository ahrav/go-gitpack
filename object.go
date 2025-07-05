package objstore

import (
	"errors"
	"io"
	"unsafe"

	"golang.org/x/exp/mmap"
)

// ObjectType enumerates the kinds of Git objects that can appear in a pack
// or loose-object store.
//
// The zero value, ObjBad, denotes an invalid or unknown object type.
// The String method returns the canonical, lower-case Git spelling.
type ObjectType byte

const (
	// ObjBad represents an invalid or unspecified object kind.
	ObjBad ObjectType = iota

	// ObjCommit is a regular commit object.
	ObjCommit

	// ObjTree is a directory tree object describing the hierarchy of a commit.
	ObjTree

	// ObjBlob is a file-content blob object.
	ObjBlob

	// ObjTag is an annotated tag object.
	ObjTag

	_ // Ununused

	// ObjOfsDelta is a delta object whose base is addressed by packfile offset.
	ObjOfsDelta

	// ObjRefDelta is a delta object whose base is addressed by object ID.
	ObjRefDelta
)

var typeNames = map[ObjectType]string{
	ObjCommit:   "commit",
	ObjTree:     "tree",
	ObjBlob:     "blob",
	ObjTag:      "tag",
	ObjOfsDelta: "ofs-delta",
	ObjRefDelta: "ref-delta",
}

func (t ObjectType) String() string { return typeNames[t] }

// detectType is a best-effort heuristic that infers the Git object kind from
// the first few bytes of an already-inflated buffer.
//
// It is used only in test helpers where the regular packfile header has been
// stripped.
// Production callers always know the type from the on-disk header and should
// never rely on this function.
func detectType(data []byte) ObjectType {
	if len(data) < 4 {
		return ObjBlob
	}

	// Use an aligned uint32 load so we can compare four bytes at once.
	first4 := *(*uint32)(unsafe.Pointer(&data[0]))

	// Compare against little-endian representations
	const (
		treeLE = 0x65657274 // "tree" in little-endian
		blobLE = 0x626f6c62 // "blob" in little-endian
	)

	if hostLittle {
		switch first4 {
		case treeLE:
			if len(data) > 4 && data[4] == ' ' {
				return ObjTree
			}
		case blobLE:
			if len(data) > 4 && data[4] == ' ' {
				return ObjBlob
			}
		}
	} else {
		// Big-endian comparisons
		if first4 == 0x74726565 && len(data) > 4 && data[4] == ' ' {
			return ObjTree
		}
		if first4 == 0x626c6f62 && len(data) > 4 && data[4] == ' ' {
			return ObjBlob
		}
	}

	// Check for commit markers ("parent " or "author ").
	if len(data) >= 7 {
		first7 := string(data[:7])
		if first7 == "parent " || first7 == "author " {
			return ObjCommit
		}
	}

	if len(data) >= 4 && string(data[:4]) == "tag " {
		return ObjTag
	}

	return ObjBlob
}

// parseObjectHeaderUnsafe parses a Git object header using unsafe pointer operations.
// The header encodes object type in bits 6-4 of the first byte and size as a
// variable-length integer.
//
// parseObjectHeaderUnsafe returns the object type, decompressed size, and number of header bytes consumed.
// The function uses unsafe operations for performance but never panics.
func parseObjectHeaderUnsafe(data []byte) (ObjectType, uint64, int) {
	if len(data) == 0 {
		return ObjBad, 0, -1
	}

	// The first byte encodes the object type and the four least significant bits of the size.
	b0 := *(*byte)(unsafe.Pointer(&data[0]))
	objType := ObjectType((b0 >> 4) & 7)
	size := uint64(b0 & 0x0f)

	if b0&0x80 == 0 {
		return objType, size, 1
	}

	// Handle common 2- and 3-byte headers efficiently.
	if len(data) >= 3 {
		b1 := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), 1))
		size |= uint64(b1&0x7f) << 4
		if b1&0x80 == 0 {
			return objType, size, 2
		}

		b2 := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), 2))
		size |= uint64(b2&0x7f) << 11
		if b2&0x80 == 0 {
			return objType, size, 3
		}
	}

	// Fallback to a loop for headers longer than three bytes.
	shift := uint(4)
	for i := 1; i < len(data) && i < 10; i++ {
		b := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), i))
		size |= uint64(b&0x7f) << shift
		shift += 7
		if b&0x80 == 0 {
			return objType, size, i + 1
		}
	}

	return ObjBad, 0, -1
}

// peekObjectType reads the object header to determine type without inflating the body.
// The function returns the object type and header length in bytes.
//
// This optimization allows skipping full decompression for objects that
// can be served from other sources like the commit-graph.
func peekObjectType(r *mmap.ReaderAt, off uint64) (ObjectType, int, error) {
	var buf [32]byte
	n, err := r.ReadAt(buf[:], int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		return ObjBad, 0, err
	}
	if n == 0 {
		return ObjBad, 0, errors.New("empty object header")
	}
	ot, _, hdrLen := parseObjectHeaderUnsafe(buf[:n])
	if hdrLen <= 0 {
		return ObjBad, 0, errors.New("cannot parse object header")
	}
	return ot, hdrLen, nil
}
