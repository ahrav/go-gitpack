package objstore

import "unsafe"

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
