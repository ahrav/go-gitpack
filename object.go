// object.go
//
// Git object type definitions and packfile object-header parsing routines.
//
// The ObjectType enumeration mirrors Git's internal type encoding exactly:
// the iota values 0-7 correspond to the 3-bit type field stored in bits
// 6-4 of a packfile object header byte. The parser functions in this file
// decode those headers using both safe and unsafe (zero-copy) techniques
// for performance-critical paths.
//
// Cross-file dependencies:
//   - hostLittle (endian.go) -- governs byte-order branches in detectType.
//   - Hash (hash.go) -- object identity type used throughout the store.

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

// INVARIANT: The iota values below MUST match Git's internal 3-bit type
// encoding stored in packfile object headers (bits 6-4 of the first byte).
// Changing the order or inserting new constants will break header parsing
// in parseObjectHeaderUnsafe and every caller that casts a raw header
// nibble to ObjectType.
const (
	// ObjBad represents an invalid or unspecified object kind.
	ObjBad ObjectType = iota // 0

	// ObjCommit is a regular commit object.
	ObjCommit // 1

	// ObjTree is a directory tree object describing the hierarchy of a commit.
	ObjTree // 2

	// ObjBlob is a file-content blob object.
	ObjBlob // 3

	// ObjTag is an annotated tag object.
	ObjTag // 4

	_ // 5 -- unused; reserved by Git for future expansion.

	// ObjOfsDelta is a delta object whose base is addressed by packfile offset.
	ObjOfsDelta // 6

	// ObjRefDelta is a delta object whose base is addressed by object ID.
	ObjRefDelta // 7
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

	// Compare against endianness-specific uint32 representations of the
	// ASCII strings "tree" and "blob". On little-endian hosts (the common
	// case) the bytes are stored in reversed order, so "tree" (0x74 0x72
	// 0x65 0x65) becomes 0x65657274 when loaded as a native uint32.
	// The big-endian constants below are the straightforward byte ordering.
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
		// Big-endian comparisons.
		if first4 == 0x74726565 && len(data) > 4 && data[4] == ' ' {
			return ObjTree
		}
		if first4 == 0x626c6f62 && len(data) > 4 && data[4] == ' ' {
			return ObjBlob
		}
	}

	const (
		parentMarker = "parent "
		authorMarker = "author "
		tagMarker    = "tag "
	)

	// Check for commit markers ("parent " or "author ").
	if len(data) >= 7 {
		first7 := string(data[:7])
		if first7 == parentMarker || first7 == authorMarker {
			return ObjCommit
		}
	}

	if len(data) >= 4 && string(data[:4]) == tagMarker {
		return ObjTag
	}

	// Default to ObjBlob when no markers match. This is intentional: in a
	// test/heuristic context, binary file content is the most common
	// unrecognised payload, and treating it as a blob is the safest
	// fallback since blobs have no structural invariants to violate.
	return ObjBlob
}

var (
	ErrEmptyObjectHeader       = errors.New("empty object header")
	ErrCannotParseObjectHeader = errors.New("cannot parse object header")
)

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
		return ObjBad, 0, ErrEmptyObjectHeader
	}
	ot, _, hdrLen := parseObjectHeaderUnsafe(buf[:n])
	if hdrLen <= 0 {
		return ObjBad, 0, ErrCannotParseObjectHeader
	}
	return ot, hdrLen, nil
}

// parseObjectHeaderUnsafe parses a Git packfile object header using unsafe
// pointer operations for speed on the hot path.
//
// The header format (defined by Git's pack format documentation):
//   - Byte 0, bits 6-4: 3-bit object type (maps directly to ObjectType iota).
//   - Byte 0, bits 3-0: lowest 4 bits of the decompressed size.
//   - Byte 0, bit 7: continuation flag. If set, subsequent bytes contribute
//     7 additional size bits each (standard variable-length integer encoding).
//
// Return convention:
//   - On success: (type, decompressedSize, headerByteCount) where
//     headerByteCount >= 1.
//   - On error (empty input or unterminated varint): (ObjBad, 0, -1).
//     Callers MUST check headerByteCount > 0 before using the other values.
//
// The loop processes at most 10 bytes (enough for a 64-bit size with 4+9*7 = 67
// available bits), which bounds worst-case execution time and prevents
// runaway reads on corrupt data.
//
// Safety: the function never panics -- it checks len(data) before every
// unsafe dereference. The unsafe.Add pointer arithmetic stays within the
// bounds of the original slice's backing array.
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
