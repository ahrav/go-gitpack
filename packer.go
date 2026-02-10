package objstore

import (
	"errors"
	"io"

	"golang.org/x/exp/mmap"
)

var (
	ErrEmptyObject            = errors.New("empty object")
	ErrOfsDeltaBaseRefTooLong = errors.New("ofs-delta base-ref too long")
)

// readRawObject inflates a Git object from a packfile.
// For delta objects, the function returns the delta prefix (base reference) prepended
// to the inflated delta instructions.
//
// The prefix is:
// - 20-byte SHA-1 for ref-delta objects.
// - Variable-length offset for ofs-delta objects.
//
// Regular objects return just the inflated content without any prefix.
func readRawObject(r *mmap.ReaderAt, off uint64) (ObjectType, []byte, error) {
	// Parse the generic variable-length object header.
	var hdr [32]byte
	n, err := r.ReadAt(hdr[:], int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		return ObjBad, nil, err
	}
	if n == 0 {
		return ObjBad, nil, ErrEmptyObject
	}

	objType, size, hdrLen := parseObjectHeaderUnsafe(hdr[:n])
	if hdrLen <= 0 {
		return ObjBad, nil, ErrCannotParseObjectHeader
	}

	pos := int64(off) + int64(hdrLen) // Position of the first byte after the generic header.

	// For delta objects, determine the prefix length and read it.
	var prefixLen int
	switch objType {
	case ObjRefDelta:
		prefixLen = 20
	case ObjOfsDelta:
		// Read up to 13 bytes to find the end of the variable-length offset.
		var pfxBuf [13]byte
		pn, _ := r.ReadAt(pfxBuf[:], pos)
		for i := 0; i < pn; i++ {
			if pfxBuf[i]&0x80 == 0 {
				prefixLen = i + 1
				break
			}
		}
		if prefixLen == 0 {
			if pn > 12 {
				return ObjBad, nil, ErrOfsDeltaBaseRefTooLong
			}
			return ObjBad, nil, ErrOfsDeltaBaseRefTooLong
		}
	}

	if prefixLen > 0 {
		// Allocate a single buffer for prefix + inflated data to avoid concatenation.
		combined := make([]byte, prefixLen+int(size))

		// Read prefix directly into the buffer.
		if _, err := r.ReadAt(combined[:prefixLen], pos); err != nil {
			return ObjBad, nil, err
		}
		pos += int64(prefixLen)

		// Inflate the zlib-compressed delta instructions into the remainder.
		src := io.NewSectionReader(r, pos, 1<<63-1)
		zr, err := getZlibReader(src)
		if err != nil {
			return ObjBad, nil, err
		}
		defer putZlibReader(zr)

		if _, err := io.ReadFull(zr, combined[prefixLen:]); err != nil {
			return ObjBad, nil, err
		}
		return objType, combined, nil
	}

	// Regular (non-delta) object: inflate directly.
	src := io.NewSectionReader(r, pos, 1<<63-1)
	zr, err := getZlibReader(src)
	if err != nil {
		return ObjBad, nil, err
	}
	defer putZlibReader(zr)

	out := make([]byte, size)
	if _, err := io.ReadFull(zr, out); err != nil {
		return ObjBad, nil, err
	}
	return objType, out, nil
}
