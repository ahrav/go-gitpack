package objstore

import (
	"compress/zlib"
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

	// For delta objects, read and store the prefix, which contains the base reference.
	var prefix []byte
	switch objType {
	case ObjRefDelta:
		prefix = make([]byte, 20)
		if _, err := r.ReadAt(prefix, pos); err != nil {
			return ObjBad, nil, err
		}
		pos += 20

	case ObjOfsDelta:
		// The offset is a variable-length negative value.
		for {
			var b [1]byte
			if _, err := r.ReadAt(b[:], pos+int64(len(prefix))); err != nil {
				return ObjBad, nil, err
			}
			prefix = append(prefix, b[0])
			if b[0]&0x80 == 0 { // The MSB is clear, indicating the last byte of the offset.
				pos += int64(len(prefix))
				break
			}
			if len(prefix) > 12 { // A sanity check to prevent parsing excessively long offsets.
				return ObjBad, nil, ErrOfsDeltaBaseRefTooLong
			}
		}
	}

	// Inflate the zlib-compressed data stream that follows the header and prefix.
	//
	// The compressed length is unknown, so provide SectionReader
	// with a virtually infinite length; it will stop at EOF.
	src := io.NewSectionReader(r, pos, 1<<63-1)
	zr, err := zlib.NewReader(src)
	if err != nil {
		return ObjBad, nil, err
	}
	defer zr.Close()

	out := make([]byte, size)
	if _, err := io.ReadFull(zr, out); err != nil {
		return ObjBad, nil, err
	}

	// For deltas, return the concatenated prefix and inflated data;
	// otherwise, return just the inflated data.
	if len(prefix) != 0 {
		return objType, append(prefix, out...), nil
	}
	return objType, out, nil
}
