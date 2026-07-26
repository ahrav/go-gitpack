// packer.go
//
// Lowest-level pack reader: inflates a single Git object from a memory-mapped
// packfile given its byte offset.
//
// This file sits below the delta-resolution layer (delta.go) and above the
// memory-mapped I/O layer (mmap). Its sole exported entry point is
// readRawObject, which:
//
//  1. Parses the variable-length object header at the given offset.
//  2. For ref-delta and ofs-delta objects, reads the base-reference prefix
//     (20-byte OID or variable-length backward offset) and prepends it to
//     the inflated delta instructions in a single combined allocation.
//  3. For regular objects (blob, tree, commit, tag), inflates the
//     zlib-compressed content directly.
//
// The function does NOT resolve delta chains; it returns the raw delta
// instructions with their prefix intact. The caller (typically
// inflateDeltaChainStreaming in delta.go) is responsible for walking the
// chain and applying deltas.

package objstore

import (
	"errors"
	"fmt"
	"io"
	"math"

	"golang.org/x/exp/mmap"
)

var (
	ErrEmptyObject = errors.New("empty object")

	// ErrOfsDeltaBaseRefTooLong is returned when the variable-length backward
	// offset of an ofs-delta object cannot be decoded within 12 continuation
	// bytes. Git's encoding uses 7 payload bits per byte with an MSB
	// continuation flag, so 12 bytes encode at most 84 bits -- far more than
	// needed for any valid pack offset. Exceeding this limit indicates a
	// corrupted or maliciously crafted packfile.
	ErrOfsDeltaBaseRefTooLong = errors.New("ofs-delta base-ref too long")

	// errObjectSizeExceedsPack rejects a header-declared decompressed size
	// that no valid DEFLATE stream in the remaining pack bytes could
	// produce, so a corrupt or hostile header cannot force a huge
	// allocation before inflation fails.
	errObjectSizeExceedsPack = errors.New("object size exceeds deflate expansion bound for remaining pack bytes")
)

// maxDeflateExpansion is the largest output:input ratio a valid DEFLATE
// stream can achieve. A dynamic-Huffman block can encode a 258-byte match
// in two bits, so one input byte can produce at most 8/2*258 = 1032 output
// bytes (zlib documents the same 1032:1 limit).
const maxDeflateExpansion = 1032

// checkDeclaredSize validates a pack header's decompressed-size field
// against what the pack bytes remaining after pos could possibly inflate
// to. Any stream that would satisfy a larger size must run past the end of
// the pack, so rejecting early is behavior-preserving for valid packs:
// inflateExact would fail on the same input after the allocation this
// check exists to avoid. The +64 slack covers block-header and bit-stream
// overhead for tiny inputs. It also implies size fits in int (the bound is
// far below math.MaxInt for any mappable pack), which callers rely on
// before converting.
func checkDeclaredSize(r *mmap.ReaderAt, pos int64, size uint64) error {
	remaining := int64(r.Len()) - pos
	if remaining < 0 {
		remaining = 0
	}
	if uint64(remaining) > (math.MaxUint64-64)/maxDeflateExpansion {
		// Bound arithmetic would overflow; no realizable pack is this
		// large, but fail open to the MaxInt guard below.
		remaining = (math.MaxInt64 - 64) / maxDeflateExpansion
	}
	bound := uint64(remaining)*maxDeflateExpansion + 64
	// The MaxInt-64 clause keeps int conversions (and small prefix
	// additions, at most 20 bytes) overflow-free even on 32-bit hosts.
	if size > bound || size > uint64(math.MaxInt-64) {
		return fmt.Errorf("%w: size=%d bound=%d", errObjectSizeExceedsPack, size, bound)
	}
	return nil
}

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
		pn, readErr := r.ReadAt(pfxBuf[:], pos)
		for i := 0; i < pn; i++ {
			if pfxBuf[i]&0x80 == 0 {
				prefixLen = i + 1
				break
			}
		}
		if prefixLen == 0 {
			if pn == 0 && readErr != nil {
				return ObjBad, nil, fmt.Errorf("read ofs-delta prefix: %w", readErr)
			}
			return ObjBad, nil, ErrOfsDeltaBaseRefTooLong
		}
	}

	if prefixLen > 0 {
		// Bound the declared body size before sizing the allocation from
		// it: size comes straight from the pack header, so a hostile pack
		// can advertise up to 2^64-1 (int(size) would wrap negative and
		// panic make, and anything below that would OOM ahead of the
		// inflate failure).
		if err := checkDeclaredSize(r, pos+int64(prefixLen), size); err != nil {
			return ObjBad, nil, err
		}

		// Combined buffer allocation strategy: the prefix (base reference)
		// and the inflated delta instructions are laid out contiguously in
		// a single allocation:
		//
		//   [  prefix (OID or var-int offset)  |  inflated delta body  ]
		//   <------------ prefixLen ----------->|<------- size -------->
		//
		// The prefix is read via ReadAt; the body is inflated via zlib.
		// This avoids a second allocation + append/copy to concatenate
		// the two pieces, which matters on hot paths during delta-chain
		// resolution where thousands of objects may be inflated per scan.
		combined := make([]byte, prefixLen+int(size))

		// Read prefix directly into the buffer.
		if _, err := r.ReadAt(combined[:prefixLen], pos); err != nil {
			return ObjBad, nil, err
		}
		pos += int64(prefixLen)

		// Inflate the zlib-compressed delta instructions into the remainder.
		if err := inflateExact(r, pos, combined[prefixLen:]); err != nil {
			return ObjBad, nil, err
		}
		return objType, combined, nil
	}

	// Regular (non-delta) object: inflate directly from the mapped pack.
	// The same declared-size bound applies before the allocation.
	if err := checkDeclaredSize(r, pos, size); err != nil {
		return ObjBad, nil, err
	}
	out := make([]byte, size)
	if err := inflateExact(r, pos, out); err != nil {
		return ObjBad, nil, err
	}
	return objType, out, nil
}
