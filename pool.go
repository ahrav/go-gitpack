// pool.go
//
// Shared object pools for hot-path resources: zlib decompressors,
// buffered readers, and header scratch buffers.
//
// All pools in this file are safe for concurrent use (sync.Pool guarantee).
// They exist to amortise the cost of allocation-heavy operations that occur
// on every object read: zlib stream creation, 8 KiB bufio buffers, and
// 4 KiB header scratch space.
//
// Cross-file dependencies:
//   - getZlibReader / putZlibReader are called from pack decompression
//     paths in idx.go and delta.go.
//   - GetBuf / PutBuf are exported for use by downstream consumers (e.g.
//     the scanner layer) that need temporary header-sized scratch space
//     without importing internal pool machinery.

package objstore

import (
	"bufio"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zlib"
)

// errZlibStreamOverrun reports a zlib stream that keeps producing data past
// the decompressed size declared by the pack object header.
var errZlibStreamOverrun = errors.New("zlib stream continues past declared object size")

// errInvalidZlibHeader reports a pack object whose 2-byte zlib wrapper fails
// RFC 1950 validation.
var errInvalidZlibHeader = errors.New("invalid zlib header")

// validateZlibHeader checks the 2-byte zlib stream header (RFC 1950): CM must
// be 8 (deflate), CINFO must not exceed 7 (32 KiB window), FDICT must be clear
// (Git never uses preset dictionaries), and the FCHECK checksum must hold.
//
// This is the single header-validation policy for every inflate backend
// (pure-Go mmap, cgo libdeflate, and the non-mmap fallback), so a stream is
// accepted or rejected identically regardless of platform or build tag.
//
// INTEGRITY POLICY: pack object streams are validated by this header check
// plus an exact-termination check of the deflate stream; the trailing adler32
// checksum is intentionally NOT verified on any backend. Opt-in integrity is
// provided by Store.VerifyCRC, which validates the *compressed* bytes against
// the pack index CRC-32 and therefore subsumes what adler32 would catch.
func validateZlibHeader(cmf, flg byte) error {
	if cmf&0x0f != 8 || cmf>>4 > 7 || flg&0x20 != 0 || (uint16(cmf)<<8|uint16(flg))%31 != 0 {
		return errInvalidZlibHeader
	}
	return nil
}

// flatePool recycles flate decompressor state (~40 KiB of Huffman tables and
// window per instance). Unlike zlib.Reader, flate.NewReader can construct a
// valid instance without source bytes, so New can populate the pool directly.
var flatePool = sync.Pool{
	New: func() any { return flate.NewReader(nil) },
}

// putFlateReader returns a flate reader obtained from flatePool. It must
// never be passed a reader from getZlibReader / zrPool; the two pools hold
// different concrete types and mixing them would corrupt stream decoding.
func putFlateReader(fr io.ReadCloser) {
	_ = fr.Close()
	flatePool.Put(fr)
}

// ensureZlibStreamEnd verifies that a fully drained decompression stream
// terminates exactly where the object header said it would: one more read
// must observe clean EOF.
//
// io.ReadFull alone proves only that *enough* bytes were produced. Without
// this check a malformed pack object that inflates to more than the declared
// size, or one whose deflate stream never reaches its final-block marker,
// would be accepted as valid object data. The extra read costs one buffer
// inspection on an already-positioned reader; for zlib-reader sources it
// also forces the trailer validation that ReadFull may not have reached.
func ensureZlibStreamEnd(zr io.Reader) error {
	// io.Copy retries transient (0, nil) reads and maps io.EOF to nil, so a
	// clean end-of-stream yields (0, nil) here.
	n, err := io.Copy(io.Discard, io.LimitReader(zr, 1))
	if err != nil {
		return err
	}
	if n != 0 {
		return errZlibStreamOverrun
	}
	return nil
}

// zrPool reuses zlib.Reader instances to reduce allocations.
//
// New intentionally returns nil rather than constructing a fresh
// zlib.Reader because zlib.NewReader requires an io.Reader with a valid
// zlib header at construction time. We cannot supply one until the
// caller provides the actual compressed stream, so the pool starts
// empty and is populated exclusively via putZlibReader.
var zrPool = sync.Pool{New: func() any { return nil }}

// brPool reuses bufio.Reader instances to avoid allocating 4KB buffers
// for every delta operation. With ~800k delta hops, this saves ~3.3 GiB.
var brPool = sync.Pool{
	New: func() any { return bufio.NewReaderSize(nil, 8<<10) }, // 8 KiB buf once
}

// getZlibReader obtains a zlib.Reader from the pool or creates a new one.
// The function resets the reader to use the provided source.
//
// getZlibReader returns an error if the zlib stream header is invalid.
//
// Implementation notes:
//   - The type assertion to the Reset interface is necessary because
//     the zlib package does not export the concrete *zlib.reader type.
//     We assert the Reset(io.Reader, []byte) error method that the
//     standard library's zlib.reader implements.
//   - If Reset fails (e.g., the new stream has a corrupt zlib header),
//     the error is intentionally swallowed and we fall through to
//     zlib.NewReader. This is safe because NewReader will surface the
//     same header error if the stream is truly corrupt, and it avoids
//     leaking a half-reset reader back into the pool.
func getZlibReader(src io.Reader) (io.ReadCloser, error) {
	if v := zrPool.Get(); v != nil {
		if zr, ok := v.(interface {
			Reset(io.Reader, []byte) error
		}); ok {
			if err := zr.Reset(src, nil); err == nil {
				return zr.(io.ReadCloser), nil
			}
		}
		// Could not reset (corrupt stream) - fall through to fresh alloc.
	}
	return zlib.NewReader(src)
}

// putZlibReader returns a zlib.Reader to the pool for reuse.
func putZlibReader(r io.ReadCloser) {
	_ = r.Close()
	zrPool.Put(r)
}

// getBR obtains a bufio.Reader from the pool and resets it to the given reader.
// This avoids allocating a new buffer for each use.
func getBR(r io.Reader) *bufio.Reader {
	br := brPool.Get().(*bufio.Reader)
	br.Reset(r) // no new allocation
	return br
}

// putBR returns a bufio.Reader to the pool for reuse.
func putBR(br *bufio.Reader) { brPool.Put(br) }

// treeIterPool reuses TreeIter instances to avoid allocating a new struct
// for every tree comparison in walkDiff. In walk-diff intensive workloads
// this eliminates ~1.5M allocations.
var treeIterPool = sync.Pool{
	New: func() any { return &TreeIter{} },
}

// getTreeIter obtains a *TreeIter from the pool and initializes it with raw.
func getTreeIter(raw []byte) *TreeIter {
	it := treeIterPool.Get().(*TreeIter)
	it.rest = raw
	return it
}

// putTreeIter returns a *TreeIter to the pool after clearing its reference
// to the underlying tree data so the GC can collect that memory.
func putTreeIter(it *TreeIter) {
	if it == nil {
		return
	}
	it.rest = nil
	treeIterPool.Put(it)
}

// MaxHdr is the maximum number of bytes we are willing to read for a
// single Git object header. 4096 bytes is generous -- real headers are
// typically under 32 bytes -- but a fixed upper bound protects against
// malformed objects consuming unbounded memory.
const MaxHdr = 4096

// bufPool returns a 0-length slice with a 4 KiB backing array,
// just large enough for the worst-case header we are willing to read.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, MaxHdr)
		return &b
	},
}

// GetBuf retrieves a *[]byte from the shared bufPool. The returned slice
// has length 0 and capacity MaxHdr (4096).
//
// GetBuf and PutBuf are exported so that downstream packages (e.g. the
// scanner layer) can borrow header scratch buffers without importing the
// internal pool directly or allocating per-call.
func GetBuf() *[]byte {
	return bufPool.Get().(*[]byte)
}

// PutBuf returns a buffer to the bufPool after resetting its length to 0.
//
// INVARIANT: the slice is truncated to length 0 before being pooled so
// that the next GetBuf caller receives a clean, zero-length slice.
// Callers MUST NOT retain a reference to *buf after calling PutBuf.
func PutBuf(buf *[]byte) {
	*buf = (*buf)[:0]
	bufPool.Put(buf)
}
