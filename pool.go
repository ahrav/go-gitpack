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
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zlib"
	"golang.org/x/exp/mmap"
)

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

// flatePool recycles flate decompressor state (~40 KiB of Huffman tables and
// window per instance). Unlike zlib.Reader, flate.NewReader can construct a
// valid instance without source bytes, so New can populate the pool directly.
var flatePool = sync.Pool{
	New: func() any { return flate.NewReader(nil) },
}

// getZlibReaderAt opens the *deflate* payload of a zlib stream positioned at
// pos inside the memory-mapped pack.
//
// Two deliberate deviations from a stock zlib reader:
//
//  1. Zero-copy source: the compressed stream is presented as a pooled
//     *bytes.Reader aliasing the mapped pages directly. klauspost/flate
//     detects *bytes.Reader sources and switches to a specialized decode
//     loop that consumes the slice without intermediate buffering.
//
//  2. No adler32: the 2-byte zlib header is validated here and the raw
//     deflate stream is handed to a pooled flate reader, skipping zlib's
//     adler32 accumulation over every decompressed byte (~4% of scan CPU)
//     and the trailing checksum comparison. Pack integrity is instead
//     covered by the optional CRC-32 verification against the pack index
//     (store.VerifyCRC), which checks the *compressed* bytes and therefore
//     subsumes what adler32 would catch.
//
// Callers must release both returned values: first putFlateReader(zr), then
// putBytesReader(br). The returned readers alias mmap'd memory and must not
// be used after the store is closed.
func getZlibReaderAt(pack *mmap.ReaderAt, pos int64) (io.ReadCloser, *bytes.Reader, error) {
	data := mmapData(pack)
	if pos < 0 || pos+2 > int64(len(data)) {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Validate the 2-byte zlib header (RFC 1950): CM must be 8 (deflate),
	// FDICT must be clear (Git never uses preset dictionaries), and the
	// header checksum must hold. Reject anything else so corrupt offsets
	// fail loudly instead of producing garbage.
	cmf, flg := data[pos], data[pos+1]
	if cmf&0x0f != 8 || flg&0x20 != 0 || (uint16(cmf)<<8|uint16(flg))%31 != 0 {
		return nil, nil, fmt.Errorf("invalid zlib header at pack offset %d", pos)
	}

	br := getBytesReader(data[pos+2:])
	fr := flatePool.Get().(io.ReadCloser)
	if err := fr.(flate.Resetter).Reset(br, nil); err != nil {
		flatePool.Put(fr)
		putBytesReader(br)
		return nil, nil, err
	}
	return fr, br, nil
}

// putFlateReader returns a flate reader obtained from getZlibReaderAt to its
// pool. It must never be passed a reader from getZlibReader / the zlib pool;
// the two pools hold different concrete types and mixing them would corrupt
// stream decoding.
func putFlateReader(fr io.ReadCloser) {
	_ = fr.Close()
	flatePool.Put(fr)
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
