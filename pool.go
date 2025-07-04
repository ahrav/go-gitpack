package objstore

import (
	"bufio"
	"compress/zlib"
	"io"
	"sync"
)

// zrPool reuses zlib.Reader instances to reduce allocations.
// We create a fresh one on demand the first time New() is hit, because
// there is no exported zero-value constructor for zlib.Reader.
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
