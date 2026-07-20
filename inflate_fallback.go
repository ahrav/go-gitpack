//go:build !linux && !darwin && !windows

package objstore

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/flate"
	"golang.org/x/exp/mmap"
)

// inflateExact uses the public ReaderAt API on platforms where x/exp/mmap
// falls back to ordinary file I/O and has no mapped byte slice.
//
// It applies the same integrity policy as the mmap and libdeflate backends
// (see validateZlibHeader in pool.go): validate the 2-byte zlib header,
// require the deflate stream to terminate exactly at the declared size, and
// skip the adler32 trailer.
func inflateExact(r *mmap.ReaderAt, pos int64, dst []byte) error {
	if pos < 0 || pos+2 > int64(r.Len()) {
		return io.ErrUnexpectedEOF
	}

	var hdr [2]byte
	if _, err := r.ReadAt(hdr[:], pos); err != nil {
		return err
	}
	if err := validateZlibHeader(hdr[0], hdr[1]); err != nil {
		return fmt.Errorf("%w at pack offset %d", err, pos)
	}

	// Buffer the section reads: on these platforms every ReadAt is real file
	// I/O, and the flate decoder issues many small reads.
	src := io.NewSectionReader(r, pos+2, int64(r.Len())-pos-2)
	br := getBR(src)
	defer putBR(br)

	fr := flatePool.Get().(io.ReadCloser)
	defer putFlateReader(fr)
	if err := fr.(flate.Resetter).Reset(br, nil); err != nil {
		return err
	}

	if _, err := io.ReadFull(fr, dst); err != nil {
		return err
	}
	// Reject streams that do not terminate at the declared object size;
	// ReadFull succeeding proves only that enough bytes were produced.
	return ensureZlibStreamEnd(fr)
}

// checkMmapLayout is a no-op on platforms where x/exp/mmap has no mapped byte
// slice; inflateExact above never uses the unsafe mmapData cast here.
func checkMmapLayout(*mmap.ReaderAt) error { return nil }

// openCommitHeaderStream positions a pooled zlib reader at the deflate
// payload of the object starting at off. On this fallback build every read
// is real file I/O through a SectionReader; the returned release func
// recycles the pooled zlib reader and must be invoked exactly once.
func openCommitHeaderStream(p *mmap.ReaderAt, off int64) (io.Reader, func(), error) {
	if off < 0 || off > int64(p.Len()) {
		return nil, nil, io.ErrUnexpectedEOF
	}
	zr, err := getZlibReader(io.NewSectionReader(p, off, int64(p.Len())-off))
	if err != nil {
		return nil, nil, err
	}
	return zr, func() { putZlibReader(zr) }, nil
}
