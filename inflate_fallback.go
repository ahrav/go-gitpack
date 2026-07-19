//go:build !linux && !darwin && !windows

package objstore

import (
	"io"

	"golang.org/x/exp/mmap"
)

// inflateExact uses the public ReaderAt API on platforms where x/exp/mmap
// falls back to ordinary file I/O and has no mapped byte slice.
func inflateExact(r *mmap.ReaderAt, pos int64, dst []byte) error {
	if pos < 0 || pos > int64(r.Len()) {
		return io.ErrUnexpectedEOF
	}
	src := io.NewSectionReader(r, pos, int64(r.Len())-pos)
	zr, err := getZlibReader(src)
	if err != nil {
		return err
	}
	defer putZlibReader(zr)

	if _, err = io.ReadFull(zr, dst); err != nil {
		return err
	}
	// Reject streams that do not terminate at the declared object size;
	// ReadFull succeeding proves only that enough bytes were produced.
	return ensureZlibStreamEnd(zr)
}
