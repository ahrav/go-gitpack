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

	_, err = io.ReadFull(zr, dst)
	return err
}
