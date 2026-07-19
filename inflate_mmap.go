//go:build linux || darwin || windows

package objstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"unsafe"

	"github.com/klauspost/compress/flate"
	"golang.org/x/exp/mmap"
)

// inflateExact decompresses the zlib stream at pos into an exactly sized dst.
func inflateExact(r *mmap.ReaderAt, pos int64, dst []byte) error {
	// The forged data slice does not keep r reachable, and x/exp/mmap sets
	// a finalizer that munmaps the region. Pin r until decompression has
	// finished reading the mapped bytes so a GC cannot unmap them mid-read.
	defer runtime.KeepAlive(r)

	data := mmapData(r)
	if pos < 0 || pos > int64(len(data)) {
		return io.ErrUnexpectedEOF
	}
	if libdeflateAvailable {
		_, err := inflateZlibOneShot(data[pos:], dst)
		return err
	}

	zr, br, err := getZlibReaderAt(data, pos)
	if err != nil {
		return err
	}
	defer putBytesReader(br)
	defer putFlateReader(zr)

	if _, err = io.ReadFull(zr, dst); err != nil {
		return err
	}
	// Reject streams that do not terminate at the declared object size;
	// ReadFull succeeding proves only that enough bytes were produced.
	return ensureZlibStreamEnd(zr)
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
//     (store.VerifyCRC): verifyCRC32 streams the object's full on-disk
//     byte range — zlib header, deflate payload, AND the adler32 trailer
//     bytes — so trailer corruption is caught there, and CRC-32 over the
//     compressed bytes subsumes what adler32 would catch in the output.
//
// Callers must release both returned values: first putFlateReader(zr), then
// putBytesReader(br). The returned readers alias mmap'd memory and must not
// be used after the store is closed.
func getZlibReaderAt(data []byte, pos int64) (io.ReadCloser, *bytes.Reader, error) {
	if pos < 0 || pos+2 > int64(len(data)) {
		return nil, nil, io.ErrUnexpectedEOF
	}

	if err := validateZlibHeader(data[pos], data[pos+1]); err != nil {
		return nil, nil, fmt.Errorf("%w at pack offset %d", err, pos)
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

// mmapData relies on the pinned x/exp/mmap layout for mmap-backed platforms.
func mmapData(r *mmap.ReaderAt) []byte {
	return (*struct{ data []byte })(unsafe.Pointer(r)).data
}

// errMmapLayout is returned when the forged slice header produced by
// mmapData does not behave like the real mapped region.
var errMmapLayout = errors.New("objstore: x/exp/mmap ReaderAt layout changed; update mmapData in inflate_mmap.go")

// checkMmapLayout verifies at pack-open time that the unsafe cast in mmapData
// still matches x/exp/mmap's ReaderAt layout, so a layout change in a future
// dependency bump (including a newer x/exp selected by a downstream module's
// MVS resolution — our go.mod pin is not a ceiling for consumers) surfaces as
// a deterministic open error instead of silent memory corruption on the read
// path.
//
// Two independent probes:
//
//  1. Length: the forged header's len must equal the public Len(). This
//     catches layouts that put a non-length word where the slice length
//     lives.
//  2. Content: bytes read through the forged slice must match bytes read
//     through the public ReadAt API at the start, middle, and end of the
//     region. A hypothetical layout that happens to preserve a plausible
//     length while pointing the data word at unrelated memory (e.g. a
//     leading *os.File field, as in x/exp/mmap's non-mmap fallback struct)
//     fails this comparison instead of corrupting object reads later.
func checkMmapLayout(r *mmap.ReaderAt) error {
	// Pin r for the duration: the forged slice alone does not keep it
	// reachable, and its finalizer munmaps the region (see inflateExact).
	defer runtime.KeepAlive(r)

	data := mmapData(r)
	if len(data) != r.Len() {
		return errMmapLayout
	}
	if len(data) == 0 {
		return nil
	}
	for _, off := range []int{0, len(data) / 2, len(data) - 1} {
		var b [1]byte
		if _, err := r.ReadAt(b[:], int64(off)); err != nil {
			return err
		}
		if b[0] != data[off] {
			return errMmapLayout
		}
	}
	return nil
}
