//go:build cgo && gitpack_libdeflate

package objstore

// One-shot zlib inflation backed by libdeflate.
//
// Pack object bodies are always inflated into a buffer of exactly the size
// declared by the object header, and the compressed bytes are already
// resident in the mmap'd pack. That matches libdeflate's whole-buffer model
// exactly — no streaming state machine, no bufio layer, no window copies —
// and libdeflate's DEFLATE decoder is roughly 2x faster than any streaming
// implementation because it decodes into the output buffer in place.
//
// This file is gated behind the `gitpack_libdeflate` build tag so the pure-Go
// build remains the default. With libdeflate installed in the default search
// paths (Debian/Ubuntu: `apt-get install libdeflate-dev`), enable with:
//
//	go build -tags gitpack_libdeflate ./...
//
// For a libdeflate in a non-standard location, or to link a static archive,
// override the search paths externally:
//
//	CGO_CFLAGS="-I/path/to/libdeflate" \
//	CGO_LDFLAGS="-L/path/to/libdeflate/build" \
//	go build -tags gitpack_libdeflate ./...

/*
#cgo CFLAGS: -O2
#cgo LDFLAGS: -ldeflate
#include <stdlib.h>
#include "libdeflate.h"
*/
import "C"

import (
	"errors"
	"runtime"
	"unsafe"
)

// libdeflateAvailable reports that this build uses the cgo backend.
const libdeflateAvailable = true

var errLibdeflateBadData = errors.New("libdeflate: corrupt zlib stream")

// decompressorPool bounds retained native allocations. Unlike sync.Pool, a
// channel never discards C pointers during GC without giving us a chance to
// release them.
var decompressorPool = make(chan unsafe.Pointer, runtime.GOMAXPROCS(0))

func getLibdeflateDecompressor() unsafe.Pointer {
	select {
	case d := <-decompressorPool:
		return d
	default:
		return unsafe.Pointer(C.libdeflate_alloc_decompressor())
	}
}

func putLibdeflateDecompressor(d unsafe.Pointer) {
	select {
	case decompressorPool <- d:
	default:
		C.libdeflate_free_decompressor((*C.struct_libdeflate_decompressor)(d))
	}
}

// inflateZlibOneShot decompresses the zlib stream at the beginning of src
// into dst, which must be sized to exactly the expected output length.
// Returns the number of compressed bytes consumed (the 2-byte zlib header
// plus the deflate payload; the 4-byte adler32 trailer is not consumed).
//
// The zlib wrapper is handled here rather than by libdeflate so that every
// backend enforces the same integrity policy (see validateZlibHeader in
// pool.go): the 2-byte header is validated, the deflate stream must
// terminate exactly at len(dst), and the adler32 trailer is intentionally
// not verified. libdeflate_zlib_decompress_ex would enforce adler32, which
// the pure-Go backend deliberately skips; using the raw deflate entry point
// keeps corrupt-trailer packs behaving identically across build tags.
//
// The src slice may alias mmap'd pack memory; libdeflate only reads it.
func inflateZlibOneShot(src []byte, dst []byte) (int, error) {
	if len(src) < 2 {
		return 0, errLibdeflateBadData
	}
	if err := validateZlibHeader(src[0], src[1]); err != nil {
		return 0, err
	}
	payload := src[2:]
	if len(payload) == 0 {
		// Even a zero-length object carries at least one deflate block.
		return 0, errLibdeflateBadData
	}

	d := getLibdeflateDecompressor()
	if d == nil {
		return 0, errors.New("libdeflate: decompressor allocation failed")
	}
	defer putLibdeflateDecompressor(d)

	var out unsafe.Pointer
	if len(dst) == 0 {
		// libdeflate still validates the complete deflate stream when the
		// expected output is empty; use a non-nil pointer with a zero-sized
		// output buffer.
		var empty byte
		out = unsafe.Pointer(&empty)
	} else {
		out = unsafe.Pointer(&dst[0])
	}

	var inConsumed, outProduced C.size_t
	res := C.libdeflate_deflate_decompress_ex(
		(*C.struct_libdeflate_decompressor)(d),
		unsafe.Pointer(&payload[0]), C.size_t(len(payload)),
		out, C.size_t(len(dst)),
		&inConsumed, &outProduced,
	)
	// Failures map to the same error classes the pure-Go backend reports so
	// errors.Is classification does not change with the build tag:
	// INSUFFICIENT_SPACE means the stream continues past the declared size
	// (overrun class), and a successful decode that produced fewer bytes
	// than declared is the short-output class. Truncated input is the one
	// class libdeflate cannot distinguish: it reports BAD_DATA for both
	// malformed and merely truncated streams, so those collapse together.
	switch {
	case res == C.LIBDEFLATE_INSUFFICIENT_SPACE:
		return 0, errDeflateOutputOverrun
	case res != C.LIBDEFLATE_SUCCESS:
		return 0, errLibdeflateBadData
	case int(outProduced) != len(dst):
		return 0, errDeflateShortOutput
	}
	return 2 + int(inConsumed), nil
}
