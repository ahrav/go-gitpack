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
// build remains the default. Enable with:
//
//	CGO_CFLAGS="-I/path/to/libdeflate" \
//	CGO_LDFLAGS="/path/to/libdeflate/build/libdeflate.a" \
//	go build -tags gitpack_libdeflate ./...

/*
#cgo CFLAGS: -O2
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
// Returns the number of compressed bytes consumed.
//
// The src slice may alias mmap'd pack memory; libdeflate only reads it.
func inflateZlibOneShot(src []byte, dst []byte) (int, error) {
	if len(src) == 0 {
		return 0, errLibdeflateBadData
	}

	d := getLibdeflateDecompressor()
	if d == nil {
		return 0, errors.New("libdeflate: decompressor allocation failed")
	}
	defer putLibdeflateDecompressor(d)

	var out unsafe.Pointer
	if len(dst) == 0 {
		// libdeflate still validates the complete zlib stream when the expected
		// output is empty; use a non-nil pointer with a zero-sized output buffer.
		var empty byte
		out = unsafe.Pointer(&empty)
	} else {
		out = unsafe.Pointer(&dst[0])
	}

	var inConsumed, outProduced C.size_t
	res := C.libdeflate_zlib_decompress_ex(
		(*C.struct_libdeflate_decompressor)(d),
		unsafe.Pointer(&src[0]), C.size_t(len(src)),
		out, C.size_t(len(dst)),
		&inConsumed, &outProduced,
	)
	if res != C.LIBDEFLATE_SUCCESS || int(outProduced) != len(dst) {
		return 0, errLibdeflateBadData
	}
	return int(inConsumed), nil
}
