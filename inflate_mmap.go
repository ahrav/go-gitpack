//go:build linux || darwin || windows

package objstore

import (
	"errors"
	"io"
	"reflect"
	"runtime"
	"unsafe"

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
	_, err := inflateZlibOneShot(data[pos:], dst)
	return err
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
// Three probes, ordered so that no forged pointer is dereferenced until the
// layout is proven:
//
//  1. Shape (reflection, no unsafe): mmap.ReaderAt must be a struct with
//     exactly one field, of type []byte, at offset 0. Reflection reads only
//     type metadata — never field values — so this cannot fault, and it
//     gates the unsafe cast: a changed layout is rejected here before any
//     forged slice header exists.
//  2. Length: the forged header's len must equal the public Len().
//  3. Content: bytes read through the forged slice must match bytes read
//     through the public ReadAt API at the start, middle, and end of the
//     region. After the shape check the forged slice IS the struct's real
//     []byte field, so indexing within its length is memory-safe; this
//     probe guards semantic drift (e.g. the field no longer holding the
//     exact mapped region).
func checkMmapLayout(r *mmap.ReaderAt) error {
	// Pin r for the duration: the forged slice alone does not keep it
	// reachable, and its finalizer munmaps the region (see inflateExact).
	defer runtime.KeepAlive(r)

	t := reflect.TypeOf(mmap.ReaderAt{})
	if t.Kind() != reflect.Struct || t.NumField() != 1 {
		return errMmapLayout
	}
	if f := t.Field(0); f.Offset != 0 || f.Type != reflect.TypeOf([]byte(nil)) {
		return errMmapLayout
	}

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
