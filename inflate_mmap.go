//go:build linux || darwin || windows

package objstore

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/klauspost/compress/flate"
	"golang.org/x/exp/mmap"
)

var flatePool = sync.Pool{
	New: func() any { return flate.NewReader(nil) },
}

// inflateExact decompresses the zlib stream at pos into an exactly sized dst.
func inflateExact(r *mmap.ReaderAt, pos int64, dst []byte) error {
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

func getZlibReaderAt(data []byte, pos int64) (io.ReadCloser, *bytes.Reader, error) {
	if pos < 0 || pos+2 > int64(len(data)) {
		return nil, nil, io.ErrUnexpectedEOF
	}

	cmf, flg := data[pos], data[pos+1]
	if cmf&0x0f != 8 || cmf>>4 > 7 || flg&0x20 != 0 || (uint16(cmf)<<8|uint16(flg))%31 != 0 {
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

func putFlateReader(fr io.ReadCloser) {
	_ = fr.Close()
	flatePool.Put(fr)
}

// mmapData relies on the pinned x/exp/mmap layout for mmap-backed platforms.
func mmapData(r *mmap.ReaderAt) []byte {
	return (*struct{ data []byte })(unsafe.Pointer(r)).data
}
