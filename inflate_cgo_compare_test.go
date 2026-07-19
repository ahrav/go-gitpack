//go:build cgo && gitpack_libdeflate

package objstore

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"testing"
)

func TestInflatePackZlibGoMatchesLibdeflate(t *testing.T) {
	sizes := []int{0, 1, 32, 257, 258, 259, 4096, 32768, 65536}
	levels := []int{
		zlib.NoCompression,
		zlib.HuffmanOnly,
		zlib.BestSpeed,
		zlib.DefaultCompression,
		zlib.BestCompression,
	}

	for i, size := range sizes {
		payload := makeDeterministicBytes(size)
		if i&1 == 0 {
			payload = bytes.Repeat([]byte{byte(i)}, size)
		}
		encoded := encodeZlib(t, payload, levels[i%len(levels)])

		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			goDst := make([]byte, size)
			cDst := make([]byte, size)
			goConsumed, goErr := inflatePackZlibGo(encoded, goDst)
			cConsumed, cErr := inflateZlibOneShot(encoded, cDst)
			if (goErr == nil) != (cErr == nil) {
				t.Fatalf("acceptance mismatch: Go=%v C=%v", goErr, cErr)
			}
			if goErr != nil {
				return
			}
			if goConsumed != cConsumed || !bytes.Equal(goDst, cDst) {
				t.Fatalf("result mismatch: Go consumed=%d C consumed=%d outputEqual=%v",
					goConsumed, cConsumed, bytes.Equal(goDst, cDst))
			}
		})
	}
}
