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
			// Every input is a freshly generated valid stream, so both
			// backends must accept it outright; a shared rejection is a
			// failure, not agreement.
			if goErr != nil || cErr != nil {
				t.Fatalf("valid stream rejected: Go=%v C=%v", goErr, cErr)
			}
			// Both backends consume the header plus deflate payload and
			// leave the 4-byte adler32 trailer as trailing input.
			wantConsumed := len(encoded) - 4
			if goConsumed != wantConsumed || cConsumed != wantConsumed {
				t.Fatalf("consumed mismatch: Go=%d C=%d want=%d",
					goConsumed, cConsumed, wantConsumed)
			}
			if !bytes.Equal(goDst, payload) || !bytes.Equal(cDst, payload) {
				t.Fatalf("output mismatch: goEqual=%v cEqual=%v",
					bytes.Equal(goDst, payload), bytes.Equal(cDst, payload))
			}
		})
	}
}
