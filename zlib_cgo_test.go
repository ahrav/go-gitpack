//go:build cgo && gitpack_libdeflate

package objstore

import (
	"bytes"
	"compress/zlib"
	"testing"
)

func TestInflateZlibOneShotValidatesEmptyOutput(t *testing.T) {
	var encoded bytes.Buffer
	zw := zlib.NewWriter(&encoded)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := inflateZlibOneShot(encoded.Bytes(), nil); err != nil {
		t.Fatalf("valid empty stream rejected: %v", err)
	}
	if _, err := inflateZlibOneShot([]byte{0x78, 0x9c}, nil); err == nil {
		t.Fatal("truncated empty stream accepted")
	}
}
