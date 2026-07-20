//go:build cgo && gitpack_libdeflate

package objstore

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
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

// TestInflateZlibOneShotErrorClassesMatchPureGo pins the cross-backend error
// contract: the same malformed pack must classify identically under
// errors.Is regardless of build tag. A stream continuing past the declared
// size is the overrun class; a stream ending before the declared size is the
// short-output (unexpected-EOF) class.
func TestInflateZlibOneShotErrorClassesMatchPureGo(t *testing.T) {
	payload := []byte("hello world hello world")
	var encoded bytes.Buffer
	zw := zlib.NewWriter(&encoded)
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	t.Run("overrun", func(t *testing.T) {
		dst := make([]byte, len(payload)-6)
		_, cErr := inflateZlibOneShot(encoded.Bytes(), dst)
		if cErr == nil {
			t.Fatal("stream continuing past declared size accepted")
		}
		if !errors.Is(cErr, errZlibStreamOverrun) {
			t.Fatalf("libdeflate backend got %v, want the stream-overrun class", cErr)
		}
		goDst := make([]byte, len(payload)-6)
		_, goErr := inflatePackZlibGo(encoded.Bytes(), goDst)
		if !errors.Is(goErr, errZlibStreamOverrun) {
			t.Fatalf("pure-Go backend got %v, want the stream-overrun class", goErr)
		}
	})

	t.Run("short output", func(t *testing.T) {
		dst := make([]byte, len(payload)+6)
		_, cErr := inflateZlibOneShot(encoded.Bytes(), dst)
		if cErr == nil {
			t.Fatal("stream ending before declared size accepted")
		}
		if !errors.Is(cErr, io.ErrUnexpectedEOF) {
			t.Fatalf("libdeflate backend got %v, want the short-output class", cErr)
		}
		goDst := make([]byte, len(payload)+6)
		_, goErr := inflatePackZlibGo(encoded.Bytes(), goDst)
		if !errors.Is(goErr, io.ErrUnexpectedEOF) {
			t.Fatalf("pure-Go backend got %v, want the short-output class", goErr)
		}
	})
}
