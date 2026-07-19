//go:build linux || darwin || windows

package objstore

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/exp/mmap"
)

// TestCheckMmapLayout guards the unsafe cast in mmapData: if a x/exp/mmap
// bump changes ReaderAt's layout, this fails deterministically instead of
// letting pack reads dereference a forged slice header.
func TestCheckMmapLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout-probe")
	content := []byte("layout probe contents")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := mmap.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := checkMmapLayout(r); err != nil {
		t.Fatal(err)
	}
	if got := mmapData(r); !bytes.Equal(got, content) {
		t.Fatalf("mmapData returned %q, want %q", got, content)
	}
}

func TestGetZlibReaderAtRejectsOversizedWindow(t *testing.T) {
	// CM=deflate and FCHECK is valid, but CINFO=8 exceeds RFC 1950's limit.
	data := []byte{0x88, 0x1c}
	if _, _, err := getZlibReaderAt(data, 0); err == nil {
		t.Fatal("expected invalid CINFO to be rejected")
	}
}

// TestInflateIgnoresAdler32Trailer documents the cross-backend integrity
// policy (see validateZlibHeader): the adler32 trailer is not verified by any
// backend. Integrity is the job of the opt-in Store.VerifyCRC pack-index
// check. If this test starts failing, a backend drifted from the policy.
func TestInflateIgnoresAdler32Trailer(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	corrupted := append([]byte(nil), buf.Bytes()...)
	corrupted[len(corrupted)-1] ^= 0xff // flip a bit in the adler32 trailer

	if err := inflatePureGo(t, corrupted, len(payload)); err != nil {
		t.Fatalf("corrupt adler32 trailer must be ignored, got: %v", err)
	}
	if libdeflateAvailable {
		dst := make([]byte, len(payload))
		if _, err := inflateZlibOneShot(corrupted, dst); err != nil {
			t.Fatalf("libdeflate backend must also ignore the trailer, got: %v", err)
		}
		if !bytes.Equal(dst, payload) {
			t.Fatalf("libdeflate produced %q, want %q", dst, payload)
		}
	}
}

// inflatePureGo mirrors the pure-Go branch of inflateExact: fill dst from the
// deflate payload, then require the stream to terminate at the declared size.
func inflatePureGo(t *testing.T, src []byte, declaredSize int) error {
	t.Helper()
	zr, br, err := getZlibReaderAt(src, 0)
	if err != nil {
		t.Fatalf("open zlib stream: %v", err)
	}
	defer putBytesReader(br)
	defer putFlateReader(zr)

	dst := make([]byte, declaredSize)
	if _, err := io.ReadFull(zr, dst); err != nil {
		return err
	}
	return ensureZlibStreamEnd(zr)
}

func TestInflateAcceptsExactlyTerminatedStream(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	if err := inflatePureGo(t, buf.Bytes(), len(payload)); err != nil {
		t.Fatalf("valid stream rejected: %v", err)
	}
}

func TestInflateRejectsStreamContinuingPastDeclaredSize(t *testing.T) {
	payload := []byte("hello world")
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	// Declare fewer bytes than the stream actually inflates to. io.ReadFull
	// stops once dst is full, so only the end-of-stream check can catch this.
	if err := inflatePureGo(t, buf.Bytes(), len(payload)-6); err == nil {
		t.Fatal("stream continuing past the declared size was accepted")
	}
}

func TestInflateRejectsStreamMissingTerminator(t *testing.T) {
	// Construct a raw deflate stream whose single stored block is NOT final
	// (BFINAL=0) and which ends at input EOF immediately after its payload:
	//
	//   0x00        BFINAL=0, BTYPE=00 (stored)
	//   LEN=4       little-endian
	//   NLEN=^4
	//   4 payload bytes, then nothing — no final block ever arrives.
	//
	// io.ReadFull fills dst with exactly the declared 4 bytes, so only the
	// end-of-stream check can notice that the deflate stream is truncated
	// rather than terminated.
	payload := []byte("abcd")
	stream := []byte{0x78, 0x9c, 0x00, 0x04, 0x00, 0xfb, 0xff}
	stream = append(stream, payload...)

	err := inflatePureGo(t, stream, len(payload))
	if err == nil {
		t.Fatal("stream with no final block accepted")
	}
	// The failure must be the truncation case, not the overrun case: EOF
	// arrived where the decoder still expected another block header.
	if errors.Is(err, errZlibStreamOverrun) {
		t.Fatalf("got overrun error, want truncation: %v", err)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want an unexpected-EOF truncation error", err)
	}
}
