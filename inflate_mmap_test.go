//go:build linux || darwin || windows

package objstore

import (
	"bytes"
	"compress/zlib"
	"io"
	"testing"
)

func TestGetZlibReaderAtRejectsOversizedWindow(t *testing.T) {
	// CM=deflate and FCHECK is valid, but CINFO=8 exceeds RFC 1950's limit.
	data := []byte{0x88, 0x1c}
	if _, _, err := getZlibReaderAt(data, 0); err == nil {
		t.Fatal("expected invalid CINFO to be rejected")
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
	payload := []byte("hello world")
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncate the compressed bytes so the deflate stream never reaches its
	// final-block marker even though it can still produce the payload prefix.
	truncated := buf.Bytes()[:buf.Len()-6]
	if err := inflatePureGo(t, truncated, 4); err == nil {
		t.Fatal("truncated stream accepted")
	}
}
