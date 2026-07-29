//go:build cgo && gitpack_libdeflate

// zlib_cgo_test.go covers the libdeflate one-shot inflate backend. It only
// compiles under the `gitpack_libdeflate` build tag, so it must be exercised
// explicitly:
//
//	go test -tags gitpack_libdeflate ./...
//
// Two complementary proofs live here. The error-class tests pin the
// cross-backend contract by calling inflatePackZlibGo directly, so the same
// malformed stream must classify identically under errors.Is regardless of
// build tag. The differential tests use the pure-Go klauspost zlib reader as
// an output oracle: for every input, libdeflate's whole-buffer decode must
// produce byte-identical output. Because the two backends are selected at
// build time, a single build can only reach one through the store; these
// tests invoke both directly and compare them.
//
// The klauspost reader is aliased as kpzlib because it is the library the
// default backend actually uses, which is what makes it a faithful oracle
// rather than a second opinion; stdlib compress/zlib writes the fixtures for
// the error-class tests.

package objstore

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"math/rand"
	"testing"

	kpzlib "github.com/klauspost/compress/zlib"
	"github.com/stretchr/testify/require"
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

// zlibDeflate returns a real zlib stream for data, matching what Git writes
// into packs.
func zlibDeflate(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := kpzlib.NewWriter(&buf)
	_, err := zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// pureGoInflate decompresses a zlib stream using the default pure-Go backend,
// serving as the differential oracle.
func pureGoInflate(t *testing.T, src []byte) []byte {
	t.Helper()
	zr, err := kpzlib.NewReader(bytes.NewReader(src))
	require.NoError(t, err)
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.NoError(t, zr.Close())
	return out
}

// requireLibdeflateMatches asserts that inflateZlibOneShot reproduces `want`
// exactly and reports the correct number of consumed compressed bytes.
func requireLibdeflateMatches(t *testing.T, want, compressed []byte) {
	t.Helper()
	dst := make([]byte, len(want))
	consumed, err := inflateZlibOneShot(compressed, dst)
	require.NoError(t, err)
	require.Equal(t, want, dst, "libdeflate output differs from pure-Go oracle")
	if len(want) > 0 {
		require.Positive(t, consumed, "consumed byte count should be positive for non-empty output")
		require.LessOrEqual(t, consumed, len(compressed))
	}
}

// TestLibdeflate_DifferentialAgainstPureGo verifies that the libdeflate backend
// agrees with the pure-Go backend across payload shapes that stress different
// DEFLATE code paths: empty, tiny, highly compressible, and incompressible.
func TestLibdeflate_DifferentialAgainstPureGo(t *testing.T) {
	require.True(t, libdeflateAvailable, "test must be built with -tags gitpack_libdeflate")

	cases := map[string][]byte{
		"empty":              {},
		"single_byte":        {0x42},
		"small_text":         []byte("the quick brown fox jumps over the lazy dog"),
		"highly_repetitive":  bytes.Repeat([]byte("AB"), 100000),
		"all_zeros":          make([]byte, 65536),
		"nul_and_high_bytes": bytes.Repeat([]byte{0x00, 0xff, 0xa8, 0x01}, 20000),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			compressed := zlibDeflate(t, data)
			// The oracle and libdeflate must agree with each other and with
			// the original input.
			require.Equal(t, data, pureGoInflate(t, compressed))
			requireLibdeflateMatches(t, data, compressed)
		})
	}

	// Incompressible (random) payloads exercise stored/dynamic-Huffman blocks.
	t.Run("random_sizes", func(t *testing.T) {
		r := rand.New(rand.NewSource(0xBEEF))
		for _, size := range []int{1, 2, 7, 31, 127, 1024, 4096, 65535, 131072} {
			data := make([]byte, size)
			r.Read(data)
			compressed := zlibDeflate(t, data)
			require.Equal(t, data, pureGoInflate(t, compressed))
			requireLibdeflateMatches(t, data, compressed)
		}
	})
}

// TestLibdeflate_EmptyOutputContract verifies that an empty destination still
// requires a complete, valid deflate stream.
func TestLibdeflate_EmptyOutputContract(t *testing.T) {
	compressed := zlibDeflate(t, nil)
	n, err := inflateZlibOneShot(compressed, nil)
	require.NoError(t, err)
	require.Positive(t, n)
	require.LessOrEqual(t, n, len(compressed))

	_, err = inflateZlibOneShot([]byte{0x78, 0x9c}, nil)
	require.Error(t, err, "truncated empty stream must be rejected")
}

// TestLibdeflate_RejectsBadInput ensures corrupt or truncated streams fail
// loudly rather than producing garbage. libdeflate needs the exact output size
// up front; a wrong size or corrupt bytes must error.
func TestLibdeflate_RejectsBadInput(t *testing.T) {
	orig := []byte("some content that will be corrupted before inflation")
	compressed := zlibDeflate(t, orig)

	t.Run("empty_source_nonempty_dst", func(t *testing.T) {
		_, err := inflateZlibOneShot(nil, make([]byte, 4))
		require.Error(t, err)
	})

	t.Run("truncated_stream", func(t *testing.T) {
		_, err := inflateZlibOneShot(compressed[:len(compressed)/2], make([]byte, len(orig)))
		require.Error(t, err)
	})

	t.Run("corrupt_bytes", func(t *testing.T) {
		bad := append([]byte(nil), compressed...)
		for i := 2; i < len(bad); i++ {
			bad[i] ^= 0xff
		}
		_, err := inflateZlibOneShot(bad, make([]byte, len(orig)))
		require.Error(t, err)
	})

	t.Run("wrong_output_size", func(t *testing.T) {
		// Declaring fewer output bytes than the stream produces must fail the
		// exact-size contract rather than silently truncating.
		_, err := inflateZlibOneShot(compressed, make([]byte, len(orig)-1))
		require.Error(t, err)
	})
}
