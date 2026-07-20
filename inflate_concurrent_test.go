package objstore

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
)

// TestInflatePackZlibGoConcurrentPoolReuse pins pooled-inflater reuse under
// concurrent contention. Worker goroutines rotate through interleaved stored,
// fixed-Huffman, dynamic-Huffman, invalid, and truncated streams so adjacent
// Put/Get pairs on goInflaterPool repeatedly hand a fixed-table-warmed
// inflater to a dynamic decode (and vice versa), exercising the fixed-table
// cache invalidation in loadDynamicTables. The sequential suite only covers
// pool reuse incidentally; running this test under the -race CI job gives the
// pool and the reuse invariant a direct workout.
func TestInflatePackZlibGoConcurrentPoolReuse(t *testing.T) {
	type inflateCase struct {
		name         string
		src          []byte
		dstSize      int
		want         []byte
		wantConsumed int
		wantErr      bool
		wantEOF      bool
	}

	mustHex := func(s string) []byte {
		raw, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	// Stored/raw blocks: NoCompression emits BTYPE=0 blocks only.
	storedPayload := makeDeterministicBytes(4096)
	storedSrc := encodeZlib(t, storedPayload, zlib.NoCompression)

	// A guaranteed fixed-Huffman member (static vector shared with
	// TestInflatePackZlibStaticVectors).
	fixedPayload := []byte("goodbye, world")
	fixedSrc := mustHex(fixedHuffmanVectorHex)

	// A guaranteed dynamic-Huffman member (static vector).
	dynVecPayload := bytes.Repeat([]byte("abcabc"), 1000)
	dynVecSrc := mustHex(dynamicHuffmanVectorHex)

	// 256 KiB of repeated deterministic chunks: dynamic tables over a full
	// byte alphabet, long matches, and an output large enough to cross
	// inflateFastYieldBytes on the assembly fast path.
	largePayload := bytes.Repeat(makeDeterministicBytes(2048), 128)
	largeSrc := encodeZlib(t, largePayload, zlib.DefaultCompression)

	// Small, highly compressible payload.
	smallPayload := bytes.Repeat([]byte("compressible "), 40)
	smallSrc := encodeZlib(t, smallPayload, zlib.BestCompression)

	cases := []inflateCase{
		{
			name:         "fixed",
			src:          fixedSrc,
			dstSize:      len(fixedPayload),
			want:         fixedPayload,
			wantConsumed: len(fixedSrc) - 4,
		},
		{
			name:         "dynamic large",
			src:          largeSrc,
			dstSize:      len(largePayload),
			want:         largePayload,
			wantConsumed: len(largeSrc) - 4,
		},
		{
			name:         "stored",
			src:          storedSrc,
			dstSize:      len(storedPayload),
			want:         storedPayload,
			wantConsumed: len(storedSrc) - 4,
		},
		{
			name:         "dynamic vector",
			src:          dynVecSrc,
			dstSize:      len(dynVecPayload),
			want:         dynVecPayload,
			wantConsumed: len(dynVecSrc) - 4,
		},
		{
			name:         "small compressible",
			src:          smallSrc,
			dstSize:      len(smallPayload),
			want:         smallPayload,
			wantConsumed: len(smallSrc) - 4,
		},
		{
			name:    "invalid reserved block type",
			src:     []byte{0x78, 0x9c, 0x07},
			wantErr: true,
		},
		{
			name:    "invalid stored LEN NLEN mismatch",
			src:     []byte{0x78, 0x9c, 0x01, 0x01, 0x00, 0xff, 0xff, 0x00},
			dstSize: 1,
			wantErr: true,
		},
		{
			name:    "truncated dynamic large",
			src:     largeSrc[:len(largeSrc)/2],
			dstSize: len(largePayload),
			wantErr: true,
			wantEOF: true,
		},
		{
			name:    "truncated dynamic header",
			src:     dynVecSrc[:10],
			dstSize: len(dynVecPayload),
			wantErr: true,
			wantEOF: true,
		},
	}

	// check runs one decode into a freshly allocated destination and
	// reports any deviation from the precomputed expectation. It must stay
	// t.Fatal-free: it runs on non-test goroutines.
	check := func(c *inflateCase) error {
		dst := make([]byte, c.dstSize)
		consumed, err := inflatePackZlibGo(c.src, dst)
		if c.wantErr {
			if err == nil {
				return fmt.Errorf("%s: malformed stream accepted (consumed=%d)", c.name, consumed)
			}
			if got := errors.Is(err, io.ErrUnexpectedEOF); got != c.wantEOF {
				return fmt.Errorf("%s: error %v has EOF class %v, want %v", c.name, err, got, c.wantEOF)
			}
			if consumed != 0 {
				return fmt.Errorf("%s: error returned consumed=%d, want 0", c.name, consumed)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: decode: %v", c.name, err)
		}
		if consumed != c.wantConsumed {
			return fmt.Errorf("%s: consumed=%d, want %d", c.name, consumed, c.wantConsumed)
		}
		if !bytes.Equal(dst, c.want) {
			return fmt.Errorf("%s: output mismatch", c.name)
		}
		return nil
	}

	// Validate every fixture once sequentially so a broken case reports
	// itself directly instead of as a storm of goroutine failures.
	for i := range cases {
		if err := check(&cases[i]); err != nil {
			t.Fatalf("sequential fixture check: %v", err)
		}
	}

	workers := min(max(runtime.GOMAXPROCS(0)*4, 8), 32)
	iterations := 200
	if testing.Short() {
		iterations = 50
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Offsetting the rotation by the goroutine index staggers the
			// case mix across workers, so concurrent Put/Get pairs hand
			// fixed-warmed inflaters to dynamic decodes and vice versa.
			for i := 0; i < iterations; i++ {
				if err := check(&cases[(g+i)%len(cases)]); err != nil {
					errs <- fmt.Errorf("goroutine %d iteration %d: %w", g, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
