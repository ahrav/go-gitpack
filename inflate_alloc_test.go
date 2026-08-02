// Allocation-count gates are non-contractual under the race detector: the
// race runtime's own allocation behavior can change across Go releases, and
// a GC-driven sync.Pool eviction mid-measurement would report a spurious
// fractional count. The stdlib skips malloc-count tests under race for the
// same reason; the gate still runs on every non-race CI job.
//
//go:build !race

package objstore

import (
	"bytes"
	"compress/zlib"
	"testing"
)

// TestInflatePackZlibGoAllocationFree gates the decoder's zero-allocation
// claim in CI. The inflate benchmarks report allocs/op, but CI never runs
// them, so a regression that adds a hot-path allocation (an escaping scratch
// buffer, a pool miss, interface boxing) would land silently. Each subtest
// asserts exactly zero allocations per warm decode.
//
// The goInflaterPool is warmed by an explicit decode before measuring: a
// cold sync.Pool Get allocates its New value, which is expected and not a
// hot-path regression. The measured closure is kept minimal (one decode
// call) because testing.AllocsPerRun attributes every allocation inside the
// closure to the decode.
func TestInflatePackZlibGoAllocationFree(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		// wantDynamic pins the fixture to a dynamic-Huffman first block so
		// the table-building path (loadDynamicTables) is inside the
		// measured region.
		wantDynamic bool
	}{
		{
			// Representative highly compressible pack payload; mirrors the
			// "repeated" pattern in inflate_bench_test.go.
			name:    "repeated-1MiB",
			payload: bytes.Repeat([]byte("package objstore\n"), (1<<20+16)/17)[:1<<20],
		},
		{
			// Text corpus that compresses to dynamic-Huffman blocks,
			// exercising loadDynamicTables plus the fast Huffman loop.
			name:        "dynamic-huffman-text-1MiB",
			payload:     makeBenchmarkText(1 << 20),
			wantDynamic: true,
		},
		{
			// A destination this small is below deflateFastOutputMargin, so
			// decodeHuffman routes straight to the portable tail loop on
			// every platform, including arm64 with the assembly kernel.
			name:    "small-tail-path",
			payload: []byte("tiny"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := encodeZlib(t, tt.payload, zlib.DefaultCompression)
			if tt.wantDynamic {
				r := deflateBits{src: src[2:]}
				header, ok := r.read(3)
				if !ok || (header>>1)&3 != 2 {
					t.Fatal("fixture does not begin with a dynamic-Huffman block")
				}
			}

			// dst is allocated once outside the measured region.
			dst := make([]byte, len(tt.payload))

			// Warm the goInflaterPool and validate the fixture decodes
			// correctly before measuring.
			if _, err := inflatePackZlibGo(src, dst); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(dst, tt.payload) {
				t.Fatal("warmup decode produced wrong output")
			}

			avg := testing.AllocsPerRun(10, func() {
				if _, err := inflatePackZlibGo(src, dst); err != nil {
					t.Fatal(err)
				}
			})
			if avg != 0 {
				t.Fatalf("inflatePackZlibGo allocated %v times per run, want 0", avg)
			}
		})
	}
}
