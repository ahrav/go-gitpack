//go:build arm64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"testing"
)

// These tests drive the inflateHuffmanFastArm64 kernel through paths that
// zlib- or compress/flate-generated fixtures cannot reach: an encoder only
// emits the matches it happens to find, and its dynamic tables essentially
// never assign litlen codes longer than litlenTableBits or offset codes
// longer than offsetTableBits. Each test hand-builds the exact bit stream
// it needs and verifies the kernel against oracles that are independent of
// the Go reference decoder wherever possible.

// arm64KernelWriteSlack bounds how far past its final output position the
// kernel may scribble inside the guaranteed deflateFastOutputMargin. The
// worst offender is the unconditional 40-byte short-match block, which
// overshoots a length-3 match by deflateFastMatchCopy-3 = 37 bytes; the
// copy_rle and copy_wide 32-byte loops overshoot by at most 31, and the
// copy_grow/copy_finish 8-byte chunks by at most 7. Any write at or beyond
// out+arm64KernelWriteSlack is therefore a kernel bug.
const arm64KernelWriteSlack = deflateFastMatchCopy

// TestInflateHuffmanFastArm64CopySweep pins the match-copy dispatch in
// inflate_fast_arm64.s across every regime boundary: the unconditional
// 40-byte block for offset >= 8 and length <= deflateFastMatchCopy, the
// copy_rle splat for offset 1, the copy_grow distance-doubling rounds for
// offsets 2..inflateFastWideCopyDistance-1, and the copy_wide 32-byte
// LDP/STP loop for read distances >= inflateFastWideCopyDistance. Every
// dense (distance, length) pair in 1..40 x 3..258 plus sparse larger
// distances runs against an independent LZ77 oracle, and a 0xC3 canary
// proves the kernel never writes at or past out+arm64KernelWriteSlack.
// Encoded fixtures cannot pin these paths because an encoder cannot be
// steered to a chosen (distance, length) pair.
func TestInflateHuffmanFastArm64CopySweep(t *testing.T) {
	denseDists := make([]int, 0, 40)
	for d := 1; d <= 40; d++ {
		denseDists = append(denseDists, d)
	}
	sparseDists := []int{
		48, 63, 64, 65, 127, 128, 129, 255, 256, 257, 258,
		511, 512, 1024, 4095, 4096, 16384, 24577, 32767, 32768,
	}
	sparseLens := []int{3, 4, 7, 8, 9, 31, 32, 33, 39, 40, 41, 63, 64, 65, 254, 255, 257, 258}

	run := func(t *testing.T, distance, length int) {
		src, want := arm64FixedMatchBlock(distance, length, 40)
		d, r := arm64PrepareHuffmanBlock(t, src, 1)

		reference := arm64RunHuffmanGo(d, r, len(want)+400)
		if reference.err != nil {
			t.Fatalf("dist=%d len=%d decodeHuffmanGo: %v", distance, length, reference.err)
		}
		if !bytes.Equal(reference.dst[:reference.out], want) {
			t.Fatalf("dist=%d len=%d reference output does not match LZ77 oracle",
				distance, length)
		}

		dst := make([]byte, len(want)+400)
		for i := range dst {
			dst[i] = 0xC3
		}
		kernel, statuses := arm64RunHuffmanKernelInto(t, d, r, dst)
		if kernel.err != nil {
			t.Fatalf("dist=%d len=%d kernel err: %v", distance, length, kernel.err)
		}
		if kernel.out != len(want) || !bytes.Equal(kernel.dst[:kernel.out], want) {
			t.Fatalf("dist=%d len=%d kernel output mismatch: out=%d want=%d",
				distance, length, kernel.out, len(want))
		}
		arm64CompareHuffmanResults(t, kernel, reference)
		arm64RequireStatuses(t, statuses, inflateFastBlockDone)
		for i := len(want) + arm64KernelWriteSlack; i < len(dst); i++ {
			if dst[i] != 0xC3 {
				t.Fatalf("dist=%d len=%d wrote %#x at dst[%d], %d bytes past final output",
					distance, length, dst[i], i, i-len(want))
			}
		}
	}

	for _, distance := range denseDists {
		t.Run(fmt.Sprintf("dense/distance=%d", distance), func(t *testing.T) {
			for length := 3; length <= 258; length++ {
				run(t, distance, length)
			}
		})
	}
	for _, distance := range sparseDists {
		t.Run(fmt.Sprintf("sparse/distance=%d", distance), func(t *testing.T) {
			for _, length := range sparseLens {
				run(t, distance, length)
			}
		})
	}
}

// TestInflateHuffmanFastArm64DeepTables pins the subtable-resolution paths
// at litlen_exception and offset_exception in inflate_fast_arm64.s: 12-bit
// litlen codes for a literal, both match lengths, and end of block, plus
// 9-bit and 15-bit offset codes whose 13 extra bits form the worst-case
// bit cost against the refill thresholds — including, via the
// literal-heavy iterations near the end of the stream, the
// inflateFastOffsetSubRefillThreshold refill on the offset_exception
// path. The stream is verified against compress/flate as an independent
// oracle before the kernel is compared with the Go reference.
// zlib-generated fixtures cannot pin these paths: their dynamic tables
// stay within litlenTableBits and offsetTableBits, so no encoded corpus
// reaches a subtable.
func TestInflateHuffmanFastArm64DeepTables(t *testing.T) {
	s := newDeepStream(t)

	// History for the deepest back-references. The sequence is
	// deterministic but non-periodic (an LCG over the ten literal
	// symbols), so a kernel regression that shifts any decoded distance
	// selects visibly different bytes; a periodic fill would mask every
	// error that is a multiple of the period.
	lcg := uint32(0x9e3779b9)
	nextLit := func() byte {
		lcg = lcg*1664525 + 1013904223
		return byte('A' + (lcg>>24)%10)
	}
	for i := 0; i < 33000; i++ {
		s.literal(nextLit())
	}
	s.literal('Z')      // 12-bit subtable literal
	s.match(258, 32768) // subtable length + 15-bit offset code + 13 extra
	s.match(3, 20000)   // subtable length 3 + symbol 28 + 13 extra
	// Fresh non-periodic bytes so the small-distance matches below read
	// distinguishable history even after the long copies above.
	for i := 0; i < 64; i++ {
		s.literal(nextLit())
	}
	s.match(3, 4)    // 1-bit direct offset code
	s.match(258, 17) // 9-bit offset subtable code + 3 extra
	s.match(258, 1)  // RLE copy through deep tables
	s.literal('A')
	s.literal('B')
	s.literal('Z')
	// A burst of worst-case-bit-cost iterations: a 12-bit length code
	// followed by a 15-bit offset code with 13 extra bits, repeatedly.
	// These pin the subtable decode itself but never the
	// inflateFastOffsetSubRefillThreshold refill: every iteration-ending
	// path refills to 56..63 bits, and a lone 12-bit length code leaves
	// >= 44 buffered bits when the offset subtable pointer is found.
	for i := 0; i < 200; i++ {
		s.match(3, 16385+i*37)
	}
	// Force the offset_exception refill (the <= 37-bit branch in
	// inflate_fast_arm64.s): two direct 10-bit literals and a 12-bit
	// length code consume 32 bits inside one iteration, so even a full
	// 63-bit refill leaves 31 <= 37 buffered bits when the 15-bit offset
	// code's subtable pointer is recognized. The kernel must refill
	// before consuming the 15 code + 13 extra bits (28 more); skipping or
	// corrupting that refill decodes a garbage distance and diverges from
	// the oracle. Repeated with both 13-extra-bit offset symbols.
	s.literal('J')
	s.literal('J')
	s.match(3, 20000) // symbol 28 + 13 extra
	s.literal('J')
	s.literal('J')
	s.match(3, 32000) // symbol 29 + 13 extra
	s.endOfBlock()

	// Independent oracle: compress/flate over the raw block.
	flateOut, err := io.ReadAll(flate.NewReader(bytes.NewReader(s.w.bytes())))
	if err != nil {
		t.Fatalf("compress/flate rejects hand-built stream: %v", err)
	}
	if !bytes.Equal(flateOut, s.want) {
		t.Fatal("compress/flate output does not match LZ77 model; stream builder is wrong")
	}

	src := append(s.w.bytes(), make([]byte, 40)...)
	d, r := arm64PrepareHuffmanBlock(t, src, 2)
	if d.litlenBits != litlenTableBits || d.offsetBits != offsetTableBits {
		t.Fatalf("tables not subtable-deep: litlenBits=%d offsetBits=%d",
			d.litlenBits, d.offsetBits)
	}

	dstLen := len(s.want) + deflateFastOutputMargin + 1
	reference := arm64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil {
		t.Fatalf("decodeHuffmanGo: %v", reference.err)
	}
	if !bytes.Equal(reference.dst[:reference.out], s.want) {
		t.Fatal("reference output does not match independent oracles")
	}

	kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
	arm64CompareHuffmanResults(t, kernel, reference)
	arm64RequireStatuses(t, statuses, inflateFastBlockDone)
}

// TestInflateHuffmanFastArm64DeepTablesSubtableEOB pins the end-of-block
// exit through the litlen subtable (litlen_exception resolving a 12-bit
// EOB code) with no preceding match, so block termination itself is the
// only exceptional path exercised.
func TestInflateHuffmanFastArm64DeepTablesSubtableEOB(t *testing.T) {
	s := newDeepStream(t)
	for i := 0; i < 100; i++ {
		s.literal(byte('A' + i%2))
	}
	s.endOfBlock()

	flateOut, err := io.ReadAll(flate.NewReader(bytes.NewReader(s.w.bytes())))
	if err != nil || !bytes.Equal(flateOut, s.want) {
		t.Fatalf("compress/flate oracle disagrees: err=%v", err)
	}

	src := append(s.w.bytes(), make([]byte, 40)...)
	d, r := arm64PrepareHuffmanBlock(t, src, 2)
	dstLen := len(s.want) + deflateFastOutputMargin + 1

	reference := arm64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], s.want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
	arm64CompareHuffmanResults(t, kernel, reference)
	arm64RequireStatuses(t, statuses, inflateFastBlockDone)
}

// TestInflateHuffmanFastArm64BadDataSites pins the kernel's own bad-data
// detection sites: the offset-beyond-history check (the BCC branch to
// return_bad_data after have_offset) at out=0 and at out=2, and the
// reserved distance symbols 30 and 31 whose table entries are invalid
// (the exceptional-entry branches in offset_exception). The destination
// is large enough that the kernel, not decodeHuffmanTail, must detect
// each site; the pure-Go malformed-stream tests only cover the tail path.
func TestInflateHuffmanFastArm64BadDataSites(t *testing.T) {
	tests := []struct {
		name  string
		build func(w *deflateTestBits)
	}{
		{
			// offset > out with out = 0: the match is the first symbol.
			name: "match before any history",
			build: func(w *deflateTestBits) {
				w.writeFixedLength(3)
				w.writeFixedDistance(1)
			},
		},
		{
			// offset > out with small history: distance 5 after 2 literals.
			name: "distance beyond history",
			build: func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				w.writeFixedLength(3)
				w.writeFixedDistance(5)
			},
		},
		{
			// The reserved symbols must be rejected by entry recognition
			// alone: the preceding history spans the full 32 KiB window,
			// so a regression that maps symbol 30 to ANY valid DEFLATE
			// distance decodes a legal match, keeps going, and diverges
			// from the reference instead of failing through the
			// offset-beyond-history check at the same position.
			name: "reserved distance symbol 30",
			build: func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				for i := 0; i < 127; i++ {
					w.writeFixedLength(258)
					w.writeFixedDistance(1)
				}
				// History is now exactly 2 + 127*258 = 32768 bytes.
				w.writeFixedLength(3)
				w.writeFixedOffset(30)
				w.write(0, 13)
			},
		},
		{
			name: "reserved distance symbol 31",
			build: func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				for i := 0; i < 127; i++ {
					w.writeFixedLength(258)
					w.writeFixedDistance(1)
				}
				w.writeFixedLength(3)
				w.writeFixedOffset(31)
				w.write(0, 13)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w deflateTestBits
			w.write(1, 1) // BFINAL
			w.write(1, 2) // fixed Huffman
			tt.build(&w)
			src := append(w.bytes(), make([]byte, 40)...)
			d, r := arm64PrepareHuffmanBlock(t, src, 1)
			// Large enough that the kernel, not the tail, must detect
			// each site, with room for the 32 KiB history the reserved
			// cases decode first.
			const dstLen = 32768 + 2*deflateFastOutputMargin

			reference := arm64RunHuffmanGo(d, r, dstLen)
			if reference.err != errDeflateBadData {
				t.Fatalf("reference err = %v, want errDeflateBadData", reference.err)
			}
			kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
			if kernel.err != reference.err {
				t.Fatalf("error mismatch: kernel=%v reference=%v", kernel.err, reference.err)
			}
			if kernel.out != reference.out {
				t.Fatalf("output position mismatch: kernel=%d reference=%d",
					kernel.out, reference.out)
			}
			if !bytes.Equal(kernel.dst[:kernel.out], reference.dst[:reference.out]) {
				t.Fatal("output mismatch")
			}
			arm64RequireStatuses(t, statuses, inflateFastBadData)
		})
	}
}
