//go:build arm64 && !purego && (!cgo || !gitpack_libdeflate)

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

// arm64CanonicalCodes assigns canonical Huffman codes for the given code
// lengths per RFC 1951 section 3.2.2. Entry i holds {codeword, length};
// symbols with a zero length keep a zero entry.
func arm64CanonicalCodes(lens []int) [][2]int {
	var blCount [maxCodeLen + 1]int
	for _, n := range lens {
		if n > 0 {
			blCount[n]++
		}
	}
	var nextCode [maxCodeLen + 1]int
	code := 0
	for bits := 1; bits <= maxCodeLen; bits++ {
		code = (code + blCount[bits-1]) << 1
		nextCode[bits] = code
	}
	out := make([][2]int, len(lens))
	for sym, n := range lens {
		if n == 0 {
			continue
		}
		out[sym] = [2]int{nextCode[n], n}
		nextCode[n]++
	}
	return out
}

// writeCode emits one canonical {codeword, length} pair in DEFLATE bit
// order (codewords are packed most-significant bit first).
func (w *deflateTestBits) writeCode(c [2]int) {
	if c[1] == 0 {
		panic("emitting symbol with zero code length")
	}
	w.write(uint64(reverseCode(c[0], c[1])), uint(c[1]))
}

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

// arm64DeepStream hand-builds one dynamic-Huffman block whose litlen codes
// exceed litlenTableBits and whose offset codes exceed offsetTableBits, so
// decoding must resolve subtable entries. It tracks the expected output in
// an LZ77 model that is independent of the decoders under test.
type arm64DeepStream struct {
	w        deflateTestBits
	want     []byte
	litCodes [][2]int
	offCodes [][2]int
}

func newArm64DeepStream(t *testing.T) *arm64DeepStream {
	t.Helper()

	litLens := make([]int, 286)
	for i := 0; i < 10; i++ {
		litLens[65+i] = 1 + i // 'A'..'J' at 1..10 bits
	}
	litLens[90] = 12  // 'Z': literal through the litlen subtable
	litLens[256] = 12 // end of block through the litlen subtable
	litLens[257] = 12 // length 3 through the litlen subtable
	litLens[285] = 12 // length 258 through the litlen subtable
	// Kraft: sum(2^-1..2^-10) + 4*2^-12 = (1 - 2^-10) + 2^-10 = 1.

	offLens := make([]int, 30)
	offLens[3] = 1 // distance 4, direct
	offLens[0] = 2 // distance 1, direct (RLE)
	offLens[1] = 3
	offLens[2] = 4
	offLens[4] = 5
	offLens[5] = 6
	offLens[6] = 7
	offLens[7] = 8
	offLens[8] = 9 // distances 33..40 (3 extra bits): first subtable depth
	offLens[9] = 10
	offLens[10] = 11
	offLens[11] = 12
	offLens[12] = 13
	offLens[13] = 14
	offLens[28] = 15 // distances 16385..24576, 13 extra bits: worst case
	offLens[29] = 15 // distances 24577..32768, 13 extra bits: worst case
	// Kraft in 2^-15 units:
	// 16384+8192+4096+2048+1024+512+256+128+64+32+16+8+4+2+1+1 = 32768.

	preLens := make([]int, 19)
	for s := 0; s <= 15; s++ {
		preLens[s] = 5
	}
	preLens[16] = 2
	preLens[17] = 3
	preLens[18] = 3
	// Kraft: 16*2^-5 + 2^-2 + 2*2^-3 = 1.

	s := &arm64DeepStream{
		litCodes: arm64CanonicalCodes(litLens),
		offCodes: arm64CanonicalCodes(offLens),
	}
	s.w.write(1, 1)  // BFINAL
	s.w.write(2, 2)  // BTYPE = dynamic
	s.w.write(29, 5) // HLIT: 286 litlen codes
	s.w.write(29, 5) // HDIST: 30 offset codes
	s.w.write(15, 4) // HCLEN: 19 precode lengths
	for _, sym := range precodeOrder {
		s.w.write(uint64(preLens[sym]), 3)
	}
	preCodes := arm64CanonicalCodes(preLens)
	for _, n := range litLens {
		s.w.writeCode(preCodes[n])
	}
	for _, n := range offLens {
		s.w.writeCode(preCodes[n])
	}
	return s
}

func (s *arm64DeepStream) literal(b byte) {
	s.w.writeCode(s.litCodes[b])
	s.want = append(s.want, b)
}

func (s *arm64DeepStream) match(length, dist int) {
	switch length {
	case 3:
		s.w.writeCode(s.litCodes[257])
	case 258:
		s.w.writeCode(s.litCodes[285])
	default:
		panic("deep stream supports lengths 3 and 258 only")
	}
	switch {
	case dist == 1:
		s.w.writeCode(s.offCodes[0])
	case dist == 4:
		s.w.writeCode(s.offCodes[3])
	case dist >= 33 && dist <= 40:
		s.w.writeCode(s.offCodes[8])
		s.w.write(uint64(dist-33), 3)
	case dist >= 16385 && dist <= 24576:
		s.w.writeCode(s.offCodes[28])
		s.w.write(uint64(dist-16385), 13)
	case dist >= 24577 && dist <= 32768:
		s.w.writeCode(s.offCodes[29])
		s.w.write(uint64(dist-24577), 13)
	default:
		panic("unsupported deep-stream distance")
	}
	if dist > len(s.want) {
		panic("match beyond history")
	}
	for range length {
		s.want = append(s.want, s.want[len(s.want)-dist])
	}
}

func (s *arm64DeepStream) endOfBlock() { s.w.writeCode(s.litCodes[256]) }

// TestInflateHuffmanFastArm64DeepTables pins the subtable-resolution paths
// at litlen_exception and offset_exception in inflate_fast_arm64.s: 12-bit
// litlen codes for a literal, both match lengths, and end of block, plus
// 9-bit and 15-bit offset codes whose 13 extra bits form the worst-case
// bit cost against the refill thresholds. The stream is verified against
// compress/flate as an independent oracle before the kernel is compared
// with the Go reference. zlib-generated fixtures cannot pin these paths:
// their dynamic tables stay within litlenTableBits and offsetTableBits, so
// no encoded corpus reaches a subtable.
func TestInflateHuffmanFastArm64DeepTables(t *testing.T) {
	s := newArm64DeepStream(t)

	// History for the deepest back-references, using 1-3 bit literals.
	for i := 0; i < 33000; i++ {
		s.literal(byte('A' + i%3))
	}
	s.literal('Z')      // 12-bit subtable literal
	s.match(258, 32768) // subtable length + 15-bit offset code + 13 extra
	s.match(3, 20000)   // subtable length 3 + symbol 28 + 13 extra
	s.match(258, 1)     // RLE copy through deep tables
	s.match(3, 4)       // 1-bit direct offset code
	s.match(258, 33)    // 9-bit offset subtable code + 3 extra
	s.literal('A')
	s.literal('B')
	s.literal('Z')
	// A burst of worst-case-bit-cost iterations: a 12-bit length code
	// followed by a 15-bit offset code with 13 extra bits, repeatedly,
	// to organically stress the subtable refill-threshold decision.
	for i := 0; i < 200; i++ {
		s.match(3, 16385+i*37)
	}
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
	s := newArm64DeepStream(t)
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
			name: "reserved distance symbol 30",
			build: func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLength(3)
				w.writeFixedOffset(30)
				w.write(0, 10)
			},
		},
		{
			name: "reserved distance symbol 31",
			build: func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLength(3)
				w.writeFixedOffset(31)
				w.write(0, 10)
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
			const dstLen = 2 * deflateFastOutputMargin

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
