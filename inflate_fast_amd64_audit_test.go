//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"testing"
)

// These tests drive the inflateHuffmanFastAMD64 kernel through paths that
// zlib- or compress/flate-generated fixtures cannot reach: an encoder only
// emits the matches it happens to find, and its dynamic tables essentially
// never assign litlen codes longer than litlenTableBits or offset codes
// longer than offsetTableBits. Each test hand-builds the exact bit stream
// (or decode table) it needs and verifies the kernel against oracles that
// are independent of the Go reference decoder wherever possible. They
// mirror the arm64 audit suite in inflate_fast_arm64_audit_test.go; the
// bit-stream builders are shared in inflate_huffman_audit_util_test.go.

// amd64KernelWriteSlack bounds how far past its final output position the
// kernel may scribble inside the guaranteed deflateFastOutputMargin. Every
// store through the output pointer in inflate_fast_amd64.s is one of the
// following (the amd64 loop structure differs from arm64: there is no
// unconditional 40-byte short-match block and no distance-doubling
// copy_grow; offsets >= 8 always take the 40-byte-block loop):
//
//   - Literal stores (literal_one, literal_two, the third-literal block,
//     subtable_literal): single MOVB writes at the output cursor. Exact,
//     no overshoot.
//   - copy_wide (offset >= 8, any length): unconditional 40-byte blocks
//     (five 8-byte MOVQ pairs), advancing 40 per iteration until the
//     cursor reaches the match end. The last block starts less than 40
//     bytes before the end, so the overshoot is 40 - (length mod 40)
//     bytes, at most 39 when length = 1 (mod 40) (e.g. length 41).
//   - copy_small_offset (2 <= offset <= 7): 8-byte stores at stride
//     offset, two per iteration with the bound checked after each pair.
//     The final store begins less than offset bytes past the match end,
//     so the overshoot is at most offset+7 = 14 bytes (offset 7,
//     length 15).
//   - copy_rle (offset == 1): 32-byte splat blocks (four 8-byte MOVQ),
//     advancing 32 per iteration. Overshoot 32 - (length mod 32), at
//     most 31 when length = 1 (mod 32) (e.g. length 33).
//
// The maximum is copy_wide's 39 bytes = deflateFastMatchCopy-1 (the block
// width is the same 40 bytes the Go fast loop copies unconditionally).
// Any write at or beyond out+amd64KernelWriteSlack is therefore a kernel
// bug. This bound is tight: distance 8 with length 41 writes the byte at
// out+38, which the negative-check procedure exploits.
const amd64KernelWriteSlack = deflateFastMatchCopy - 1

// TestInflateHuffmanFastAMD64CopySweep pins the match-copy dispatch in
// inflate_fast_amd64.s across every regime boundary: the copy_wide
// 40-byte-block loop for offset >= 8 (which, unlike arm64, also handles
// short matches), the copy_rle splat for offset 1, and the
// copy_small_offset stride-offset loop for offsets 2..7. Every dense
// (distance, length) pair in 1..40 x 3..258 plus sparse larger distances
// runs against an independent LZ77 oracle, and a 0xC3 canary proves the
// kernel never writes at or past out+amd64KernelWriteSlack. Encoded
// fixtures cannot pin these paths because an encoder cannot be steered to
// a chosen (distance, length) pair.
func TestInflateHuffmanFastAMD64CopySweep(t *testing.T) {
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
		src, want := amd64FixedMatchBlock(distance, length, 40)
		d, r := amd64PrepareHuffmanBlock(t, src, 1)

		reference := amd64RunHuffmanGo(d, r, len(want)+400)
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
		kernel, statuses := amd64RunHuffmanKernelInto(t, d, r, dst)
		if kernel.err != nil {
			t.Fatalf("dist=%d len=%d kernel err: %v", distance, length, kernel.err)
		}
		if kernel.out != len(want) || !bytes.Equal(kernel.dst[:kernel.out], want) {
			t.Fatalf("dist=%d len=%d kernel output mismatch: out=%d want=%d",
				distance, length, kernel.out, len(want))
		}
		amd64CompareHuffmanResults(t, kernel, reference)
		amd64RequireStatuses(t, statuses, inflateFastBlockDone)
		for i := len(want) + amd64KernelWriteSlack; i < len(dst); i++ {
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

// TestInflateHuffmanFastAMD64DeepTables pins the subtable-resolution paths
// at litlen_exception and offset_exception in inflate_fast_amd64.s: 12-bit
// litlen codes for a literal, both match lengths, and end of block, plus
// 9-bit and 15-bit offset codes whose 13 extra bits form the worst-case
// bit cost against the refill thresholds — including the DX<=30
// direct-offset refill and, via the literal-heavy iterations near the end
// of the stream, the DX<=37 offset_exception refill. The stream is
// verified against compress/flate as an independent oracle before the
// kernel is compared with the Go reference. zlib-generated fixtures cannot
// pin these paths: their dynamic tables stay within litlenTableBits and
// offsetTableBits, so no encoded corpus reaches a subtable.
func TestInflateHuffmanFastAMD64DeepTables(t *testing.T) {
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
	// These pin the subtable decode itself but never the DX<=37
	// offset_exception refill: every iteration-ending path refills to
	// 56..63 bits, and a lone 12-bit length code leaves DX >= 44 when the
	// offset subtable pointer is found.
	for i := 0; i < 200; i++ {
		s.match(3, 16385+i*37)
	}
	// Force the offset_exception refill (the DX <= 37 branch in
	// inflate_fast_amd64.s): two direct 10-bit literals and a 12-bit
	// length code consume 32 bits inside one iteration, so even a full
	// 63-bit refill leaves DX = 31 <= 37 when the 15-bit offset code's
	// subtable pointer is recognized. The kernel must refill before
	// consuming the 15 code + 13 extra bits (28 more); skipping or
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
	d, r := amd64PrepareHuffmanBlock(t, src, 2)
	if d.litlenBits != litlenTableBits || d.offsetBits != offsetTableBits {
		t.Fatalf("tables not subtable-deep: litlenBits=%d offsetBits=%d",
			d.litlenBits, d.offsetBits)
	}

	dstLen := len(s.want) + deflateFastOutputMargin + 1
	reference := amd64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil {
		t.Fatalf("decodeHuffmanGo: %v", reference.err)
	}
	if !bytes.Equal(reference.dst[:reference.out], s.want) {
		t.Fatal("reference output does not match independent oracles")
	}

	kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
	amd64CompareHuffmanResults(t, kernel, reference)
	amd64RequireStatuses(t, statuses, inflateFastBlockDone)
}

// TestInflateHuffmanFastAMD64DeepTablesSubtableEOB pins the end-of-block
// exit through the litlen subtable (litlen_exception resolving a 12-bit
// EOB code: the TESTL $0x2000 branch after the subtable load) with no
// preceding match, so block termination itself is the only exceptional
// path exercised.
func TestInflateHuffmanFastAMD64DeepTablesSubtableEOB(t *testing.T) {
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
	d, r := amd64PrepareHuffmanBlock(t, src, 2)
	dstLen := len(s.want) + deflateFastOutputMargin + 1

	reference := amd64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], s.want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
	amd64CompareHuffmanResults(t, kernel, reference)
	amd64RequireStatuses(t, statuses, inflateFastBlockDone)
}

// TestInflateHuffmanFastAMD64BadDataSites pins every branch to
// return_bad_data in inflate_fast_amd64.s. Reading the kernel, there are
// exactly five detection sites:
//
//  1. have_offset: the distance-beyond-history check (CMPQ CX, DI;
//     JB return_bad_data), pinned at out=0 and at out=2.
//  2. litlen_exception, root: an exceptional litlen root entry that is
//     neither end-of-block nor a subtable pointer (TESTL $0x4000;
//     JZ return_bad_data), pinned via the reserved fixed-table litlen
//     symbols 286 and 287.
//  3. litlen_exception, subtable: a subtable-resolved litlen entry with
//     huffExceptional set and neither literal nor end-of-block (TESTL
//     $0x8000; JNZ return_bad_data after the subtable load).
//  4. offset_exception, root: an exceptional offset root entry without a
//     subtable pointer (TESTL $0x4000; JZ return_bad_data), pinned via
//     the reserved distance symbols 30 and 31.
//  5. offset_exception, subtable: a subtable-resolved offset entry with
//     huffExceptional set (TESTL $0x8000; JNZ return_bad_data).
//
// The arm64 kernel has the same five site classes (TBZ/TBNZ branches to
// return_bad_data in inflate_fast_arm64.s), so there is no reachability
// difference between the architectures; the arm64 audit file pins sites 1
// and 4, and its companion tests pin site 2. Sites 3 and 5 are
// unreachable through loadFixedTables/loadDynamicTables — buildTable
// rejects incomplete codes except the root-level singleton and empty
// cases, and a complete code fills every subtable slot with a valid
// entry — so those two subtests hand-build decode tables (the same
// technique as TestInflateHuffmanFastAMD64SubtableDifferential) to reach
// the defensive checks. The destination is large enough that the kernel,
// not decodeHuffmanTail, must detect each site; the pure-Go
// malformed-stream tests only cover the tail path.
func TestInflateHuffmanFastAMD64BadDataSites(t *testing.T) {
	// fixedStream preparers build a fixed-Huffman block and load the
	// fixed tables; table preparers hand-build the decode tables and the
	// raw bit buffer directly.
	fixedStream := func(build func(w *deflateTestBits)) func(t *testing.T) (goInflater, deflateBits) {
		return func(t *testing.T) (goInflater, deflateBits) {
			t.Helper()
			var w deflateTestBits
			w.write(1, 1) // BFINAL
			w.write(1, 2) // fixed Huffman
			build(&w)
			src := append(w.bytes(), make([]byte, 40)...)
			return amd64PrepareHuffmanBlock(t, src, 1)
		}
	}

	tests := []struct {
		name    string
		prepare func(t *testing.T) (goInflater, deflateBits)
	}{
		{
			// Site 1: offset > out with out = 0; the match is the first
			// symbol.
			name: "match before any history",
			prepare: fixedStream(func(w *deflateTestBits) {
				w.writeFixedLength(3)
				w.writeFixedDistance(1)
			}),
		},
		{
			// Site 1: offset > out with small history: distance 5 after
			// 2 literals.
			name: "distance beyond history",
			prepare: fixedStream(func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				w.writeFixedLength(3)
				w.writeFixedDistance(5)
			}),
		},
		{
			// Site 2: the reserved litlen symbols 286 and 287 hold
			// huffInvalid root entries in the fixed table. Two literals
			// first prove the failure position is preserved.
			name: "reserved litlen symbol 286",
			prepare: fixedStream(func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				w.writeFixedLitlen(286)
			}),
		},
		{
			name: "reserved litlen symbol 287",
			prepare: fixedStream(func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				w.writeFixedLitlen(287)
			}),
		},
		{
			// Site 4: the reserved symbols must be rejected by entry
			// recognition alone: the preceding history spans the full
			// 32 KiB window, so a regression that maps symbol 30 to ANY
			// valid DEFLATE distance decodes a legal match, keeps going,
			// and diverges from the reference instead of failing through
			// the offset-beyond-history check at the same position.
			name: "reserved distance symbol 30",
			prepare: fixedStream(func(w *deflateTestBits) {
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
			}),
		},
		{
			name: "reserved distance symbol 31",
			prepare: fixedStream(func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen('B')
				for i := 0; i < 127; i++ {
					w.writeFixedLength(258)
					w.writeFixedDistance(1)
				}
				w.writeFixedLength(3)
				w.writeFixedOffset(31)
				w.write(0, 13)
			}),
		},
		{
			// Site 3: a litlen subtable slot holding huffInvalid,
			// unreachable through real table construction. The root
			// entry at index 0 chains to the subtable at entries 2..3
			// (start=2, subBits=1, mainBits=1); bit stream 0b10 selects
			// root index 0 then subtable index 3, the invalid slot.
			name: "invalid litlen subtable entry",
			prepare: func(t *testing.T) (goInflater, deflateBits) {
				t.Helper()
				var d goInflater
				d.litlenBits = 1
				d.offsetBits = 1
				d.litlen[0] = 2<<16 | huffExceptional | huffSubtable | 1<<8 | 1
				d.litlen[1] = huffLiteral | uint32('A')<<16 | 1<<8 | 1
				d.litlen[2] = huffExceptional | huffEndOfBlock | 1<<8 | 1
				d.litlen[3] = huffInvalid | 1<<8 | 1
				d.offset[0] = 1<<16 | 1<<8 | 1
				d.offset[1] = huffInvalid
				r := deflateBits{src: make([]byte, 40), buf: 0b10, nbits: 2}
				return d, r
			},
		},
		{
			// Site 5: an offset subtable slot holding huffInvalid,
			// likewise unreachable through real table construction. The
			// stream decodes literal 'A' (00), a length-3 code (01),
			// then offset root index 0, which chains to the subtable at
			// entries 2..3; the next bit (1) selects subtable index 3,
			// the invalid slot, with out=1 already produced.
			name: "invalid offset subtable entry",
			prepare: func(t *testing.T) (goInflater, deflateBits) {
				t.Helper()
				var d goInflater
				d.litlenBits = 2
				d.offsetBits = 1
				d.litlen[0] = huffLiteral | uint32('A')<<16 | 2<<8 | 2
				d.litlen[1] = 3<<16 | 2<<8 | 2
				d.litlen[2] = huffExceptional | huffEndOfBlock | 2<<8 | 2
				d.litlen[3] = huffInvalid | 2<<8 | 2
				d.offset[0] = 2<<16 | huffExceptional | huffSubtable | 1<<8 | 1
				d.offset[1] = huffInvalid | 1<<8 | 1
				d.offset[2] = 1<<16 | 1<<8 | 1
				d.offset[3] = huffInvalid | 1<<8 | 1
				r := deflateBits{src: make([]byte, 40), buf: 0b100100, nbits: 6}
				return d, r
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, r := tt.prepare(t)
			// Large enough that the kernel, not the tail, must detect
			// each site, with room for the 32 KiB history the reserved
			// cases decode first.
			const dstLen = 32768 + 2*deflateFastOutputMargin

			reference := amd64RunHuffmanGo(d, r, dstLen)
			if !errors.Is(reference.err, errDeflateBadData) {
				t.Fatalf("reference err = %v, want errDeflateBadData", reference.err)
			}
			kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
			if !errors.Is(kernel.err, reference.err) {
				t.Fatalf("error mismatch: kernel=%v reference=%v", kernel.err, reference.err)
			}
			if kernel.out != reference.out {
				t.Fatalf("output position mismatch: kernel=%d reference=%d",
					kernel.out, reference.out)
			}
			if !bytes.Equal(kernel.dst[:kernel.out], reference.dst[:reference.out]) {
				t.Fatal("output mismatch")
			}
			amd64RequireStatuses(t, statuses, inflateFastBadData)
		})
	}
}
