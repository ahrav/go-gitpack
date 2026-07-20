//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"unsafe"
)

func TestInflateHuffmanFastAMD64FixedDifferential(t *testing.T) {
	tests := []struct {
		length   int
		distance int
	}{
		{length: 3, distance: 1},
		{length: 7, distance: 2},
		{length: 8, distance: 7},
		{length: 9, distance: 8},
		{length: 39, distance: 9},
		{length: 40, distance: 31},
		{length: 41, distance: 32},
		{length: 258, distance: 15},
		{length: 258, distance: 16},
		{length: 63, distance: 17},
		{length: 64, distance: 17},
		{length: 258, distance: 17},
		{length: 258, distance: 39},
		{length: 258, distance: 40},
		{length: 257, distance: 257},
		{length: 258, distance: 32768},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("length=%d/distance=%d", tt.length, tt.distance)
		t.Run(name, func(t *testing.T) {
			src, want := amd64FixedMatchBlock(tt.distance, tt.length, 40)
			d, r := amd64PrepareHuffmanBlock(t, src, 1)
			dstLen := len(want) + deflateFastOutputMargin + 1

			reference := amd64RunHuffmanGo(d, r, dstLen)
			if reference.err != nil {
				t.Fatalf("decodeHuffmanGo: %v", reference.err)
			}
			if !bytes.Equal(reference.dst[:reference.out], want) {
				t.Fatal("reference output does not match fixture")
			}

			dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
			amd64CompareHuffmanResults(t, dispatch, reference)

			kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
			amd64CompareHuffmanResults(t, kernel, reference)
			amd64RequireStatuses(t, statuses, inflateFastBlockDone)
		})
	}
}

func TestInflateHuffmanFastAMD64DynamicDifferential(t *testing.T) {
	src, want := amd64DynamicMatchBlock(t)
	d, r := amd64PrepareHuffmanBlock(t, src, 2)
	dstLen := len(want) + deflateFastOutputMargin + 1

	reference := amd64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil {
		t.Fatalf("decodeHuffmanGo: %v", reference.err)
	}
	if !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatal("reference output does not match fixture")
	}

	dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
	amd64CompareHuffmanResults(t, dispatch, reference)

	kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
	amd64CompareHuffmanResults(t, kernel, reference)
	amd64RequireStatuses(t, statuses, inflateFastBlockDone)
}

func TestInflateHuffmanFastAMD64DispatchPaths(t *testing.T) {
	if inflateFastDisabled {
		t.Skip("GOGITPACK_NOASM_INFLATE is set; decodeHuffman never dispatches to assembly")
	}
	smallSrc, smallWant := amd64DynamicMatchBlock(t)
	normalSrc, normalWant := amd64FixedMatchBlock(40, 258, 40)

	tests := []struct {
		name         string
		src          []byte
		want         []byte
		blockType    uint64
		wantAssembly bool
	}{
		{
			name:      "small dynamic table uses Go",
			src:       smallSrc,
			want:      smallWant,
			blockType: 2,
		},
		{
			name:         "normal table uses assembly",
			src:          normalSrc,
			want:         normalWant,
			blockType:    1,
			wantAssembly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, r := amd64PrepareHuffmanBlock(t, tt.src, tt.blockType)
			if got := d.litlenBits >= inflateFastAMD64MinLitlenBits; got != tt.wantAssembly {
				t.Fatalf("litlenBits=%d selects assembly=%v, want %v",
					d.litlenBits, got, tt.wantAssembly)
			}
			dstLen := len(tt.want) + deflateFastOutputMargin + 1

			reference := amd64RunHuffmanGo(d, r, dstLen)
			if reference.err != nil ||
				!bytes.Equal(reference.dst[:reference.out], tt.want) {
				t.Fatalf("reference decode: out=%d err=%v",
					reference.out, reference.err)
			}

			dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
			amd64CompareHuffmanResults(t, dispatch, reference)

			kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
			amd64CompareHuffmanResults(t, kernel, reference)
			amd64RequireStatuses(t, statuses, inflateFastBlockDone)

			// Each fast loop leaves a distinct pattern in the permitted
			// output-margin overrun, making the selected path observable.
			dispatchMatchesGo := bytes.Equal(dispatch.dst, reference.dst)
			dispatchMatchesAssembly := bytes.Equal(dispatch.dst, kernel.dst)
			if tt.wantAssembly {
				if dispatchMatchesGo || !dispatchMatchesAssembly {
					t.Fatalf("dispatch buffer matches Go=%v assembly=%v, want false/true",
						dispatchMatchesGo, dispatchMatchesAssembly)
				}
			} else if !dispatchMatchesGo || dispatchMatchesAssembly {
				t.Fatalf("dispatch buffer matches Go=%v assembly=%v, want true/false",
					dispatchMatchesGo, dispatchMatchesAssembly)
			}
		})
	}
}

func amd64DynamicMatchBlock(t *testing.T) ([]byte, []byte) {
	t.Helper()

	encoded, err := hex.DecodeString(dynamicHuffmanVectorHex)
	if err != nil {
		t.Fatal(err)
	}
	src := append([]byte(nil), encoded[2:len(encoded)-4]...)
	src = append(src, make([]byte, 40)...)
	want := bytes.Repeat([]byte("abcabc"), 1000)
	return src, want
}

func TestInflateHuffmanFastAMD64SubtableDifferential(t *testing.T) {
	t.Run("litlen", func(t *testing.T) {
		var d goInflater
		d.litlenBits = 1
		d.offsetBits = 1
		d.litlen[0] = 2<<16 | huffExceptional | huffSubtable | 1<<8 | 1
		d.litlen[1] = huffInvalid | 1<<8 | 1
		d.litlen[2] = huffLiteral | uint32('A')<<16 | 1<<8 | 1
		d.litlen[3] = huffExceptional | huffEndOfBlock | 1<<8 | 1

		r := deflateBits{
			src:   make([]byte, 40),
			buf:   0b1000,
			nbits: 4,
		}
		dstLen := deflateFastOutputMargin + 10
		reference := amd64RunHuffmanGo(d, r, dstLen)
		if reference.err != nil || string(reference.dst[:reference.out]) != "A" {
			t.Fatalf("reference decode: out=%q err=%v",
				reference.dst[:reference.out], reference.err)
		}

		kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
		amd64CompareHuffmanResults(t, kernel, reference)
		amd64RequireStatuses(t, statuses, inflateFastBlockDone)
	})

	t.Run("offset", func(t *testing.T) {
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

		r := deflateBits{
			src:   make([]byte, 40),
			buf:   0b10000100,
			nbits: 8,
		}
		dstLen := deflateFastOutputMargin + 10
		reference := amd64RunHuffmanGo(d, r, dstLen)
		if reference.err != nil || string(reference.dst[:reference.out]) != "AAAA" {
			t.Fatalf("reference decode: out=%q err=%v",
				reference.dst[:reference.out], reference.err)
		}

		kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
		amd64CompareHuffmanResults(t, kernel, reference)
		amd64RequireStatuses(t, statuses, inflateFastBlockDone)
	})
}

func TestInflateHuffmanFastAMD64DispatchLitlenCutoff(t *testing.T) {
	if inflateFastDisabled {
		t.Skip("GOGITPACK_NOASM_INFLATE is set; decodeHuffman never dispatches to assembly")
	}
	tests := []struct {
		name         string
		litlenBits   uint8
		wantAssembly bool
	}{
		{
			name:         "seven root bits uses assembly",
			litlenBits:   inflateFastAMD64MinLitlenBits,
			wantAssembly: true,
		},
		{
			name:       "six root bits uses Go",
			litlenBits: inflateFastAMD64MinLitlenBits - 1,
		},
	}

	// One stream serves both tables: the codes shorter than four bits are
	// identical. A 'Y' literal, a length-258 distance-1 match, then end
	// of block. The distance-1 match matters: its RLE copy overruns the
	// output margin differently per fast loop, exposing the taken path.
	var w deflateTestBits
	w.write(0, 1)                             // literal 'Y': code 0
	w.write(uint64(reverseCode(0b10, 2)), 2)  // symbol 285: length 258
	w.write(0, 1)                             // distance 1: code 0
	w.write(uint64(reverseCode(0b110, 3)), 3) // end of block
	src := append(w.bytes(), make([]byte, 40)...)
	want := bytes.Repeat([]byte{'Y'}, 259)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := amd64CutoffInflater(tt.litlenBits)
			if got := d.litlenBits >= inflateFastAMD64MinLitlenBits; got != tt.wantAssembly {
				t.Fatalf("litlenBits=%d selects assembly=%v, want %v",
					d.litlenBits, got, tt.wantAssembly)
			}
			r := deflateBits{src: src}
			dstLen := len(want) + deflateFastOutputMargin + 1

			reference := amd64RunHuffmanGo(d, r, dstLen)
			if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
				t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
			}

			dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
			amd64CompareHuffmanResults(t, dispatch, reference)

			kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
			amd64CompareHuffmanResults(t, kernel, reference)
			amd64RequireStatuses(t, statuses, inflateFastBlockDone)

			if bytes.Equal(reference.dst, kernel.dst) {
				t.Fatal("fixture cannot distinguish Go and assembly margin patterns")
			}
			dispatchMatchesGo := bytes.Equal(dispatch.dst, reference.dst)
			dispatchMatchesAssembly := bytes.Equal(dispatch.dst, kernel.dst)
			if tt.wantAssembly {
				if dispatchMatchesGo || !dispatchMatchesAssembly {
					t.Fatalf("dispatch buffer matches Go=%v assembly=%v, want false/true",
						dispatchMatchesGo, dispatchMatchesAssembly)
				}
			} else if !dispatchMatchesGo || dispatchMatchesAssembly {
				t.Fatalf("dispatch buffer matches Go=%v assembly=%v, want true/false",
					dispatchMatchesGo, dispatchMatchesAssembly)
			}
		})
	}
}

// amd64CutoffInflater hand-builds complete decode tables whose litlen root
// table is exactly litlenBits wide, mirroring buildTable's layout: each
// code's entry is its decode result plus codeLen*0x101, replicated from the
// bit-reversed codeword at stride 1<<codeLen. The canonical code assigns
// 'Y' a one-bit code, length 258 (symbol 285) a two-bit code, end-of-block
// a three-bit code, and fills the remaining codespace with one literal per
// length plus two at the longest so the Kraft sum is exact and every root
// index holds a valid entry.
func amd64CutoffInflater(litlenBits uint8) goInflater {
	var d goInflater
	d.litlenBits = litlenBits
	d.offsetBits = 1
	// Distance 1 as a one-bit singleton, packed like buildTable's
	// incomplete-singleton case.
	d.offset[0] = 1<<16 | 1<<8 | 1
	d.offset[1] = huffInvalid

	fill := func(code, codeLen int, result uint32) {
		entry := result + uint32(codeLen)*0x101
		for i := reverseCode(code, codeLen); i < 1<<litlenBits; i += 1 << codeLen {
			d.litlen[i] = entry
		}
	}
	fill(0b0, 1, huffLiteral|uint32('Y')<<16)
	fill(0b10, 2, 258<<16) // symbol 285: length 258, no extra bits
	fill(0b110, 3, huffExceptional|huffEndOfBlock)
	filler := uint32('a')
	for codeLen := 4; codeLen < int(litlenBits); codeLen++ {
		fill(1<<codeLen-2, codeLen, huffLiteral|filler<<16)
		filler++
	}
	fill(1<<litlenBits-2, int(litlenBits), huffLiteral|filler<<16)
	fill(1<<litlenBits-1, int(litlenBits), huffLiteral|(filler+1)<<16)
	return d
}

func TestInflateHuffmanFastAMD64FallbackDifferential(t *testing.T) {
	payload := []byte("fallback")

	t.Run("input margin", func(t *testing.T) {
		src := amd64FixedLiteralBlock(payload, 0)
		if len(src) > deflateFastInputMargin {
			t.Fatalf("fixture is %d bytes, want at most %d", len(src), deflateFastInputMargin)
		}
		src = append(src, make([]byte, deflateFastInputMargin-len(src))...)
		d, r := amd64PrepareHuffmanBlock(t, src, 1)
		dstLen := len(payload) + deflateFastOutputMargin + 1

		reference := amd64RunHuffmanGo(d, r, dstLen)
		// Anchor the fixture absolutely: a cross-comparison alone would
		// pass if all three decoders regressed identically.
		if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], payload) {
			t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
		}
		dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
		amd64CompareHuffmanResults(t, dispatch, reference)

		kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
		amd64CompareHuffmanResults(t, kernel, reference)
		amd64RequireStatuses(t, statuses, inflateFastTail)
	})

	t.Run("output margin", func(t *testing.T) {
		src := amd64FixedLiteralBlock(payload, 40)
		d, r := amd64PrepareHuffmanBlock(t, src, 1)

		reference := amd64RunHuffmanGo(d, r, deflateFastOutputMargin)
		if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], payload) {
			t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
		}
		dispatch := amd64RunHuffmanDispatch(d, r, deflateFastOutputMargin)
		amd64CompareHuffmanResults(t, dispatch, reference)

		kernel, statuses := amd64RunHuffmanKernel(t, d, r, deflateFastOutputMargin)
		amd64CompareHuffmanResults(t, kernel, reference)
		amd64RequireStatuses(t, statuses, inflateFastTail)
	})
}

func TestInflateHuffmanFastAMD64MarginBoundaries(t *testing.T) {
	if inflateFastDisabled {
		t.Skip("GOGITPACK_NOASM_INFLATE is set; decodeHuffman never dispatches to assembly")
	}
	tests := []struct {
		name         string
		inputGap     int // exact len(r.src)-r.pos at dispatch; 0 leaves slack
		dstLen       int // exact len(dst); 0 leaves slack
		wantAssembly bool
	}{
		{
			name:     "input gap at margin uses tail",
			inputGap: deflateFastInputMargin,
		},
		{
			name:         "input gap above margin uses assembly",
			inputGap:     deflateFastInputMargin + 1,
			wantAssembly: true,
		},
		{
			name:   "output at margin uses tail",
			dstLen: deflateFastOutputMargin,
		},
		{
			name:         "output above margin uses assembly",
			dstLen:       deflateFastOutputMargin + 1,
			wantAssembly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A distance-1 match makes the taken path observable: the
			// assembly RLE copy overruns farther into the permitted
			// output margin than the Go fast loop, and the exact tail
			// decoder not at all.
			src, want := amd64FixedMatchBlock(1, 258, 0)
			if tt.inputGap != 0 {
				// Reading the 3-bit block header refills eight bytes,
				// so dispatch compares len(src)-8 against the margin.
				total := 8 + tt.inputGap
				if len(src) > total {
					t.Fatalf("fixture is %d bytes, want at most %d", len(src), total)
				}
				src = append(src, make([]byte, total-len(src))...)
			} else {
				src = append(src, make([]byte, 40)...)
			}
			dstLen := tt.dstLen
			if dstLen == 0 {
				dstLen = len(want) + deflateFastOutputMargin + 1
			}
			if len(want) > dstLen {
				t.Fatalf("fixture output is %d bytes, want at most %d", len(want), dstLen)
			}

			d, r := amd64PrepareHuffmanBlock(t, src, 1)
			gap := len(r.src) - r.pos
			if tt.inputGap != 0 && gap != tt.inputGap {
				t.Fatalf("fixture input gap = %d, want %d", gap, tt.inputGap)
			}
			if tt.inputGap == 0 && gap <= deflateFastInputMargin {
				t.Fatalf("fixture input gap = %d, want > %d", gap, deflateFastInputMargin)
			}

			reference := amd64RunHuffmanGo(d, r, dstLen)
			if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
				t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
			}

			tailOnly := amd64RunHuffmanTail(d, r, dstLen)
			amd64CompareHuffmanResults(t, tailOnly, reference)

			dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
			amd64CompareHuffmanResults(t, dispatch, reference)

			kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
			amd64CompareHuffmanResults(t, kernel, reference)
			amd64RequireStatuses(t, statuses, inflateFastTail)

			// Exactly at the margin the kernel must refuse to decode, so
			// its buffer stays identical to the pure-tail run. One byte
			// past it, the assembly match copy marks the output margin.
			kernelDecoded := !bytes.Equal(kernel.dst, tailOnly.dst)
			if kernelDecoded != tt.wantAssembly {
				t.Fatalf("kernel decoded = %v, want %v", kernelDecoded, tt.wantAssembly)
			}
			dispatchMatchesTail := bytes.Equal(dispatch.dst, tailOnly.dst)
			dispatchMatchesKernel := bytes.Equal(dispatch.dst, kernel.dst)
			if tt.wantAssembly {
				if dispatchMatchesTail || !dispatchMatchesKernel {
					t.Fatalf("dispatch buffer matches tail=%v kernel=%v, want false/true",
						dispatchMatchesTail, dispatchMatchesKernel)
				}
			} else if !dispatchMatchesTail || !dispatchMatchesKernel {
				t.Fatalf("dispatch buffer matches tail=%v kernel=%v, want true/true",
					dispatchMatchesTail, dispatchMatchesKernel)
			}
		})
	}
}

func TestInflateHuffmanFastAMD64YieldDifferential(t *testing.T) {
	const matches = 260

	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
	w.writeFixedLitlen('Y')
	for range matches {
		w.writeFixedLength(258)
		w.writeFixedDistance(1)
	}
	w.writeFixedLitlen(256)
	src := append(w.bytes(), make([]byte, 40)...)
	want := bytes.Repeat([]byte{'Y'}, 1+matches*258)

	d, r := amd64PrepareHuffmanBlock(t, src, 1)
	dstLen := len(want) + deflateFastOutputMargin + 1
	reference := amd64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
	amd64CompareHuffmanResults(t, dispatch, reference)

	kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
	amd64CompareHuffmanResults(t, kernel, reference)
	amd64RequireStatuses(t, statuses, inflateFastYield, inflateFastBlockDone)
}

func TestInflateHuffmanFastAMD64DoubleYield(t *testing.T) {
	// 520 matches of length 258 at distance 1 after one literal produce
	// 1+520*258 = 134161 bytes. Decoding crosses the yield boundary at
	// 64 KiB and again at 128 KiB before the end-of-block symbol, and
	// stays below 192 KiB, so the kernel must yield exactly twice.
	const matches = 520

	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
	w.writeFixedLitlen('Y')
	for range matches {
		w.writeFixedLength(258)
		w.writeFixedDistance(1)
	}
	w.writeFixedLitlen(256)
	src := append(w.bytes(), make([]byte, 40)...)
	want := bytes.Repeat([]byte{'Y'}, 1+matches*258)

	if len(want) <= 2*inflateFastYieldBytes || len(want) >= 3*inflateFastYieldBytes {
		t.Fatalf("fixture output = %d bytes, want in (%d, %d) for exactly two yields",
			len(want), 2*inflateFastYieldBytes, 3*inflateFastYieldBytes)
	}

	d, r := amd64PrepareHuffmanBlock(t, src, 1)
	dstLen := len(want) + deflateFastOutputMargin + 1
	reference := amd64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	// The dispatch run drives the production resume loop across both
	// yields; the direct-kernel run observes the yield statuses.
	dispatch := amd64RunHuffmanDispatch(d, r, dstLen)
	amd64CompareHuffmanResults(t, dispatch, reference)

	kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)
	amd64CompareHuffmanResults(t, kernel, reference)
	amd64RequireStatuses(t, statuses,
		inflateFastYield, inflateFastYield, inflateFastBlockDone)
}

// TestInflateHuffmanFastAMD64TailBeatsYield pins the return_boundary branch
// order in inflate_fast_amd64.s: when one kernel return observes both an
// exhausted input (pos >= inLimit) and an output cursor at or past
// yieldAt, it must report inflateFastTail, not inflateFastYield. The order
// matters for liveness: the production driver in inflate_fast_state.go
// re-enters the kernel on yield without reloading any state, so a yield
// that does not guarantee input headroom for the next entry refill would
// spin forever. The state is hand-built because the driver can never
// produce one this small: inLimit is shrunk so the first refill exhausts
// the input, and yieldAt = 1 is crossed by the first literal, making both
// conditions true at the first boundary check.
func TestInflateHuffmanFastAMD64TailBeatsYield(t *testing.T) {
	payload := makeDeterministicBytes(64)
	src := amd64FixedLiteralBlock(payload, 40)
	d, r := amd64PrepareHuffmanBlock(t, src, 1)

	dstLen := len(payload) + deflateFastOutputMargin + 1
	reference := amd64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], payload) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	dst := make([]byte, dstLen)
	state := inflateFastState{
		src:        unsafe.SliceData(r.src),
		dst:        unsafe.SliceData(dst),
		litlen:     &d.litlen[0],
		offset:     &d.offset[0],
		bitbuf:     r.buf,
		nbits:      uint64(r.nbits),
		pos:        uint64(r.pos),
		out:        0,
		inLimit:    uint64(r.pos) + 1, // exhausted by the first refill
		outLimit:   uint64(len(dst) - deflateFastOutputMargin),
		litlenMask: bitMask(uint(d.litlenBits)),
		offsetMask: bitMask(uint(d.offsetBits)),
		yieldAt:    1, // crossed by the first literal
	}
	inflateHuffmanFastAMD64(&state)

	if state.status != inflateFastTail {
		t.Fatalf("status = %d, want inflateFastTail (input exhaustion must outrank yield)",
			state.status)
	}
	if state.yieldAt != 1 {
		t.Fatalf("yieldAt = %d, want 1 (a tail return must not advance the yield mark)",
			state.yieldAt)
	}
	if state.out == 0 || state.pos <= uint64(r.pos) {
		t.Fatalf("kernel made no progress: out=%d pos=%d", state.out, state.pos)
	}

	// The stored state must let the tail decoder finish exactly where the
	// full-margin reference decode lands.
	r.buf = state.bitbuf
	r.nbits = uint(state.nbits)
	r.pos = int(state.pos)
	out, err := d.decodeHuffmanTail(&r, dst, int(state.out))
	got := amd64HuffmanResult{r: r, dst: dst, out: out, err: err}
	amd64CompareHuffmanResults(t, got, reference)
}

func TestInflateHuffmanFastAMD64BadDataStatus(t *testing.T) {
	tests := []struct {
		name  string
		write func(*deflateTestBits)
	}{
		{
			name: "invalid litlen",
			write: func(w *deflateTestBits) {
				w.writeFixedLitlen(286)
			},
		},
		{
			name: "invalid backreference",
			write: func(w *deflateTestBits) {
				w.writeFixedLength(3)
				w.writeFixedDistance(1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w deflateTestBits
			w.write(1, 1)
			w.write(1, 2)
			tt.write(&w)
			src := append(w.bytes(), make([]byte, 40)...)

			d, r := amd64PrepareHuffmanBlock(t, src, 1)
			const dstLen = deflateFastOutputMargin + 1
			reference := amd64RunHuffmanGo(d, r, dstLen)
			kernel, statuses := amd64RunHuffmanKernel(t, d, r, dstLen)

			if !errors.Is(reference.err, errDeflateBadData) || !errors.Is(kernel.err, errDeflateBadData) {
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

type amd64HuffmanResult struct {
	r   deflateBits
	dst []byte
	out int
	err error
}

func amd64PrepareHuffmanBlock(
	t *testing.T,
	src []byte,
	wantType uint64,
) (goInflater, deflateBits) {
	t.Helper()

	var d goInflater
	r := deflateBits{src: src}
	header, ok := r.read(3)
	if !ok {
		t.Fatal("truncated block header")
	}
	if got := (header >> 1) & 3; got != wantType {
		t.Fatalf("block type = %d, want %d", got, wantType)
	}

	switch wantType {
	case 1:
		if !d.loadFixedTables() {
			t.Fatal("loadFixedTables failed")
		}
	case 2:
		if err := d.loadDynamicTables(&r); err != nil {
			t.Fatalf("loadDynamicTables failed: %v", err)
		}
	default:
		t.Fatalf("unsupported Huffman block type %d", wantType)
	}
	return d, r
}

func amd64RunHuffmanGo(d goInflater, r deflateBits, dstLen int) amd64HuffmanResult {
	dst := make([]byte, dstLen)
	out, err := d.decodeHuffmanGo(&r, dst, 0)
	return amd64HuffmanResult{r: r, dst: dst, out: out, err: err}
}

// amd64RunHuffmanTail decodes entirely through decodeHuffmanTail. Its exact
// writes never mark the output margin, giving path-observation tests a
// garbage-free reference buffer.
func amd64RunHuffmanTail(d goInflater, r deflateBits, dstLen int) amd64HuffmanResult {
	dst := make([]byte, dstLen)
	out, err := d.decodeHuffmanTail(&r, dst, 0)
	return amd64HuffmanResult{r: r, dst: dst, out: out, err: err}
}

func amd64RunHuffmanDispatch(d goInflater, r deflateBits, dstLen int) amd64HuffmanResult {
	dst := make([]byte, dstLen)
	out, err := d.decodeHuffman(&r, dst, 0)
	return amd64HuffmanResult{r: r, dst: dst, out: out, err: err}
}

func amd64RunHuffmanKernel(
	t *testing.T,
	d goInflater,
	r deflateBits,
	dstLen int,
) (amd64HuffmanResult, []inflateFastStatus) {
	t.Helper()

	if len(r.src) < deflateFastInputMargin || dstLen < deflateFastOutputMargin {
		t.Fatalf("invalid direct-kernel margins: src=%d dst=%d", len(r.src), dstLen)
	}
	dst := make([]byte, dstLen)
	return amd64RunHuffmanKernelInto(t, d, r, dst)
}

func amd64RunHuffmanKernelInto(
	t *testing.T,
	d goInflater,
	r deflateBits,
	dst []byte,
) (amd64HuffmanResult, []inflateFastStatus) {
	t.Helper()

	if len(r.src) < deflateFastInputMargin || len(dst) < deflateFastOutputMargin {
		t.Fatalf("invalid direct-kernel margins: src=%d dst=%d", len(r.src), len(dst))
	}
	state := inflateFastState{
		src:        unsafe.SliceData(r.src),
		dst:        unsafe.SliceData(dst),
		litlen:     &d.litlen[0],
		offset:     &d.offset[0],
		bitbuf:     r.buf,
		nbits:      uint64(r.nbits),
		pos:        uint64(r.pos),
		out:        0,
		inLimit:    uint64(len(r.src) - deflateFastInputMargin),
		outLimit:   uint64(len(dst) - deflateFastOutputMargin),
		litlenMask: bitMask(uint(d.litlenBits)),
		offsetMask: bitMask(uint(d.offsetBits)),
		yieldAt:    inflateFastYieldBytes,
	}

	var statuses []inflateFastStatus
	for range 100 {
		inflateHuffmanFastAMD64(&state)
		statuses = append(statuses, state.status)
		if state.status == inflateFastYield {
			continue
		}

		r.buf = state.bitbuf
		r.nbits = uint(state.nbits)
		r.pos = int(state.pos)
		out := int(state.out)
		switch state.status {
		case inflateFastTail:
			out, err := d.decodeHuffmanTail(&r, dst, out)
			return amd64HuffmanResult{r: r, dst: dst, out: out, err: err}, statuses
		case inflateFastBlockDone:
			return amd64HuffmanResult{r: r, dst: dst, out: out}, statuses
		case inflateFastBadData:
			return amd64HuffmanResult{
				r: r, dst: dst, out: out, err: errDeflateBadData,
			}, statuses
		default:
			t.Fatalf("invalid kernel status %d", state.status)
		}
	}
	t.Fatal("kernel did not terminate after 100 calls")
	return amd64HuffmanResult{}, nil
}

func amd64CompareHuffmanResults(t *testing.T, got, want amd64HuffmanResult) {
	t.Helper()

	if !errors.Is(got.err, want.err) {
		t.Fatalf("error mismatch: got=%v want=%v", got.err, want.err)
	}
	if got.out != want.out {
		t.Fatalf("output position mismatch: got=%d want=%d", got.out, want.out)
	}
	if !bytes.Equal(got.dst[:got.out], want.dst[:want.out]) {
		t.Fatal("output bytes mismatch")
	}
	if got.r.consumed() != want.r.consumed() || !amd64SameUnreadBits(got.r, want.r) {
		t.Fatalf(
			"input state mismatch: got={pos:%d nbits:%d buf:%016x consumed:%d} "+
				"want={pos:%d nbits:%d buf:%016x consumed:%d}",
			got.r.pos, got.r.nbits, got.r.buf, got.r.consumed(),
			want.r.pos, want.r.nbits, want.r.buf, want.r.consumed(),
		)
	}
}

func amd64SameUnreadBits(a, b deflateBits) bool {
	for {
		av, aok := a.read(1)
		bv, bok := b.read(1)
		if aok != bok || av != bv {
			return false
		}
		if !aok {
			return true
		}
	}
}

func amd64RequireStatuses(
	t *testing.T,
	got []inflateFastStatus,
	want ...inflateFastStatus,
) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statuses = %v, want %v", got, want)
		}
	}
}

func amd64FixedLiteralBlock(payload []byte, padding int) []byte {
	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
	for _, b := range payload {
		w.writeFixedLitlen(int(b))
	}
	w.writeFixedLitlen(256)
	return append(w.bytes(), make([]byte, padding)...)
}

func amd64FixedMatchBlock(distance, length, padding int) ([]byte, []byte) {
	seed := makeDeterministicBytes(distance)

	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
	for _, b := range seed {
		w.writeFixedLitlen(int(b))
	}
	w.writeFixedLength(length)
	w.writeFixedDistance(distance)
	w.writeFixedLitlen(256)

	want := append([]byte(nil), seed...)
	for range length {
		want = append(want, want[len(want)-distance])
	}
	return append(w.bytes(), make([]byte, padding)...), want
}
