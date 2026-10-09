package objstore

import "testing"

// Shared, architecture-independent helpers for the assembly Huffman kernel
// audit tests (inflate_fast_arm64_audit_test.go and
// inflate_fast_amd64_audit_test.go). Everything here builds DEFLATE bit
// streams and expected outputs from the RFC 1951 rules alone; nothing
// references kernel-gated symbols, so this file compiles under every build
// tag set, including purego and gitpack_libdeflate.

// huffCanonicalCodes assigns canonical Huffman codes for the given code
// lengths per RFC 1951 section 3.2.2. Entry i holds {codeword, length};
// symbols with a zero length keep a zero entry.
func huffCanonicalCodes(lens []int) [][2]int {
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

// deepStream hand-builds one dynamic-Huffman block whose litlen codes
// exceed litlenTableBits and whose offset codes exceed offsetTableBits, so
// decoding must resolve subtable entries. It tracks the expected output in
// an LZ77 model that is independent of the decoders under test.
type deepStream struct {
	w        deflateTestBits
	want     []byte
	litCodes [][2]int
	offCodes [][2]int
}

func newDeepStream(t *testing.T) *deepStream {
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
	offLens[8] = 9 // distances 17..24 (3 extra bits): first subtable depth
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

	s := &deepStream{
		litCodes: huffCanonicalCodes(litLens),
		offCodes: huffCanonicalCodes(offLens),
	}
	s.w.write(1, 1)  // BFINAL
	s.w.write(2, 2)  // BTYPE = dynamic
	s.w.write(29, 5) // HLIT: 286 litlen codes
	s.w.write(29, 5) // HDIST: 30 offset codes
	s.w.write(15, 4) // HCLEN: 19 precode lengths
	for _, sym := range precodeOrder {
		s.w.write(uint64(preLens[sym]), 3)
	}
	preCodes := huffCanonicalCodes(preLens)
	for _, n := range litLens {
		s.w.writeCode(preCodes[n])
	}
	for _, n := range offLens {
		s.w.writeCode(preCodes[n])
	}
	return s
}

func (s *deepStream) literal(b byte) {
	s.w.writeCode(s.litCodes[b])
	s.want = append(s.want, b)
}

func (s *deepStream) match(length, dist int) {
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
	case dist >= 17 && dist <= 24:
		s.w.writeCode(s.offCodes[8])
		s.w.write(uint64(dist-17), 3)
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

func (s *deepStream) endOfBlock() { s.w.writeCode(s.litCodes[256]) }
