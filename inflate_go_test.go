package objstore

import (
	"bytes"
	stdflate "compress/flate"
	"compress/zlib"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
)

// Shared static pack-zlib stream vectors (2-byte header + DEFLATE stream +
// 4-byte Adler-32 trailer), referenced by the static-vector, truncation,
// ARM64 differential, and concurrent pool-reuse tests.
//
// fixedHuffmanVectorHex decodes to "goodbye, world" via a single fixed-
// Huffman (BTYPE=1) block; dynamicHuffmanVectorHex decodes to
// bytes.Repeat([]byte("abcabc"), 1000) via a single dynamic-Huffman
// (BTYPE=2) block.
const (
	fixedHuffmanVectorHex   = "789c4bcfcf4f49aa4cd55128cf2fca49010028a5055e"
	dynamicHuffmanVectorHex = "789cedc2411100000c02a0ac6aff0e0bb12f1ce9a2aaaaaaaaaafe1e2f01f959"
)

func TestInflatePackZlibStaticVectors(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want []byte
	}{
		{
			name: "empty fixed",
			hex:  "789c030000000001",
			want: nil,
		},
		{
			name: "one-byte stored",
			hex:  "7801010100feff1100120012",
			want: []byte{0x11},
		},
		{
			name: "fixed huffman",
			hex:  fixedHuffmanVectorHex,
			want: []byte("goodbye, world"),
		},
		{
			name: "dynamic huffman",
			hex:  dynamicHuffmanVectorHex,
			want: bytes.Repeat([]byte("abcabc"), 1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := hex.DecodeString(tt.hex)
			if err != nil {
				t.Fatal(err)
			}
			got, consumed, err := guardedGoInflate(t, src, len(tt.want))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if want := len(src) - 4; consumed != want {
				t.Fatalf("consumed %d bytes, want %d", consumed, want)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("output mismatch: got %x, want %x", got, tt.want)
			}
		})
	}
}

func TestInflatePackZlibHeaderExhaustive(t *testing.T) {
	for h := 0; h <= 0xffff; h++ {
		cmf, flg := byte(h>>8), byte(h)
		src := []byte{cmf, flg, 0x03, 0x00}
		consumed, err := inflatePackZlibGo(src, nil)
		valid := validPackZlibHeader(cmf, flg)
		if valid && (err != nil || consumed != len(src)) {
			t.Fatalf("valid header %04x rejected: consumed=%d err=%v", h, consumed, err)
		}
		if !valid && (err == nil || consumed != 0) {
			t.Fatalf("invalid header %04x accepted: consumed=%d err=%v", h, consumed, err)
		}
	}
}

func TestInflatePackZlibFixedLengthAndDistanceBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		lengthSym  int
		lengthBits uint64
		wantLen    int
	}{
		{name: "length 257", lengthSym: 284, lengthBits: 30, wantLen: 258},
		{name: "length 258", lengthSym: 285, wantLen: 259},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w deflateTestBits
			w.write(1, 1) // BFINAL
			w.write(1, 2) // fixed Huffman
			w.writeFixedLitlen('A')
			w.writeFixedLitlen(tt.lengthSym)
			if tt.lengthSym == 284 {
				w.write(tt.lengthBits, 5)
			}
			w.writeFixedOffset(0) // distance 1
			w.writeFixedLitlen(256)

			src := append([]byte{0x78, 0x9c}, w.bytes()...)
			got, consumed, err := guardedGoInflate(t, src, tt.wantLen)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if consumed != len(src) {
				t.Fatalf("consumed %d bytes, want %d", consumed, len(src))
			}
			if !bytes.Equal(got, bytes.Repeat([]byte{'A'}, tt.wantLen)) {
				t.Fatal("overlapping distance-1 copy produced incorrect output")
			}
		})
	}
}

func TestInflatePackZlibFastMatchCopies(t *testing.T) {
	distances := []int{
		1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 31, 32,
		255, 256, 1023, 4096, 32768,
	}

	for _, distance := range distances {
		t.Run(fmt.Sprintf("distance=%d", distance), func(t *testing.T) {
			seed := makeDeterministicBytes(distance)
			tail := []byte("tail-end")

			var w deflateTestBits
			w.write(1, 1) // BFINAL
			w.write(1, 2) // fixed Huffman
			for _, b := range seed {
				w.writeFixedLitlen(int(b))
			}
			w.writeFixedLitlen(285) // length 258
			w.writeFixedDistance(distance)
			for _, b := range tail {
				w.writeFixedLitlen(int(b))
			}
			w.writeFixedLitlen(256)

			want := append([]byte(nil), seed...)
			for range 258 {
				want = append(want, want[len(want)-distance])
			}
			want = append(want, tail...)

			src := append([]byte{0x78, 0x9c}, w.bytes()...)
			got, consumed, err := guardedGoInflate(t, src, len(want))
			if err != nil {
				t.Fatal(err)
			}
			if consumed != len(src) || !bytes.Equal(got, want) {
				t.Fatalf("consumed=%d want=%d outputEqual=%v",
					consumed, len(src), bytes.Equal(got, want))
			}
		})
	}

	t.Run("short-overlapping-word-copies", func(t *testing.T) {
		for distance := 8; distance < deflateFastMatchCopy; distance++ {
			for length := 3; length <= deflateFastMatchCopy; length++ {
				seed := makeDeterministicBytes(distance)
				tail := makeDeterministicBytes(deflateFastOutputMargin - length)

				var w deflateTestBits
				w.write(1, 1) // BFINAL
				w.write(1, 2) // fixed Huffman
				for _, b := range seed {
					w.writeFixedLitlen(int(b))
				}
				w.writeFixedLength(length)
				w.writeFixedDistance(distance)
				for _, b := range tail {
					w.writeFixedLitlen(int(b))
				}
				w.writeFixedLitlen(256)

				want := append([]byte(nil), seed...)
				for range length {
					want = append(want, want[len(want)-distance])
				}
				want = append(want, tail...)
				if len(want)-deflateFastOutputMargin != distance {
					t.Fatal("test fixture does not place the match at the fast-loop boundary")
				}

				src := append([]byte{0x78, 0x9c}, w.bytes()...)
				got, consumed, err := guardedGoInflate(t, src, len(want))
				if err != nil {
					t.Fatalf("distance=%d length=%d: %v", distance, length, err)
				}
				if consumed != len(src) || !bytes.Equal(got, want) {
					t.Fatalf("distance=%d length=%d: consumed=%d want=%d outputEqual=%v",
						distance, length, consumed, len(src), bytes.Equal(got, want))
				}
			}
		}
	})

	t.Run("two-literals-before-maximum-match", func(t *testing.T) {
		const prefixLen = 9
		tailLen := deflateFastOutputMargin - 2 - 258
		prefix := makeDeterministicBytes(prefixLen)
		tail := makeDeterministicBytes(tailLen)

		var w deflateTestBits
		w.write(1, 1) // BFINAL
		w.write(1, 2) // fixed Huffman
		for _, b := range prefix {
			w.writeFixedLitlen(int(b))
		}
		w.writeFixedLitlen('X')
		w.writeFixedLitlen('Y')
		w.writeFixedLength(258)
		w.writeFixedDistance(1)
		for _, b := range tail {
			w.writeFixedLitlen(int(b))
		}
		w.writeFixedLitlen(256)

		want := append([]byte(nil), prefix...)
		want = append(want, 'X', 'Y')
		want = append(want, bytes.Repeat([]byte{'Y'}, 258)...)
		want = append(want, tail...)
		if len(want)-deflateFastOutputMargin != prefixLen {
			t.Fatal("test fixture does not begin at the fast-loop output boundary")
		}

		src := append([]byte{0x78, 0x9c}, w.bytes()...)
		got, consumed, err := guardedGoInflate(t, src, len(want))
		if err != nil {
			t.Fatal(err)
		}
		if consumed != len(src) || !bytes.Equal(got, want) {
			t.Fatalf("consumed=%d want=%d outputEqual=%v",
				consumed, len(src), bytes.Equal(got, want))
		}
	})
}

func TestInflatePackZlibIncompleteHuffmanCodes(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want []byte
	}{
		{
			name: "empty offset code",
			raw:  incompleteEmptyOffsetStream(),
			want: []byte("ABAA"),
		},
		{
			name: "singleton literal-length code",
			raw:  incompleteSingletonLitlenStream(),
			want: nil,
		},
		{
			name: "singleton offset code",
			raw:  incompleteSingletonOffsetStream(false),
			want: []byte{255, 255, 255, 255},
		},
		{
			name: "singleton nonzero offset symbol",
			raw:  incompleteSingletonOffsetStream(true),
			want: []byte{254, 255, 254, 255, 254},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := append([]byte{0x78, 0x9c}, tt.raw...)
			got, consumed, err := guardedGoInflate(t, src, len(tt.want))
			if err != nil {
				t.Fatal(err)
			}
			if consumed != len(src) || !bytes.Equal(got, tt.want) {
				t.Fatalf("consumed=%d want=%d output=%x want=%x",
					consumed, len(src), got, tt.want)
			}
			assertGoMatchesReference(t, src, len(tt.want))
		})
	}
}

func TestInflatePackZlibAdversarialDynamicStreams(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		size int
	}{
		{
			name: "code lengths exceed HLIT plus HDIST",
			raw:  tooManyCodewordLengthsStream(),
			size: 1,
		},
		{
			name: "reserved HLIT count",
			raw:  reservedAlphabetCountStream(true),
			size: 0,
		},
		{
			name: "reserved HDIST count",
			raw:  reservedAlphabetCountStream(false),
			size: 0,
		},
		{
			name: "truncated all-zero literal codeword",
			raw:  overreadStream(),
			size: 256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := append([]byte{0x78, 0x9c}, tt.raw...)
			_, consumed, err := guardedGoInflate(t, src, tt.size)
			if err == nil || consumed != 0 {
				t.Fatalf("malformed stream accepted: consumed=%d err=%v", consumed, err)
			}
		})
	}
}

func TestInflatePackZlibExhaustiveRawInputsUpToTwoBytes(t *testing.T) {
	var br bytes.Reader
	fr := stdflate.NewReader(&br)
	resetter := fr.(stdflate.Resetter)
	defer fr.Close()

	check := func(raw []byte) {
		t.Helper()

		br.Reset(raw)
		if err := resetter.Reset(&br, nil); err != nil {
			t.Fatal(err)
		}
		n, refErr := io.Copy(io.Discard, io.LimitReader(fr, 1))
		refOK := refErr == nil && n == 0

		src := make([]byte, 2+len(raw))
		src[0], src[1] = 0x78, 0x9c
		copy(src[2:], raw)
		consumed, goErr := inflatePackZlibGo(src, nil)
		goOK := goErr == nil
		if goOK != refOK {
			t.Fatalf("raw %x: Go consumed=%d err=%v; stdlib wrote=%d err=%v",
				raw, consumed, goErr, n, refErr)
		}
		if goOK {
			wantConsumed := 2 + len(raw) - br.Len()
			if consumed != wantConsumed {
				t.Fatalf("raw %x consumed=%d, want %d", raw, consumed, wantConsumed)
			}
		} else if consumed != 0 {
			t.Fatalf("raw %x failed with consumed=%d", raw, consumed)
		}
	}

	check(nil)
	for a := 0; a < 256; a++ {
		check([]byte{byte(a)})
	}
	var raw [2]byte
	for a := 0; a < 256; a++ {
		raw[0] = byte(a)
		for b := 0; b < 256; b++ {
			raw[1] = byte(b)
			check(raw[:])
		}
	}
}

func TestInflatePackZlibMalformedFixedStreams(t *testing.T) {
	fixed := func(write func(*deflateTestBits)) []byte {
		var w deflateTestBits
		w.write(1, 1)
		w.write(1, 2)
		write(&w)
		return append([]byte{0x78, 0x9c}, w.bytes()...)
	}

	tests := []struct {
		name string
		src  []byte
		size int
	}{
		{
			name: "reserved block type",
			src:  []byte{0x78, 0x9c, 0x07},
		},
		{
			name: "stored LEN NLEN mismatch",
			src:  []byte{0x78, 0x9c, 0x01, 0x01, 0x00, 0xff, 0xff, 0x00},
			size: 1,
		},
		{
			name: "truncated stored bytes",
			src:  []byte{0x78, 0x9c, 0x01, 0x02, 0x00, 0xfd, 0xff, 0x00},
			size: 2,
		},
		{
			name: "distance before history",
			src: fixed(func(w *deflateTestBits) {
				w.writeFixedLitlen(257)
				w.writeFixedOffset(0)
				w.writeFixedLitlen(256)
			}),
			size: 3,
		},
		{
			name: "reserved literal-length symbol 286",
			src: fixed(func(w *deflateTestBits) {
				w.writeFixedLitlen(286)
				w.writeFixedLitlen(256)
			}),
		},
		{
			name: "reserved distance symbol 30",
			src: fixed(func(w *deflateTestBits) {
				w.writeFixedLitlen('A')
				w.writeFixedLitlen(257)
				w.writeFixedOffset(30)
				w.writeFixedLitlen(256)
			}),
			size: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, consumed, err := guardedGoInflate(t, tt.src, tt.size)
			if err == nil {
				t.Fatal("malformed stream accepted")
			}
			if consumed != 0 {
				t.Fatalf("error consumed %d bytes, want 0", consumed)
			}
		})
	}
}

func TestGoInflaterInvalidatesFixedCacheBeforeDynamicParse(t *testing.T) {
	var d goInflater
	if !d.loadFixedTables() || !d.static {
		t.Fatal("failed to initialize fixed tables")
	}

	r := deflateBits{}
	if err := d.loadDynamicTables(&r); err == nil {
		t.Fatal("truncated dynamic header accepted")
	} else if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated dynamic header reported %v, want unexpected-EOF identity", err)
	}
	if d.static {
		t.Fatal("failed dynamic parse left fixed-table cache valid")
	}
	if !d.loadFixedTables() || !d.static {
		t.Fatal("fixed tables were not rebuilt after failed dynamic parse")
	}
}

func TestDecodeTableEntryUsesLongCodeSubtable(t *testing.T) {
	var d goInflater
	var lens [maxLitlenSyms]uint8

	// A complete canonical code with one symbol at each length 1..11 and
	// two at length 12. The final two symbols must use an 11-bit main-table
	// prefix plus a one-bit subtable.
	for i := 0; i < 11; i++ {
		lens[i] = uint8(i + 1)
	}
	lens[11], lens[12] = 12, 12

	tableBits, ok := d.buildTable(
		d.litlen[:], lens[:], litlenTable, litlenTableBits, maxCodeLen, false,
	)
	if !ok || tableBits != litlenTableBits {
		t.Fatalf("buildTable: bits=%d ok=%v", tableBits, ok)
	}

	var counts [maxCodeLen + 1]int
	for _, n := range lens {
		if n != 0 {
			counts[n]++
		}
	}
	var next [maxCodeLen + 1]int
	code := 0
	for n := 1; n <= maxCodeLen; n++ {
		code = (code + counts[n-1]) << 1
		next[n] = code
	}
	for sym, n := range lens {
		if n == 0 {
			continue
		}
		if sym == 11 || sym == 12 {
			raw := []byte{
				byte(reverseCode(next[n]+sym-11, int(n))),
				byte(reverseCode(next[n]+sym-11, int(n)) >> 8),
			}
			r := deflateBits{src: raw}
			entry, _, err := decodeTableEntry(&r, d.litlen[:], uint(tableBits))
			if err != nil || entry&huffLiteral == 0 || int(byte(entry>>16)) != sym {
				t.Fatalf("symbol %d: entry=%08x err=%v", sym, entry, err)
			}
		}
	}
}

func TestBuildTableInitializesEveryReachableEntry(t *testing.T) {
	tests := []struct {
		name         string
		kind         huffmanTableKind
		numSyms      int
		maxTableBits int
		maxLen       int
		table        func(*goInflater) []uint32
	}{
		{
			name:         "precode",
			kind:         precodeTable,
			numSyms:      19,
			maxTableBits: precodeTableBits,
			maxLen:       7,
			table:        func(d *goInflater) []uint32 { return d.precode[:] },
		},
		{
			name:         "literal-length",
			kind:         litlenTable,
			numSyms:      maxLitlenSyms,
			maxTableBits: litlenTableBits,
			maxLen:       maxCodeLen,
			table:        func(d *goInflater) []uint32 { return d.litlen[:] },
		},
		{
			name:         "offset",
			kind:         offsetTable,
			numSyms:      maxOffsetSyms,
			maxTableBits: offsetTableBits,
			maxLen:       maxCodeLen,
			table:        func(d *goInflater) []uint32 { return d.offset[:] },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d goInflater
			for seed := 0; seed < 256; seed++ {
				lens := completeCodeLens(tt.numSyms, tt.maxLen, seed)
				table := tt.table(&d)
				clear(table)

				tableBits, ok := d.buildTable(
					table, lens, tt.kind, tt.maxTableBits, tt.maxLen, false,
				)
				if !ok {
					t.Fatalf("seed %d: complete code rejected", seed)
				}
				requireReachableTableEntries(t, table, tableBits, seed)
			}

			lens := make([]uint8, tt.numSyms)
			lens[tt.numSyms/2] = 1
			table := tt.table(&d)
			clear(table)
			tableBits, ok := d.buildTable(
				table, lens, tt.kind, tt.maxTableBits, tt.maxLen, false,
			)
			if !ok {
				t.Fatal("singleton code rejected")
			}
			requireReachableTableEntries(t, table, tableBits, -1)
		})
	}

	t.Run("empty offset", func(t *testing.T) {
		var d goInflater
		tableBits, ok := d.buildTable(
			d.offset[:], make([]uint8, maxOffsetSyms),
			offsetTable, offsetTableBits, maxCodeLen, true,
		)
		if !ok {
			t.Fatal("empty offset code rejected")
		}
		requireReachableTableEntries(t, d.offset[:], tableBits, -1)
	})
}

func TestOffsetDecodeResultsHavePositiveBase(t *testing.T) {
	for sym, entry := range offsetDecodeResults {
		if entry&huffExceptional != 0 {
			continue
		}
		if entry>>16 == 0 {
			t.Fatalf("distance symbol %d has a zero base", sym)
		}
	}
}

func completeCodeLens(numSyms, maxLen, seed int) []uint8 {
	target := 2 + seed%(numSyms-1)
	depths := []uint8{1, 1}
	state := uint64(seed + 1)
	next := func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}

	for len(depths) < target {
		eligible := 0
		for _, depth := range depths {
			if int(depth) < maxLen {
				eligible++
			}
		}
		pick := int(next() % uint64(eligible))
		for i, depth := range depths {
			if int(depth) == maxLen {
				continue
			}
			if pick != 0 {
				pick--
				continue
			}
			depths[i]++
			depths = append(depths, depths[i])
			break
		}
	}

	for i := len(depths) - 1; i > 0; i-- {
		j := int(next() % uint64(i+1))
		depths[i], depths[j] = depths[j], depths[i]
	}
	lens := make([]uint8, numSyms)
	copy(lens, depths)
	return lens
}

func requireReachableTableEntries(t *testing.T, table []uint32, tableBits, seed int) {
	t.Helper()

	mainSize := 1 << tableBits
	for i, entry := range table[:mainSize] {
		if entry == 0 {
			t.Fatalf("seed %d: main entry %d is uninitialized", seed, i)
		}
		if entry&huffSubtable == 0 {
			continue
		}

		if got := int(entry & huffBitCountMask); got != tableBits {
			t.Fatalf("seed %d: main entry %d consumes %d bits, want %d",
				seed, i, got, tableBits)
		}
		width := int((entry >> 8) & 0x1f)
		start := int(entry >> 16)
		end := start + 1<<width
		if width == 0 || start < mainSize || end > len(table) {
			t.Fatalf("seed %d: invalid subtable at main entry %d: start=%d width=%d",
				seed, i, start, width)
		}
		for j, subentry := range table[start:end] {
			if subentry == 0 {
				t.Fatalf("seed %d: subtable entry %d is uninitialized", seed, start+j)
			}
			if subentry&huffSubtable != 0 {
				t.Fatalf("seed %d: nested subtable at entry %d", seed, start+j)
			}
		}
	}
}

func TestInflatePackZlibDestinationSizeAndTrailingInput(t *testing.T) {
	payload := bytes.Repeat([]byte("direct-to-destination "), 73)
	encoded := encodeZlib(t, payload, zlib.DefaultCompression)
	memberEnd := len(encoded) - 4

	for _, size := range []int{len(payload) - 1, len(payload), len(payload) + 1} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			got, consumed, err := guardedGoInflate(t, encoded, size)
			if size != len(payload) {
				if err == nil || consumed != 0 {
					t.Fatalf("wrong destination size accepted: consumed=%d err=%v", consumed, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if consumed != memberEnd || !bytes.Equal(got, payload) {
				t.Fatalf("valid decode: consumed=%d want=%d outputEqual=%v",
					consumed, memberEnd, bytes.Equal(got, payload))
			}
		})
	}

	withTrailing := append(append([]byte(nil), encoded...), []byte("next pack object")...)
	got, consumed, err := guardedGoInflate(t, withTrailing, len(payload))
	if err != nil {
		t.Fatal(err)
	}
	if consumed != memberEnd || !bytes.Equal(got, payload) {
		t.Fatalf("trailing decode: consumed=%d want=%d outputEqual=%v",
			consumed, memberEnd, bytes.Equal(got, payload))
	}

	second := encodeZlib(t, []byte("second"), zlib.BestSpeed)
	concatenated := append(append([]byte(nil), encoded...), second...)
	got, consumed, err = guardedGoInflate(t, concatenated, len(payload))
	if err != nil {
		t.Fatal(err)
	}
	if consumed != memberEnd || !bytes.Equal(got, payload) {
		t.Fatalf("concatenated decode: consumed=%d want=%d outputEqual=%v",
			consumed, memberEnd, bytes.Equal(got, payload))
	}
}

func TestInflatePackZlibTruncationAtEveryByte(t *testing.T) {
	payloads := [][]byte{
		nil,
		[]byte("small fixed stream"),
		bytes.Repeat([]byte("dynamic huffman payload"), 100),
		makeDeterministicBytes(8192),
	}
	levels := []int{
		zlib.DefaultCompression,
		zlib.HuffmanOnly,
		zlib.BestCompression,
		zlib.NoCompression,
	}

	for i, payload := range payloads {
		encoded := encodeZlib(t, payload, levels[i])
		memberEnd := len(encoded) - 4
		t.Run(fmt.Sprintf("case=%d", i), func(t *testing.T) {
			for cut := 0; cut <= len(encoded); cut++ {
				dst := make([]byte, len(payload))
				consumed, err := inflatePackZlibGo(encoded[:cut], dst)
				if cut < memberEnd {
					if err == nil || consumed != 0 {
						t.Fatalf("truncation at %d/%d accepted: consumed=%d err=%v",
							cut, memberEnd, consumed, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("cut %d includes complete DEFLATE stream: %v", cut, err)
				}
				if consumed != memberEnd || !bytes.Equal(dst, payload) {
					t.Fatalf("cut %d: consumed=%d want=%d outputEqual=%v",
						cut, consumed, memberEnd, bytes.Equal(dst, payload))
				}
			}
		})
	}
}

func TestInflatePackZlibRoundTripMatrix(t *testing.T) {
	levels := []int{
		zlib.NoCompression,
		zlib.HuffmanOnly,
		zlib.BestSpeed,
		zlib.DefaultCompression,
		zlib.BestCompression,
	}
	sizes := []int{0, 1, 2, 257, 258, 259, 32767, 32768, 32769, 65535, 65536}

	for i, size := range sizes {
		payloads := [][]byte{
			bytes.Repeat([]byte{byte(i)}, size),
			makeDeterministicBytes(size),
		}
		for pattern, payload := range payloads {
			level := levels[(i+pattern)%len(levels)]
			name := fmt.Sprintf("size=%d/pattern=%d/level=%d", size, pattern, level)
			t.Run(name, func(t *testing.T) {
				encoded := encodeZlib(t, payload, level)
				got, consumed, err := guardedGoInflate(t, encoded, len(payload))
				if err != nil {
					t.Fatal(err)
				}
				if consumed != len(encoded)-4 || !bytes.Equal(got, payload) {
					t.Fatalf("consumed=%d want=%d outputEqual=%v",
						consumed, len(encoded)-4, bytes.Equal(got, payload))
				}

				corruptTrailer := append([]byte(nil), encoded...)
				corruptTrailer[len(corruptTrailer)-1] ^= 0xff
				got, consumed, err = guardedGoInflate(t, corruptTrailer, len(payload))
				if err != nil || consumed != len(encoded)-4 || !bytes.Equal(got, payload) {
					t.Fatalf("Adler-only corruption changed pack decoder result: consumed=%d err=%v",
						consumed, err)
				}
			})
		}
	}
}

func TestInflatePackZlibSingleBitDifferential(t *testing.T) {
	payloads := [][]byte{
		[]byte("fixed fixture"),
		bytes.Repeat([]byte("dynamic fixture "), 40),
	}
	levels := []int{zlib.HuffmanOnly, zlib.DefaultCompression}

	for i, payload := range payloads {
		encoded := encodeZlib(t, payload, levels[i])
		memberEnd := len(encoded) - 4
		for at := 0; at < memberEnd; at++ {
			for bit := byte(1); bit != 0; bit <<= 1 {
				mutated := append([]byte(nil), encoded...)
				mutated[at] ^= bit
				assertGoMatchesReference(t, mutated, len(payload))
			}
		}
	}
}

// TestInflatePackZlibTruncatedPrefixesReportUnexpectedEOF pins the
// truncation-error contract: every strict prefix of a valid member (cut
// anywhere in the deflate payload, including mid-header, mid-dynamic-table,
// and mid-codeword) must classify as truncation via
// errors.Is(err, io.ErrUnexpectedEOF), never as generic bad data.
func TestInflatePackZlibTruncatedPrefixesReportUnexpectedEOF(t *testing.T) {
	type fixture struct {
		encoded []byte
		size    int
	}
	var fixtures []fixture

	payloads := [][]byte{
		[]byte("fixed fixture"),
		bytes.Repeat([]byte("dynamic fixture "), 40),
		makeDeterministicBytes(512),
	}
	levels := []int{zlib.HuffmanOnly, zlib.DefaultCompression, zlib.NoCompression}
	for i, payload := range payloads {
		fixtures = append(fixtures, fixture{encodeZlib(t, payload, levels[i]), len(payload)})
	}

	// A dynamic-block member (BTYPE=2), so cuts land inside the 14-bit
	// counts, the precode lengths, and the code-length symbol stream.
	dynamic, err := hex.DecodeString(dynamicHuffmanVectorHex)
	if err != nil {
		t.Fatal(err)
	}
	fixtures = append(fixtures, fixture{dynamic, 6000})

	for i, f := range fixtures {
		memberEnd := len(f.encoded) - 4
		for cut := 2; cut < memberEnd; cut++ {
			_, consumed, err := guardedGoInflate(t, f.encoded[:cut], f.size)
			if err == nil {
				t.Fatalf("fixture=%d cut=%d: truncated stream accepted", i, cut)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("fixture=%d cut=%d: got %v, want unexpected-EOF identity",
					i, cut, err)
			}
			if consumed != 0 {
				t.Fatalf("fixture=%d cut=%d: error returned consumed=%d, want 0",
					i, cut, consumed)
			}
		}
	}
}

func guardedGoInflate(tb testing.TB, src []byte, size int) ([]byte, int, error) {
	tb.Helper()

	// guardSize must be >= deflateFastMatchCopy: the fast loop's worst-case
	// whole-word match copy can overwrite up to that many bytes at once, so
	// a narrower guard would let an out-of-dst overrun land entirely past
	// the pattern and go undetected on platforms without the mprotect
	// guard-page test.
	const guardSize = 2 * deflateFastMatchCopy
	guarded := bytes.Repeat([]byte{0xa5}, guardSize+size+guardSize)
	dst := guarded[guardSize : guardSize+size : guardSize+size]
	srcBefore := append([]byte(nil), src...)

	consumed, err := inflatePackZlibGo(src, dst)
	if !bytes.Equal(src, srcBefore) {
		tb.Fatal("decoder mutated source")
	}
	if !allByte(guarded[:guardSize], 0xa5) || !allByte(guarded[guardSize+size:], 0xa5) {
		tb.Fatal("decoder wrote outside destination")
	}
	return append([]byte(nil), dst...), consumed, err
}

func allByte(p []byte, want byte) bool {
	for _, b := range p {
		if b != want {
			return false
		}
	}
	return true
}

func validPackZlibHeader(cmf, flg byte) bool {
	return cmf&0x0f == 8 &&
		cmf>>4 <= 7 &&
		flg&0x20 == 0 &&
		(uint16(cmf)<<8|uint16(flg))%31 == 0
}

func encodeZlib(tb testing.TB, payload []byte, level int) []byte {
	tb.Helper()

	var encoded bytes.Buffer
	zw, err := zlib.NewWriterLevel(&encoded, level)
	if err != nil {
		tb.Fatal(err)
	}
	for off := 0; off < len(payload); {
		n := min(1+(off%97), len(payload)-off)
		if _, err := zw.Write(payload[off : off+n]); err != nil {
			tb.Fatal(err)
		}
		off += n
	}
	if err := zw.Close(); err != nil {
		tb.Fatal(err)
	}
	return encoded.Bytes()
}

func makeDeterministicBytes(n int) []byte {
	p := make([]byte, n)
	x := uint64(0x9e3779b97f4a7c15)
	for i := range p {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		p[i] = byte(x)
	}
	return p
}

func assertGoMatchesReference(t *testing.T, src []byte, size int) {
	t.Helper()

	got, consumed, gotErr := guardedGoInflate(t, src, size)
	want, wantConsumed, wantErr := referencePackInflate(src, size)
	if (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("acceptance mismatch for %x: go consumed=%d err=%v, reference consumed=%d err=%v",
			src, consumed, gotErr, wantConsumed, wantErr)
	}
	if gotErr != nil {
		if consumed != 0 {
			t.Fatalf("error returned consumed=%d, want 0", consumed)
		}
		return
	}
	if consumed != wantConsumed || !bytes.Equal(got, want) {
		t.Fatalf("result mismatch: consumed=%d want=%d outputEqual=%v",
			consumed, wantConsumed, bytes.Equal(got, want))
	}
}

func referencePackInflate(src []byte, size int) ([]byte, int, error) {
	if len(src) < 2 || !validPackZlibHeader(src[0], src[1]) {
		return nil, 0, errors.New("invalid pack zlib header")
	}

	br := bytes.NewReader(src[2:])
	fr := stdflate.NewReader(br)
	out, err := io.ReadAll(io.LimitReader(fr, int64(size)+1))
	closeErr := fr.Close()
	if err != nil {
		return nil, 0, err
	}
	if closeErr != nil {
		return nil, 0, closeErr
	}
	if len(out) != size {
		return nil, 0, fmt.Errorf("output size %d, want %d", len(out), size)
	}
	return out, 2 + len(src[2:]) - br.Len(), nil
}

type deflateTestBits struct {
	data []byte
	n    uint
}

func (w *deflateTestBits) write(v uint64, n uint) {
	for i := uint(0); i < n; i++ {
		if w.n&7 == 0 {
			w.data = append(w.data, 0)
		}
		w.data[len(w.data)-1] |= byte((v>>i)&1) << (w.n & 7)
		w.n++
	}
}

func (w *deflateTestBits) writeFixedLitlen(sym int) {
	var code, n int
	switch {
	case sym <= 143:
		code, n = 0x30+sym, 8
	case sym <= 255:
		code, n = 0x190+(sym-144), 9
	case sym <= 279:
		code, n = sym-256, 7
	default:
		code, n = 0xc0+(sym-280), 8
	}
	w.write(uint64(reverseCode(code, n)), uint(n))
}

func (w *deflateTestBits) writeFixedOffset(sym int) {
	w.write(uint64(reverseCode(sym, 5)), 5)
}

func (w *deflateTestBits) writeFixedLength(length int) {
	if length == 258 {
		w.writeFixedLitlen(285)
		return
	}
	for i := 0; i < len(lengthBases)-1; i++ {
		base := int(lengthBases[i])
		if length < base || length >= int(lengthBases[i+1]) {
			continue
		}
		extraBits := uint(lengthExtraBits[i])
		w.writeFixedLitlen(257 + i)
		w.write(uint64(length-base), extraBits)
		return
	}
	panic("invalid fixed match length")
}

func (w *deflateTestBits) writeFixedDistance(distance int) {
	for sym, base16 := range offsetBases {
		base := int(base16)
		extraBits := uint(offsetExtraBits[sym])
		if distance < base || distance >= base+1<<extraBits {
			continue
		}
		w.writeFixedOffset(sym)
		w.write(uint64(distance-base), extraBits)
		return
	}
	panic("invalid fixed distance")
}

func (w *deflateTestBits) bytes() []byte {
	return append([]byte(nil), w.data...)
}

func incompleteEmptyOffsetStream() []byte {
	var w deflateTestBits
	w.write(1, 1)
	w.write(2, 2)
	w.write(0, 5)
	w.write(0, 5)
	w.write(14, 4)

	w.write(0, 3)
	w.write(0, 3)
	w.write(1, 3)
	w.write(3, 3)
	for range 11 {
		w.write(0, 3)
	}
	w.write(2, 3)
	w.write(0, 3)
	w.write(3, 3)

	w.write(0, 1)
	w.write(54, 7)
	w.write(7, 3)
	w.write(1, 2)
	w.write(0, 1)
	w.write(89, 7)
	w.write(0, 1)
	w.write(78, 7)
	w.write(1, 2)
	w.write(3, 3)

	w.write(0, 1)
	w.write(1, 2)
	w.write(0, 1)
	w.write(0, 1)
	w.write(3, 2)
	return w.bytes()
}

func incompleteSingletonLitlenStream() []byte {
	var w deflateTestBits
	w.write(1, 1)
	w.write(2, 2)
	w.write(0, 5)
	w.write(0, 5)
	w.write(14, 4)

	w.write(0, 3)
	w.write(0, 3)
	w.write(1, 3)
	w.write(2, 3)
	for range 13 {
		w.write(0, 3)
	}
	w.write(2, 3)

	for range 2 {
		w.write(0, 1)
		w.write(117, 7)
	}
	w.write(3, 2)
	w.write(1, 2)
	w.write(0, 1)
	return w.bytes()
}

func incompleteSingletonOffsetStream(nonzero bool) []byte {
	var w deflateTestBits
	w.write(1, 1)
	w.write(2, 2)
	w.write(1, 5)
	if nonzero {
		w.write(1, 5)
	} else {
		w.write(0, 5)
	}
	w.write(14, 4)

	w.write(0, 3)
	w.write(0, 3)
	if nonzero {
		w.write(2, 3)
		w.write(2, 3)
		for range 11 {
			w.write(0, 3)
		}
		w.write(2, 3)
		w.write(0, 3)
		w.write(2, 3)

		w.write(3, 2)
		w.write(117, 7)
		w.write(3, 2)
		w.write(115, 7)
		for range 4 {
			w.write(1, 2)
		}
		w.write(0, 2)
		w.write(2, 2)

		w.write(0, 2)
		w.write(2, 2)
		w.write(3, 2)
		w.write(0, 1)
		w.write(1, 2)
		return w.bytes()
	}

	w.write(1, 3)
	for range 12 {
		w.write(0, 3)
	}
	w.write(2, 3)
	w.write(0, 3)
	w.write(2, 3)

	w.write(0, 1)
	w.write(117, 7)
	w.write(0, 1)
	w.write(116, 7)
	w.write(1, 2)
	w.write(3, 2)
	w.write(3, 2)
	w.write(1, 2)

	w.write(0, 1)
	w.write(3, 2)
	w.write(0, 1)
	w.write(1, 2)
	return w.bytes()
}

// reservedAlphabetCountStream builds a dynamic block whose HLIT or HDIST
// field carries a reserved value (HLIT=30 encodes 287 literal/length codes;
// HDIST=30 encodes 31 distance codes). Everything else in the stream is
// well-formed and the unused trailing code lengths are zero, so only the
// reserved count itself makes the stream invalid (RFC 1951 section 3.2.7).
func reservedAlphabetCountStream(litlen bool) []byte {
	var w deflateTestBits
	w.write(1, 1) // BFINAL
	w.write(2, 2) // BTYPE = dynamic
	if litlen {
		w.write(30, 5) // HLIT = 30 -> 287 litlen codes (valid max is 29 -> 286)
		w.write(0, 5)  // HDIST = 0 -> 1 offset code
	} else {
		w.write(0, 5)  // HLIT = 0 -> 257 litlen codes
		w.write(30, 5) // HDIST = 30 -> 31 offset codes (valid max is 29 -> 30)
	}
	w.write(14, 4) // HCLEN = 14 -> 18 precode lengths

	// Precode lengths in precodeOrder: sym18=1, sym0=2, sym1=2.
	w.write(0, 3) // 16
	w.write(0, 3) // 17
	w.write(1, 3) // 18
	w.write(2, 3) // 0
	for range 13 {
		w.write(0, 3)
	}
	w.write(2, 3) // 1

	// Canonical precode, bit-reversed for the stream: sym18 -> 0 (1 bit),
	// sym1 -> 3 (2 bits). Code lengths for all litlen+offset entries:
	// lit 0 and EOB get 1-bit codes; every other litlen length is zero.
	w.write(3, 2) // lens[0] = 1 (literal 0)
	w.write(0, 1)
	w.write(127, 7) // repeat-zero 138 (symbols 1..138)
	w.write(0, 1)
	w.write(106, 7) // repeat-zero 117 (symbols 139..255)
	w.write(3, 2)   // lens[256] = 1 (end of block)
	if litlen {
		w.write(0, 1)
		w.write(19, 7) // repeat-zero 30 (reserved litlen symbols 257..286)
		w.write(3, 2)  // singleton offset symbol 0
	} else {
		w.write(3, 2) // offset symbol 0 = 1 bit
		w.write(0, 1)
		w.write(19, 7) // repeat-zero 30 (reserved offset symbols 1..30)
	}

	// Block body: immediate end of block (1-bit code 1).
	w.write(1, 1)
	return w.bytes()
}

func tooManyCodewordLengthsStream() []byte {
	var w deflateTestBits
	w.write(1, 1)
	w.write(2, 2)
	w.write(0, 5)
	w.write(0, 5)
	w.write(14, 4)

	w.write(0, 3)
	w.write(0, 3)
	w.write(1, 3)
	w.write(0, 3)
	for range 13 {
		w.write(0, 3)
	}
	w.write(1, 3)

	w.write(1, 1)
	w.write(117, 7)
	w.write(1, 1)
	w.write(116, 7)
	w.write(0, 1)
	w.write(0, 1)
	w.write(1, 1)
	w.write(117, 7)
	w.write(1, 1)
	return w.bytes()
}

func overreadStream() []byte {
	var w deflateTestBits
	w.write(0, 1)
	w.write(2, 2)
	w.write(0, 5)
	w.write(0, 5)
	w.write(14, 4)

	w.write(0, 3)
	w.write(0, 3)
	w.write(1, 3)
	for range 14 {
		w.write(0, 3)
	}
	w.write(1, 3)

	w.write(0, 1)
	w.write(1, 1)
	w.write(117, 7)
	w.write(1, 1)
	w.write(116, 7)
	w.write(0, 1)
	w.write(0, 1)
	return w.bytes()
}
