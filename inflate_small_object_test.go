package objstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
)

// smallObjectSizes brackets every boundary of the small-object decode path:
// the empty object, sizes around deflateFastOutputMargin where the fast loop
// used to hand the whole object to the tail decoder, and sizes around
// smallDecodeLimit where decoding switches back to the caller's buffer.
var smallObjectSizes = []int{
	0, 1, 2, 7, 8, 9, 31, 32, 63, 64, 255,
	deflateFastOutputMargin - 1, deflateFastOutputMargin, deflateFastOutputMargin + 1,
	511, 512, 1000, 1131, 4095, 4096,
	smallDecodeLimit - 1, smallDecodeLimit, smallDecodeLimit + 1, smallDecodeLimit + 1000,
}

// TestInflateSmallObjectRoundTrip decodes compressible, incompressible, and
// repetitive payloads of every bracketed size at several compression levels
// and requires byte-exact output plus the exact consumed length, with the
// destination guarded on both sides.
func TestInflateSmallObjectRoundTrip(t *testing.T) {
	patterns := map[string]func(int) []byte{
		"text":     makeBenchmarkText,
		"random":   makeDeterministicBytes,
		"repeated": func(n int) []byte { return bytes.Repeat([]byte("tree 0123456789abcdef\n"), n/22+1)[:n] },
	}
	levels := []int{0, 1, 6, 9, -2}
	for name, mk := range patterns {
		for _, size := range smallObjectSizes {
			payload := mk(size)
			for _, level := range levels {
				encoded := encodeZlib(t, payload, level)
				memberEnd := len(encoded) - 4
				encoded = append(encoded, 0xde, 0xad, 0xbe, 0xef)

				got, consumed, err := guardedGoInflate(t, encoded, size)
				if err != nil {
					t.Fatalf("%s size=%d level=%d: %v", name, size, level, err)
				}
				if consumed != memberEnd {
					t.Fatalf("%s size=%d level=%d: consumed %d, want %d", name, size, level, consumed, memberEnd)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("%s size=%d level=%d: output mismatch", name, size, level)
				}
			}
		}
	}
}

// TestInflateSmallObjectDeclaredSizeMismatch pins the error classes when the
// declared size disagrees with a valid stream: a stream that ends past the
// declared size is an overrun whether the excess fits the slack (a few bytes)
// or exceeds it (hundreds of bytes), and a stream that ends short is a
// truncation-class failure. Every case also satisfies the differential
// oracle against compress/flate.
func TestInflateSmallObjectDeclaredSizeMismatch(t *testing.T) {
	payload := makeBenchmarkText(600)
	encoded := encodeZlib(t, payload, 6)

	for _, excess := range []int{1, 2, 50, deflateFastOutputMargin - 1, deflateFastOutputMargin, deflateFastOutputMargin + 1, 599} {
		declared := len(payload) - excess
		_, consumed, err := guardedGoInflate(t, encoded, declared)
		if !errors.Is(err, errZlibStreamOverrun) {
			t.Fatalf("declared %d (excess %d): got %v, want the stream-overrun class", declared, excess, err)
		}
		if consumed != 0 {
			t.Fatalf("declared %d: consumed %d on error, want 0", declared, consumed)
		}
		assertGoMatchesReference(t, encoded, declared)
	}

	for _, short := range []int{1, 2, 100, deflateFastOutputMargin, 1000} {
		declared := len(payload) + short
		_, _, err := guardedGoInflate(t, encoded, declared)
		if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("declared %d (short %d): got %v, want unexpected-EOF identity", declared, short, err)
		}
		if errors.Is(err, errZlibStreamOverrun) {
			t.Fatalf("declared %d: short output misclassified as overrun", declared)
		}
		assertGoMatchesReference(t, encoded, declared)
	}
}

// TestInflateSmallObjectCorruptionDifferential flips every bit of a small
// dynamic-block member and a stored member and requires the decoder to
// agree with the reference on acceptance and error class.
func TestInflateSmallObjectCorruptionDifferential(t *testing.T) {
	members := []struct {
		payload []byte
		level   int
	}{
		{makeBenchmarkText(700), 6},
		{makeDeterministicBytes(300), 0},
		{bytes.Repeat([]byte("ab"), 150), 9},
	}
	for _, m := range members {
		encoded := encodeZlib(t, m.payload, m.level)
		memberEnd := len(encoded) - 4
		for at := 0; at < memberEnd; at++ {
			for bit := byte(1); bit != 0; bit <<= 1 {
				mutated := append([]byte(nil), encoded...)
				mutated[at] ^= bit
				assertGoMatchesReference(t, mutated, len(m.payload))
			}
		}
	}
}

// TestInflateSmallObjectPooledScratchIsolation decodes objects of different
// sizes back to back through the pooled inflater and checks that no stale
// slack bytes from an earlier, larger decode leak into a later, smaller one.
func TestInflateSmallObjectPooledScratchIsolation(t *testing.T) {
	big := bytes.Repeat([]byte{0xee}, 4000)
	small := makeBenchmarkText(37)
	bigEnc := encodeZlib(t, big, 6)
	smallEnc := encodeZlib(t, small, 6)
	for i := 0; i < 50; i++ {
		got, _, err := guardedGoInflate(t, bigEnc, len(big))
		if err != nil || !bytes.Equal(got, big) {
			t.Fatalf("big decode %d: err=%v equal=%v", i, err, bytes.Equal(got, big))
		}
		got, _, err = guardedGoInflate(t, smallEnc, len(small))
		if err != nil || !bytes.Equal(got, small) {
			t.Fatalf("small decode %d: err=%v got=%q", i, err, got)
		}
	}
}

// TestBuildPrecodeTableMatchesGeneric checks the specialized precode table
// builder against the generic builder on random length assignments,
// including over-subscribed, incomplete, empty, and singleton codes: both
// must agree on acceptance, table width, and every entry of the table.
func TestBuildPrecodeTableMatchesGeneric(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	var want, got goInflater
	checked := 0
	for iter := 0; iter < 20000; iter++ {
		var lens [19]uint8
		var count [precodeTableBits + 1]int
		used := 1 + rng.IntN(19)
		maxLen := 1 + rng.IntN(precodeTableBits)
		for i := 0; i < used; i++ {
			sym := rng.IntN(19)
			n := uint8(1 + rng.IntN(maxLen))
			if rng.IntN(4) == 0 {
				n = 0
			}
			lens[sym] = n
		}
		if iter%97 == 0 {
			// Exact canonical assignments are the common case in real
			// streams; build one so complete codes are well represented.
			lens = [19]uint8{}
			k := 1 + rng.IntN(7)
			for i := 0; i < 1<<k && i < 19; i++ {
				lens[i] = uint8(k)
			}
			if 1<<k > 19 {
				// Fill the remaining code space with a shorter code.
				lens = [19]uint8{}
				lens[0], lens[1] = 1, 1
			}
		}
		for _, n := range lens {
			count[n]++
		}
		count[0] = 0

		want.precodeLens = lens
		got.precodeLens = lens
		clear(want.precode[:])
		clear(got.precode[:])

		wantBits, wantOK := want.buildTable(want.precode[:], want.precodeLens[:], precodeTable, precodeTableBits, 7, false)
		gotBits, gotOK := got.buildPrecodeTable(&count)
		if wantOK != gotOK || wantBits != gotBits {
			t.Fatalf("lens=%v: generic (bits=%d ok=%v) vs specialized (bits=%d ok=%v)", lens, wantBits, wantOK, gotBits, gotOK)
		}
		if !gotOK {
			continue
		}
		checked++
		n := 1 << gotBits
		if wantBits == 1 && !bytes.Equal(u32bytes(want.precode[:2]), u32bytes(got.precode[:2])) {
			t.Fatalf("lens=%v: singleton tables differ", lens)
		}
		for i := 0; i < n; i++ {
			if want.precode[i] != got.precode[i] {
				t.Fatalf("lens=%v: entry %d differs: generic %#x specialized %#x", lens, i, want.precode[i], got.precode[i])
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("only %d valid codes exercised; generator too strict", checked)
	}
}

func u32bytes(v []uint32) []byte {
	out := make([]byte, 0, 4*len(v))
	for _, x := range v {
		out = append(out, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
	}
	return out
}

// TestInflateDynamicTablesRandomAlphabets compresses payloads drawn from
// alphabets of varying size (2 to 256 symbols) so the dynamic header
// exercises code lengths from 1 to 15 bits, many zero runs, and litlen
// subtables, and requires exact round trips.
func TestInflateDynamicTablesRandomAlphabets(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	for iter := 0; iter < 300; iter++ {
		alpha := 2 + rng.IntN(255)
		size := 1 + rng.IntN(3000)
		payload := make([]byte, size)
		for i := range payload {
			// Skewed draw: a few symbols dominate, the rest are rare, which
			// pushes rare symbols to long codes.
			if rng.IntN(3) == 0 {
				payload[i] = byte(rng.IntN(alpha))
			} else {
				payload[i] = byte(rng.IntN(min(alpha, 4)))
			}
		}
		encoded := encodeZlib(t, payload, 9)
		got, _, err := guardedGoInflate(t, encoded, size)
		if err != nil {
			t.Fatalf("iter %d alpha=%d size=%d: %v", iter, alpha, size, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("iter %d alpha=%d size=%d: %s", iter, alpha, size, fmt.Sprintf("output mismatch at %d", firstDiff(got, payload)))
		}
	}
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}
