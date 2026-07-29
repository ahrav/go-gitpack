package objstore

import (
	"bytes"
	"compress/zlib"
	"testing"
)

func FuzzInflatePackZlibRoundTrip(f *testing.F) {
	f.Add([]byte(nil), uint8(0))
	f.Add([]byte("hello"), uint8(1))
	f.Add(bytes.Repeat([]byte("abc"), 100), uint8(2))
	f.Add(makeDeterministicBytes(1024), uint8(4))

	levels := [...]int{
		zlib.NoCompression,
		zlib.HuffmanOnly,
		zlib.BestSpeed,
		zlib.DefaultCompression,
		zlib.BestCompression,
	}

	f.Fuzz(func(t *testing.T, payload []byte, mode uint8) {
		// The cap must exceed inflateFastYieldBytes (64 KiB) plus
		// deflateFastOutputMargin so generated payloads can drive the
		// assembly fast loops (ARM64 and AMD64) through their
		// yield-and-resume path; a 64 KiB cap would make that path
		// unreachable from this target.
		if len(payload) > 128<<10 {
			payload = payload[:128<<10]
		}
		encoded := encodeZlib(t, payload, levels[int(mode)%len(levels)])
		memberEnd := len(encoded) - 4
		encoded = append(encoded, 0xde, 0xad, 0xbe, 0xef)

		got, consumed, err := guardedGoInflate(t, encoded, len(payload))
		if err != nil {
			t.Fatal(err)
		}
		if consumed != memberEnd {
			t.Fatalf("consumed %d bytes, want %d", consumed, memberEnd)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("round-trip output mismatch")
		}
	})
}

func FuzzInflatePackZlibDifferential(f *testing.F) {
	seeds := []struct {
		src  []byte
		size uint16
	}{
		{src: []byte{0x78, 0x9c, 0x03, 0x00}, size: 0},
		{src: []byte{0x78, 0x9c, 0x07}, size: 0},
		{src: []byte{0x78, 0x01, 0x01, 0x01, 0x00, 0xfe, 0xff, 0x11}, size: 1},
	}
	for _, seed := range seeds {
		f.Add(seed.src, seed.size)
	}

	f.Fuzz(func(t *testing.T, src []byte, size uint16) {
		if len(src) > 4<<10 {
			src = src[:4<<10]
		}
		assertGoMatchesReference(t, src, int(size))
	})
}
