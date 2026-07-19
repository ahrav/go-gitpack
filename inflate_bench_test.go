package objstore

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/compress/flate"
)

//go:embed README.md blog/md/*.md
var inflateBenchmarkText embed.FS

func BenchmarkInflateOneShot(b *testing.B) {
	sizes := []int{64, 512, 4 << 10, 16 << 10, 64 << 10, 1 << 20}
	patterns := []struct {
		name string
		make func(int) []byte
	}{
		{
			name: "repeated",
			make: func(n int) []byte {
				return bytes.Repeat([]byte("package objstore\n"), (n+16)/17)[:n]
			},
		},
		{name: "text", make: makeBenchmarkText},
		{name: "random", make: makeDeterministicBytes},
	}

	for _, size := range sizes {
		for _, pattern := range patterns {
			payload := pattern.make(size)
			src := encodeZlib(b, payload, 6)
			name := fmt.Sprintf("%s/%d", pattern.name, size)

			b.Run(name+"/go-direct", func(b *testing.B) {
				benchmarkInflater(b, src, payload, inflatePackZlibGo)
			})
			b.Run(name+"/go-streaming", func(b *testing.B) {
				benchmarkInflater(b, src, payload, inflatePackZlibStreaming)
			})
			if libdeflateAvailable {
				b.Run(name+"/libdeflate-cgo", func(b *testing.B) {
					benchmarkInflater(b, src, payload, inflateZlibOneShot)
				})
			}
		}
	}
}

func BenchmarkInflateDynamicTables(b *testing.B) {
	payload := makeBenchmarkText(1 << 20)
	src := encodeZlib(b, payload, 6)
	r := deflateBits{src: src[2:]}
	header, ok := r.read(3)
	if !ok || (header>>1)&3 != 2 {
		b.Fatal("benchmark stream does not begin with a dynamic block")
	}

	var d goInflater
	if probe := r; !d.loadDynamicTables(&probe) {
		b.Fatal("failed to parse benchmark dynamic tables")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next := r
		if !d.loadDynamicTables(&next) {
			b.Fatal("failed to parse benchmark dynamic tables")
		}
	}
}

func makeBenchmarkText(n int) []byte {
	names := [...]string{
		"README.md",
		"blog/md/part1.md",
		"blog/md/part2.md",
		"blog/md/part3.md",
		"blog/md/part4.md",
		"blog/md/part5.md",
	}
	var corpus []byte
	for _, name := range names {
		data, err := inflateBenchmarkText.ReadFile(name)
		if err != nil {
			panic(err)
		}
		corpus = append(corpus, data...)
	}
	return bytes.Repeat(corpus, (n+len(corpus)-1)/len(corpus))[:n]
}

func benchmarkInflater(
	b *testing.B,
	src, want []byte,
	fn func([]byte, []byte) (int, error),
) {
	b.Helper()

	dst := make([]byte, len(want))
	consumed, err := fn(src, dst)
	if err != nil {
		b.Fatal(err)
	}
	if consumed != len(src)-4 || !bytes.Equal(dst, want) {
		b.Fatalf("invalid warmup: consumed=%d want=%d outputEqual=%v",
			consumed, len(src)-4, bytes.Equal(dst, want))
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(want)))
	b.ResetTimer()
	for b.Loop() {
		if _, err := fn(src, dst); err != nil {
			b.Fatal(err)
		}
	}
}

func inflatePackZlibStreaming(src, dst []byte) (int, error) {
	if len(src) < 2 {
		return 0, errDeflateBadData
	}
	if err := validateZlibHeader(src[0], src[1]); err != nil {
		return 0, err
	}

	br := getBytesReader(src[2:])
	fr := flatePool.Get().(io.ReadCloser)
	if err := fr.(flate.Resetter).Reset(br, nil); err != nil {
		flatePool.Put(fr)
		putBytesReader(br)
		return 0, err
	}

	_, readErr := io.ReadFull(fr, dst)
	endErr := error(nil)
	if readErr == nil {
		endErr = ensureZlibStreamEnd(fr)
	}
	putFlateReader(fr)
	consumed := 2 + len(src[2:]) - br.Len()
	putBytesReader(br)
	if readErr != nil {
		return 0, readErr
	}
	if endErr != nil {
		return 0, endErr
	}

	// bytes.Reader is consumed exactly to the DEFLATE byte boundary by this
	// fork's byte-at-a-time bit refill.
	return consumed, nil
}
