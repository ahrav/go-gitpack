//go:build arm64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"unsafe"
)

func TestInflateHuffmanFastArm64FixedDifferential(t *testing.T) {
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
		{length: 257, distance: 257},
		{length: 258, distance: 32768},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("length=%d/distance=%d", tt.length, tt.distance)
		t.Run(name, func(t *testing.T) {
			src, want := arm64FixedMatchBlock(tt.distance, tt.length, 40)
			d, r := arm64PrepareHuffmanBlock(t, src, 1)
			dstLen := len(want) + deflateFastOutputMargin + 1

			reference := arm64RunHuffmanGo(d, r, dstLen)
			if reference.err != nil {
				t.Fatalf("decodeHuffmanGo: %v", reference.err)
			}
			if !bytes.Equal(reference.dst[:reference.out], want) {
				t.Fatal("reference output does not match fixture")
			}

			dispatch := arm64RunHuffmanDispatch(d, r, dstLen)
			arm64CompareHuffmanResults(t, dispatch, reference)

			kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
			arm64CompareHuffmanResults(t, kernel, reference)
			arm64RequireStatuses(t, statuses, inflateFastBlockDone)
		})
	}
}

func TestInflateHuffmanFastArm64DynamicDifferential(t *testing.T) {
	encoded, err := hex.DecodeString(dynamicHuffmanVectorHex)
	if err != nil {
		t.Fatal(err)
	}
	src := append([]byte(nil), encoded[2:len(encoded)-4]...)
	src = append(src, make([]byte, 40)...)
	want := bytes.Repeat([]byte("abcabc"), 1000)
	d, r := arm64PrepareHuffmanBlock(t, src, 2)
	dstLen := len(want) + deflateFastOutputMargin + 1

	reference := arm64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil {
		t.Fatalf("decodeHuffmanGo: %v", reference.err)
	}
	if !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatal("reference output does not match fixture")
	}

	dispatch := arm64RunHuffmanDispatch(d, r, dstLen)
	arm64CompareHuffmanResults(t, dispatch, reference)

	kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
	arm64CompareHuffmanResults(t, kernel, reference)
	arm64RequireStatuses(t, statuses, inflateFastBlockDone)
}

func TestInflateHuffmanFastArm64FallbackDifferential(t *testing.T) {
	payload := []byte("fallback")

	t.Run("input margin", func(t *testing.T) {
		src := arm64FixedLiteralBlock(payload, 0)
		if len(src) > deflateFastInputMargin {
			t.Fatalf("fixture is %d bytes, want at most %d", len(src), deflateFastInputMargin)
		}
		src = append(src, make([]byte, deflateFastInputMargin-len(src))...)
		d, r := arm64PrepareHuffmanBlock(t, src, 1)
		dstLen := len(payload) + deflateFastOutputMargin + 1

		reference := arm64RunHuffmanGo(d, r, dstLen)
		dispatch := arm64RunHuffmanDispatch(d, r, dstLen)
		arm64CompareHuffmanResults(t, dispatch, reference)

		kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
		arm64CompareHuffmanResults(t, kernel, reference)
		arm64RequireStatuses(t, statuses, inflateFastTail)
	})

	t.Run("output margin", func(t *testing.T) {
		src := arm64FixedLiteralBlock(payload, 40)
		d, r := arm64PrepareHuffmanBlock(t, src, 1)

		reference := arm64RunHuffmanGo(d, r, deflateFastOutputMargin)
		dispatch := arm64RunHuffmanDispatch(d, r, deflateFastOutputMargin)
		arm64CompareHuffmanResults(t, dispatch, reference)

		kernel, statuses := arm64RunHuffmanKernel(t, d, r, deflateFastOutputMargin)
		arm64CompareHuffmanResults(t, kernel, reference)
		arm64RequireStatuses(t, statuses, inflateFastTail)
	})
}

func TestInflateHuffmanFastArm64YieldDifferential(t *testing.T) {
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

	d, r := arm64PrepareHuffmanBlock(t, src, 1)
	dstLen := len(want) + deflateFastOutputMargin + 1
	reference := arm64RunHuffmanGo(d, r, dstLen)
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	dispatch := arm64RunHuffmanDispatch(d, r, dstLen)
	arm64CompareHuffmanResults(t, dispatch, reference)

	kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)
	arm64CompareHuffmanResults(t, kernel, reference)
	arm64RequireStatuses(t, statuses, inflateFastYield, inflateFastBlockDone)
}

func TestInflateHuffmanFastArm64BadDataStatus(t *testing.T) {
	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
	w.writeFixedLitlen(286)
	src := append(w.bytes(), make([]byte, 40)...)

	d, r := arm64PrepareHuffmanBlock(t, src, 1)
	const dstLen = deflateFastOutputMargin + 1
	reference := arm64RunHuffmanGo(d, r, dstLen)
	kernel, statuses := arm64RunHuffmanKernel(t, d, r, dstLen)

	if reference.err != errDeflateBadData || kernel.err != reference.err {
		t.Fatalf("error mismatch: kernel=%v reference=%v", kernel.err, reference.err)
	}
	if kernel.out != reference.out {
		t.Fatalf("output position mismatch: kernel=%d reference=%d", kernel.out, reference.out)
	}
	if !bytes.Equal(kernel.dst[:kernel.out], reference.dst[:reference.out]) {
		t.Fatal("output mismatch")
	}
	arm64RequireStatuses(t, statuses, inflateFastBadData)
}

type arm64HuffmanResult struct {
	r   deflateBits
	dst []byte
	out int
	err error
}

func arm64PrepareHuffmanBlock(
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

func arm64RunHuffmanGo(d goInflater, r deflateBits, dstLen int) arm64HuffmanResult {
	dst := make([]byte, dstLen)
	out, err := d.decodeHuffmanGo(&r, dst, 0)
	return arm64HuffmanResult{r: r, dst: dst, out: out, err: err}
}

func arm64RunHuffmanDispatch(d goInflater, r deflateBits, dstLen int) arm64HuffmanResult {
	dst := make([]byte, dstLen)
	out, err := d.decodeHuffman(&r, dst, 0)
	return arm64HuffmanResult{r: r, dst: dst, out: out, err: err}
}

func arm64RunHuffmanKernel(
	t *testing.T,
	d goInflater,
	r deflateBits,
	dstLen int,
) (arm64HuffmanResult, []inflateFastStatus) {
	t.Helper()

	if len(r.src) < deflateFastInputMargin || dstLen < deflateFastOutputMargin {
		t.Fatalf("invalid direct-kernel margins: src=%d dst=%d", len(r.src), dstLen)
	}
	dst := make([]byte, dstLen)
	return arm64RunHuffmanKernelInto(t, d, r, dst)
}

func arm64RunHuffmanKernelInto(
	t *testing.T,
	d goInflater,
	r deflateBits,
	dst []byte,
) (arm64HuffmanResult, []inflateFastStatus) {
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
		inflateHuffmanFastArm64(&state)
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
			return arm64HuffmanResult{r: r, dst: dst, out: out, err: err}, statuses
		case inflateFastBlockDone:
			return arm64HuffmanResult{r: r, dst: dst, out: out}, statuses
		case inflateFastBadData:
			return arm64HuffmanResult{
				r: r, dst: dst, out: out, err: errDeflateBadData,
			}, statuses
		default:
			t.Fatalf("invalid kernel status %d", state.status)
		}
	}
	t.Fatal("kernel did not terminate after 100 calls")
	return arm64HuffmanResult{}, nil
}

func arm64CompareHuffmanResults(
	t *testing.T,
	got, want arm64HuffmanResult,
) {
	t.Helper()

	if got.err != want.err {
		t.Fatalf("error mismatch: got=%v want=%v", got.err, want.err)
	}
	if got.out != want.out {
		t.Fatalf("output position mismatch: got=%d want=%d", got.out, want.out)
	}
	if !bytes.Equal(got.dst[:got.out], want.dst[:want.out]) {
		t.Fatal("output bytes mismatch")
	}
	if got.r.consumed() != want.r.consumed() || !arm64SameUnreadBits(got.r, want.r) {
		t.Fatalf(
			"input state mismatch: got={pos:%d nbits:%d buf:%016x consumed:%d} "+
				"want={pos:%d nbits:%d buf:%016x consumed:%d}",
			got.r.pos, got.r.nbits, got.r.buf, got.r.consumed(),
			want.r.pos, want.r.nbits, want.r.buf, want.r.consumed(),
		)
	}
}

func arm64SameUnreadBits(a, b deflateBits) bool {
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

func arm64RequireStatuses(
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

func arm64FixedLiteralBlock(payload []byte, padding int) []byte {
	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
	for _, b := range payload {
		w.writeFixedLitlen(int(b))
	}
	w.writeFixedLitlen(256)
	return append(w.bytes(), make([]byte, padding)...)
}

func arm64FixedMatchBlock(distance, length, padding int) ([]byte, []byte) {
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
