//go:build arm64 && linux && !purego && !gitpack_libdeflate

package objstore

import (
	"bytes"
	"syscall"
	"testing"
)

func TestInflateHuffmanFastArm64GuardPages(t *testing.T) {
	const prefixLen = 9
	const tailLen = 40

	prefix := makeDeterministicBytes(prefixLen)
	tail := makeDeterministicBytes(tailLen)
	var w deflateTestBits
	w.write(1, 1)
	w.write(1, 2)
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
	src := append(w.bytes(), make([]byte, 40)...)

	want := append([]byte(nil), prefix...)
	want = append(want, 'X', 'Y')
	want = append(want, bytes.Repeat([]byte{'Y'}, 258)...)
	want = append(want, tail...)
	if got := len(want) - deflateFastOutputMargin; got != prefixLen+1 {
		t.Fatalf("fixture output limit = %d, want %d", got, prefixLen+1)
	}

	d, r := arm64PrepareHuffmanBlock(t, src, 1)
	reference := arm64RunHuffmanGo(d, r, len(want))
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	guardedSrc := arm64RightGuardedPage(t, len(src))
	copy(guardedSrc, src)
	r.src = guardedSrc
	guardedDst := arm64RightGuardedPage(t, len(want))
	got, statuses := arm64RunHuffmanKernelInto(t, d, r, guardedDst)

	arm64CompareHuffmanResults(t, got, reference)
	if !bytes.Equal(got.dst[:got.out], want) {
		t.Fatal("guarded output does not match fixture")
	}
	arm64RequireStatuses(t, statuses, inflateFastTail)
}

func arm64RightGuardedPage(t *testing.T, size int) []byte {
	t.Helper()

	pageSize := syscall.Getpagesize()
	if size > pageSize {
		t.Fatalf("guarded buffer size %d exceeds page size %d", size, pageSize)
	}
	mapping, err := syscall.Mmap(
		-1,
		0,
		3*pageSize,
		syscall.PROT_NONE,
		syscall.MAP_PRIVATE|syscall.MAP_ANON,
	)
	if err != nil {
		t.Fatalf("mmap guard pages: %v", err)
	}
	t.Cleanup(func() {
		if err := syscall.Munmap(mapping); err != nil {
			t.Errorf("munmap guard pages: %v", err)
		}
	})

	page := mapping[pageSize : 2*pageSize]
	if err := syscall.Mprotect(page, syscall.PROT_READ|syscall.PROT_WRITE); err != nil {
		t.Fatalf("mprotect guarded buffer: %v", err)
	}
	return page[pageSize-size:]
}
