//go:build amd64 && linux && !purego && !(gitpack_libdeflate && cgo)

package objstore

import (
	"bytes"
	"syscall"
	"testing"
)

func TestInflateHuffmanFastAMD64GuardPages(t *testing.T) {
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

	d, r := amd64PrepareHuffmanBlock(t, src, 1)
	reference := amd64RunHuffmanGo(d, r, len(want))
	if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
		t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
	}

	guardedSrc := amd64RightGuardedPage(t, len(src))
	copy(guardedSrc, src)
	r.src = guardedSrc
	guardedDst := amd64RightGuardedPage(t, len(want))
	got, statuses := amd64RunHuffmanKernelInto(t, d, r, guardedDst)

	amd64CompareHuffmanResults(t, got, reference)
	if !bytes.Equal(got.dst[:got.out], want) {
		t.Fatal("guarded output does not match fixture")
	}
	amd64RequireStatuses(t, statuses, inflateFastTail)
}

func amd64RightGuardedPage(t *testing.T, size int) []byte {
	t.Helper()
	return amd64GuardedPage(t, size, false)
}

// amd64LeftGuardedPage returns a size-byte buffer that starts exactly at a
// page boundary with the preceding page unmapped, so any kernel load or
// store below the buffer's first byte faults instead of silently reading
// or corrupting adjacent memory.
func amd64LeftGuardedPage(t *testing.T, size int) []byte {
	t.Helper()
	return amd64GuardedPage(t, size, true)
}

func amd64GuardedPage(t *testing.T, size int, left bool) []byte {
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
	if left {
		return page[:size]
	}
	return page[pageSize-size:]
}

// TestInflateHuffmanFastAMD64GuardPagesMatchCopies runs the kernel's match
// copy loops directly against unmapped pages. The primary guard-page test
// above only drives literals and copy_rle next to the boundary; these
// fixtures put copy_wide and copy_small_offset there, and add the
// underrun direction the right-guard layout cannot observe (a read or
// write below the buffer start would land in the same RW page and pass
// silently).
func TestInflateHuffmanFastAMD64GuardPagesMatchCopies(t *testing.T) {
	buildRun := func(distance, length, repeats int) ([]byte, []byte) {
		seed := makeDeterministicBytes(distance)
		var w deflateTestBits
		w.write(1, 1)
		w.write(1, 2)
		for _, b := range seed {
			w.writeFixedLitlen(int(b))
		}
		want := append([]byte(nil), seed...)
		for i := 0; i < repeats; i++ {
			w.writeFixedLength(length)
			w.writeFixedDistance(distance)
			for j := 0; j < length; j++ {
				want = append(want, want[len(want)-distance])
			}
		}
		w.writeFixedLitlen(256)
		return append(w.bytes(), make([]byte, 40)...), want
	}

	// Right edge: the guarded destination is exactly len(want), so the
	// kernel decodes matches until the output stop at len(want)-299 with
	// copy overshoot reaching within ~220 bytes of the unmapped page, and
	// decodeHuffmanTail finishes the remainder flush against it. This is
	// fault-level containment for the copy loops (a gross write or read
	// overrun faults); the byte-exact overshoot bound is separately pinned
	// by the CopySweep canary, which a page-granular guard cannot resolve.
	rightEdge := []struct {
		name     string
		distance int
		length   int
		repeats  int
	}{
		{"copy_wide", 8, 41, 25},
		{"copy_small_offset", 7, 15, 70},
	}
	for _, tt := range rightEdge {
		t.Run("right/"+tt.name, func(t *testing.T) {
			src, want := buildRun(tt.distance, tt.length, tt.repeats)
			d, r := amd64PrepareHuffmanBlock(t, src, 1)
			reference := amd64RunHuffmanGo(d, r, len(want))
			if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
				t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
			}

			guardedSrc := amd64RightGuardedPage(t, len(src))
			copy(guardedSrc, src)
			r.src = guardedSrc
			guardedDst := amd64RightGuardedPage(t, len(want))
			got, statuses := amd64RunHuffmanKernelInto(t, d, r, guardedDst)

			amd64CompareHuffmanResults(t, got, reference)
			if !bytes.Equal(got.dst[:got.out], want) {
				t.Fatal("guarded output does not match fixture")
			}
			amd64RequireStatuses(t, statuses, inflateFastTail)
		})
	}

	// Left edge: a match at the maximum legal distance (offset == out)
	// reads dst[0] exactly; one byte lower faults. The source buffer is
	// left-guarded too, so a refill below src[0] faults as well.
	t.Run("left/max-distance", func(t *testing.T) {
		src, want := buildRun(8, 250, 1)
		d, r := amd64PrepareHuffmanBlock(t, src, 1)
		dstLen := len(want) + deflateFastOutputMargin + 1
		reference := amd64RunHuffmanGo(d, r, dstLen)
		if reference.err != nil || !bytes.Equal(reference.dst[:reference.out], want) {
			t.Fatalf("reference decode: out=%d err=%v", reference.out, reference.err)
		}

		guardedSrc := amd64LeftGuardedPage(t, len(src))
		copy(guardedSrc, src)
		r.src = guardedSrc
		guardedDst := amd64LeftGuardedPage(t, dstLen)
		got, statuses := amd64RunHuffmanKernelInto(t, d, r, guardedDst)

		amd64CompareHuffmanResults(t, got, reference)
		if !bytes.Equal(got.dst[:got.out], want) {
			t.Fatal("guarded output does not match fixture")
		}
		amd64RequireStatuses(t, statuses, inflateFastBlockDone)
	})
}
