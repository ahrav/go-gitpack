package objstore

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
)

var (
	ErrNonMonotonicOffsets = errors.New("idx corrupt: non‑monotonic offsets")
)

// verifyCRC32 validates the CRC-32 checksum of a packed object.
//
// verifyCRC32 streams the *packed* (still-compressed) byte sequence that
// starts at objOff inside pf.pack into a CRC-32 hash and compares the result
// with want, the checksum that Git wrote to the companion *.idx file at
// pack-creation time.
//
// The object's end is inferred from the next object's offset (as recorded in
// pf.sortedOffsets) or, for the final object, from the beginning of the pack
// trailer.
//
// Error semantics:
//   - If objOff cannot be found in pf.sortedOffsets, the function returns an
//     error that mentions the missing offset.
//   - ErrNonMonotonicOffsets is returned when the calculated end precedes or
//     equals objOff, indicating a corrupt index.
//   - I/O failures from the underlying mmap.ReaderAt are propagated
//     unchanged.
//   - A checksum mismatch yields a descriptive error that includes both the
//     expected and the actual CRC-32 in hexadecimal.
//
// A nil return value signals that the on-disk CRC matches the expected one.
func verifyCRC32(pf *idxFile, objOff uint64, want uint32) error {
	// Locate objOff in the monotonically-sorted offset slice.
	idx := sort.Search(len(pf.sortedOffsets), func(i int) bool { return pf.sortedOffsets[i] >= objOff })
	if idx >= len(pf.sortedOffsets) || pf.sortedOffsets[idx] != objOff {
		return fmt.Errorf("offset %d not found in index", objOff)
	}

	// The object ends where the next one begins, or at the pack trailer.
	var objEnd uint64
	if idx+1 < len(pf.sortedOffsets) {
		objEnd = pf.sortedOffsets[idx+1]
	} else {
		objEnd = uint64(pf.pack.Len()) - hashSize // exclude SHA-1 pack checksum trailer
	}
	if objEnd <= objOff {
		return ErrNonMonotonicOffsets
	}

	// Stream the exact on-disk bytes into a CRC-32 hash.
	sec := io.NewSectionReader(pf.pack, int64(objOff), int64(objEnd-objOff))
	h := crc32.New(crc32.MakeTable(crc32.IEEE))
	if _, err := io.Copy(h, sec); err != nil {
		return err
	}
	if got := h.Sum32(); got != want {
		return fmt.Errorf("crc mismatch @%d: got %08x want %08x", objOff, got, want)
	}
	return nil
}
