// crc.go
//
// CRC‑32 integrity verification for Git packfile objects and trailers.
// The module validates *pack object offsets* → *computed checksums* against
// the CRC‑32 values recorded in pack‑index files to detect corruption during
// transport or storage. This provides end‑to‑end data integrity checking for
// critical repository operations without requiring full object decompression.
//
// The implementation streams compressed object data directly from memory‑mapped
// pack files and computes IEEE CRC‑32 checksums with pack boundary validation.
// Pack trailer SHA‑1 checksums are verified separately to ensure overall pack
// integrity beyond individual object corruption detection.

package objstore

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"golang.org/x/exp/mmap"
)

var (
	ErrNonMonotonicOffsets     = errors.New("idx corrupt: non‑monotonic offsets")
	ErrObjectExceedsPackBounds = errors.New("object extends past pack trailer")
	ErrPackTrailerCorrupt      = errors.New("pack trailer checksum mismatch")
)

// verifyCRC32 validates the CRC-32 checksum of a packed object with additional
// integrity checks for pack boundaries.
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
//   - ErrObjectExceedsPackBounds is returned when an object extends into or
//     past the pack trailer.
//   - I/O failures from the underlying mmap.ReaderAt are propagated
//     unchanged.
//   - A checksum mismatch yields a descriptive error that includes both the
//     expected and the actual CRC-32 in hexadecimal.
//
// A nil return value signals that the on-disk CRC matches the expected one.
func verifyCRC32(pf *idxFile, objOff uint64, want uint32) error {
	// Locate objOff in the monotonically-sorted offset slice.
	idx := sort.Search(
		len(pf.sortedOffsets),
		func(i int) bool { return pf.sortedOffsets[i] >= objOff },
	)
	if idx >= len(pf.sortedOffsets) || pf.sortedOffsets[idx] != objOff {
		return fmt.Errorf("offset %d not found in index", objOff)
	}

	// The object ends where the next one begins, or at the pack trailer.
	packSize := uint64(pf.pack.Len())
	trailerStart := packSize - hashSize

	var objEnd uint64
	if idx+1 < len(pf.sortedOffsets) {
		objEnd = pf.sortedOffsets[idx+1]
	} else {
		objEnd = trailerStart
	}

	// Edge case: ensure object doesn't extend into or past trailer.
	if objEnd > trailerStart {
		return ErrObjectExceedsPackBounds
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

// verifyPackTrailer validates the SHA-1 checksum at the end of a pack file.
// This should be called once per pack to ensure the trailer hasn't been corrupted.
func verifyPackTrailer(pack *mmap.ReaderAt) error {
	size := pack.Len()
	if size < hashSize {
		return fmt.Errorf("pack too small for trailer")
	}

	// Read the trailer checksum.
	trailer := make([]byte, hashSize)
	if _, err := pack.ReadAt(trailer, int64(size-hashSize)); err != nil {
		return fmt.Errorf("failed to read pack trailer: %w", err)
	}

	// Compute checksum over entire pack except the trailer itself.
	h := sha1.New()
	sec := io.NewSectionReader(pack, 0, int64(size-hashSize))
	if _, err := io.Copy(h, sec); err != nil {
		return fmt.Errorf("failed to checksum pack: %w", err)
	}

	computed := h.Sum(nil)
	if !bytes.Equal(computed, trailer) {
		return ErrPackTrailerCorrupt
	}

	return nil
}
