// idx.go
//
// Pack‑index ("IDX") parser for Git packfiles.
// The file maps *object SHA‑1 hashes* → *pack byte offsets* + *CRC‑32 checksums*
// enabling fast object lookup within a single packfile using a fanout table
// for O(1) range selection followed by binary search.
//
// The implementation supports both regular (≤2GB) and large (>2GB) packfiles
// via a two‑tier offset system. All lookup tables remain memory‑resident for
// constant‑time access, while the pack data itself stays memory‑mapped.

package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"unsafe"

	"golang.org/x/exp/mmap"
)

const (
	headerSize = 8 // 4-byte magic + 4-byte version.
	crcSize    = 4 // Big-endian CRC-32 value per object.
	offsetSize = 4 // 31-bit offset or MSB-set index into large-offset table.
)

// idxEntry describes a single object as recorded in a pack-index (*.idx).
// The struct is internal to the package but its invariants are relied on
// throughout the lookup path.
//
// An entry maps the object's SHA-1 to its absolute byte offset inside the
// companion *.pack and records the CRC-32 checksum that Git calculated when
// the pack was created.
type idxEntry struct {
	// offset holds the starting byte position of the object header inside
	// the packfile. The field is 64-bit so that repositories whose packs
	// exceed 2 GiB are still addressable.
	offset uint64

	// crc stores the CRC-32 checksum of the on-disk (compressed) object
	// data, exactly as written in the *.idx file. The value is compared
	// against a freshly calculated checksum when Store.VerifyCRC is true.
	crc uint32
}

// idxFile holds the memory-mapped view and lookup tables for a single
// *.pack / *.idx pair.
//
// A Store creates one idxFile per pack it opens.
// The struct is immutable after Open returns, so callers may share it across
// goroutines without additional synchronization.
type idxFile struct {
	// pack is the read-only, memory-mapped view of the *.pack file.
	pack *mmap.ReaderAt

	// idx is the memory-mapped companion *.idx file.
	idx *mmap.ReaderAt

	// fanout is the 256-entry fan-out table from the idx header.
	// fanout[b] stores the number of objects whose SHA-1 starts with a
	// byte ≤ b, enabling O(1) range selection before binary search.
	fanout [fanoutEntries]uint32

	// oidTable lists all object IDs (SHA-1 hashes) in canonical index
	// order. entries[i] describes oidTable[i].
	oidTable []Hash

	// entries runs parallel to oidTable and records the byte offset inside
	// the pack and the CRC-32 checksum of each object.
	entries []idxEntry

	// largeOffsets stores 64-bit offsets for objects located beyond the
	// 2 GiB boundary. The slice is nil when the pack is smaller than that.
	largeOffsets []uint64

	// sortedOffsets holds all pack offsets in ascending order.
	sortedOffsets []uint64

	// ridx is the reverse‑index table that maps pack file byte offsets back
	// to zero‑based object indices within the sorted oidTable. When present,
	// ridx[i] contains the object index whose pack data starts at
	// sortedOffsets[i], enabling efficient reverse lookups during pack
	// traversal and delta resolution. The slice is nil when no reverse
	// index is available and has length equal to the object count when loaded.
	ridx []uint32

	// ridxCRCTrusted reports whether ridx was loaded from an on-disk .ridx/.rev
	// file (true) versus synthesized from sortedOffsets (false). Only trusted
	// reverse indexes can map offset order back to idx-entry order for CRC lookups.
	ridxCRCTrusted bool
}

// findObject looks up hash in the tables that belong to a single
// *.idx / *.pack pair and returns the absolute byte offset of the object
// inside the pack.
//
// The method first consults the 256-entry fan-out table to narrow the search
// window to objects whose first digest byte matches hash[0].
// Within that window it performs a binary search over the sorted SHA-1 slice.
//
// The boolean result reports whether the object was present; when it is
// false offset is zero.
//
// The receiver is immutable after Open has returned, therefore the method is
// safe for concurrent callers.
func (f *idxFile) findObject(hash Hash) (offset uint64, found bool) {
	// Identify the search range via the fan-out table.
	first := hash[0]

	start := uint32(0)
	if first > 0 {
		start = f.fanout[first-1]
	}
	end := f.fanout[first]
	if start == end {
		return 0, false // bucket empty
	}

	// Binary-search the slice [start:end) for the target hash.
	relIdx, ok := slices.BinarySearchFunc(
		f.oidTable[start:end],
		hash,
		func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
	)
	if !ok {
		return 0, false
	}
	absIdx := int(start) + relIdx
	return f.entries[absIdx].offset, true
}

// crcAtOffset resolves the CRC-32 checksum for an object at the given pack
// byte offset.
//
// The method first binary-searches sortedOffsets (ascending) to find the
// position i of off, then uses the reverse index (ridx) to map that position
// to the corresponding index in the entries slice. The ridx is stored in
// *descending*-offset order (largest offset first), so the conversion is:
//
//	desc = len(sortedOffsets) - 1 - i   // ascending → descending
//	idxPos = ridx[desc]                 // → entries[idxPos].crc
//
// When ridx is absent, synthesized from sortedOffsets, or malformed, the method
// falls back to a linear scan over entries. This is O(n) but still correct; the
// slow path is acceptable because CRC verification is opt-in and runs
// infrequently.
func (f *idxFile) crcAtOffset(off uint64) (uint32, bool) {
	if f == nil || len(f.sortedOffsets) == 0 {
		return 0, false
	}
	i, ok := slices.BinarySearch(f.sortedOffsets, off)
	if !ok {
		return 0, false
	}

	// Only trust on-disk ridx mappings for CRC lookups. Synthetic fallback
	// tables encode descending rank, not offset->entries index mapping.
	if f.ridxCRCTrusted && len(f.ridx) == len(f.sortedOffsets) {
		desc := len(f.sortedOffsets) - 1 - i
		idxPos := int(f.ridx[desc])
		if idxPos >= 0 && idxPos < len(f.entries) {
			return f.entries[idxPos].crc, true
		}
	}

	// Fallback: linear scan over all entries when ridx is absent, incomplete,
	// or yields an out-of-range index. This is O(n) but still correct and is
	// only reached in degenerate cases.
	for _, e := range f.entries {
		if e.offset == off {
			return e.crc, true
		}
	}
	return 0, false
}

// largeOffsetEntry relates an object index to its entry in the large-offset
// table.
//
// Git pack-index version 2 stores 64-bit offsets separately for objects that
// live beyond the 2 GiB mark of the companion *.pack file. The regular
// 32-bit offset field then contains a sentinel with the most-significant bit
// set and the remaining 31 bits forming an index into that auxiliary table.
// largeOffsetEntry captures that mapping while the index file is parsed so
// that those placeholders can be resolved into real 64-bit offsets before
// the idxFile is finalized. The type is internal to the package and is not
// exposed to callers.
type largeOffsetEntry struct {
	// objIdx is the zero-based position of the object within the sorted
	// object-ID arrays (oidTable, entries, …) that are read from the
	// *.idx file. The value fits in 32 bits because a single pack never
	// contains more than 2³²-1 objects.
	objIdx uint32

	// largeIdx is the zero-based position of the corresponding 64-bit
	// offset inside the large-offset table that follows the regular
	// 4-byte offset block in the *.idx file. The parser validates that
	// largeIdx is in range before dereferencing it.
	largeIdx uint32
}

var (
	ErrNonMonotonicFanout = errors.New("idx corrupt: fan‑out table not monotonic")
	ErrBadIdxChecksum     = errors.New("idx corrupt: checksum mismatch")
)

// parseIdx reads a Git pack index file using unsafe operations for maximum performance.
//
// Git pack index (.idx) file format (version 2):
// - 8-byte header: magic bytes (0xff744f63) + version (2)
// - 1024-byte fanout table: 256 entries, cumulative object counts for hash prefixes
// - N×20-byte object IDs: SHA-1 hashes in sorted order
// - N×4-byte CRC-32 checksums + N×4-byte offsets
// - Optional large offset table: 8-byte offsets for objects beyond 2GB
//
// WARNING: This implementation uses unsafe operations and should only be used when
// performance is critical and the code has been thoroughly tested.
func parseIdx(ix *mmap.ReaderAt) (*idxFile, error) {
	// Git stores data in big-endian format, so we need to byte-swap on little-endian systems.
	littleEndian := hostLittle

	header := make([]byte, headerSize)
	if _, err := ix.ReadAt(header, 0); err != nil {
		return nil, err
	}

	// Validate magic bytes: 0xff744f63 identifies this as a Git pack index.
	if !bytes.Equal(header[0:4], []byte{0xff, 0x74, 0x4f, 0x63}) {
		return nil, fmt.Errorf("unsupported idx version or v1 not handled")
	}

	if version := binary.BigEndian.Uint32(header[4:]); version != 2 {
		return nil, fmt.Errorf("unsupported idx version %d", version)
	}

	size := int64(ix.Len())
	// hdr(8) + fan‑out(1024) + trailing hashes(40) is the absolute minimum.
	if size < 8+256*4+hashSize*2 {
		return nil, ErrBadIdxChecksum
	}

	// Fanout table: fanout[i] = total number of objects whose first byte is ≤ i.
	// This enables O(1) range queries for object lookup by hash prefix.
	fanoutData := make([]byte, fanoutSize)
	if _, err := ix.ReadAt(fanoutData, headerSize); err != nil {
		return nil, err
	}

	// Use unsafe casting to reinterpret the byte slice as a [256]uint32 array
	// in place, avoiding a 1024-byte copy. This is safe because:
	//   1. fanoutData was allocated by make([]byte, 1024), which the Go runtime
	//      guarantees to be at least 4-byte aligned on all supported platforms.
	//   2. The slice length exactly matches the array size (256 * 4 = 1024).
	//   3. The resulting array is immediately copied into a stack-local value
	//      (fanout) below, so the pointer does not escape or alias mutable data.
	fanoutPtr := (*[fanoutEntries]uint32)(unsafe.Pointer(&fanoutData[0]))
	fanout := *fanoutPtr

	if littleEndian {
		for i := range fanout {
			fanout[i] = binary.BigEndian.Uint32(fanoutData[i*4:])
		}
	}

	// Guard against truncated or tampered indexes.
	for i := 1; i < fanoutEntries; i++ {
		if fanout[i] < fanout[i-1] {
			return nil, ErrNonMonotonicFanout
		}
	}

	objCount := fanout[255]
	if objCount == 0 {
		return &idxFile{fanout: fanout, entries: nil, oidTable: nil}, nil
	}

	// bounds: do the tables we are about to slice actually fit inside the file?
	fanStart := int64(headerSize)
	minSize := fanStart + // header
		256*4 + // fan‑out
		int64(objCount)*(hashSize+4+4) + // oid + crc + ofs tables
		hashSize*2 // trailer hashes
	if size < minSize {
		return nil, ErrBadIdxChecksum
	}

	// Guard against integer overflow when allocating giant slices.
	// Prevents wrapped len calculations on malicious files.
	if objCount > math.MaxUint32/hashSize {
		return nil, fmt.Errorf("idx claims %d objects – impl refuses >%d", objCount,
			math.MaxUint32/hashSize)
	}

	oidBase := int64(headerSize + fanoutSize)
	crcBase := oidBase + int64(objCount*hashSize)
	offBase := crcBase + int64(objCount*crcSize)

	// Read object IDs directly into the destination slice to avoid extra copies.
	oids := make([]Hash, objCount)
	if len(oids) > 0 {
		oidBytes := unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), len(oids)*hashSize)
		if _, err := ix.ReadAt(oidBytes, oidBase); err != nil {
			return nil, err
		}
	}

	entries := make([]idxEntry, objCount)
	// tableChunk controls how many CRC / offset entries are read per I/O call.
	// 65,536 entries (~256 KiB for 4-byte tables) balances two concerns:
	//   - Small enough to keep the temporary read buffer (crcBuf / offsetBuf)
	//     out of the large-object heap, reducing GC pressure.
	//   - Large enough to amortize the per-ReadAt syscall overhead so that
	//     even multi-million-object packs are parsed in a modest number of
	//     iterations.
	const tableChunk = 1 << 16 // 65,536 entries (~256 KiB for 4-byte tables)

	// Parse CRC table in chunks to keep peak allocations bounded.
	crcBuf := make([]byte, tableChunk*crcSize)
	for base := uint32(0); base < objCount; {
		n := tableChunk
		remaining := int(objCount - base)
		if remaining < n {
			n = remaining
		}
		chunk := crcBuf[:n*crcSize]
		if _, err := ix.ReadAt(chunk, crcBase+int64(base)*crcSize); err != nil {
			return nil, err
		}
		for i := range n {
			entries[int(base)+i].crc = binary.BigEndian.Uint32(chunk[i*crcSize:])
		}
		base += uint32(n)
	}

	// Track objects that reference the large offset table (for packs > 2GB).
	largeOffsetList := make([]largeOffsetEntry, 0, objCount/1000)
	maxLargeIdx := uint32(0)

	// Parse offset table in chunks.
	offsetBuf := make([]byte, tableChunk*offsetSize)
	for base := uint32(0); base < objCount; {
		n := tableChunk
		remaining := int(objCount - base)
		if remaining < n {
			n = remaining
		}
		chunk := offsetBuf[:n*offsetSize]
		if _, err := ix.ReadAt(chunk, offBase+int64(base)*offsetSize); err != nil {
			return nil, err
		}

		for i := range n {
			offset := binary.BigEndian.Uint32(chunk[i*offsetSize:])
			objIdx := uint32(int(base) + i)

			// If MSB is 0, it's a direct 31-bit offset.
			// If MSB is 1, it's an index into the large offset table.
			if offset&0x80000000 == 0 {
				entries[int(objIdx)].offset = uint64(offset)
			} else {
				largeIdx := offset & 0x7fffffff
				largeOffsetList = append(largeOffsetList, largeOffsetEntry{objIdx, largeIdx})
				if largeIdx > maxLargeIdx {
					maxLargeIdx = largeIdx
				}
			}
		}
		base += uint32(n)
	}

	// Handle large offsets if any objects require them.
	var largeOffsets []uint64
	if len(largeOffsetList) > 0 {
		largeOffsetCount := maxLargeIdx + 1
		largeOffsetData := make([]byte, largeOffsetCount*largeOffSize)

		if _, err := ix.ReadAt(largeOffsetData, offBase+int64(objCount*offsetSize)); err != nil {
			return nil, err
		}

		largeOffsets = make([]uint64, largeOffsetCount)

		// Read uint64 values properly respecting byte order.
		for i := range largeOffsetCount {
			offset := i * largeOffSize
			if int(offset+largeOffSize) <= len(largeOffsetData) {
				largeOffsets[i] = binary.BigEndian.Uint64(largeOffsetData[offset : offset+largeOffSize])
			}
		}

		for _, entry := range largeOffsetList {
			// Bit 31 of the on-disk 32-bit offset is the "large-offset" flag.
			// Always clear it before using the value as an index so that a
			// stray, unmasked bit can never take us past the end of the LOFF
			// slice.
			idx := entry.largeIdx & 0x7fffffff
			if idx >= uint32(len(largeOffsets)) {
				return nil, fmt.Errorf("invalid large offset index %d", idx)
			}
			entries[entry.objIdx].offset = largeOffsets[idx]
		}
	}

	offs := make([]uint64, objCount)
	for i, e := range entries {
		offs[i] = e.offset
	}
	slices.Sort(offs)

	// Trailer verification.
	trailer := make([]byte, 40)
	if _, err := ix.ReadAt(trailer, int64(ix.Len()-40)); err != nil {
		return nil, err
	}

	wantIdxSHA := trailer[hashSize:]

	// Recompute idx‑SHA over everything **except** the final idx hash itself.
	h := sha1.New()
	if _, err := io.Copy(h, io.NewSectionReader(ix, 0, size-int64(hashSize))); err != nil {
		return nil, err
	}
	if !bytes.Equal(h.Sum(nil), wantIdxSHA) {
		return nil, ErrBadIdxChecksum
	}

	return &idxFile{
		fanout:        fanout,
		entries:       entries,
		oidTable:      oids,
		largeOffsets:  largeOffsets,
		sortedOffsets: offs,
	}, nil
}

// resolveIdxPos converts a bit-index from a bitmap (which is ordered by pack
// offset) to the corresponding position in the *.idx oid/entries tables.
//
// The method deliberately panics instead of returning an error because a
// bad bit value signals a programming error in the caller (e.g. iterating
// past the end of a bitmap), not recoverable runtime data corruption. The
// panic makes such bugs immediately visible during development rather than
// silently returning an invalid index that could corrupt downstream state.
//
// Preconditions (caller must guarantee):
//   - pf.ridx is non-nil (a reverse index was loaded for this pack).
//   - 0 <= bit < len(pf.ridx).
func (pf *idxFile) resolveIdxPos(bit int) uint32 {
	if pf.ridx == nil || bit < 0 || bit >= len(pf.ridx) {
		panic("idxFile.resolveIdxPos: out‑of‑range")
	}
	return pf.ridx[bit]
}
