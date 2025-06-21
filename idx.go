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

	// crcByOffset maps pack file offset to CRC-32 checksum for O(1) lookup
	// during CRC verification.
	crcByOffset map[uint64]uint32

	// largeOffsets stores 64-bit offsets for objects located beyond the
	// 2 GiB boundary. The slice is nil when the pack is smaller than that.
	largeOffsets []uint64

	// fast look‑ups for CRC verification.
	entriesByOff  map[uint64]idxEntry // exact offset → entry (constant‑time)
	sortedOffsets []uint64            // monotonically ascending list of offsets
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

	// Use unsafe casting to avoid allocating and copying 1024 bytes.
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

	// Read all fixed-size data in a single syscall to reduce I/O overhead.
	allDataSize := objCount*hashSize + objCount*crcSize + objCount*offsetSize
	allData := make([]byte, allDataSize)

	if _, err := ix.ReadAt(allData, oidBase); err != nil {
		return nil, err
	}

	oidData := allData[:objCount*hashSize]
	crcData := allData[objCount*hashSize : objCount*hashSize+objCount*crcSize]
	offsetData := allData[objCount*hashSize+objCount*crcSize:]

	// Convert raw bytes to Hash structs using unsafe operations to avoid parsing overhead.
	oids := make([]Hash, objCount)
	if len(oids) > 0 {
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), len(oids)*hashSize), oidData)
	}

	crcs := make([]uint32, objCount)
	if len(crcs) > 0 {
		if littleEndian {
			for i := range objCount {
				crcs[i] = binary.BigEndian.Uint32(crcData[i*4:])
			}
		} else {
			srcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&crcData[0]))
			crcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&crcs[0]))
			copy(crcPtr[:objCount], srcPtr[:objCount])
		}
	}

	entries := make([]idxEntry, objCount)
	offsetPtr := (*[1 << 28]uint32)(unsafe.Pointer(&offsetData[0]))

	// Track objects that reference the large offset table (for packs > 2GB).
	largeOffsetList := make([]largeOffsetEntry, 0, objCount/1000)
	maxLargeIdx := uint32(0)

	for i := range objCount {
		offset := offsetPtr[i]
		if littleEndian {
			offset = binary.BigEndian.Uint32(offsetData[i*4:])
		}

		entries[i].crc = crcs[i]

		// If MSB is 0, it's a direct 31-bit offset.
		// If MSB is 1, it's an index into the large offset table.
		if offset&0x80000000 == 0 {
			entries[i].offset = uint64(offset)
		} else {
			largeIdx := offset & 0x7fffffff
			largeOffsetList = append(largeOffsetList, largeOffsetEntry{i, largeIdx})
			if largeIdx > maxLargeIdx {
				maxLargeIdx = largeIdx
			}
		}
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

	crcByOffset := make(map[uint64]uint32, objCount)
	for i := range objCount {
		crcByOffset[entries[i].offset] = entries[i].crc
	}

	byOff := make(map[uint64]idxEntry, objCount)
	offs := make([]uint64, objCount)
	for i, e := range entries {
		byOff[e.offset] = e
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
		crcByOffset:   crcByOffset,
		largeOffsets:  largeOffsets,
		entriesByOff:  byOff,
		sortedOffsets: offs,
	}, nil
}
