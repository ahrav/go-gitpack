package objstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"unsafe"

	"golang.org/x/exp/mmap"
)

// midxEntry describes a single object as recorded in a multi-pack index.
// The struct maps an object to its containing pack and byte offset within that pack.
type midxEntry struct {
	// packID is the index into the packReaders slice, identifying which
	// pack file contains this object.
	packID uint32

	// offset is the absolute byte position of the object header inside
	// the specified pack file. The field is 64-bit to support packs
	// that exceed 2 GiB.
	offset uint64
}

// midxFile represents one "multi-pack-index" (midx) file together with the
// packfiles it references.
//
// The struct is immutable after parseMidx returns and is safe for concurrent
// readers – exactly like idxFile.
type midxFile struct {
	// packReaders are mmap-handles for the N packfiles listed in PNAM order.
	// len(packReaders) == len(packNames).
	packReaders []*mmap.ReaderAt
	packNames   []string // full basenames, e.g. "pack-abcd1234.pack"

	// fanout[i] == #objects whose first digest byte ≤ i.
	fanout [fanoutEntries]uint32

	// objectIDs and entries run in parallel and have identical length.
	objectIDs []Hash
	entries   []midxEntry
}

// Multi-pack index chunk identifiers.
const (
	chunkPNAM = 0x504e414d // 'PNAM' - pack names
	chunkOIDF = 0x4f494446 // 'OIDF' - object ID fanout table
	chunkOIDL = 0x4f49444c // 'OIDL' - object ID list
	chunkOOFF = 0x4f4f4646 // 'OOFF' - object offsets
	chunkLOFF = 0x4c4f4646 // 'LOFF' - large object offsets
)

// findObject performs the same two-stage lookup that idxFile.findObject does
// but returns both the *mmap.ReaderAt for the pack and the byte offset.
func (m *midxFile) findObject(h Hash) (p *mmap.ReaderAt, off uint64, ok bool) {
	first := h[0]
	start := uint32(0)
	if first > 0 {
		start = m.fanout[first-1]
	}
	end := m.fanout[first]
	if start == end {
		return nil, 0, false
	}

	rel, hit := slices.BinarySearchFunc(
		m.objectIDs[start:end],
		h,
		func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
	)
	if !hit {
		return nil, 0, false
	}
	abs := int(start) + rel
	ent := m.entries[abs]
	return m.packReaders[ent.packID], ent.offset, true
}

// parseMidx reads the file mapped in mr and returns a fully‑populated
// midxFile. Only version‑1 / SHA‑1 repositories are supported.
//
// dir must be the "…/objects/pack" directory so that pack names from the
// PNAM chunk can be mmap'ed right away.
//
// Note: midx v1 is the current format (different from pack index v2)
// midx files have their own versioning scheme starting at 1
func parseMidx(dir string, mr *mmap.ReaderAt, packCache map[string]*mmap.ReaderAt) (*midxFile, error) {
	// Header.
	var hdr [12]byte
	if _, err := mr.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	if !bytes.Equal(hdr[0:4], []byte("MIDX")) {
		return nil, fmt.Errorf("not a MIDX file")
	}
	if hdr[4] != 1 /* version */ {
		return nil, fmt.Errorf("unsupported midx version %d", hdr[4])
	}
	if hdr[5] != 1 /* SHA‑1 */ {
		return nil, fmt.Errorf("only SHA‑1 midx supported")
	}

	chunks := int(hdr[6])
	packCount := int(binary.BigEndian.Uint32(hdr[8:12]))

	// Chunk table.
	type cdesc struct {
		id  [4]byte
		off uint64
	}
	cd := make([]cdesc, chunks+1) // +1 for the terminating row
	for i := range cd {
		var row [12]byte
		base := int64(12 + i*12)
		if _, err := mr.ReadAt(row[:], base); err != nil {
			return nil, err
		}
		copy(cd[i].id[:], row[0:4])
		cd[i].off = binary.BigEndian.Uint64(row[4:12])
	}
	// Calculate size for each chunk by looking at the next offset.
	sort.Slice(cd, func(i, j int) bool { return cd[i].off < cd[j].off })

	findChunk := func(id uint32) (off int64, size int64, err error) {
		for i := 0; i < len(cd)-1; i++ {
			chunkID := binary.BigEndian.Uint32(cd[i].id[:])
			if chunkID == id {
				return int64(cd[i].off),
					int64(cd[i+1].off) - int64(cd[i].off),
					nil
			}
		}
		return 0, 0, fmt.Errorf("chunk %08x not found", id)
	}

	// PNAM.
	pnOff, pnSize, err := findChunk(chunkPNAM)
	if err != nil {
		return nil, err
	}
	pn := make([]byte, pnSize)
	if _, err = mr.ReadAt(pn, pnOff); err != nil {
		return nil, err
	}
	// Filenames are NUL‑terminated.
	var packNames []string
	// Stop after we have collected <packCount> names and
	// silently skip any alignment padding that follows.
	for i, start := 0, 0; i < packCount; i++ {
		if start >= len(pn) {
			return nil, fmt.Errorf("PNAM truncated; expected %d names", packCount)
		}
		end := bytes.IndexByte(pn[start:], 0)
		if end < 0 {
			return nil, fmt.Errorf("unterminated PNAM entry")
		}
		// end-start > 0 guarantees we ignore the 0-length padding "names".
		if end == 0 {
			return nil, fmt.Errorf("empty PNAM entry before padding")
		}
		packNames = append(packNames, string(pn[start:start+end]))
		start += end + 1
	}
	if len(packNames) != packCount {
		return nil, fmt.Errorf("PNAM count mismatch (%d vs %d)", len(packNames), packCount)
	}

	// Mmap every pack straight away so we have a stable *ReaderAt slice.
	packs := make([]*mmap.ReaderAt, len(packNames))
	for i, name := range packNames {
		p := filepath.Join(dir, name)
		if h, ok := packCache[p]; ok {
			packs[i] = h
			continue
		}
		r, err := mmap.Open(p)
		if err != nil {
			for _, p := range packs[:i] {
				if p != nil {
					_ = p.Close()
				}
			}
			return nil, fmt.Errorf("mmap pack %q: %w", name, err)
		}
		packs[i] = r
		packCache[p] = r
	}

	// OIDF.
	fanOff, _, err := findChunk(chunkOIDF)
	if err != nil {
		return nil, err
	}
	var fanout [fanoutEntries]uint32
	if _, err = mr.ReadAt(unsafe.Slice((*byte)(unsafe.Pointer(&fanout[0])), fanoutSize), fanOff); err != nil {
		return nil, err
	}
	if hostLittle {
		for i := range fanout {
			fanout[i] = binary.BigEndian.Uint32(unsafe.Slice((*byte)(unsafe.Pointer(&fanout[i])), 4))
		}
	}
	objCount := fanout[255]

	// OIDL.
	oidOff, _, err := findChunk(chunkOIDL)
	if err != nil {
		return nil, err
	}
	oids := make([]Hash, objCount)
	read := unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), int(objCount*hashSize))
	if _, err = mr.ReadAt(read, oidOff); err != nil {
		return nil, err
	}

	// OOFF / LOFF.
	offOff, _, err := findChunk(chunkOOFF)
	if err != nil {
		return nil, err
	}
	offRaw := make([]byte, objCount*8) // two uint32 per object
	if _, err = mr.ReadAt(offRaw, offOff); err != nil {
		return nil, err
	}

	// LOFF is optional.
	loffOff, loffSize, _ := findChunk(chunkLOFF) // ignore "not found" error
	var loff []uint64
	if loffSize > 0 {
		loff = make([]uint64, loffSize/8)
		if _, err = mr.ReadAt(
			unsafe.Slice((*byte)(unsafe.Pointer(&loff[0])), int(loffSize)),
			loffOff,
		); err != nil {
			return nil, err
		}
	}

	entries := make([]midxEntry, objCount)

	for i := range objCount {
		packID := binary.BigEndian.Uint32(offRaw[i*8 : i*8+4])
		rawOff := binary.BigEndian.Uint32(offRaw[i*8+4 : i*8+8])

		var off64 uint64
		if rawOff&0x80000000 == 0 {
			off64 = uint64(rawOff)
		} else {
			idx := rawOff & 0x7FFFFFFF
			if int(idx) >= len(loff) {
				return nil, fmt.Errorf("invalid LOFF index %d", idx)
			}
			off64 = loff[idx]
		}
		entries[i] = midxEntry{packID, off64}
	}

	return &midxFile{
		packReaders: packs,
		packNames:   packNames,
		fanout:      fanout,
		objectIDs:   oids,
		entries:     entries,
	}, nil
}
