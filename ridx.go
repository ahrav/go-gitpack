// ridx.go
//
// Reverse-index (".rev") loader for Git packfiles.
//
// Git writes pack-<hash>.rev beside each pack (pack.writeReverseIndex, the
// default since Git 2.41): a table with one entry per packed object, sorted
// by pack offset, each entry naming the object's position in the .idx tables.
// This file reads that table and derives both the in-memory reverse index
// and the ascending offset table from it, so opening a pack with a .rev file
// sorts nothing. Without a .rev file both are built from the .idx offsets.
//
// In memory the reverse index is kept in *descending* offset order (largest
// offset first): ridx[k] is the .idx position of the object at
// sortedOffsets[len-1-k]. crcAtOffset depends on that orientation.

package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"golang.org/x/exp/mmap"
)

const (
	ridxMagic = "RIDX"
	// ridxHeaderSize covers the magic, the format version, and the hash
	// function identifier.
	ridxHeaderSize = 4 + 4 + 4
	// ridxTrailerSize covers the pack checksum and the file's own checksum.
	ridxTrailerSize = hashSize * 2
	// ridxHashSHA1 is the hash function identifier Git writes for SHA-1
	// repositories; SHA-256 repositories write 2.
	ridxHashSHA1 = 1
)

// loadReverseIndex returns the descending-offset reverse index for the pack
// at packPath. When a valid .rev file is present it also fills
// pf.sortedOffsets from it (when pf.sortedOffsets is nil) so the caller
// skips sorting; otherwise pf.sortedOffsets is sorted here if still nil and
// the reverse index is built from the .idx offsets. Both paths produce a
// mapping that is trusted for CRC lookups.
func loadReverseIndex(packPath string, pf *idxFile) ([]uint32, error) {
	if pf == nil {
		panic("loadReverseIndex called with a nil idxFile")
	}
	// Git originally used the ".rev" extension for reverse-index files;
	// ".ridx" is probed as well for repositories written by tools that use
	// that name for the same format.
	var ridxPath string
	for _, ext := range []string{".rev", ".ridx"} {
		try := strings.TrimSuffix(packPath, ".pack") + ext
		if _, err := os.Stat(try); err == nil {
			ridxPath = try
			break
		}
	}
	if ridxPath != "" {
		if ridx, err := tryLoadRidxFile(ridxPath, pf); err == nil {
			pf.ridxCRCTrusted = true
			return ridx, nil
		}
		// A corrupt, stale, or foreign file is non-fatal: the mapping can
		// always be rebuilt from the .idx offsets already in memory.
	}
	if pf.sortedOffsets == nil {
		pf.sortedOffsets = sortedPackOffsets(pf.entries)
	}
	pf.ridxCRCTrusted = true
	return buildReverseFromEntries(pf), nil
}

// sortedPackOffsets returns every entry's pack offset in ascending order.
func sortedPackOffsets(entries []idxEntry) []uint64 {
	offs := make([]uint64, len(entries))
	for i, e := range entries {
		offs[i] = e.offset
	}
	slices.Sort(offs)
	return offs
}

// tryLoadRidxFile parses a Git .rev file and returns the reverse index in
// descending offset order. The file's entry count must match the .idx, its
// pack checksum must match the pack trailer when the pack is mapped, and the
// offsets it implies must be strictly increasing; any other shape is an
// error so the caller rebuilds the mapping instead of trusting the file.
//
// When pf.sortedOffsets is nil the ascending offsets implied by the table are
// stored there; when it is already populated the table is checked against it.
func tryLoadRidxFile(ridxPath string, pf *idxFile) ([]uint32, error) {
	mr, err := mmap.Open(ridxPath)
	if err != nil {
		return nil, err
	}
	defer mr.Close()

	size := int64(mr.Len())
	if size < ridxHeaderSize+ridxTrailerSize {
		return nil, errors.New("ridx: file too short")
	}
	var header [ridxHeaderSize]byte
	if _, err := mr.ReadAt(header[:], 0); err != nil {
		return nil, err
	}
	if btostr(header[0:4]) != ridxMagic {
		return nil, errors.New("ridx: bad magic")
	}
	if ver := binary.BigEndian.Uint32(header[4:8]); ver != 1 {
		return nil, fmt.Errorf("ridx: unsupported version %d", ver)
	}
	if hashID := binary.BigEndian.Uint32(header[8:12]); hashID != ridxHashSHA1 {
		return nil, fmt.Errorf("ridx: unsupported hash function %d", hashID)
	}

	tableLen := size - ridxHeaderSize - ridxTrailerSize
	if tableLen%4 != 0 {
		return nil, errors.New("ridx: table length is not a multiple of 4")
	}
	objCount := int(tableLen / 4)
	if objCount != len(pf.entries) {
		return nil, fmt.Errorf("ridx: object count mismatch (idx=%d ridx=%d)", len(pf.entries), objCount)
	}

	// The file's own checksum covers everything before it. Verifying it is
	// cheap relative to the sort it replaces and rejects truncated or
	// partially written files before their entries are trusted. Git defines
	// this trailer as SHA-1 (the pack and idx trailers use the same hash);
	// it detects corruption and is not a security boundary.
	h := sha1.New() //nolint:gosec // Git .rev trailer format mandates SHA-1.
	if _, err := h.Write(mmapData(mr)[:size-hashSize]); err != nil {
		return nil, err
	}
	var wantSelf [hashSize]byte
	if _, err := mr.ReadAt(wantSelf[:], size-hashSize); err != nil {
		return nil, err
	}
	if !bytes.Equal(h.Sum(nil), wantSelf[:]) {
		return nil, errors.New("ridx trailer: file checksum mismatch")
	}
	if pf.pack != nil {
		var wantPack, gotPack [hashSize]byte
		if _, err := mr.ReadAt(wantPack[:], size-ridxTrailerSize); err != nil {
			return nil, err
		}
		if _, err := pf.pack.ReadAt(gotPack[:], int64(pf.pack.Len()-hashSize)); err == nil &&
			!bytes.Equal(gotPack[:], wantPack[:]) {
			return nil, errors.New("ridx trailer: pack checksum mismatch")
		}
	}

	table := mmapData(mr)[ridxHeaderSize : ridxHeaderSize+tableLen]
	ridx := make([]uint32, objCount)
	offs := make([]uint64, objCount)
	var prev uint64
	for i := 0; i < objCount; i++ {
		pos := binary.BigEndian.Uint32(table[i*4:])
		if int(pos) >= len(pf.entries) {
			return nil, fmt.Errorf("ridx: entry %d names idx position %d of %d", i, pos, len(pf.entries))
		}
		off := pf.entries[pos].offset
		if i > 0 && off <= prev {
			return nil, fmt.Errorf("ridx: offsets not strictly increasing at entry %d", i)
		}
		prev = off
		offs[i] = off
		ridx[objCount-1-i] = pos
	}
	if pf.sortedOffsets == nil {
		pf.sortedOffsets = offs
	} else if !slices.Equal(pf.sortedOffsets, offs) {
		return nil, errors.New("ridx: offsets disagree with the idx")
	}
	return ridx, nil
}

// buildReverseFromEntries derives the descending-offset reverse index from
// pf.sortedOffsets and pf.entries.
func buildReverseFromEntries(pf *idxFile) []uint32 {
	n := len(pf.sortedOffsets)
	r := make([]uint32, n)

	offsetToIdx := make(map[uint64]uint32, n)
	for i, e := range pf.entries {
		offsetToIdx[e.offset] = uint32(i)
	}

	for k := range n {
		off := pf.sortedOffsets[n-1-k]
		r[k] = offsetToIdx[off]
	}
	return r
}
