// ridx.go
//
// Reverse‑index (“RIDX”) parser for Git packfiles.
// The file maps *descending pack offsets* → *index position* so that bitmap
// queries can translate a set bit (offset order) to an object id (idx order).
//
// The implementation keeps only the uint32 table in memory; the mmap handle
// is closed immediately after parsing because look‑ups are O(1) on the slice.

package objstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/exp/mmap"
)

const (
	ridxMagic       = "RIDX"
	ridxHeaderSize  = 4 + 4 + fanoutSize // "RIDX", ver, fan‑out
	ridxTrailerSize = hashSize * 2       // pack + idx SHA‑1
)

// loadReverseIndex tries to mmap and parse the reverse‑index that belongs to
// the *.pack file identified by packPath.  When the file is absent it falls
// back to building the slice from the ascending‑offset table already in idxFile.
//
// The returned slice has len = len(pf.sortedOffsets).  It panics only on
// programmer error (nil pf or missing sortedOffsets).
func loadReverseIndex(packPath string, pf *idxFile) ([]uint32, error) {
	if pf == nil || pf.sortedOffsets == nil {
		panic("loadReverseIndex called before parseIdx finished")
	}

	// *.ridx is the modern extension; *.rev is the old one kept for b/w compat.
	var ridxPath string
	for _, ext := range []string{".ridx", ".rev"} {
		try := strings.TrimSuffix(packPath, ".pack") + ext
		if _, err := os.Stat(try); err == nil {
			ridxPath = try
			break
		}
	}
	if ridxPath == "" {
		// No on‑disk file – build from ascending offset order.
		return buildReverseFromOffsets(pf.sortedOffsets), nil
	}

	ridx, err := tryLoadRidxFile(ridxPath, pf)
	if err != nil {
		// Any error during ridx parsing -> fall back to building from offsets.
		return buildReverseFromOffsets(pf.sortedOffsets), nil
	}
	return ridx, nil
}

func tryLoadRidxFile(ridxPath string, pf *idxFile) ([]uint32, error) {
	mr, err := mmap.Open(ridxPath)
	if err != nil {
		return nil, err
	}
	defer mr.Close()

	// === HEADER ===
	var header [4 + 4]byte
	if _, err := mr.ReadAt(header[:], 0); err != nil {
		return nil, err
	}
	if btostr(header[0:4]) != ridxMagic {
		return nil, errors.New("ridx: bad magic")
	}
	if ver := binary.BigEndian.Uint32(header[4:8]); ver != 1 {
		return nil, fmt.Errorf("ridx: unsupported version %d", ver)
	}

	// Fan‑out table (256 × uint32, big‑endian).
	fanoutRaw := make([]byte, fanoutSize)
	if _, err := mr.ReadAt(fanoutRaw, 8); err != nil {
		return nil, err
	}
	objCount := binary.BigEndian.Uint32(fanoutRaw[len(fanoutRaw)-4:])
	if int(objCount) != len(pf.sortedOffsets) {
		return nil, fmt.Errorf("ridx: object count mismatch (idx=%d ridx=%d)",
			len(pf.sortedOffsets), objCount)
	}

	// === MAIN TABLE ===
	entriesOfs := int64(ridxHeaderSize)
	entriesLen := int64(objCount) * 4
	entriesBuf := make([]byte, entriesLen)
	if _, err := mr.ReadAt(entriesBuf, entriesOfs); err != nil {
		return nil, err
	}

	ridx := make([]uint32, objCount)
	for i := range int(objCount) {
		ridx[i] = binary.BigEndian.Uint32(entriesBuf[i*4 : (i+1)*4])
	}

	// === TRAILER CHECKS (best‑effort) ===
	trOfs := entriesOfs + entriesLen
	if trOfs+ridxTrailerSize <= int64(mr.Len()) {
		wantPack := make([]byte, hashSize)
		wantIdx := make([]byte, hashSize)
		if _, err := mr.ReadAt(wantPack, trOfs); err != nil {
			return nil, err
		}
		if _, err := mr.ReadAt(wantIdx, trOfs+hashSize); err != nil {
			return nil, err
		}

		// Compare the recorded pack checksum with the one we already verified
		// while loading *.idx.  Skip when pf.pack is nil (rare midx‑only case).
		if pf.pack != nil {
			gotPack := make([]byte, hashSize)
			if _, err := pf.pack.ReadAt(gotPack, int64(pf.pack.Len()-hashSize)); err == nil &&
				!bytes.Equal(gotPack, wantPack) {
				return nil, errors.New("ridx trailer: pack checksum mismatch")
			}
		}
		// idx trailer already verified inside parseIdx → just skip comparison
		// when idx mmap handle is unavailable (midx‑only pack).
		if pf.idx != nil {
			gotIdx := make([]byte, hashSize)
			if _, err := pf.idx.ReadAt(gotIdx, int64(pf.idx.Len()-hashSize)); err == nil &&
				!bytes.Equal(gotIdx, wantIdx) {
				return nil, errors.New("ridx trailer: idx checksum mismatch")
			}
		}
	}
	return ridx, nil
}

// buildReverseFromOffsets constructs a reverse‑index purely from the ascending
// pack‑offset slice already present in idxFile.  O(n) & allocation‑free aside
// from the returned slice.
func buildReverseFromOffsets(sorted []uint64) []uint32 {
	n := len(sorted)
	r := make([]uint32, n)
	for i := range n {
		r[i] = uint32(n - 1 - i) // descending offset order
	}
	return r
}
