package objstore

import (
	"bytes"
	"slices"

	"golang.org/x/exp/mmap"
)

// inMemoryMidxEntry maps a single object ID to its source pack, byte offset,
// and CRC-32 checksum. It mirrors the data we need from on-disk MIDX tables.
type inMemoryMidxEntry struct {
	pack   *mmap.ReaderAt
	offset uint64
	crc    uint32
}

// inMemoryMidx is a synthesized multi-pack index derived from parsed *.idx
// files when no on-disk multi-pack-index is available.
type inMemoryMidx struct {
	fanout   [fanoutEntries]uint32
	objectID []Hash
	entries  []inMemoryMidxEntry
}

type inMemoryMidxRecord struct {
	oid       Hash
	packOrder int
	entry     inMemoryMidxEntry
}

func buildInMemoryMidx(packs []*idxFile) *inMemoryMidx {
	total := 0
	for _, pf := range packs {
		if pf == nil || pf.pack == nil {
			continue
		}
		if len(pf.oidTable) == 0 || len(pf.entries) == 0 {
			continue
		}
		if len(pf.oidTable) < len(pf.entries) {
			total += len(pf.oidTable)
			continue
		}
		total += len(pf.entries)
	}
	if total == 0 {
		return nil
	}

	records := make([]inMemoryMidxRecord, 0, total)
	for packOrder, pf := range packs {
		if pf == nil || pf.pack == nil {
			continue
		}

		limit := len(pf.entries)
		if len(pf.oidTable) < limit {
			limit = len(pf.oidTable)
		}
		for i := range limit {
			entry := pf.entries[i]
			records = append(records, inMemoryMidxRecord{
				oid:       pf.oidTable[i],
				packOrder: packOrder,
				entry: inMemoryMidxEntry{
					pack:   pf.pack,
					offset: entry.offset,
					crc:    entry.crc,
				},
			})
		}
	}
	if len(records) == 0 {
		return nil
	}

	slices.SortFunc(records, func(a, b inMemoryMidxRecord) int {
		if cmp := bytes.Compare(a.oid[:], b.oid[:]); cmp != 0 {
			return cmp
		}
		if a.packOrder < b.packOrder {
			return -1
		}
		if a.packOrder > b.packOrder {
			return 1
		}
		if a.entry.offset < b.entry.offset {
			return -1
		}
		if a.entry.offset > b.entry.offset {
			return 1
		}
		return 0
	})

	objectIDs := make([]Hash, 0, len(records))
	entries := make([]inMemoryMidxEntry, 0, len(records))
	var prev Hash
	hasPrev := false
	for _, rec := range records {
		if hasPrev && rec.oid == prev {
			continue
		}
		objectIDs = append(objectIDs, rec.oid)
		entries = append(entries, rec.entry)
		prev = rec.oid
		hasPrev = true
	}

	var fanout [fanoutEntries]uint32
	for _, oid := range objectIDs {
		fanout[oid[0]]++
	}
	for i := 1; i < fanoutEntries; i++ {
		fanout[i] += fanout[i-1]
	}

	return &inMemoryMidx{
		fanout:   fanout,
		objectID: objectIDs,
		entries:  entries,
	}
}

func (m *inMemoryMidx) findObject(oid Hash) (*mmap.ReaderAt, uint64, bool) {
	entry, ok := m.findEntry(oid)
	if !ok {
		return nil, 0, false
	}
	return entry.pack, entry.offset, true
}

func (m *inMemoryMidx) findEntry(oid Hash) (inMemoryMidxEntry, bool) {
	first := oid[0]
	start := uint32(0)
	if first > 0 {
		start = m.fanout[first-1]
	}
	end := m.fanout[first]
	if start == end {
		return inMemoryMidxEntry{}, false
	}

	rel, ok := slices.BinarySearchFunc(
		m.objectID[start:end],
		oid,
		func(a, b Hash) int {
			return bytes.Compare(a[:], b[:])
		},
	)
	if !ok {
		return inMemoryMidxEntry{}, false
	}

	return m.entries[int(start)+rel], true
}
