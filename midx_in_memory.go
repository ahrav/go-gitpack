// midx_in_memory.go
//
// Synthesized in-memory multi-pack index for Git repositories that lack an
// on-disk MIDX file. When no "multi-pack-index" file is present in the
// objects/pack directory, the store builds this structure from the individual
// *.idx files so that all object lookups go through a single unified index.
//
// The resulting inMemoryMidx is immutable after construction and safe for
// concurrent reads without additional synchronization. It mirrors the
// lookup behaviour of midxFile (see midx.go) but is backed by Go slices
// instead of a memory-mapped file.

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

// inMemoryMidxRecord is a temporary intermediate used only during
// buildInMemoryMidx construction. It pairs an object ID with the pack's
// ordinal position so that the sort-then-dedup pass can break ties
// deterministically (lowest pack order wins).
type inMemoryMidxRecord struct {
	oid       Hash
	packOrder int
	entry     inMemoryMidxEntry
}

// buildInMemoryMidx merges the object tables from all provided idxFile
// instances into a single sorted, deduplicated index.
//
// The algorithm:
//  1. Collect every (oid, pack, offset) triple from each idx.
//  2. Sort by (oid ASC, packOrder ASC, offset ASC). The secondary sort on
//     packOrder ensures that when duplicate OIDs exist across packs, the
//     copy in the lowest-numbered pack is retained -- matching Git's own
//     "first pack wins" deduplication semantics.
//  3. Deduplicate by skipping consecutive records with the same OID.
//  4. Build a cumulative fanout table from the first byte of each OID.
//
// INVARIANT: after construction, objectID and entries are parallel slices
// of identical length, sorted in ascending OID order with no duplicates.
// The fanout table is cumulative: fanout[b] == number of objects whose
// first byte is <= b.
//
// Returns nil if no valid objects are found across all packs.
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

// findObject looks up an object by its hash and returns the mmap reader for
// the pack that contains it together with the byte offset within that pack.
// It delegates to findEntry and unpacks the result.
func (m *inMemoryMidx) findObject(oid Hash) (*mmap.ReaderAt, uint64, bool) {
	entry, ok := m.findEntry(oid)
	if !ok {
		return nil, 0, false
	}
	return entry.pack, entry.offset, true
}

// findEntry performs a two-stage lookup identical to the on-disk midxFile:
//  1. Use the fanout table to narrow the search to all objects sharing the
//     same first byte as oid.
//  2. Binary search within that range for an exact match.
//
// Returns the entry and true on hit, or a zero-value entry and false on miss.
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
