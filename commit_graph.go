// commit_graph.go
//
// Commit-graph ("CGPH") parser for Git repositories.
//
// A commit-graph stores commit metadata in a compact, binary table so that
// parent lookup, ancestry walks, and date-based queries can be answered
// without inflating individual commit objects.  This file reads one or more
// *.graph files (single or split-chain) and turns them into an in-memory
// CommitGraphData structure:
//
//     Parents      map[Hash][]Hash   // immediate parents per commit
//     OrderedOIDs  []Hash            // commits in on-disk graph order
//     TreeOIDs     []Hash            // root tree for each commit
//     Timestamps   []int64           // commit timestamps
//     OIDToIndex   map[Hash]int      // commit → row index
//
// Both graph format versions 1 and 2 (SHA-1) are supported. The on-disk data
// is read via mmap for performance; all mmaps are closed once parsing
// completes.  The rest of the file contains helpers for locating graph files,
// parsing individual chunks, and resolving parent lists across a split chain.

package objstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/exp/mmap"
)

const (
	graphSignature = "CGPH"

	// chunk ids - chunkOIDF and chunkOIDL are defined in midx.go
	chunkCDAT = 0x43444154 // CDAT
	chunkEDGE = 0x45444745 // EDGE

	graphParentNone = 0x70000000
	graphLastEdge   = 0x80000000
)

const cdatRecordSize = hashSize + 16

// Parents maps each commit OID to the OIDs of its immediate parents.
//
// The alias exists so callers can use a descriptive name instead of the more
// verbose map literal when working with parent relationships returned from
// LoadCommitGraph.
type Parents = map[Hash][]Hash

// CommitGraphData provides a read-only, in-memory view of one or more
// on-disk Git commit-graph files.
//
// The value is returned by LoadCommitGraph and is intended for callers that
// need efficient ancestry, history, or metadata look-ups without inflating
// individual commit objects.  All slices are index-aligned: the commit at
// OrderedOIDs[i] has metadata at TreeOIDs[i] and Timestamps[i], and can be
// found in O(1) via OIDToIndex.  Once constructed the instance is immutable
// and safe for concurrent reads.
type CommitGraphData struct {
	// Parents maps a commit OID to the OIDs of its immediate parents.
	// The map contains an entry for every commit included in the commit-graph.
	Parents Parents

	// OrderedOIDs lists commit OIDs in the precise order they appear
	// in the commit-graph file.
	OrderedOIDs []Hash

	// TreeOIDs records the root tree OID for each commit in OrderedOIDs.
	TreeOIDs []Hash

	// Timestamps stores the author timestamp, in seconds since the Unix
	// epoch, for each commit in OrderedOIDs.
	Timestamps []int64

	// OIDToIndex provides O(1) reverse lookup from a commit OID to its
	// index within OrderedOIDs and the parallel metadata slices.
	OIDToIndex map[Hash]int
}

// LoadCommitGraph parses the commit-graph files that belong to the Git
// repository located at objectsDir and returns an in-memory
// CommitGraphData.
//
// A commit-graph can be materialised as either a single file
// $objects/info/commit-graph or as a "split chain" referenced from
// $objects/info/commit-graphs/commit-graph-chain.
// LoadCommitGraph transparently handles both layouts.
//
// If no commit-graph is present, the function returns (nil, nil).
// On success the returned CommitGraphData is completely detached from the
// memory-mapped files – they are closed before the function returns – and is
// therefore safe for concurrent reads.
//
// Error semantics
//   - Any I/O or format violation encountered while reading a graph file is
//     reported immediately; all previously opened mmaps are closed before the
//     error is propagated.
//   - A length or parent-index mismatch across chained graph files results in
//     a descriptive *errors.errorString.
//
// The caller should cache the returned structure if multiple look-ups are
// expected; constructing it is O(number of commits).
func LoadCommitGraph(objectsDir string) (*CommitGraphData, error) {
	chain, err := discoverGraphFiles(objectsDir)
	if err != nil {
		return nil, err
	}
	if len(chain) == 0 {
		return nil, nil
	}

	// Parse every graph file in the chain and concatenate their data.
	var allOids []Hash
	var allTrees []Hash
	var allTimes []int64
	fileInfo := make([]parsedGraph, len(chain))

	for i, path := range chain {
		pg, err := parseGraphFile(path)
		if err != nil {
			// Close opened mmaps before returning.
			for j := 0; j < i; j++ {
				fileInfo[j].mr.Close()
			}
			return nil, err
		}
		fileInfo[i] = pg
		allOids = append(allOids, pg.oids...)
		allTrees = append(allTrees, pg.trees...)
		allTimes = append(allTimes, pg.times...)
	}

	if len(allOids) != len(allTrees) || len(allOids) != len(allTimes) {
		for _, g := range fileInfo {
			g.mr.Close()
		}
		return nil, errors.New("commit-graph data length mismatch")
	}

	// Build parent map and OID-to-index mapping.
	parents := make(Parents, len(allOids))
	oidToIndex := make(map[Hash]int, len(allOids))
	offset := 0

	for _, g := range fileInfo {
		if err := g.resolveParentsInto(parents, allOids, offset); err != nil {
			for _, g := range fileInfo {
				g.mr.Close()
			}
			return nil, err
		}
		offset += len(g.oids)
	}

	for i, oid := range allOids {
		// First occurrence (newest layer) wins.
		if _, dup := oidToIndex[oid]; !dup {
			oidToIndex[oid] = i
		}
	}

	// Close all mmaps; the returned slices hold their own copies.
	for _, g := range fileInfo {
		g.mr.Close()
	}

	return &CommitGraphData{
		Parents:     parents,
		OrderedOIDs: allOids,
		TreeOIDs:    allTrees,
		Timestamps:  allTimes,
		OIDToIndex:  oidToIndex,
	}, nil
}

// parsedGraph represents the raw, per-file view of a commit-graph.
// It is produced by parseGraphFile and is subsequently merged into the
// higher-level CommitGraphData structure.
// The struct is internal to this package and is therefore unexported,
// but its invariants still matter:
//
//   - All slice fields have the same length, one entry per commit row
//     recorded in the *.graph file.
//   - Indices stored in p1, p2raw, and edge refer either to other rows in
//     the same commit-graph file or, after offset adjustment, to rows in the
//     concatenated commit list supplied to resolveParentsInto.
//   - mr must be closed by the caller once the parsedGraph is no longer
//     needed—typically right after the data has been copied or merged.
//
// No method mutates the slices after construction, so a parsedGraph is
// safe for concurrent read-only access.
type parsedGraph struct {
	// mr provides random-access reads over the memory-mapped *.graph file.
	// The mapping is valid for the lifetime of the parsedGraph and must be
	// closed by the caller.
	mr *mmap.ReaderAt

	// oids holds the commit object IDs in on-disk order.
	oids []Hash

	// trees stores the root tree object ID for each commit in oids.
	trees []Hash

	// times records the author timestamp, in seconds since the Unix epoch,
	// for each commit in oids.
	times []int64

	// p1 contains the index of the first parent for each commit.
	// graphParentNone signals that the commit is a root.
	p1 []uint32

	// p2raw contains either the index of the second parent, an encoded edge
	// table reference, or graphParentNone if no second parent exists.
	p2raw []uint32

	// edge is the flattened edge table used to resolve additional parents
	// in octopus merges. Entries are encoded according to the Git
	// commit-graph specification.
	edge []uint32
}

// resolveParentsInto converts parent indices from this graph file into actual OIDs,
// storing the result in dst. It handles split commit-graph chains by applying the
// given offset and respects layer precedence (newer layers override older ones).
// This optimized version pre-scans commits to estimate parent counts and pre-allocates
// slices accordingly to reduce memory allocations and improve performance.
func (g parsedGraph) resolveParentsInto(dst Parents, all []Hash, offset int) error {
	// Pre-scan to estimate parent counts for each commit.
	// Benchmarks show this additional step is worth it.
	parentCounts := make([]int, len(g.oids))
	for i := range g.oids {
		count := 0

		if g.p1[i] != graphParentNone {
			count++
		}

		// Count second parent or octopus parents.
		if v := g.p2raw[i]; v != graphParentNone {
			if v&graphLastEdge == 0 {
				// Direct second parent.
				count++
			} else {
				// Edge pointer for octopus merges - count actual parents.
				idx := int(v & ^uint32(graphLastEdge))
				if idx < len(g.edge) {
					for idx < len(g.edge) {
						w := g.edge[idx]
						count++
						idx++
						if w&graphLastEdge != 0 {
							break
						}
					}
				}
			}
		}
		parentCounts[i] = count
	}

	for i, oid := range g.oids {
		// Newer layers take precedence.
		if _, seen := dst[oid]; seen {
			continue
		}

		// Pre-allocate with exact capacity to avoid reallocations.
		ps := make([]Hash, 0, parentCounts[i])

		// First parent.
		if p := g.p1[i]; p != graphParentNone {
			adjustedP := int(p) + offset
			if adjustedP >= len(all) {
				return fmt.Errorf("parent index %d out of bounds", adjustedP)
			}
			ps = append(ps, all[adjustedP])
		}

		// Fast path for the common case: exactly 2 parents (90%+ of commits).
		if g.p2raw[i] != graphParentNone && g.p2raw[i]&graphLastEdge == 0 {
			adj := int(g.p2raw[i]) + offset
			if adj >= len(all) {
				return fmt.Errorf("parent index %d out of bounds", adj)
			}
			ps = append(ps, all[adj])
			dst[oid] = ps
			continue
		}

		// Second parent or edge pointer.
		v := g.p2raw[i]
		switch {
		case v == graphParentNone:
			// No second parent.
		case v&graphLastEdge == 0:
			// Direct second parent index.
			adjustedV := int(v) + offset
			if adjustedV >= len(all) {
				return fmt.Errorf("parent index %d out of bounds", adjustedV)
			}
			ps = append(ps, all[adjustedV])
		default:
			// Edge pointer for octopus merges.
			idx := int(v & ^uint32(graphLastEdge))
			if idx >= len(g.edge) {
				return fmt.Errorf("edge index %d out of bounds (edge len=%d)", idx, len(g.edge))
			}

			// Read parents from edge list until terminator.
			for idx < len(g.edge) {
				w := g.edge[idx]
				parentIdx := int(w & ^uint32(graphLastEdge)) + offset
				if parentIdx >= len(all) {
					return fmt.Errorf("edge parent index %d out of bounds", parentIdx)
				}
				ps = append(ps, all[parentIdx])
				idx++
				if w&graphLastEdge != 0 {
					break
				}
			}
		}
		dst[oid] = ps
	}
	return nil
}

// discoverGraphFiles finds commit-graph files for the given Git repository.
// It follows Git's search order: split chains first, then single files.
// Returns nil if no commit-graph is found.
func discoverGraphFiles(objectsDir string) ([]string, error) {
	infoDir := filepath.Join(objectsDir, "info")

	// Try split chain first.
	chainFile := filepath.Join(infoDir, "commit-graphs", "commit-graph-chain")
	if f, err := os.Open(chainFile); err == nil {
		defer f.Close()

		var paths []string
		var hash string
		for {
			_, err := fmt.Fscanln(f, &hash)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			paths = append(paths,
				filepath.Join(infoDir, "commit-graphs", "graph-"+hash+".graph"))
		}
		if len(paths) > 0 {
			return paths, nil
		}
	}

	// Try single file.
	single := filepath.Join(infoDir, "commit-graph")
	if _, err := os.Stat(single); err == nil {
		return []string{single}, nil
	}

	return nil, nil
}

// parseGraphFile reads and validates a commit-graph file, returning the parsed data.
// The returned parsedGraph keeps the memory mapping open for the caller to close.
func parseGraphFile(path string) (parsedGraph, error) {
	mr, err := mmap.Open(path)
	if err != nil {
		return parsedGraph{}, err
	}

	// Ensure cleanup on all error paths.
	var result parsedGraph
	defer func() {
		if result.mr == nil && mr != nil {
			mr.Close()
		}
	}()

	// Parse header.
	var hdr [8]byte
	if _, err = mr.ReadAt(hdr[:], 0); err != nil {
		return parsedGraph{}, err
	}
	if string(hdr[0:4]) != graphSignature {
		return parsedGraph{}, fmt.Errorf("bad commit-graph signature: got %q, expected %q", string(hdr[0:4]), graphSignature)
	}
	if hdr[4] != 1 && hdr[4] != 2 {
		return parsedGraph{}, fmt.Errorf("unsupported graph version %d", hdr[4])
	}
	if hdr[5] != 1 {
		return parsedGraph{}, fmt.Errorf("unsupported hash version %d (only SHA-1 supported)", hdr[5])
	}

	chunks := int(hdr[6])

	// Parse chunk table with single ReadAt.
	type chunkDesc struct {
		id  uint32
		off uint64
	}
	desc := make([]chunkDesc, chunks+1)

	chunkTableSize := (chunks + 1) * 12
	buf := make([]byte, chunkTableSize)
	if _, err := mr.ReadAt(buf, 8); err != nil {
		return parsedGraph{}, err
	}

	// Parse chunk descriptors from buffer.
	for i := range desc {
		base := i * 12
		desc[i].id = binary.BigEndian.Uint32(buf[base : base+4])
		desc[i].off = binary.BigEndian.Uint64(buf[base+4 : base+12])
	}

	// Validate no overlapping chunks.
	for i := 0; i < len(desc)-1; i++ {
		if desc[i].off > desc[i+1].off && desc[i].id != 0 {
			return parsedGraph{}, fmt.Errorf("overlapping chunks: chunk %x at offset %d overlaps with next chunk", desc[i].id, desc[i].off)
		}
	}

	find := func(id uint32) (int64, int64, bool) {
		for i := 0; i < len(desc)-1; i++ {
			if desc[i].id == id {
				off := int64(desc[i].off)
				size := int64(desc[i+1].off) - off
				if size < 0 {
					return 0, 0, false
				}
				return off, size, true
			}
		}
		return 0, 0, false
	}

	off, size, ok := find(chunkOIDF)
	if !ok || size != fanoutSize {
		return parsedGraph{}, errors.New("OIDF missing/wrong size")
	}
	fanout := make([]byte, fanoutSize)
	if _, err := mr.ReadAt(fanout, off); err != nil {
		return parsedGraph{}, err
	}
	total := binary.BigEndian.Uint32(fanout[fanoutSize-4:])

	// Validate fanout monotonicity.
	var prev uint32
	for i := range fanoutEntries {
		val := binary.BigEndian.Uint32(fanout[i*4:])
		if val < prev {
			return parsedGraph{}, fmt.Errorf("fanout not monotonic at index %d: %d < %d", i, val, prev)
		}
		prev = val
	}

	off, size, ok = find(chunkOIDL)
	if !ok {
		return parsedGraph{}, errors.New("OIDL chunk missing")
	}
	expectedSize := int64(total * hashSize)
	if size != expectedSize {
		return parsedGraph{}, fmt.Errorf("OIDL size mismatch: got %d, expected %d", size, expectedSize)
	}

	var oids []Hash
	if total > 0 {
		oids = make([]Hash, total)
		oidsBuf := unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), int(size))
		if _, err := mr.ReadAt(oidsBuf, off); err != nil {
			return parsedGraph{}, fmt.Errorf("reading OIDL chunk: %w", err)
		}
	}

	off, size, ok = find(chunkCDAT)
	if !ok {
		return parsedGraph{}, errors.New("CDAT chunk missing")
	}
	expectedCdatSize := int64(total) * cdatRecordSize
	if size != expectedCdatSize {
		return parsedGraph{}, fmt.Errorf("CDAT size mismatch: got %d, expected %d", size, expectedCdatSize)
	}

	// Parse commit data records directly from mmap using recycled buffer.
	n := int(total)
	p1 := make([]uint32, n)
	p2 := make([]uint32, n)
	trees := make([]Hash, n)
	times := make([]int64, n)

	if total > 0 {
		buf := make([]byte, cdatRecordSize) // recycled buffer
		for i := range n {
			if _, err := mr.ReadAt(buf, off+int64(i*cdatRecordSize)); err != nil {
				return parsedGraph{}, fmt.Errorf("reading CDAT record %d: %w", i, err)
			}
			copy(trees[i][:], buf[:hashSize])
			p1[i] = binary.BigEndian.Uint32(buf[hashSize:])
			p2[i] = binary.BigEndian.Uint32(buf[hashSize+4:])
			genTime := binary.BigEndian.Uint64(buf[hashSize+8:])
			times[i] = int64(genTime & 0x3FFFFFFFF)
		}
	}

	// Parse optional edge chunk for octopus merges.
	var edge []uint32
	if off, size, ok = find(chunkEDGE); ok {
		if size%4 != 0 {
			return parsedGraph{}, errors.New("EDGE chunk not aligned")
		}
		edge = make([]uint32, size/4)
		edata := make([]byte, size)
		if _, err := mr.ReadAt(edata, off); err != nil {
			return parsedGraph{}, err
		}
		for i := range edge {
			edge[i] = binary.BigEndian.Uint32(edata[i*4:])
		}
	}

	result = parsedGraph{
		mr:    mr,
		oids:  oids,
		trees: trees,
		times: times,
		p1:    p1,
		p2raw: p2,
		edge:  edge,
	}
	return result, nil
}
