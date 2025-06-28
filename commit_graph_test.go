// commit_graph_test.go
package objstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/* ------------------------------------------------------------------------- */
/*                               Helpers                                     */
/* ------------------------------------------------------------------------- */

const (
	noParent     = 0x70000000
	lastEdgeMask = 0x80000000
	hashLen      = 20
)

type cgCommit struct {
	oid         Hash     // Commit ID.
	tree        Hash     // Root tree (arbitrary).
	parents     []uint32 // Positional parent indexes (first two go in CDAT).
	edgeParents []uint32 // Extra parent indexes stored in EDGE.
	edgePointer uint32   // Index in EDGE slice (only for octopus).
	useEdge     bool     // Explicitly indicates this commit should use edge encoding.
}

// makeCGHash creates a Hash from a hex string, padding to 20 bytes.
func makeCGHash(hex string) Hash {
	var h Hash
	for i := 0; i < len(hex) && i < 40; i += 2 {
		var b byte
		if i+1 < len(hex) {
			if hex[i] >= '0' && hex[i] <= '9' {
				b = (hex[i] - '0') << 4
			} else if hex[i] >= 'a' && hex[i] <= 'f' {
				b = (hex[i] - 'a' + 10) << 4
			} else if hex[i] >= 'A' && hex[i] <= 'F' {
				b = (hex[i] - 'A' + 10) << 4
			}
			if hex[i+1] >= '0' && hex[i+1] <= '9' {
				b |= hex[i+1] - '0'
			} else if hex[i+1] >= 'a' && hex[i+1] <= 'f' {
				b |= hex[i+1] - 'a' + 10
			} else if hex[i+1] >= 'A' && hex[i+1] <= 'F' {
				b |= hex[i+1] - 'A' + 10
			}
		}
		h[i/2] = b
	}
	return h
}

// buildCGFile builds an in-memory commit-graph version-1 file.
// When edge is nil, the EDGE chunk is omitted.
func buildCGFile(commits []cgCommit, edge []uint32) []byte {
	var fan [fanoutEntries]uint32
	for _, c := range commits {
		fan[c.oid[0]]++
	}
	// Make fanout values cumulative.
	var total uint32
	for i := range fanoutEntries {
		total += fan[i]
		fan[i] = total
	}

	var (
		buf         bytes.Buffer
		chunkOffTbl []struct {
			id  uint32
			off uint64
		}
	)

	// Write file header.
	buf.WriteString("CGPH")
	buf.WriteByte(1)
	buf.WriteByte(1)
	buf.WriteByte(byte(3 + btoi(edge != nil)))
	buf.WriteByte(0)

	// Prepare chunk table rows (offsets will be filled later).
	ids := []uint32{chunkOIDF, chunkOIDL, chunkCDAT}
	if edge != nil {
		ids = append(ids, chunkEDGE)
	}
	for _, id := range ids {
		chunkOffTbl = append(chunkOffTbl, struct {
			id  uint32
			off uint64
		}{id: id, off: 0})
	}
	chunkOffTbl = append(chunkOffTbl, struct {
		id  uint32
		off uint64
	}{id: 0, off: 0})

	// Reserve space for chunk table.
	buf.Write(make([]byte, len(chunkOffTbl)*12))

	// Helper function to append a chunk and record its offset.
	appendChunk := func(id uint32, data []byte) {
		chunkOffTbl = setChunkOffset(chunkOffTbl, id, uint64(buf.Len()))
		buf.Write(data)
	}

	// Build and append OIDF chunk (fanout table).
	fanBytes := make([]byte, fanoutEntries*4)
	for i, v := range fan {
		binary.BigEndian.PutUint32(fanBytes[i*4:], v)
	}
	appendChunk(chunkOIDF, fanBytes)

	// Build and append OIDL chunk (object IDs).
	oidBytes := make([]byte, len(commits)*hashLen)
	for i, c := range commits {
		copy(oidBytes[i*hashLen:], c.oid[:])
	}
	appendChunk(chunkOIDL, oidBytes)

	// Build and append CDAT chunk (commit data).
	const recSize = hashLen + 16
	cdat := make([]byte, len(commits)*recSize)
	for i, c := range commits {
		off := i * recSize
		copy(cdat[off:], c.tree[:])

		// Handle parent indices properly.
		var p1, p2 uint32 = noParent, noParent

		if len(c.parents) > 0 {
			p1 = c.parents[0]
		}

		// Use edge pointer for octopus merges.
		if c.useEdge {
			p2 = c.edgePointer | lastEdgeMask
		} else if len(c.parents) > 1 {
			p2 = c.parents[1]
		}

		binary.BigEndian.PutUint32(cdat[off+hashLen:], p1)
		binary.BigEndian.PutUint32(cdat[off+hashLen+4:], p2)
	}
	appendChunk(chunkCDAT, cdat)

	// Build and append EDGE chunk if provided.
	if edge != nil {
		edgeBytes := make([]byte, len(edge)*4)
		for i, v := range edge {
			binary.BigEndian.PutUint32(edgeBytes[i*4:], v)
		}
		appendChunk(chunkEDGE, edgeBytes)
	}

	// Write back chunk table with calculated offsets.
	startTable := 8
	for i, row := range chunkOffTbl {
		if i == len(chunkOffTbl)-1 {
			row.off = uint64(buf.Len())
		}
		binary.BigEndian.PutUint32(buf.Bytes()[startTable+i*12:], row.id)
		binary.BigEndian.PutUint64(buf.Bytes()[startTable+i*12+4:], row.off)
	}

	return buf.Bytes()
}

// btoi converts a boolean to an integer (0 or 1).
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func setChunkOffset(tbl []struct {
	id  uint32
	off uint64
}, id uint32, off uint64) []struct {
	id  uint32
	off uint64
} {
	for i := range tbl {
		if tbl[i].id == id {
			tbl[i].off = off
			return tbl
		}
	}
	return tbl
}

/* ------------------------------------------------------------------------- */
/*                                Tests                                      */
/* ------------------------------------------------------------------------- */

// TestCommitGraphChainParentAcrossLayers tests that parent indices in the tip layer
// can correctly reference commits in the base layer of a chained commit graph.
func TestCommitGraphChainParentAcrossLayers(t *testing.T) {
	tmp := t.TempDir()

	// Create base layer with commits A and B.
	A := makeCGHash("01")
	B := makeCGHash("02")
	baseCommits := []cgCommit{
		{oid: A, tree: makeCGHash("31"), parents: nil},
		{oid: B, tree: makeCGHash("32"), parents: []uint32{0}},
	}
	baseData := buildCGFile(baseCommits, nil)
	baseHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	basePath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+baseHash+".graph")
	mustWrite(t, basePath, baseData)

	// Create tip layer with commit C that has a parent at index 2 (which lives in base).
	C := makeCGHash("03")
	tipCommits := []cgCommit{
		{oid: C, tree: makeCGHash("33"), parents: []uint32{2}},
	}
	tipData := buildCGFile(tipCommits, nil)
	tipHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tipPath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+tipHash+".graph")
	mustWrite(t, tipPath, tipData)

	// Create chain file (tip first).
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(tipHash+"\n"+baseHash+"\n"))

	// Parse the commit graph chain.
	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load chain: %v", err)
	require.NotNil(t, graphData, "expected commit graph data, got nil")
	require.Len(t, graphData.OrderedOIDs, 3, "want 3 commits, got %d", len(graphData.OrderedOIDs))

	// Verify that C has B as its single parent.
	parents := graphData.Parents[C]
	if assert.Len(t, parents, 1) {
		assert.Equal(t, B, parents[0], "cross-layer parent not resolved: got %v, want [%x]", parents, B)
	}
}

// TestCommitGraphEdgePointerMisaligned tests that EDGE pointers work correctly
// even when not starting at index 0, ensuring the current code handles non-zero offsets.
func TestCommitGraphEdgePointerMisaligned(t *testing.T) {
	tmp := t.TempDir()

	// Create commits A, B, and octopus merge M.
	A := makeCGHash("11")
	B := makeCGHash("22")
	M := makeCGHash("33")

	edge := []uint32{
		lastEdgeMask,     // Dummy list 0 (one bogus word).
		lastEdgeMask,     // Dummy list 1.
		1 | lastEdgeMask, // Real list starts here: parent index 1 (B).
	}

	commits := []cgCommit{
		{oid: A, tree: makeCGHash("41"), parents: nil},
		{oid: B, tree: makeCGHash("42"), parents: []uint32{0}},
		{
			oid:         M,
			tree:        makeCGHash("43"),
			parents:     []uint32{0},
			edgePointer: 2, // Point to third word in EDGE.
			useEdge:     true,
		},
	}
	data := buildCGFile(commits, edge)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "parse: %v", err)

	// Get parents for merge commit M.
	parents := graphData.Parents[M]

	// Expected parents = [A, B].
	want := []Hash{A, B}
	assert.Equal(t, want, parents, "octopus parents wrong: got %v, want %v", parents, want)
}

// mustWrite is a test helper that writes data to a file, creating parent directories as needed.
func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755), "mkdir: %v")
	require.NoError(t, os.WriteFile(path, data, 0o644), "write %s: %v", path)
}

/* ------------------------------------------------------------------------- */
/*                        File Discovery & Chain Tests                       */
/* ------------------------------------------------------------------------- */

// TestCommitGraphEmptyChainFallback tests that when a chain file is empty,
// the loader falls back to using a single commit-graph file.
func TestCommitGraphEmptyChainFallback(t *testing.T) {
	tmp := t.TempDir()

	// Create empty chain file.
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(""))

	// Create valid single commit-graph file.
	A := makeCGHash("aa")
	commits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: nil},
	}
	data := buildCGFile(commits, nil)
	singlePath := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, singlePath, data)

	// Load and verify fallback behavior.
	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)
	require.NotNil(t, graphData, "expected graph data")
	assert.Len(t, graphData.OrderedOIDs, 1, "expected 1 commit, got %d", len(graphData.OrderedOIDs))
}

// TestCommitGraphChainMissingFiles tests error handling when chain file
// references non-existent graph files.
func TestCommitGraphChainMissingFiles(t *testing.T) {
	tmp := t.TempDir()

	// Create chain file referencing non-existent graphs.
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n"+
			"cafebabecafebabecafebabecafebabecafebabe\n"))

	// Should fail to load.
	_, err := LoadCommitGraph(tmp)
	require.Error(t, err, "expected error for missing graph files")
}

// TestCommitGraphChainPrecedence tests that when both chain and single files exist,
// the chain takes precedence over the single file.
func TestCommitGraphChainPrecedence(t *testing.T) {
	tmp := t.TempDir()

	// Create single file with commit A.
	A := makeCGHash("aa")
	singleCommits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: nil},
	}
	singleData := buildCGFile(singleCommits, nil)
	singlePath := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, singlePath, singleData)

	// Create chain with commit B.
	B := makeCGHash("bb")
	chainCommits := []cgCommit{
		{oid: B, tree: makeCGHash("22"), parents: nil},
	}
	chainData := buildCGFile(chainCommits, nil)
	chainHash := "1234567890123456789012345678901234567890"
	chainPath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+chainHash+".graph")
	mustWrite(t, chainPath, chainData)

	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(chainHash+"\n"))

	// Load and verify chain takes precedence.
	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)
	if assert.Len(t, graphData.OrderedOIDs, 1) {
		assert.Equal(t, B, graphData.OrderedOIDs[0], "chain should take precedence")
	}
}

/* ------------------------------------------------------------------------- */
/*                          Chunk Validation Tests                           */
/* ------------------------------------------------------------------------- */

// TestCommitGraphMissingChunks tests error handling when required chunks
// are missing from the commit graph file.
func TestCommitGraphMissingChunks(t *testing.T) {
	tmp := t.TempDir()

	t.Run("MissingOIDF", func(t *testing.T) {
		buf := bytes.Buffer{}
		// Write header.
		buf.WriteString("CGPH")
		buf.WriteByte(1) // Version.
		buf.WriteByte(1) // Hash version.
		buf.WriteByte(2) // Chunk count (only OIDL, CDAT).
		buf.WriteByte(0) // Reserved.

		// Write chunk table (no OIDF).
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
		binary.Write(&buf, binary.BigEndian, uint64(44)) // Offset after table.
		binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
		binary.Write(&buf, binary.BigEndian, uint64(64))  // Offset.
		binary.Write(&buf, binary.BigEndian, uint32(0))   // Terminator.
		binary.Write(&buf, binary.BigEndian, uint64(100)) // Final offset.

		// Write OIDL chunk.
		buf.Write(make([]byte, 20)) // One hash.
		// Write CDAT chunk.
		buf.Write(make([]byte, 36)) // One record.

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, buf.Bytes())

		_, err := LoadCommitGraph(tmp)
		assert.Error(t, err, "expected error for missing OIDF")
	})
}

// TestCommitGraphChunkSizeMismatch tests error handling when chunk sizes
// don't match the expected values based on other chunks.
func TestCommitGraphChunkSizeMismatch(t *testing.T) {
	tmp := t.TempDir()

	t.Run("OIDLSizeMismatch", func(t *testing.T) {
		// Manually build a file with OIDL size that doesn't match fanout.
		buf := bytes.Buffer{}
		// Write header.
		buf.WriteString("CGPH")
		buf.WriteByte(1) // Version.
		buf.WriteByte(1) // Hash version.
		buf.WriteByte(3) // Chunk count.
		buf.WriteByte(0) // Reserved.

		// Write chunk table.
		tableStart := buf.Len()
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
		binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
		binary.Write(&buf, binary.BigEndian, uint32(0)) // Terminator.
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.

		// Write OIDF indicating 2 commits.
		oidfOffset := buf.Len()
		fanout := make([]byte, fanoutSize)
		// Set fanout to indicate 2 commits.
		for i := 0xaa; i < 256; i++ {
			binary.BigEndian.PutUint32(fanout[i*4:], 2)
		}
		buf.Write(fanout)

		// Write OIDL with only 1 OID (mismatch!).
		oidlOffset := buf.Len()
		h := makeCGHash("aa")
		buf.Write(h[:]) // Only 1 OID instead of 2.

		// Write CDAT with 2 records to match fanout.
		cdatOffset := buf.Len()
		// First commit.
		h = makeCGHash("11")
		buf.Write(h[:]) // Tree.
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Generation/time.
		// Second commit.
		h = makeCGHash("22")
		buf.Write(h[:]) // Tree.
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Generation/time.

		// Final offset.
		finalOffset := buf.Len()

		// Fix up chunk table.
		data := buf.Bytes()
		binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
		binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
		binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
		binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if assert.Error(t, err, "expected error for OIDL size mismatch") {
			assert.True(t, strings.Contains(err.Error(), "OIDL") && strings.Contains(err.Error(), "size"), "expected error about OIDL size, got: %v", err)
		}
	})

	t.Run("CDATSizeMismatch", func(t *testing.T) {
		// Build normal file.
		A := makeCGHash("aa")
		B := makeCGHash("bb")
		commits := []cgCommit{
			{oid: A, tree: makeCGHash("11"), parents: nil},
			{oid: B, tree: makeCGHash("22"), parents: []uint32{0}},
		}
		data := buildCGFile(commits, nil)

		// Truncate CDAT chunk.
		data = data[:len(data)-20]

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		assert.Error(t, err, "expected error for CDAT size mismatch")
	})

	t.Run("FANOUTWrongSize", func(t *testing.T) {
		// Manually build a file with wrong OIDF size.
		buf := bytes.Buffer{}
		// Write header.
		buf.WriteString("CGPH")
		buf.WriteByte(1) // Version.
		buf.WriteByte(1) // Hash version.
		buf.WriteByte(3) // Chunk count.
		buf.WriteByte(0) // Reserved.

		// Write chunk table.
		tableStart := buf.Len()
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
		binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
		binary.Write(&buf, binary.BigEndian, uint32(0)) // Terminator.
		binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.

		// Write OIDF with wrong size (only 100 bytes instead of 1024).
		oidfOffset := buf.Len()
		buf.Write(make([]byte, 100))

		// Write OIDL (empty).
		oidlOffset := buf.Len()

		// Write CDAT (empty).
		cdatOffset := buf.Len()

		// Final offset.
		finalOffset := buf.Len()

		// Fix up chunk table.
		data := buf.Bytes()
		binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
		binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
		binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
		binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if assert.Error(t, err, "expected error for wrong OIDF size") {
			assert.Contains(t, err.Error(), "OIDF", "expected error about OIDF, got: %v", err)
		}
	})
}

// TestCommitGraphOverlappingChunks tests error handling when chunk offsets
// indicate overlapping data regions.
func TestCommitGraphOverlappingChunks(t *testing.T) {
	tmp := t.TempDir()

	buf := bytes.Buffer{}
	// Write header.
	buf.WriteString("CGPH")
	buf.WriteByte(1) // Version.
	buf.WriteByte(1) // Hash version.
	buf.WriteByte(3) // Chunk count.
	buf.WriteByte(0) // Reserved.

	// Write chunk table with overlapping offsets.
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
	binary.Write(&buf, binary.BigEndian, uint64(56)) // Starts at 56.
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
	binary.Write(&buf, binary.BigEndian, uint64(100)) // Overlaps with OIDF!
	binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
	binary.Write(&buf, binary.BigEndian, uint64(200))
	binary.Write(&buf, binary.BigEndian, uint32(0)) // Terminator.
	binary.Write(&buf, binary.BigEndian, uint64(300))

	// Add some data.
	buf.Write(make([]byte, 300))

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, buf.Bytes())

	_, err := LoadCommitGraph(tmp)
	assert.Error(t, err, "expected error for overlapping chunks")
}

/* ------------------------------------------------------------------------- */
/*                    Edge Cases for Parent Resolution                       */
/* ------------------------------------------------------------------------- */

// TestCommitGraphCircularEdgeRefs tests error handling when edge references
// form a circular pattern.
func TestCommitGraphCircularEdgeRefs(t *testing.T) {
	tmp := t.TempDir()

	// Create commits that can be referenced.
	commits := []cgCommit{
		{oid: makeCGHash("00"), tree: makeCGHash("10")},
		{oid: makeCGHash("01"), tree: makeCGHash("11")},
		{oid: makeCGHash("02"), tree: makeCGHash("12")},
		{oid: makeCGHash("03"), tree: makeCGHash("13")},
	}

	// Create a circular edge list that references commits 0->1->2->3->0.
	edge := []uint32{
		1,
		2,
		3,
		0,
		1,
		2,
		3,
		0 | lastEdgeMask,
	}

	// Add the merge commit that uses this edge list.
	M := makeCGHash("aa")
	commits = append(commits, cgCommit{
		oid:         M,
		tree:        makeCGHash("1a"),
		parents:     []uint32{0},
		edgePointer: 0,
		useEdge:     true,
	})

	data := buildCGFile(commits, edge)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	// Should handle gracefully without infinite loop.
	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "failed to parse: %v", err)

	// Should have collected all parents until hitting terminator.
	parents := graphData.Parents[M]
	require.Len(t, parents, 9, "expected 9 parents, got %d: %v", len(parents), parents)

	// Verify expected parent order including duplicates.
	expected := []Hash{
		makeCGHash("00"),
		makeCGHash("01"),
		makeCGHash("02"),
		makeCGHash("03"),
		makeCGHash("00"),
		makeCGHash("01"),
		makeCGHash("02"),
		makeCGHash("03"),
		makeCGHash("00"),
	}

	for i, p := range parents {
		assert.Equal(t, expected[i], p, "parent[%d]: got %x, want %x", i, p, expected[i])
	}
}

// TestCommitGraphManyParents tests error handling when a commit has a very long
// parent list.
func TestCommitGraphManyParents(t *testing.T) {
	tmp := t.TempDir()

	// Create 100 parent commits.
	var commits []cgCommit
	for i := range 100 {
		commits = append(commits, cgCommit{
			oid:  makeCGHash(fmt.Sprintf("%02x", i)),
			tree: makeCGHash(fmt.Sprintf("1%02x", i)),
		})
	}

	// Create octopus merge with all as parents.
	var edge []uint32
	for i := uint32(1); i < 100; i++ {
		if i == 99 {
			edge = append(edge, i|lastEdgeMask)
		} else {
			edge = append(edge, i)
		}
	}

	M := makeCGHash("ff")
	commits = append(commits, cgCommit{
		oid:         M,
		tree:        makeCGHash("ff"),
		parents:     []uint32{0},
		edgePointer: 0,
		useEdge:     true,
	})

	data := buildCGFile(commits, edge)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "parse failed: %v", err)

	parents := graphData.Parents[M]
	assert.Len(t, parents, 100, "expected 100 parents, got %d", len(parents))
}

// TestCommitGraphEdgeNoTerminator tests error handling when edge list
// does not end with a terminator.
func TestCommitGraphEdgeNoTerminator(t *testing.T) {
	tmp := t.TempDir()

	A := makeCGHash("aa")
	B := makeCGHash("bb")
	M := makeCGHash("cc")

	// Edge list without lastEdgeMask terminator.
	edge := []uint32{
		1,
		0,
		1,
	}

	commits := []cgCommit{
		{oid: A, tree: makeCGHash("11")},
		{oid: B, tree: makeCGHash("22")},
		{
			oid:         M,
			tree:        makeCGHash("33"),
			parents:     []uint32{0},
			edgePointer: 0,
			useEdge:     true,
		},
	}

	data := buildCGFile(commits, edge)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "parse failed: %v", err)

	// Should read all edges until end of chunk.
	parents := graphData.Parents[M]
	assert.Len(t, parents, 4, "expected 4 parents, got %d", len(parents))
}

/* ------------------------------------------------------------------------- */
/*                              Fanout Tests                                 */
/* ------------------------------------------------------------------------- */

// TestCommitGraphBadFanout tests error handling when fanout values are not monotonic.
func TestCommitGraphBadFanout(t *testing.T) {
	tmp := t.TempDir()

	buf := bytes.Buffer{}
	// Write header.
	buf.WriteString("CGPH")
	buf.WriteByte(1) // Version.
	buf.WriteByte(1) // Hash version.
	buf.WriteByte(3) // Chunk count.
	buf.WriteByte(0) // Reserved.

	// Write chunk table.
	tableStart := buf.Len()
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
	binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
	binary.Write(&buf, binary.BigEndian, uint32(0)) // Terminator.
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.

	// Write OIDF with non-monotonic values.
	oidfOffset := buf.Len()
	fanout := make([]uint32, 256)
	for i := range 256 {
		if i < 128 {
			fanout[i] = uint32(i + 1)
		} else {
			fanout[i] = uint32(256 - i) // Goes down!
		}
	}
	for _, v := range fanout {
		binary.Write(&buf, binary.BigEndian, v)
	}

	// Write OIDL (empty for now).
	oidlOffset := buf.Len()

	// Write CDAT (empty for now).
	cdatOffset := buf.Len()

	// Final offset.
	finalOffset := buf.Len()

	// Fix up chunk table.
	data := buf.Bytes()
	binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
	binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
	binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
	binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	_, err := LoadCommitGraph(tmp)
	assert.Error(t, err, "expected error for non-monotonic fanout")
}

// TestCommitGraphEmpty tests loading an empty commit graph file.
func TestCommitGraphEmpty(t *testing.T) {
	tmp := t.TempDir()

	commits := []cgCommit{}
	data := buildCGFile(commits, nil)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "failed to load empty graph: %v", err)
	assert.Empty(t, graphData.OrderedOIDs, "expected 0 commits")
}

// TestCommitGraphFanoutMismatch tests error handling when fanout values
// don't match the actual number of commits.
func TestCommitGraphFanoutMismatch(t *testing.T) {
	tmp := t.TempDir()

	commits := []cgCommit{
		{oid: makeCGHash("aa"), tree: makeCGHash("11")},
	}
	data := buildCGFile(commits, nil)

	// Corrupt fanout[255] to claim 100 commits instead of 1.
	// Fanout starts at offset 8 (header) + 48 (chunk table) = 56.
	fanoutLastOffset := 56 + 255*4
	binary.BigEndian.PutUint32(data[fanoutLastOffset:], 100)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	_, err := LoadCommitGraph(tmp)
	assert.Error(t, err, "expected error for fanout mismatch")
}

/* ------------------------------------------------------------------------- */
/*                          Chain-specific Tests                             */
/* ------------------------------------------------------------------------- */

// TestCommitGraphDeepChainParent tests parent index resolution in chained commit graphs.
func TestCommitGraphDeepChainParent(t *testing.T) {
	tmp := t.TempDir()

	// File 1: 3 commits with internal parent references.
	file1Commits := []cgCommit{
		{oid: makeCGHash("00"), tree: makeCGHash("10")},
		{oid: makeCGHash("01"), tree: makeCGHash("11"), parents: []uint32{0}},
		{oid: makeCGHash("02"), tree: makeCGHash("12"), parents: []uint32{1}},
	}

	// File 2: 2 commits with internal references only.
	file2Commits := []cgCommit{
		{oid: makeCGHash("10"), tree: makeCGHash("20")},
		{oid: makeCGHash("11"), tree: makeCGHash("21"), parents: []uint32{0}},
	}

	data1 := buildCGFile(file1Commits, nil)
	hash1 := "1111111111111111111111111111111111111111"
	path1 := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash1+".graph")
	mustWrite(t, path1, data1)

	data2 := buildCGFile(file2Commits, nil)
	hash2 := "2222222222222222222222222222222222222222"
	path2 := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash2+".graph")
	mustWrite(t, path2, data2)

	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(hash2+"\n"+hash1+"\n"))

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify parent relationships work within each file.
	if parents := graphData.Parents[makeCGHash("01")]; assert.Len(t, parents, 1) {
		assert.Equal(t, makeCGHash("00"), parents[0], "commit 01: wrong parents %v", parents)
	}
	if parents := graphData.Parents[makeCGHash("02")]; assert.Len(t, parents, 1) {
		assert.Equal(t, makeCGHash("01"), parents[0], "commit 02: wrong parents %v", parents)
	}
	if parents := graphData.Parents[makeCGHash("11")]; assert.Len(t, parents, 1) {
		assert.Equal(t, makeCGHash("10"), parents[0], "commit 11: wrong parents %v", parents)
	}
}

// TestCommitGraphChainDuplicates tests handling of duplicate commits across chain files.
func TestCommitGraphChainDuplicates(t *testing.T) {
	tmp := t.TempDir()

	A := makeCGHash("aa")

	// Base layer with commit A having no parents.
	baseCommits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: nil},
	}
	baseData := buildCGFile(baseCommits, nil)
	baseHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	basePath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+baseHash+".graph")
	mustWrite(t, basePath, baseData)

	// Tip layer with same commit A but now having a parent.
	B := makeCGHash("bb")
	tipCommits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: []uint32{1}},
		{oid: B, tree: makeCGHash("22"), parents: nil},
	}
	tipData := buildCGFile(tipCommits, nil)
	tipHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tipPath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+tipHash+".graph")
	mustWrite(t, tipPath, tipData)

	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(tipHash+"\n"+baseHash+"\n"))

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Tip version should take precedence over base version.
	parents := graphData.Parents[A]
	if assert.Len(t, parents, 1) {
		assert.Equal(t, B, parents[0], "tip layer should override base for duplicate OID")
	}
}

// TestCommitGraphLongChain tests error handling when a commit graph file is very long.
func TestCommitGraphLongChain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long chain test")
	}

	tmp := t.TempDir()

	// Create 50-file chain
	var chainContent []string
	for i := range 50 {
		commits := []cgCommit{
			{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			},
		}
		data := buildCGFile(commits, nil)
		hash := fmt.Sprintf("%039d%x", i, i)
		path := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash+".graph")
		mustWrite(t, path, data)
		chainContent = append(chainContent, hash)
	}

	// Write chain file
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(strings.Join(chainContent, "\n")+"\n"))

	// Time the load
	start := time.Now()
	graphData, err := LoadCommitGraph(tmp)
	elapsed := time.Since(start)

	require.NoError(t, err, "load failed: %v", err)
	assert.Len(t, graphData.OrderedOIDs, 50, "expected 50 commits, got %d", len(graphData.OrderedOIDs))
	assert.Less(t, elapsed, 100*time.Millisecond, "loading 50-chain took too long: %v", elapsed)
}

/* ------------------------------------------------------------------------- */
/*                         Data Extraction Tests                             */
/* ------------------------------------------------------------------------- */

// TestCommitGraphTreeOIDs tests error handling when tree OIDs are not populated correctly.
func TestCommitGraphTreeOIDs(t *testing.T) {
	tmp := t.TempDir()

	treeA := makeCGHash("aabbccddee")
	treeB := makeCGHash("1122334455")
	treeC := makeCGHash("9876543210")

	commits := []cgCommit{
		{oid: makeCGHash("01"), tree: treeA},
		{oid: makeCGHash("02"), tree: treeB, parents: []uint32{0}},
		{oid: makeCGHash("03"), tree: treeC, parents: []uint32{1}},
	}

	data := buildCGFile(commits, nil)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify TreeOIDs populated correctly
	require.Len(t, graphData.TreeOIDs, 3, "expected 3 trees, got %d", len(graphData.TreeOIDs))
	assert.Equal(t, treeA, graphData.TreeOIDs[0], "tree 0: got %v, want %v", graphData.TreeOIDs[0], treeA)
	assert.Equal(t, treeB, graphData.TreeOIDs[1], "tree 1: got %v, want %v", graphData.TreeOIDs[1], treeB)
	assert.Equal(t, treeC, graphData.TreeOIDs[2], "tree 2: got %v, want %v", graphData.TreeOIDs[2], treeC)
}

// TestCommitGraphTimestamps tests error handling when timestamps are not populated correctly.
func TestCommitGraphTimestamps(t *testing.T) {
	tmp := t.TempDir()

	// We'll create a custom builder for this test
	var buf bytes.Buffer

	// Write header.
	buf.WriteString("CGPH")
	buf.WriteByte(1) // Version.
	buf.WriteByte(1) // Hash version.
	buf.WriteByte(3) // Chunk count.
	buf.WriteByte(0) // Reserved.

	// We'll have 3 commits with different timestamps
	timestamps := []int64{
		0,                // epoch
		1234567890,       // Fri Feb 13 2009
		int64(1<<34 - 1), // max 34-bit value
	}

	// Write chunk table.
	tableStart := buf.Len()
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
	binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.
	binary.Write(&buf, binary.BigEndian, uint32(0)) // Terminator.
	binary.Write(&buf, binary.BigEndian, uint64(0)) // Will fill.

	// Write OIDF.
	oidfOffset := buf.Len()
	fanout := make([]byte, fanoutSize)
	// All commits start with different bytes for even distribution
	fanout[0x01*4-1] = 1
	fanout[0x02*4-1] = 2
	fanout[0x03*4-1] = 3
	for i := 0x03; i < 256; i++ {
		binary.BigEndian.PutUint32(fanout[i*4:], 3)
	}
	buf.Write(fanout)

	// Write OIDL.
	oidlOffset := buf.Len()
	oids := []Hash{
		makeCGHash("01"),
		makeCGHash("02"),
		makeCGHash("03"),
	}
	for _, oid := range oids {
		buf.Write(oid[:])
	}

	// Write CDAT with custom timestamps.
	cdatOffset := buf.Len()
	for i, ts := range timestamps {
		// Tree.
		treeHash := makeCGHash(fmt.Sprintf("1%02x", i))
		buf.Write(treeHash[:])
		// Parents.
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		// Generation (0) and timestamp.
		genTime := uint64(ts) & 0x3FFFFFFFF // 34-bit timestamp
		binary.Write(&buf, binary.BigEndian, genTime)
	}

	// Final offset.
	finalOffset := buf.Len()

	// Fix up chunk table.
	data := buf.Bytes()
	binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
	binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
	binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
	binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify timestamps.
	require.Len(t, graphData.Timestamps, 3, "expected 3 timestamps, got %d", len(graphData.Timestamps))

	for i, expected := range timestamps {
		assert.Equal(t, expected, graphData.Timestamps[i], "timestamp[%d]: got %d, want %d", i, graphData.Timestamps[i], expected)
	}
}

// TestCommitGraphOIDIndexMap tests error handling when OID to index mapping
// is not populated correctly.
func TestCommitGraphOIDIndexMap(t *testing.T) {
	tmp := t.TempDir()

	// Create commits with known OIDs
	oids := []Hash{
		makeCGHash("aa"),
		makeCGHash("bb"),
		makeCGHash("cc"),
		makeCGHash("dd"),
		makeCGHash("ee"),
	}

	var commits []cgCommit
	for _, oid := range oids {
		commits = append(commits, cgCommit{
			oid:  oid,
			tree: makeCGHash("11"),
		})
	}

	data := buildCGFile(commits, nil)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify mapping.
	for i, oid := range oids {
		idx, ok := graphData.OIDToIndex[oid]
		assert.True(t, ok)
		assert.Equal(t, i, idx, "OID %v: got index %d, want %d", oid, idx, i)
	}

	// Test non-existent OID
	fake := makeCGHash("ff")
	_, ok := graphData.OIDToIndex[fake]
	assert.False(t, ok, "found index for non-existent OID")
}

/* ------------------------------------------------------------------------- */
/*                         Error Recovery Tests                              */
/* ------------------------------------------------------------------------- */

// TestCommitGraphCleanupOnError tests error handling when a commit graph file
// is corrupted and cannot be parsed.
func TestCommitGraphCleanupOnError(t *testing.T) {
	tmp := t.TempDir()

	// Create valid first file
	commits1 := []cgCommit{
		{oid: makeCGHash("aa"), tree: makeCGHash("11")},
	}
	data1 := buildCGFile(commits1, nil)
	hash1 := "1111111111111111111111111111111111111111"
	path1 := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash1+".graph")
	mustWrite(t, path1, data1)

	// Create invalid second file (truncated)
	data2 := []byte("CGPH")
	hash2 := "2222222222222222222222222222222222222222"
	path2 := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash2+".graph")
	mustWrite(t, path2, data2)

	// Chain file
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(hash1+"\n"+hash2+"\n"))

	// Should fail but clean up properly
	_, err := LoadCommitGraph(tmp)
	require.Error(t, err, "expected error for truncated file")

	// Verify error mentions the issue
	assert.True(t, strings.Contains(err.Error(), "read") || strings.Contains(err.Error(), "parse") || strings.Contains(err.Error(), "EOF"), "error should indicate parse failure: %v", err)
}

// TestCommitGraphCorruptHeaders tests error handling when file headers are corrupted.
func TestCommitGraphCorruptHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		errMsg string
	}{
		{
			name:   "BadMagic",
			header: []byte("NOPE\x01\x01\x03\x00"),
			errMsg: "signature",
		},
		{
			name:   "UnsupportedVersion",
			header: []byte("CGPH\xFF\x01\x03\x00"),
			errMsg: "version",
		},
		{
			name:   "NonSHA1Hash",
			header: []byte("CGPH\x01\x02\x03\x00"),
			errMsg: "SHA-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a fresh temp dir for each test to avoid conflicts
			testTmp := t.TempDir()

			// Build rest of file
			buf := bytes.Buffer{}
			buf.Write(tc.header)

			// Add minimal chunk table
			binary.Write(&buf, binary.BigEndian, uint32(0))
			binary.Write(&buf, binary.BigEndian, uint64(0))

			// Use the correct filename that LoadCommitGraph expects
			path := filepath.Join(testTmp, "info", "commit-graph")
			mustWrite(t, path, buf.Bytes())

			_, err := LoadCommitGraph(testTmp)
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.errMsg, "expected error about %s, got: %v", tc.errMsg, err)
			}
		})
	}
}

/* ------------------------------------------------------------------------- */
/*                              Stress Tests                                 */
/* ------------------------------------------------------------------------- */

// TestCommitGraphLargeFile tests error handling when a commit graph file is very large.
func TestCommitGraphLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test")
	}

	tmp := t.TempDir()

	// Generate 10k commits (not 1M for practicality)
	var commits []cgCommit
	for i := range 10000 {
		parent := []uint32{}
		if i > 0 {
			parent = []uint32{uint32(i - 1)}
		}
		commits = append(commits, cgCommit{
			oid:     makeCGHash(fmt.Sprintf("%08x", i)),
			tree:    makeCGHash(fmt.Sprintf("1%07x", i)),
			parents: parent,
		})
	}

	start := time.Now()
	data := buildCGFile(commits, nil)
	buildTime := time.Since(start)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	start = time.Now()
	graphData, err := LoadCommitGraph(tmp)
	loadTime := time.Since(start)

	require.NoError(t, err, "load failed: %v", err)
	assert.Len(t, graphData.OrderedOIDs, 10000, "expected 10k commits, got %d", len(graphData.OrderedOIDs))

	t.Logf("Build time: %v, Load time: %v, File size: %d bytes",
		buildTime, loadTime, len(data))

	// Verify memory usage is reasonable (rough check)
	assert.Less(t, loadTime, 500*time.Millisecond, "loading 10k commits took too long: %v", loadTime)
}

// TestCommitGraphComplexMerges tests error handling when complex merge commits
// are not handled correctly.
func TestCommitGraphComplexMerges(t *testing.T) {
	tmp := t.TempDir()
	// Create base commits
	var commits []cgCommit
	var edge []uint32
	edgeOffset := uint32(0)
	// 20 regular commits
	for i := range 20 {
		commits = append(commits, cgCommit{
			oid:  makeCGHash(fmt.Sprintf("%02x", i)),
			tree: makeCGHash(fmt.Sprintf("1%02x", i)),
		})
	}

	// Merge with 3 parents [0, 1, 2]
	commits = append(commits, cgCommit{
		oid:         makeCGHash("20"),
		tree:        makeCGHash("120"),
		parents:     []uint32{0}, // Only first parent here!
		edgePointer: edgeOffset,
		useEdge:     true, // Flag to use edge pointer
	})
	// Second and third parents go in EDGE
	edge = append(edge, 1)              // second parent
	edge = append(edge, 2|lastEdgeMask) // third parent with terminator
	edgeOffset += 2

	// Merge with 5 parents [3, 4, 5, 6, 7]
	commits = append(commits, cgCommit{
		oid:         makeCGHash("21"),
		tree:        makeCGHash("121"),
		parents:     []uint32{3}, // Only first parent here!
		edgePointer: edgeOffset,
		useEdge:     true, // Flag to use edge pointer
	})
	// Remaining 4 parents go in EDGE
	edge = append(edge, 4)              // second parent
	edge = append(edge, 5)              // third parent
	edge = append(edge, 6)              // fourth parent
	edge = append(edge, 7|lastEdgeMask) // fifth parent with terminator
	edgeOffset += 4

	// Octopus with 11 parents [8, 9, 10, ..., 18]
	commits = append(commits, cgCommit{
		oid:         makeCGHash("22"),
		tree:        makeCGHash("122"),
		parents:     []uint32{8}, // Only first parent here!
		edgePointer: edgeOffset,
		useEdge:     true, // Flag to use edge pointer
	})
	// Remaining 10 parents go in EDGE
	for i := uint32(9); i < 18; i++ {
		edge = append(edge, i)
	}
	edge = append(edge, 18|lastEdgeMask)

	data := buildCGFile(commits, edge)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify merge parents
	m1 := makeCGHash("20")
	parents1 := graphData.Parents[m1]
	assert.Len(t, parents1, 3, "merge 1: expected 3 parents, got %d (parents: %v)", len(parents1), parents1)

	m2 := makeCGHash("21")
	parents2 := graphData.Parents[m2]
	assert.Len(t, parents2, 5, "merge 2: expected 5 parents, got %d (parents: %v)", len(parents2), parents2)

	m3 := makeCGHash("22")
	parents3 := graphData.Parents[m3]
	assert.Len(t, parents3, 11, "octopus: expected 11 parents, got %d (parents: %v)", len(parents3), parents3)

	// Optionally verify the actual parent OIDs are correct
	if len(parents1) == 3 {
		expected := []Hash{makeCGHash("00"), makeCGHash("01"), makeCGHash("02")}
		for i, p := range parents1 {
			assert.Equal(t, expected[i], p, "merge 1 parent %d: got %x, want %x", i, p, expected[i])
		}
	}
}

/* ------------------------------------------------------------------------- */
/*                            Special Patterns                               */
/* ------------------------------------------------------------------------- */

// TestCommitGraphAllRoots tests error handling when all commits are roots.
func TestCommitGraphAllRoots(t *testing.T) {
	tmp := t.TempDir()

	// Create 100 root commits
	var commits []cgCommit
	for i := range 100 {
		commits = append(commits, cgCommit{
			oid:     makeCGHash(fmt.Sprintf("%02x", i)),
			tree:    makeCGHash(fmt.Sprintf("1%02x", i)),
			parents: nil, // all roots
		})
	}

	data := buildCGFile(commits, nil)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify all have no parents
	for _, oid := range graphData.OrderedOIDs {
		assert.Empty(t, graphData.Parents[oid], "root commit %v has parents", oid)
	}
}

// TestCommitGraphLinearHistory tests error handling when a commit graph
// represents a linear history.
func TestCommitGraphLinearHistory(t *testing.T) {
	tmp := t.TempDir()

	// Create 1000 commits in a line
	var commits []cgCommit
	for i := range 1000 {
		var parents []uint32
		if i > 0 {
			parents = []uint32{uint32(i - 1)}
		}
		commits = append(commits, cgCommit{
			oid:     makeCGHash(fmt.Sprintf("%04x", i)),
			tree:    makeCGHash(fmt.Sprintf("1%03x", i)),
			parents: parents,
		})
	}

	start := time.Now()
	data := buildCGFile(commits, nil)
	buildTime := time.Since(start)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	start = time.Now()
	graphData, err := LoadCommitGraph(tmp)
	loadTime := time.Since(start)

	require.NoError(t, err, "load failed: %v", err)

	// Verify linear chain
	for i := 1; i < 1000; i++ {
		oid := makeCGHash(fmt.Sprintf("%04x", i))
		parents := graphData.Parents[oid]
		require.Len(t, parents, 1, "commit %d: expected 1 parent, got %d", i, len(parents))
		expectedParent := makeCGHash(fmt.Sprintf("%04x", i-1))
		assert.Equal(t, expectedParent, parents[0], "commit %d: wrong parent", i)
	}

	t.Logf("Linear history (1000): build=%v, load=%v", buildTime, loadTime)
}

// TestCommitGraphBinaryTree tests error handling when a commit graph
// represents a binary tree pattern.
func TestCommitGraphBinaryTree(t *testing.T) {
	tmp := t.TempDir()
	// Create commits in binary tree pattern
	// Level 0: 1 root
	// Level 1: 2 commits (each has root as parent)
	// Level 2: 4 commits (each has 2 parents from level 1)
	// Level 3: 8 commits
	// Level 4: 16 commits
	// Total: 1 + 2 + 4 + 8 + 16 = 31
	var commits []cgCommit

	// Root
	commits = append(commits, cgCommit{
		oid:  makeCGHash("00"),
		tree: makeCGHash("10"),
	})

	// Build levels
	for level := 1; level <= 4; level++ {
		levelSize := 1 << level // 2^level gives us 2,4,8,16
		prevLevelStart := 0
		prevLevelSize := 1 << (level - 1) // Size of previous level

		// Calculate where previous level starts
		for l := range level - 1 {
			prevLevelStart += 1 << l
		}

		for i := range levelSize {
			// For level 1, all commits have root as single parent
			// For other levels, commits can have 1 or 2 parents
			var parents []uint32

			if level == 1 {
				// Level 1: only root as parent
				parents = []uint32{0}
			} else {
				// For higher levels, each pair of commits shares parents
				// from adjacent commits in the previous level
				parent1Idx := prevLevelStart + (i/2)%prevLevelSize
				parent2Idx := prevLevelStart + ((i/2)+1)%prevLevelSize

				if parent1Idx == parent2Idx {
					parents = []uint32{uint32(parent1Idx)}
				} else {
					parents = []uint32{uint32(parent1Idx), uint32(parent2Idx)}
				}
			}

			commits = append(commits, cgCommit{
				oid:     makeCGHash(fmt.Sprintf("%02x%02x", level, i)),
				tree:    makeCGHash(fmt.Sprintf("1%01x%02x", level, i)),
				parents: parents,
			})
		}
	}

	data := buildCGFile(commits, nil)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	require.NoError(t, err, "load failed: %v", err)

	// Verify structure
	require.Len(t, graphData.OrderedOIDs, 31, "expected 31 commits, got %d", len(graphData.OrderedOIDs)) // 1+2+4+8+16

	// Verify the tree structure
	// Root should have no parents
	rootParents := graphData.Parents[makeCGHash("00")]
	assert.Empty(t, rootParents, "root should have 0 parents, got %d", len(rootParents))

	// Level 1 commits should have 1 parent (the root)
	for i := range 2 {
		oid := makeCGHash(fmt.Sprintf("01%02x", i))
		parents := graphData.Parents[oid]
		if assert.Len(t, parents, 1, "level 1 commit %x should have 1 parent, got %d", oid, len(parents)) {
			assert.Equal(t, makeCGHash("00"), parents[0], "level 1 commit %x should have root as parent", oid)
		}
	}

	// Check all commits have 0-2 parents
	for i := range commits {
		parents := graphData.Parents[commits[i].oid]
		assert.LessOrEqual(t, len(parents), 2, "commit %v: unexpected parent count %d (max 2)", commits[i].oid, len(parents))
	}

	// Verify some specific relationships
	// Level 2, commit 0 should have level 1 commits 0 and 1 as parents
	level2_0 := makeCGHash("0200")
	parents2_0 := graphData.Parents[level2_0]
	assert.Len(t, parents2_0, 2, "level 2 commit 0 should have 2 parents, got %d", len(parents2_0))
}

// TestCommitGraphParentBoundaryIndex tests parent index boundary conditions.
func TestCommitGraphParentBoundaryIndex(t *testing.T) {
	tmp := t.TempDir()

	t.Run("ValidBoundary", func(t *testing.T) {
		// Create 10 commits.
		var commits []cgCommit
		for i := range 10 {
			commits = append(commits, cgCommit{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			})
		}
		// Add commit with parent at last valid index.
		commits = append(commits, cgCommit{
			oid:     makeCGHash("aa"),
			tree:    makeCGHash("aa"),
			parents: []uint32{9},
		})

		data := buildCGFile(commits, nil)
		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		graphData, err := LoadCommitGraph(tmp)
		require.NoError(t, err, "unexpected error for valid boundary: %v", err)

		// Verify the parent relationship.
		child := makeCGHash("aa")
		parents := graphData.Parents[child]
		if assert.Len(t, parents, 1) {
			assert.Equal(t, makeCGHash("09"), parents[0], "wrong parent: got %v", parents)
		}
	})

	t.Run("InvalidBoundary", func(t *testing.T) {
		// Create 10 commits.
		var commits []cgCommit
		for i := range 10 {
			commits = append(commits, cgCommit{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			})
		}
		// Add commit with out-of-bounds parent index.
		commits = append(commits, cgCommit{
			oid:     makeCGHash("aa"),
			tree:    makeCGHash("aa"),
			parents: []uint32{11},
		})

		data := buildCGFile(commits, nil)
		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if assert.Error(t, err, "expected error for out-of-bounds parent") {
			assert.True(t, strings.Contains(err.Error(), "parent index") && strings.Contains(err.Error(), "out of bounds"), "expected error about parent index out of bounds, got: %v", err)
		}
	})

	t.Run("InvalidBoundaryInEDGE", func(t *testing.T) {
		// Create 5 commits.
		var commits []cgCommit
		for i := range 5 {
			commits = append(commits, cgCommit{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			})
		}

		// Add octopus merge with out-of-bounds parent in edge.
		edge := []uint32{
			1,
			2,
			10 | lastEdgeMask,
		}

		commits = append(commits, cgCommit{
			oid:         makeCGHash("aa"),
			tree:        makeCGHash("aa"),
			parents:     []uint32{0},
			edgePointer: 0,
			useEdge:     true,
		})

		data := buildCGFile(commits, edge)
		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if assert.Error(t, err, "expected error for out-of-bounds edge parent") {
			assert.True(t, strings.Contains(err.Error(), "edge parent index") && strings.Contains(err.Error(), "out of bounds"), "expected error about edge parent index out of bounds, got: %v", err)
		}
	})
}

/* ------------------------------------------------------------------------- */
/*                         Benchmark Support Helpers                        */
/* ------------------------------------------------------------------------- */

// mustWriteTB is a test helper that writes data to a file, creating parent directories as needed.
// This version accepts testing.TB interface to work with both tests and benchmarks.
func mustWriteTB(t testing.TB, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal("mkdir:", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal("write", path, ":", err)
	}
}

// generateTestGraphFile creates a commit graph file with the specified number of commits.
// This reuses existing buildCGFile logic and creates deterministic but varied commit structures.
func generateTestGraphFile(t testing.TB, path string, numCommits int, hasEdges bool) {
	t.Helper()

	var commits []cgCommit
	var edge []uint32
	edgeIndex := 0

	for i := range numCommits {
		oid := makeCGHash(fmt.Sprintf("%08x", i))
		tree := makeCGHash(fmt.Sprintf("1%07x", i))

		var parents []uint32
		var edgePointer uint32
		var useEdge bool

		if i == 0 {
			// Root commit has no parents.
		} else if hasEdges && i%10 == 0 && i > 2 {
			// Create octopus merge every 10th commit with first parent in CDAT, rest in EDGE.
			parents = []uint32{uint32(i - 1)}
			edgePointer = uint32(edgeIndex)
			useEdge = true

			edge = append(edge, uint32(i-2))
			edge = append(edge, uint32(i-3)|lastEdgeMask)
			edgeIndex += 2
		} else if i > 1 && i%5 == 0 {
			// Create regular merge every 5th commit.
			parents = []uint32{uint32(i - 1), uint32(i - 2)}
		} else {
			// Linear history.
			parents = []uint32{uint32(i - 1)}
		}

		commits = append(commits, cgCommit{
			oid:         oid,
			tree:        tree,
			parents:     parents,
			edgePointer: edgePointer,
			useEdge:     useEdge,
		})
	}

	var edgeData []uint32
	if hasEdges {
		edgeData = edge
	}

	data := buildCGFile(commits, edgeData)
	mustWriteTB(t, path, data)
}

// generateTestGraphChain creates a chain of commit graph files for testing chain scenarios.
func generateTestGraphChain(t testing.TB, dir string, layers []int) []string {
	t.Helper()

	chainDir := filepath.Join(dir, "info", "commit-graphs")
	if err := os.MkdirAll(chainDir, 0755); err != nil {
		t.Fatal(err)
	}

	var paths []string
	var hashes []string

	for i, numCommits := range layers {
		hash := fmt.Sprintf("%040d", i)
		hashes = append(hashes, hash)

		graphPath := filepath.Join(chainDir, fmt.Sprintf("graph-%s.graph", hash))
		paths = append(paths, graphPath)

		hasEdges := i == 0 // Only first layer has edges for variety.
		generateTestGraphFile(t, graphPath, numCommits, hasEdges)
	}

	chainPath := filepath.Join(chainDir, "commit-graph-chain")
	f, err := os.Create(chainPath)
	if err != nil {
		t.Fatal(err)
	}

	func() {
		defer f.Close()
		for _, h := range hashes {
			if _, err := fmt.Fprintln(f, h); err != nil {
				t.Fatal(err)
			}
		}
	}()

	return paths
}

/* ------------------------------------------------------------------------- */
/*                              Benchmarks                                   */
/* ------------------------------------------------------------------------- */

func BenchmarkLoadCommitGraph(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("single_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			infoDir := filepath.Join(dir, "info")
			require.NoError(b, os.MkdirAll(infoDir, 0755))

			graphPath := filepath.Join(infoDir, "commit-graph")
			generateTestGraphFile(b, graphPath, size, true)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				data, err := LoadCommitGraph(dir)
				require.NoError(b, err)
				require.Len(b, data.OrderedOIDs, size)
			}
		})

		b.Run(fmt.Sprintf("chain_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			// Split into 3 layers
			layer1 := size / 2
			layer2 := size / 3
			layer3 := size - layer1 - layer2
			generateTestGraphChain(b, dir, []int{layer1, layer2, layer3})

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				data, err := LoadCommitGraph(dir)
				require.NoError(b, err)
				require.Len(b, data.OrderedOIDs, size)
			}
		})
	}
}

func BenchmarkParseGraphFile(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "test.graph")
			generateTestGraphFile(b, path, size, true)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				pg, err := parseGraphFile(path)
				require.NoError(b, err)
				if pg.mr != nil {
					pg.mr.Close()
				}
			}
		})

		b.Run(fmt.Sprintf("size_%d_noedge", size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "test.graph")
			generateTestGraphFile(b, path, size, false)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				pg, err := parseGraphFile(path)
				require.NoError(b, err)
				if pg.mr != nil {
					pg.mr.Close()
				}
			}
		})
	}
}

func BenchmarkResolveParents(b *testing.B) {
	sizes := []int{1_000, 10_000, 50_000, 100_000, 500_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "test.graph")
			generateTestGraphFile(b, path, size, true)

			pg, err := parseGraphFile(path)
			require.NoError(b, err)
			defer func() {
				if pg.mr != nil {
					pg.mr.Close()
				}
			}()

			// Prepare full OID list
			allOids := make([]Hash, size)
			copy(allOids, pg.oids)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				parents := make(Parents, size)
				require.NoError(b, pg.resolveParentsInto(parents, allOids, 0))
			}
		})
	}
}

func BenchmarkDiscoverGraphFiles(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		dir := b.TempDir()
		infoDir := filepath.Join(dir, "info")
		require.NoError(b, os.MkdirAll(infoDir, 0755))

		graphPath := filepath.Join(infoDir, "commit-graph")
		generateTestGraphFile(b, graphPath, 1000, false)

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			files, err := discoverGraphFiles(dir)
			require.NoError(b, err)
			require.Len(b, files, 1)
		}
	})

	b.Run("chain", func(b *testing.B) {
		dir := b.TempDir()
		generateTestGraphChain(b, dir, []int{1_000, 10_000, 100_000, 1_000_000})

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			files, err := discoverGraphFiles(dir)
			require.NoError(b, err)
			require.Len(b, files, 4)
		}
	})

	b.Run("none", func(b *testing.B) {
		dir := b.TempDir()

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			files, err := discoverGraphFiles(dir)
			require.NoError(b, err)
			require.Empty(b, files)
		}
	})
}
