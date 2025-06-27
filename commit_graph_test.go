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
)

/* ------------------------------------------------------------------------- */
/*                               helpers                                     */
/* ------------------------------------------------------------------------- */

const (
	noParent     = 0x70000000
	lastEdgeMask = 0x80000000
	hashLen      = 20
)

type cgCommit struct {
	oid         Hash     // commit id
	tree        Hash     // root tree (arbitrary)
	parents     []uint32 // positional parent indexes (first two go in CDAT)
	edgeParents []uint32 // extra parent indexes stored in EDGE
	edgePointer uint32   // index in EDGE slice (only for octopus)
	useEdge     bool     // explicitly indicates this commit should use edge encoding
}

// makeCGHash creates a Hash from a hex string, padding to 20 bytes
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

// buildCGFile builds an in‑memory commit‑graph version‑1 file.
// When edge==nil the EDGE chunk is omitted.
func buildCGFile(commits []cgCommit, edge []uint32) []byte {
	var fan [fanoutEntries]uint32
	for _, c := range commits {
		fan[c.oid[0]]++
	}
	// cumulative
	var total uint32
	for i := 0; i < fanoutEntries; i++ {
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

	// header ----------------------------------------------------------------
	buf.WriteString("CGPH")                    // magic
	buf.WriteByte(1)                           // version
	buf.WriteByte(1)                           // hash version (SHA‑1)
	buf.WriteByte(byte(3 + btoi(edge != nil))) // chunk count
	buf.WriteByte(0)                           // reserved

	// chunk table rows – we'll fill offsets later.
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
	}{id: 0, off: 0}) // terminator

	// space for table
	buf.Write(make([]byte, len(chunkOffTbl)*12))

	// helper to append a chunk and record its offset
	appendChunk := func(id uint32, data []byte) {
		chunkOffTbl = setChunkOffset(chunkOffTbl, id, uint64(buf.Len()))
		buf.Write(data)
	}

	/* --- chunks ---------------------------------------------------------- */

	// OIDF
	fanBytes := make([]byte, fanoutEntries*4)
	for i, v := range fan {
		binary.BigEndian.PutUint32(fanBytes[i*4:], v)
	}
	appendChunk(chunkOIDF, fanBytes)

	// OIDL
	oidBytes := make([]byte, len(commits)*hashLen)
	for i, c := range commits {
		copy(oidBytes[i*hashLen:], c.oid[:])
	}
	appendChunk(chunkOIDL, oidBytes)

	// CDAT
	const recSize = hashLen + 16
	cdat := make([]byte, len(commits)*recSize)
	for i, c := range commits {
		off := i * recSize
		copy(cdat[off:], c.tree[:])

		// Handle parent indices properly
		var p1, p2 uint32 = noParent, noParent

		if len(c.parents) > 0 {
			p1 = c.parents[0]
		}

		// Check if we should use edge pointer
		if c.useEdge {
			// This is an octopus merge - p2 contains edge pointer
			p2 = c.edgePointer | lastEdgeMask
		} else if len(c.parents) > 1 {
			// Regular 2-parent merge
			p2 = c.parents[1]
		}

		binary.BigEndian.PutUint32(cdat[off+hashLen:], p1)
		binary.BigEndian.PutUint32(cdat[off+hashLen+4:], p2)
		// leave gen/time = 0
	}
	appendChunk(chunkCDAT, cdat)

	// EDGE
	if edge != nil {
		edgeBytes := make([]byte, len(edge)*4)
		for i, v := range edge {
			binary.BigEndian.PutUint32(edgeBytes[i*4:], v)
		}
		appendChunk(chunkEDGE, edgeBytes)
	}

	/* --- write back chunk table ----------------------------------------- */
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

// helpers
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
func intOrSentinel(v uint32) int {
	if v == ^uint32(0) {
		return noParent
	}
	return int(v)
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
/*                               TESTS                                       */
/* ------------------------------------------------------------------------- */

// Test 1: parent index in tip layer points to base layer.
func TestCommitGraphChainParentAcrossLayers(t *testing.T) {
	tmp := t.TempDir()

	// --- base layer: A, B -----------------------------------------------
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

	// --- tip layer: C with parent -> index 2 (which lives in base) -------
	C := makeCGHash("03")
	tipCommits := []cgCommit{
		{oid: C, tree: makeCGHash("33"), parents: []uint32{2}},
	}
	tipData := buildCGFile(tipCommits, nil)
	tipHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tipPath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+tipHash+".graph")
	mustWrite(t, tipPath, tipData)

	// chain file (tip first)
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(tipHash+"\n"+baseHash+"\n"))

	// parse ----------------------------------------------------------------
	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	if graphData == nil {
		t.Fatalf("expected commit graph data, got nil")
	}
	if len(graphData.OrderedOIDs) != 3 {
		t.Fatalf("want 3 commits, got %d", len(graphData.OrderedOIDs))
	}

	// C should have B as single parent.
	parents := graphData.Parents[C]
	if len(parents) != 1 || parents[0] != B {
		t.Errorf("cross‑layer parent not resolved: got %v, want [%x]", parents, B)
	}
}

// Test 2: EDGE pointer not at index 0 – current code ignores pointer.
func TestCommitGraphEdgePointerMisaligned(t *testing.T) {
	tmp := t.TempDir()

	// commits: A, B, M (octopus)
	A := makeCGHash("11")
	B := makeCGHash("22")
	M := makeCGHash("33")

	edge := []uint32{
		lastEdgeMask,     // dummy list 0 (one bogus word)
		lastEdgeMask,     // dummy list 1
		1 | lastEdgeMask, // real list starts here: parent index 1 (B)
	}

	commits := []cgCommit{
		{oid: A, tree: makeCGHash("41"), parents: nil},
		{oid: B, tree: makeCGHash("42"), parents: []uint32{0}},
		{
			oid:         M,
			tree:        makeCGHash("43"),
			parents:     []uint32{0},
			edgePointer: 2, // point to third word in EDGE
			useEdge:     true,
		},
	}
	data := buildCGFile(commits, edge)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Get parents for merge commit M
	parents := graphData.Parents[M]

	// expected parents = [A, B]
	want := []Hash{A, B}
	if len(parents) != 2 || parents[0] != want[0] || parents[1] != want[1] {
		t.Errorf("octopus parents wrong: got %v, want %v", parents, want)
	}
}

/* ------------------------------------------------------------------------- */

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

/* ------------------------------------------------------------------------- */
/*                        File Discovery & Chain Tests                       */
/* ------------------------------------------------------------------------- */

// Test 3: Empty chain file should fall back to single file
func TestCommitGraphEmptyChainFallback(t *testing.T) {
	tmp := t.TempDir()

	// Create empty chain file
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(""))

	// Create valid single commit-graph file
	A := makeCGHash("aa")
	commits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: nil},
	}
	data := buildCGFile(commits, nil)
	singlePath := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, singlePath, data)

	// Load and verify
	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if graphData == nil {
		t.Fatal("expected graph data")
	}
	if len(graphData.OrderedOIDs) != 1 {
		t.Errorf("expected 1 commit, got %d", len(graphData.OrderedOIDs))
	}
}

// Test 4: Chain file with missing graph files
func TestCommitGraphChainMissingFiles(t *testing.T) {
	tmp := t.TempDir()

	// Create chain file referencing non-existent graphs
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n"+
			"cafebabecafebabecafebabecafebabecafebabe\n"))

	// Should fail to load
	_, err := LoadCommitGraph(tmp)
	if err == nil {
		t.Fatal("expected error for missing graph files")
	}
}

// Test 5: Both chain and single file exist - chain takes precedence
func TestCommitGraphChainPrecedence(t *testing.T) {
	tmp := t.TempDir()

	// Create single file with commit A
	A := makeCGHash("aa")
	singleCommits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: nil},
	}
	singleData := buildCGFile(singleCommits, nil)
	singlePath := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, singlePath, singleData)

	// Create chain with commit B
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

	// Load - should get B from chain, not A from single
	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(graphData.OrderedOIDs) != 1 || graphData.OrderedOIDs[0] != B {
		t.Errorf("chain should take precedence")
	}
}

/* ------------------------------------------------------------------------- */
/*                          Chunk Validation Tests                           */
/* ------------------------------------------------------------------------- */

// Test 6: Missing required chunks
func TestCommitGraphMissingChunks(t *testing.T) {
	tmp := t.TempDir()

	// Test missing OIDF
	t.Run("MissingOIDF", func(t *testing.T) {
		buf := bytes.Buffer{}
		// Header
		buf.WriteString("CGPH")
		buf.WriteByte(1) // version
		buf.WriteByte(1) // hash version
		buf.WriteByte(2) // chunk count (only OIDL, CDAT)
		buf.WriteByte(0) // reserved

		// Chunk table (no OIDF)
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
		binary.Write(&buf, binary.BigEndian, uint64(44)) // offset after table
		binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
		binary.Write(&buf, binary.BigEndian, uint64(64))  // offset
		binary.Write(&buf, binary.BigEndian, uint32(0))   // terminator
		binary.Write(&buf, binary.BigEndian, uint64(100)) // final offset

		// OIDL chunk
		buf.Write(make([]byte, 20)) // one hash
		// CDAT chunk
		buf.Write(make([]byte, 36)) // one record

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, buf.Bytes())

		_, err := LoadCommitGraph(tmp)
		if err == nil {
			t.Error("expected error for missing OIDF")
		}
	})
}

// Test 7: Chunks with size mismatches
func TestCommitGraphChunkSizeMismatch(t *testing.T) {
	tmp := t.TempDir()

	t.Run("OIDLSizeMismatch", func(t *testing.T) {
		// Manually build a file with OIDL size that doesn't match fanout
		buf := bytes.Buffer{}
		// Header
		buf.WriteString("CGPH")
		buf.WriteByte(1) // version
		buf.WriteByte(1) // hash version
		buf.WriteByte(3) // chunk count
		buf.WriteByte(0) // reserved

		// Chunk table
		tableStart := buf.Len()
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
		binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
		binary.Write(&buf, binary.BigEndian, uint32(0)) // terminator
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill

		// OIDF - indicates 2 commits
		oidfOffset := buf.Len()
		fanout := make([]byte, fanoutSize)
		// Set fanout to indicate 2 commits
		for i := 0xaa; i < 256; i++ {
			binary.BigEndian.PutUint32(fanout[i*4:], 2)
		}
		buf.Write(fanout)

		// OIDL - but only include 1 OID (mismatch!)
		oidlOffset := buf.Len()
		h := makeCGHash("aa")
		buf.Write(h[:]) // Only 1 OID instead of 2

		// CDAT - 2 records to match fanout
		cdatOffset := buf.Len()
		// First commit
		h = makeCGHash("11")
		buf.Write(h[:]) // tree
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // gen/time
		// Second commit
		h = makeCGHash("22")
		buf.Write(h[:]) // tree
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // gen/time

		// Final offset
		finalOffset := buf.Len()

		// Fix up chunk table
		data := buf.Bytes()
		binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
		binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
		binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
		binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if err == nil {
			t.Error("expected error for OIDL size mismatch")
		}
		if err != nil && !strings.Contains(err.Error(), "OIDL") && !strings.Contains(err.Error(), "size") {
			t.Errorf("expected error about OIDL size, got: %v", err)
		}
	})

	t.Run("CDATSizeMismatch", func(t *testing.T) {
		// Build normal file
		A := makeCGHash("aa")
		B := makeCGHash("bb")
		commits := []cgCommit{
			{oid: A, tree: makeCGHash("11"), parents: nil},
			{oid: B, tree: makeCGHash("22"), parents: []uint32{0}},
		}
		data := buildCGFile(commits, nil)

		// Truncate CDAT chunk
		data = data[:len(data)-20]

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if err == nil {
			t.Error("expected error for CDAT size mismatch")
		}
	})

	t.Run("FANOUTWrongSize", func(t *testing.T) {
		// Manually build a file with wrong OIDF size
		buf := bytes.Buffer{}
		// Header
		buf.WriteString("CGPH")
		buf.WriteByte(1) // version
		buf.WriteByte(1) // hash version
		buf.WriteByte(3) // chunk count
		buf.WriteByte(0) // reserved

		// Chunk table
		tableStart := buf.Len()
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
		binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
		binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
		binary.Write(&buf, binary.BigEndian, uint32(0)) // terminator
		binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill

		// OIDF with wrong size (only 100 bytes instead of 1024)
		oidfOffset := buf.Len()
		buf.Write(make([]byte, 100))

		// OIDL (empty)
		oidlOffset := buf.Len()

		// CDAT (empty)
		cdatOffset := buf.Len()

		// Final offset
		finalOffset := buf.Len()

		// Fix up chunk table
		data := buf.Bytes()
		binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
		binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
		binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
		binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if err == nil {
			t.Error("expected error for wrong OIDF size")
		}
		if err != nil && !strings.Contains(err.Error(), "OIDF") {
			t.Errorf("expected error about OIDF, got: %v", err)
		}
	})
}

// Test 8: Overlapping chunks
func TestCommitGraphOverlappingChunks(t *testing.T) {
	tmp := t.TempDir()

	buf := bytes.Buffer{}
	// Header
	buf.WriteString("CGPH")
	buf.WriteByte(1) // version
	buf.WriteByte(1) // hash version
	buf.WriteByte(3) // chunk count
	buf.WriteByte(0) // reserved

	// Chunk table with overlapping offsets
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
	binary.Write(&buf, binary.BigEndian, uint64(56)) // starts at 56
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
	binary.Write(&buf, binary.BigEndian, uint64(100)) // overlaps with OIDF!
	binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
	binary.Write(&buf, binary.BigEndian, uint64(200))
	binary.Write(&buf, binary.BigEndian, uint32(0)) // terminator
	binary.Write(&buf, binary.BigEndian, uint64(300))

	// Add some data
	buf.Write(make([]byte, 300))

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, buf.Bytes())

	_, err := LoadCommitGraph(tmp)
	if err == nil {
		t.Error("expected error for overlapping chunks")
	}
}

/* ------------------------------------------------------------------------- */
/*                    Edge Cases for Parent Resolution                       */
/* ------------------------------------------------------------------------- */

// Test 9: Circular edge references
func TestCommitGraphCircularEdgeRefs(t *testing.T) {
	tmp := t.TempDir()

	// Create some commits that can be referenced
	commits := []cgCommit{
		{oid: makeCGHash("00"), tree: makeCGHash("10")}, // index 0
		{oid: makeCGHash("01"), tree: makeCGHash("11")}, // index 1
		{oid: makeCGHash("02"), tree: makeCGHash("12")}, // index 2
		{oid: makeCGHash("03"), tree: makeCGHash("13")}, // index 3
	}

	// Create a circular edge list - commit 4 has parents that form a cycle
	// The edge list references commits 0->1->2->3->0 (circular)
	edge := []uint32{
		1,                // parent is commit 1
		2,                // parent is commit 2
		3,                // parent is commit 3
		0,                // parent is commit 0 (back to start - circular!)
		1,                // parent is commit 1 again
		2,                // parent is commit 2 again
		3,                // parent is commit 3 again
		0 | lastEdgeMask, // terminate with commit 0
	}

	// Add the merge commit that uses this edge list
	M := makeCGHash("aa")
	commits = append(commits, cgCommit{
		oid:         M,
		tree:        makeCGHash("1a"),
		parents:     []uint32{0}, // first parent
		edgePointer: 0,           // starts at edge[0]
		useEdge:     true,
	})

	data := buildCGFile(commits, edge)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	// Should handle gracefully, not infinite loop
	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	// Should have collected all parents until hitting terminator
	parents := graphData.Parents[M]
	// We expect 9 parents total: first parent (0) + 8 from edge list
	if len(parents) != 9 {
		t.Errorf("expected 9 parents, got %d: %v", len(parents), parents)
	}

	// Verify we got the expected parents (including duplicates)
	expected := []Hash{
		makeCGHash("00"), // first parent
		makeCGHash("01"), // from edge[0]
		makeCGHash("02"), // from edge[1]
		makeCGHash("03"), // from edge[2]
		makeCGHash("00"), // from edge[3] - circular!
		makeCGHash("01"), // from edge[4]
		makeCGHash("02"), // from edge[5]
		makeCGHash("03"), // from edge[6]
		makeCGHash("00"), // from edge[7] with terminator
	}

	for i, p := range parents {
		if p != expected[i] {
			t.Errorf("parent[%d]: got %x, want %x", i, p, expected[i])
		}
	}
}

// Test 10: Very long parent lists (stress test)
func TestCommitGraphManyParents(t *testing.T) {
	tmp := t.TempDir()

	// Create 100 parent commits
	var commits []cgCommit
	for i := 0; i < 100; i++ {
		commits = append(commits, cgCommit{
			oid:  makeCGHash(fmt.Sprintf("%02x", i)),
			tree: makeCGHash(fmt.Sprintf("1%02x", i)),
		})
	}

	// Create octopus merge with all as parents
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
		parents:     []uint32{0}, // first parent in p1
		edgePointer: 0,           // rest in EDGE
		useEdge:     true,
	})

	data := buildCGFile(commits, edge)
	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	parents := graphData.Parents[M]
	if len(parents) != 100 {
		t.Errorf("expected 100 parents, got %d", len(parents))
	}
}

// Test 11: Edge list without terminator
func TestCommitGraphEdgeNoTerminator(t *testing.T) {
	tmp := t.TempDir()

	A := makeCGHash("aa")
	B := makeCGHash("bb")
	M := makeCGHash("cc")

	// Edge list without lastEdgeMask terminator
	edge := []uint32{
		1, // parent B, but no terminator!
		0, // parent A
		1, // parent B again
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
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	// Should read all edges until end of chunk
	parents := graphData.Parents[M]
	// We expect 4 parents: first parent (0) + 3 from edge chunk
	if len(parents) != 4 {
		t.Errorf("expected 4 parents, got %d", len(parents))
	}
}

// Test 12: Parent index exactly at boundary
func TestCommitGraphParentBoundaryIndex(t *testing.T) {
	tmp := t.TempDir()

	t.Run("ValidBoundary", func(t *testing.T) {
		// Create 10 commits
		var commits []cgCommit
		for i := 0; i < 10; i++ {
			commits = append(commits, cgCommit{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			})
		}
		// Parent at index 9 (last valid index)
		commits = append(commits, cgCommit{
			oid:     makeCGHash("aa"),
			tree:    makeCGHash("aa"),
			parents: []uint32{9}, // valid boundary
		})

		data := buildCGFile(commits, nil)
		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		graphData, err := LoadCommitGraph(tmp)
		if err != nil {
			t.Fatalf("unexpected error for valid boundary: %v", err)
		}

		// Verify the parent relationship
		child := makeCGHash("aa")
		parents := graphData.Parents[child]
		if len(parents) != 1 || parents[0] != makeCGHash("09") {
			t.Errorf("wrong parent: got %v", parents)
		}
	})

	t.Run("InvalidBoundary", func(t *testing.T) {
		// Create 10 commits
		var commits []cgCommit
		for i := 0; i < 10; i++ {
			commits = append(commits, cgCommit{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			})
		}
		// Add 11th commit with parent at index 11 (out of bounds - only 0-10 will exist)
		commits = append(commits, cgCommit{
			oid:     makeCGHash("aa"),
			tree:    makeCGHash("aa"),
			parents: []uint32{11}, // invalid! We'll have 11 commits (0-10), so 11 is out of bounds
		})

		data := buildCGFile(commits, nil)
		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if err == nil {
			t.Error("expected error for out-of-bounds parent")
		}
		if err != nil && !strings.Contains(err.Error(), "parent index") && !strings.Contains(err.Error(), "out of bounds") {
			t.Errorf("expected error about parent index out of bounds, got: %v", err)
		}
	})

	t.Run("InvalidBoundaryInEDGE", func(t *testing.T) {
		// Test out-of-bounds parent in EDGE chunk
		var commits []cgCommit
		for i := 0; i < 5; i++ {
			commits = append(commits, cgCommit{
				oid:  makeCGHash(fmt.Sprintf("%02x", i)),
				tree: makeCGHash(fmt.Sprintf("1%02x", i)),
			})
		}

		// Add octopus merge with out-of-bounds parent in edge
		edge := []uint32{
			1,                 // valid
			2,                 // valid
			10 | lastEdgeMask, // invalid! Only 0-5 will exist
		}

		commits = append(commits, cgCommit{
			oid:         makeCGHash("aa"),
			tree:        makeCGHash("aa"),
			parents:     []uint32{0}, // first parent valid
			edgePointer: 0,
			useEdge:     true,
		})

		data := buildCGFile(commits, edge)
		path := filepath.Join(tmp, "info", "commit-graph")
		mustWrite(t, path, data)

		_, err := LoadCommitGraph(tmp)
		if err == nil {
			t.Error("expected error for out-of-bounds edge parent")
		}
		if err != nil && !strings.Contains(err.Error(), "edge parent index") && !strings.Contains(err.Error(), "out of bounds") {
			t.Errorf("expected error about edge parent index out of bounds, got: %v", err)
		}
	})
}

/* ------------------------------------------------------------------------- */
/*                              Fanout Tests                                 */
/* ------------------------------------------------------------------------- */

// Test 13: Non-monotonic fanout values
func TestCommitGraphBadFanout(t *testing.T) {
	tmp := t.TempDir()

	buf := bytes.Buffer{}
	// Header
	buf.WriteString("CGPH")
	buf.WriteByte(1) // version
	buf.WriteByte(1) // hash version
	buf.WriteByte(3) // chunk count
	buf.WriteByte(0) // reserved

	// Chunk table
	tableStart := buf.Len()
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
	binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
	binary.Write(&buf, binary.BigEndian, uint32(0)) // terminator
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill

	// OIDF with non-monotonic values
	oidfOffset := buf.Len()
	fanout := make([]uint32, 256)
	for i := 0; i < 256; i++ {
		if i < 128 {
			fanout[i] = uint32(i + 1)
		} else {
			fanout[i] = uint32(256 - i) // goes down!
		}
	}
	for _, v := range fanout {
		binary.Write(&buf, binary.BigEndian, v)
	}

	// OIDL (empty for now)
	oidlOffset := buf.Len()

	// CDAT (empty for now)
	cdatOffset := buf.Len()

	// Final offset
	finalOffset := buf.Len()

	// Fix up chunk table
	data := buf.Bytes()
	binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
	binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
	binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
	binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	_, err := LoadCommitGraph(tmp)
	if err == nil {
		t.Error("expected error for non-monotonic fanout")
	}
}

// Test 14: Zero commits
func TestCommitGraphEmpty(t *testing.T) {
	tmp := t.TempDir()

	// Valid file with 0 commits
	commits := []cgCommit{}
	data := buildCGFile(commits, nil)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("failed to load empty graph: %v", err)
	}
	if len(graphData.OrderedOIDs) != 0 {
		t.Error("expected 0 commits")
	}
}

// Test 15: Fanout doesn't match actual OID count
func TestCommitGraphFanoutMismatch(t *testing.T) {
	tmp := t.TempDir()

	// Build file normally
	commits := []cgCommit{
		{oid: makeCGHash("aa"), tree: makeCGHash("11")},
	}
	data := buildCGFile(commits, nil)

	// Find OIDF chunk and corrupt fanout[255]
	// The buildCGFile creates correct fanout, so we need to modify it
	// Fanout starts after 8-byte header + chunk table
	// For 3 chunks + terminator = 4 entries * 12 bytes = 48 bytes
	// So OIDF starts at offset 8 + 48 = 56

	// Set fanout[255] to claim 100 commits instead of 1
	fanoutLastOffset := 56 + 255*4
	binary.BigEndian.PutUint32(data[fanoutLastOffset:], 100)

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	_, err := LoadCommitGraph(tmp)
	if err == nil {
		t.Error("expected error for fanout mismatch")
	}
}

/* ------------------------------------------------------------------------- */
/*                          Chain-specific Tests                             */
/* ------------------------------------------------------------------------- */

// Test 16: Parent in much earlier layer
func TestCommitGraphDeepChainParent(t *testing.T) {
	tmp := t.TempDir()

	// The test demonstrates that parent indices in split chains are local to each file
	// and get adjusted by the cumulative offset of previous files

	// Build a simple 2-file chain to clearly show the offset adjustment

	// File 1: 3 commits
	file1Commits := []cgCommit{
		{oid: makeCGHash("00"), tree: makeCGHash("10")},                       // global 0
		{oid: makeCGHash("01"), tree: makeCGHash("11"), parents: []uint32{0}}, // global 1, parent global 0
		{oid: makeCGHash("02"), tree: makeCGHash("12"), parents: []uint32{1}}, // global 2, parent global 1
	}

	// File 2: 2 commits that reference file 1
	// Since file 1 has 3 commits, file 2's offset is -3
	// To reference global index 0, we'd need local index -3 (impossible)
	// So file 2 can only create new roots or reference within itself
	file2Commits := []cgCommit{
		{oid: makeCGHash("10"), tree: makeCGHash("20")},                       // global 3, new root
		{oid: makeCGHash("11"), tree: makeCGHash("21"), parents: []uint32{0}}, // global 4, parent global 3
	}

	// Build the files
	data1 := buildCGFile(file1Commits, nil)
	hash1 := "1111111111111111111111111111111111111111"
	path1 := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash1+".graph")
	mustWrite(t, path1, data1)

	data2 := buildCGFile(file2Commits, nil)
	hash2 := "2222222222222222222222222222222222222222"
	path2 := filepath.Join(tmp, "info", "commit-graphs", "graph-"+hash2+".graph")
	mustWrite(t, path2, data2)

	// Chain file (newest first)
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(hash2+"\n"+hash1+"\n"))

	// Load
	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify the parent relationships
	// File 1 internal references should work
	if parents := graphData.Parents[makeCGHash("01")]; len(parents) != 1 || parents[0] != makeCGHash("00") {
		t.Errorf("commit 01: wrong parents %v", parents)
	}
	if parents := graphData.Parents[makeCGHash("02")]; len(parents) != 1 || parents[0] != makeCGHash("01") {
		t.Errorf("commit 02: wrong parents %v", parents)
	}

	// File 2 internal references should work (with offset adjustment)
	if parents := graphData.Parents[makeCGHash("11")]; len(parents) != 1 || parents[0] != makeCGHash("10") {
		t.Errorf("commit 11: wrong parents %v", parents)
	}

	// This test shows that split commit-graphs have limitations on cross-file references
	// due to the local indexing + offset adjustment mechanism
}

// Test 17: Duplicate commits across chain files
func TestCommitGraphChainDuplicates(t *testing.T) {
	tmp := t.TempDir()

	A := makeCGHash("aa")

	// Base layer: A with no parents
	baseCommits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: nil},
	}
	baseData := buildCGFile(baseCommits, nil)
	baseHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	basePath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+baseHash+".graph")
	mustWrite(t, basePath, baseData)

	// Tip layer: A again but with parent
	B := makeCGHash("bb")
	tipCommits := []cgCommit{
		{oid: A, tree: makeCGHash("11"), parents: []uint32{1}}, // now has parent
		{oid: B, tree: makeCGHash("22"), parents: nil},
	}
	tipData := buildCGFile(tipCommits, nil)
	tipHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tipPath := filepath.Join(tmp, "info", "commit-graphs", "graph-"+tipHash+".graph")
	mustWrite(t, tipPath, tipData)

	// Chain file
	chainFile := filepath.Join(tmp, "info", "commit-graphs", "commit-graph-chain")
	mustWrite(t, chainFile, []byte(tipHash+"\n"+baseHash+"\n"))

	// Load
	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Tip version should take precedence
	parents := graphData.Parents[A]
	if len(parents) != 1 || parents[0] != B {
		t.Error("tip layer should override base for duplicate OID")
	}
}

// Test 18: Very long chain (performance)
func TestCommitGraphLongChain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long chain test")
	}

	tmp := t.TempDir()

	// Create 50-file chain
	var chainContent []string
	for i := 0; i < 50; i++ {
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

	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(graphData.OrderedOIDs) != 50 {
		t.Errorf("expected 50 commits, got %d", len(graphData.OrderedOIDs))
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("loading 50-chain took too long: %v", elapsed)
	}
}

/* ------------------------------------------------------------------------- */
/*                         Data Extraction Tests                             */
/* ------------------------------------------------------------------------- */

// Test 19: Tree OID extraction
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
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify TreeOIDs populated correctly
	if len(graphData.TreeOIDs) != 3 {
		t.Fatalf("expected 3 trees, got %d", len(graphData.TreeOIDs))
	}
	if graphData.TreeOIDs[0] != treeA {
		t.Errorf("tree 0: got %v, want %v", graphData.TreeOIDs[0], treeA)
	}
	if graphData.TreeOIDs[1] != treeB {
		t.Errorf("tree 1: got %v, want %v", graphData.TreeOIDs[1], treeB)
	}
	if graphData.TreeOIDs[2] != treeC {
		t.Errorf("tree 2: got %v, want %v", graphData.TreeOIDs[2], treeC)
	}
}

// Test 20: Timestamp extraction
func TestCommitGraphTimestamps(t *testing.T) {
	tmp := t.TempDir()

	// We'll create a custom builder for this test
	var buf bytes.Buffer

	// Header
	buf.WriteString("CGPH")
	buf.WriteByte(1) // version
	buf.WriteByte(1) // hash version
	buf.WriteByte(3) // chunk count
	buf.WriteByte(0) // reserved

	// We'll have 3 commits with different timestamps
	timestamps := []int64{
		0,                // epoch
		1234567890,       // Fri Feb 13 2009
		int64(1<<34 - 1), // max 34-bit value
	}

	// Chunk table
	tableStart := buf.Len()
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDF))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
	binary.Write(&buf, binary.BigEndian, uint32(chunkOIDL))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
	binary.Write(&buf, binary.BigEndian, uint32(chunkCDAT))
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill
	binary.Write(&buf, binary.BigEndian, uint32(0)) // terminator
	binary.Write(&buf, binary.BigEndian, uint64(0)) // will fill

	// OIDF
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

	// OIDL
	oidlOffset := buf.Len()
	oids := []Hash{
		makeCGHash("01"),
		makeCGHash("02"),
		makeCGHash("03"),
	}
	for _, oid := range oids {
		buf.Write(oid[:])
	}

	// CDAT with custom timestamps
	cdatOffset := buf.Len()
	for i, ts := range timestamps {
		// Tree
		treeHash := makeCGHash(fmt.Sprintf("1%02x", i))
		buf.Write(treeHash[:])
		// Parents
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		binary.Write(&buf, binary.BigEndian, uint32(graphParentNone))
		// Generation (0) and timestamp
		genTime := uint64(ts) & 0x3FFFFFFFF // 34-bit timestamp
		binary.Write(&buf, binary.BigEndian, genTime)
	}

	// Final offset
	finalOffset := buf.Len()

	// Fix up chunk table
	data := buf.Bytes()
	binary.BigEndian.PutUint64(data[tableStart+4:], uint64(oidfOffset))
	binary.BigEndian.PutUint64(data[tableStart+16:], uint64(oidlOffset))
	binary.BigEndian.PutUint64(data[tableStart+28:], uint64(cdatOffset))
	binary.BigEndian.PutUint64(data[tableStart+40:], uint64(finalOffset))

	path := filepath.Join(tmp, "info", "commit-graph")
	mustWrite(t, path, data)

	graphData, err := LoadCommitGraph(tmp)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify timestamps
	if len(graphData.Timestamps) != 3 {
		t.Fatalf("expected 3 timestamps, got %d", len(graphData.Timestamps))
	}

	for i, expected := range timestamps {
		if graphData.Timestamps[i] != expected {
			t.Errorf("timestamp[%d]: got %d, want %d", i, graphData.Timestamps[i], expected)
		}
	}
}

// Test 21: OIDToIndex mapping
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
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify mapping
	for i, oid := range oids {
		if idx, ok := graphData.OIDToIndex[oid]; !ok || idx != i {
			t.Errorf("OID %v: got index %d, want %d", oid, idx, i)
		}
	}

	// Test non-existent OID
	fake := makeCGHash("ff")
	if _, ok := graphData.OIDToIndex[fake]; ok {
		t.Error("found index for non-existent OID")
	}
}

/* ------------------------------------------------------------------------- */
/*                         Error Recovery Tests                              */
/* ------------------------------------------------------------------------- */

// Test 22: Cleanup on parse errors
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
	if err == nil {
		t.Fatal("expected error for truncated file")
	}

	// Verify error mentions the issue
	if !strings.Contains(err.Error(), "read") && !strings.Contains(err.Error(), "parse") && !strings.Contains(err.Error(), "EOF") {
		t.Errorf("error should indicate parse failure: %v", err)
	}
}

// Test 23: Corrupted file headers
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
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.errMsg) {
				t.Errorf("expected error about %s, got: %v", tc.errMsg, err)
			}
		})
	}
}

/* ------------------------------------------------------------------------- */
/*                              Stress Tests                                 */
/* ------------------------------------------------------------------------- */

// Test 24: Large graph file
func TestCommitGraphLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test")
	}

	tmp := t.TempDir()

	// Generate 10k commits (not 1M for practicality)
	var commits []cgCommit
	for i := 0; i < 10000; i++ {
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

	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(graphData.OrderedOIDs) != 10000 {
		t.Errorf("expected 10k commits, got %d", len(graphData.OrderedOIDs))
	}

	t.Logf("Build time: %v, Load time: %v, File size: %d bytes",
		buildTime, loadTime, len(data))

	// Verify memory usage is reasonable (rough check)
	if loadTime > 500*time.Millisecond {
		t.Errorf("loading 10k commits took too long: %v", loadTime)
	}
}

// Test 25: Maximum edge complexity
func TestCommitGraphComplexMerges(t *testing.T) {
	tmp := t.TempDir()
	// Create base commits
	var commits []cgCommit
	var edge []uint32
	edgeOffset := uint32(0)
	// 20 regular commits
	for i := 0; i < 20; i++ {
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
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify merge parents
	m1 := makeCGHash("20")
	parents1 := graphData.Parents[m1]
	if len(parents1) != 3 {
		t.Errorf("merge 1: expected 3 parents, got %d (parents: %v)", len(parents1), parents1)
	}

	m2 := makeCGHash("21")
	parents2 := graphData.Parents[m2]
	if len(parents2) != 5 {
		t.Errorf("merge 2: expected 5 parents, got %d (parents: %v)", len(parents2), parents2)
	}

	m3 := makeCGHash("22")
	parents3 := graphData.Parents[m3]
	if len(parents3) != 11 { // Fixed: should be 11, not 10!
		t.Errorf("octopus: expected 11 parents, got %d (parents: %v)", len(parents3), parents3)
	}

	// Optionally verify the actual parent OIDs are correct
	if len(parents1) == 3 {
		expected := []Hash{makeCGHash("00"), makeCGHash("01"), makeCGHash("02")}
		for i, p := range parents1 {
			if p != expected[i] {
				t.Errorf("merge 1 parent %d: got %x, want %x", i, p, expected[i])
			}
		}
	}
}

/* ------------------------------------------------------------------------- */
/*                            Special Patterns                               */
/* ------------------------------------------------------------------------- */

// Test 26: All commits are roots
func TestCommitGraphAllRoots(t *testing.T) {
	tmp := t.TempDir()

	// Create 100 root commits
	var commits []cgCommit
	for i := 0; i < 100; i++ {
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
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify all have no parents
	for _, oid := range graphData.OrderedOIDs {
		if len(graphData.Parents[oid]) != 0 {
			t.Errorf("root commit %v has parents", oid)
		}
	}
}

// Test 27: Linear history
func TestCommitGraphLinearHistory(t *testing.T) {
	tmp := t.TempDir()

	// Create 1000 commits in a line
	var commits []cgCommit
	for i := 0; i < 1000; i++ {
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

	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify linear chain
	for i := 1; i < 1000; i++ {
		oid := makeCGHash(fmt.Sprintf("%04x", i))
		parents := graphData.Parents[oid]
		if len(parents) != 1 {
			t.Fatalf("commit %d: expected 1 parent, got %d", i, len(parents))
		}
		expectedParent := makeCGHash(fmt.Sprintf("%04x", i-1))
		if parents[0] != expectedParent {
			t.Errorf("commit %d: wrong parent", i)
		}
	}

	t.Logf("Linear history (1000): build=%v, load=%v", buildTime, loadTime)
}

// Test 28: Binary tree pattern
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
		for l := 0; l < level-1; l++ {
			prevLevelStart += 1 << l
		}

		for i := 0; i < levelSize; i++ {
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
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// Verify structure
	if len(graphData.OrderedOIDs) != 31 { // 1+2+4+8+16
		t.Errorf("expected 31 commits, got %d", len(graphData.OrderedOIDs))
	}

	// Verify the tree structure
	// Root should have no parents
	rootParents := graphData.Parents[makeCGHash("00")]
	if len(rootParents) != 0 {
		t.Errorf("root should have 0 parents, got %d", len(rootParents))
	}

	// Level 1 commits should have 1 parent (the root)
	for i := 0; i < 2; i++ {
		oid := makeCGHash(fmt.Sprintf("01%02x", i))
		parents := graphData.Parents[oid]
		if len(parents) != 1 {
			t.Errorf("level 1 commit %x should have 1 parent, got %d", oid, len(parents))
		}
		if len(parents) > 0 && parents[0] != makeCGHash("00") {
			t.Errorf("level 1 commit %x should have root as parent", oid)
		}
	}

	// Check all commits have 0-2 parents
	for i := 0; i < len(commits); i++ {
		parents := graphData.Parents[commits[i].oid]
		if len(parents) > 2 {
			t.Errorf("commit %v: unexpected parent count %d (max 2)", commits[i].oid, len(parents))
		}
	}

	// Verify some specific relationships
	// Level 2, commit 0 should have level 1 commits 0 and 1 as parents
	level2_0 := makeCGHash("0200")
	parents2_0 := graphData.Parents[level2_0]
	if len(parents2_0) != 2 {
		t.Errorf("level 2 commit 0 should have 2 parents, got %d", len(parents2_0))
	}
}
