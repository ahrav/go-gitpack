package objstore

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

// createValidMidxFile creates a minimal, *single‑pack* multi‑pack‑index
// covering exactly the <hashes> it is given.
//
// Preconditions
//   - len(hashes) == len(offsets)
//   - Every offset < 2 GiB (so the LOFF chunk is unnecessary)
func createValidMidxFile(
	t *testing.T,
	packDir string, // "…/objects/pack"
	packName string, // e.g. "test.pack"
	hashes []Hash,
	offsets []uint64,
) string {
	require.Equal(t, len(hashes), len(offsets), "hash/offset slice mismatch")

	// Sort objects by hash (required by the spec).
	type obj struct {
		h     Hash
		off32 uint32
	}
	objs := make([]obj, len(hashes))
	for i := range hashes {
		require.Less(t, offsets[i], uint64(0x80000000), "offset must fit in 31 bits")
		objs[i] = obj{hashes[i], uint32(offsets[i])}
	}
	sort.Slice(objs, func(i, j int) bool {
		return bytes.Compare(objs[i].h[:], objs[j].h[:]) < 0
	})

	// Build PNAM.
	var pnam bytes.Buffer
	pnam.WriteString(packName)
	pnam.WriteByte(0) // NUL terminator

	// Build OIDF.
	var fanout [256]uint32
	for i, o := range objs {
		idx := o.h[0]
		for j := int(idx); j < 256; j++ {
			fanout[j] = uint32(i + 1)
		}
	}
	var oidf bytes.Buffer
	for _, c := range fanout {
		binary.Write(&oidf, binary.BigEndian, c)
	}

	// Build OIDL.
	var oidl bytes.Buffer
	for _, o := range objs {
		oidl.Write(o.h[:])
	}

	// Build OOFF.
	var ooff bytes.Buffer
	for _, o := range objs {
		// Pack‑id (uint32, always 0 because we have one pack).
		binary.Write(&ooff, binary.BigEndian, uint32(0))
		// Offset (uint32 with MSB = 0 → "small offset").
		binary.Write(&ooff, binary.BigEndian, o.off32)
	}

	// Assemble file.
	type chunk struct {
		id   string
		data []byte
	}
	chunks := []chunk{
		{"OIDF", oidf.Bytes()},
		{"OIDL", oidl.Bytes()},
		{"OOFF", ooff.Bytes()},
		{"PNAM", pnam.Bytes()},
	}
	const headerSize = 12
	chunkTableSize := (len(chunks) + 1) * 12 // +1 sentinel
	offset := headerSize + chunkTableSize

	for i := range chunks {
		chunks[i].id = fmt.Sprintf("%-4s", chunks[i].id)[:4]
	}

	var file bytes.Buffer

	// Header.
	file.WriteString("MIDX")                         // signature
	file.WriteByte(1)                                // version
	file.WriteByte(1)                                // hash‑id: 1 = SHA‑1
	file.WriteByte(byte(len(chunks)))                // #chunks
	file.WriteByte(0)                                // reserved
	binary.Write(&file, binary.BigEndian, uint32(1)) // pack‑count

	// Chunk‑table.
	for _, c := range chunks {
		file.WriteString(c.id)
		binary.Write(&file, binary.BigEndian, uint64(offset))
		offset += len(c.data)
	}
	// Sentinel row.
	file.Write([]byte{0, 0, 0, 0})
	binary.Write(&file, binary.BigEndian, uint64(offset))

	// Payload.
	for _, c := range chunks {
		file.Write(c.data)
	}

	midxPath := filepath.Join(packDir, "multi-pack-index")
	require.NoError(t, os.WriteFile(midxPath, file.Bytes(), 0644))
	return midxPath
}

func TestParseMidx(t *testing.T) {
	dir := t.TempDir()

	// Minimal pack + companion idx.
	packPath := filepath.Join(dir, "test.pack")
	blob := []byte("hello midx")
	hash := calculateHash(ObjBlob, blob)

	require.NoError(t, createMinimalPack(packPath, blob))
	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(packPath, ".pack")+".idx",
		[]Hash{hash},
		[]uint64{12},
	))

	// Build minimal .midx.
	createValidMidxFile(t, dir, filepath.Base(packPath), []Hash{hash}, []uint64{12})

	midxRA, err := mmap.Open(filepath.Join(dir, "multi-pack-index"))
	require.NoError(t, err)
	defer midxRA.Close()

	packCache := make(map[string]*mmap.ReaderAt)
	midx, err := parseMidx(dir, midxRA, packCache)
	require.NoError(t, err)

	assert.Equal(t, uint32(1), midx.fanout[255], "object count in fanout")
	assert.Len(t, midx.objectIDs, 1)
	assert.Equal(t, hash, midx.objectIDs[0])

	// FindObject must return the mmap.ReaderAt of *our* pack plus offset 12.
	p, off, ok := midx.findObject(hash)
	assert.True(t, ok)
	assert.Equal(t, uint64(12), off)
	assert.NotNil(t, p)
}

func TestStoreWithMidx(t *testing.T) {
	dir := t.TempDir()

	// 1 pack, 1 object.
	packPath := filepath.Join(dir, "test.pack")
	content := []byte("hello via midx")
	oid := calculateHash(ObjBlob, content)

	require.NoError(t, createMinimalPack(packPath, content))
	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(packPath, ".pack")+".idx",
		[]Hash{oid},
		[]uint64{12},
	))

	// Create multi‑pack‑index pointing at that single pack.
	createValidMidxFile(t, dir, filepath.Base(packPath), []Hash{oid}, []uint64{12})

	// Open() must pick the .midx automatically.
	store, err := Open(dir)
	require.NoError(t, err)
	defer store.Close()

	assert.NotNil(t, store.midx, "Store should contain parsed midx")
	assert.True(t, len(store.midx.objectIDs) > 0, "midx should index at least one object")

	data, typ, err := store.Get(oid)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, content, data)
}

func TestParseMidx_InvalidFiles(t *testing.T) {
	dir := t.TempDir()

	// Basic empty file → bad magic.
	midx := filepath.Join(dir, "multi-pack-index")
	require.NoError(t, os.WriteFile(midx, []byte("bogus"), 0644))

	ra, _ := mmap.Open(midx)
	defer ra.Close()

	packCache := make(map[string]*mmap.ReaderAt)
	_, err := parseMidx(dir, ra, packCache)
	assert.Error(t, err, "should reject file with invalid magic")
}

// setupBenchmarkRepoWithMidx creates a test repository structure with multiple pack files
// and a multi-pack index for benchmarking operations. This function bypasses Git
// initialization to create a controlled test environment with exactly three pack files
// containing unique objects, followed by a manually generated multi-pack index that
// references all objects across the packs.
func setupBenchmarkRepoWithMidx(tb testing.TB) string {
	tempDir := tb.TempDir()
	packDir := filepath.Join(tempDir, ".git", "objects", "pack")
	require.NoError(tb, os.MkdirAll(packDir, 0755))

	// Create packs directly without Git initialization to ensure predictable
	// test data and avoid dependency on external Git commands.
	createThreeDistinctTestPacks(tb, packDir)

	// Generate a multi-pack index that references all objects across the packs.
	createManualMidx(tb, packDir)

	return packDir
}

// createThreeDistinctTestPacks generates exactly three pack files with varying object
// counts and unique content patterns. Each pack contains objects with distinct
// content signatures to ensure proper testing of multi-pack index functionality
// across different pack sizes and object distributions.
func createThreeDistinctTestPacks(tb testing.TB, packDir string) {
	tb.Helper()

	// Remove any existing pack files to ensure a clean test environment.
	existingPacks, _ := filepath.Glob(filepath.Join(packDir, "*.pack"))
	for _, p := range existingPacks {
		os.Remove(p)
		os.Remove(strings.TrimSuffix(p, ".pack") + ".idx")
	}

	// Create packs with different object counts to test various scenarios.
	objectCounts := []int{15, 10, 8}

	for packIdx := range 3 {
		packPath := filepath.Join(packDir, fmt.Sprintf("pack-%02d.pack", packIdx))

		// Track object metadata for index file creation.
		var hashes []Hash
		var offsets []uint64

		// Initialize pack file with Git pack format header.
		var packBuf bytes.Buffer
		packBuf.Write([]byte("PACK"))                                           // Git pack signature
		binary.Write(&packBuf, binary.BigEndian, uint32(2))                     // Pack version 2
		binary.Write(&packBuf, binary.BigEndian, uint32(objectCounts[packIdx])) // Object count

		// Generate unique objects for this pack with distinct content patterns.
		for objIdx := 0; objIdx < objectCounts[packIdx]; objIdx++ {
			// Record the current buffer position as the object's offset.
			offset := uint64(packBuf.Len())

			// Create content with different patterns per pack to ensure uniqueness
			// and test various object sizes and content distributions.
			var content string
			switch packIdx {
			case 0:
				content = fmt.Sprintf("Pack ALPHA Object %d\nTimestamp: %d\nData: %s\n",
					objIdx, packIdx*1000+objIdx, strings.Repeat("A", objIdx*10+50))
			case 1:
				content = fmt.Sprintf("Pack BETA Object %d\nID: %d\nContent: %s\n",
					objIdx, packIdx*2000+objIdx, strings.Repeat("B", objIdx*15+40))
			case 2:
				content = fmt.Sprintf("Pack GAMMA Object %d\nSerial: %d\nPayload: %s\n",
					objIdx, packIdx*3000+objIdx, strings.Repeat("C", objIdx*20+30))
			}

			data := []byte(content)
			hash := calculateHash(ObjBlob, data)

			hashes = append(hashes, hash)
			offsets = append(offsets, offset)

			// Write Git object header with variable-length size encoding.
			size := len(data)
			objType := ObjBlob

			// Handle small objects with single-byte header encoding.
			if size < 16 {
				header := byte((uint8(objType) << 4) | (uint8(size) & 0x0F))
				packBuf.WriteByte(header)
			} else {
				// Use multi-byte encoding for larger objects following Git's
				// variable-length integer format specification.
				header := byte((uint8(objType) << 4) | 0x80 | (uint8(size) & 0x0F))
				packBuf.WriteByte(header)
				size >>= 4
				for size > 0 {
					b := byte(size & 0x7F)
					size >>= 7
					if size > 0 {
						b |= 0x80
					}
					packBuf.WriteByte(b)
				}
			}

			// Compress object data using zlib as required by Git pack format.
			var zlibBuf bytes.Buffer
			zw := zlib.NewWriter(&zlibBuf)
			zw.Write(data)
			zw.Close()
			packBuf.Write(zlibBuf.Bytes())
		}

		// Append SHA-1 checksum of the entire pack content for integrity verification.
		packChecksum := sha1.Sum(packBuf.Bytes())
		packBuf.Write(packChecksum[:])

		// Write the complete pack file to disk.
		err := os.WriteFile(packPath, packBuf.Bytes(), 0644)
		require.NoError(tb, err)

		// Create corresponding index file for efficient object lookup.
		idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"
		err = createV2IndexFile(idxPath, hashes, offsets)
		require.NoError(tb, err)

		tb.Logf("Created pack %d (%s) with %d objects", packIdx, filepath.Base(packPath), len(hashes))
	}

	// Verify that exactly three pack files were created as expected.
	packs, _ := filepath.Glob(filepath.Join(packDir, "*.pack"))
	require.Equal(tb, 3, len(packs), "Should have created exactly 3 pack files")
}

// createManualMidx generates a multi-pack index file by analyzing existing pack
// index files in the directory. This function provides a fallback when Git's
// built-in multi-pack index generation is unavailable, creating a valid midx
// file that indexes all objects across multiple pack files.
func createManualMidx(tb testing.TB, packDir string) {
	// Discover all pack files in the directory for indexing.
	pattern := filepath.Join(packDir, "*.pack")
	packs, err := filepath.Glob(pattern)
	require.NoError(tb, err)

	if len(packs) == 0 {
		tb.Skip("No pack files found for midx creation")
		return
	}

	// Sort pack names to ensure consistent ordering across test runs.
	sort.Strings(packs)

	// Extract object metadata from each pack's index file to build the midx.
	var allPacks []packObjectsInfo

	for packIdx, packPath := range packs {
		packName := filepath.Base(packPath)
		idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"

		// Parse the pack index file to extract object hashes and offsets.
		ra, err := mmap.Open(idxPath)
		if err != nil {
			tb.Logf("Failed to open idx file %s: %v", idxPath, err)
			continue
		}

		idx, err := parseIdx(ra)
		ra.Close()
		if err != nil {
			tb.Logf("Failed to parse idx file %s: %v", idxPath, err)
			continue
		}

		// Collect object metadata from this pack for midx construction.
		packInfo := packObjectsInfo{
			name:    packName,
			hashes:  make([]Hash, len(idx.oidTable)),
			offsets: make([]uint64, len(idx.entries)),
		}

		copy(packInfo.hashes, idx.oidTable)
		for i, entry := range idx.entries {
			packInfo.offsets[i] = entry.offset
		}

		allPacks = append(allPacks, packInfo)
		tb.Logf("Pack %d (%s): %d objects", packIdx, packName, len(packInfo.hashes))
	}

	if len(allPacks) == 0 {
		tb.Skip("No valid pack files found for midx creation")
		return
	}

	// Generate the multi-pack index file from collected pack information.
	createManualMidxFileMultiPack(tb, packDir, allPacks)

	// Verify that the multi-pack index file was successfully created.
	midxPath := filepath.Join(packDir, "multi-pack-index")
	_, err = os.Stat(midxPath)
	require.NoError(tb, err, "Failed to create multi-pack-index")
}

// createManualMidxFileMultiPack constructs a complete multi-pack index file
// following Git's midx format specification. This function handles multiple
// createManualMidxFileMultiPack creates a multi-pack index file that properly handles multiple packs
func createManualMidxFileMultiPack(
	tb testing.TB,
	packDir string,
	packs []packObjectsInfo,
) {
	require.NotEmpty(tb, packs, "No packs provided to createManualMidxFileMultiPack")

	// Build combined object list with pack IDs
	type objWithPack struct {
		h      Hash
		off32  uint32
		packID uint32
	}

	var allObjs []objWithPack
	packNames := make([]string, len(packs))

	// Count total objects and log distribution
	totalObjects := 0
	for packIdx, pack := range packs {
		packNames[packIdx] = pack.name
		totalObjects += len(pack.hashes)
		tb.Logf("Adding %d objects from pack %d (%s) to midx", len(pack.hashes), packIdx, pack.name)

		for i, hash := range pack.hashes {
			if pack.offsets[i] >= uint64(0x80000000) {
				tb.Logf("Warning: offset %d in pack %d exceeds 31 bits", pack.offsets[i], packIdx)
				continue // Skip objects with large offsets for this test
			}
			allObjs = append(allObjs, objWithPack{
				h:      hash,
				off32:  uint32(pack.offsets[i]),
				packID: uint32(packIdx),
			})
		}
	}

	tb.Logf("Total objects in midx: %d (from %d objects across %d packs)", len(allObjs), totalObjects, len(packs))

	// Verify we have objects from all packs
	packCounts := make(map[uint32]int)
	for _, obj := range allObjs {
		packCounts[obj.packID]++
	}
	for packID, count := range packCounts {
		tb.Logf("Pack %d has %d objects in midx", packID, count)
	}

	// Sort objects by hash as required by the midx specification.
	sort.Slice(allObjs, func(i, j int) bool {
		return bytes.Compare(allObjs[i].h[:], allObjs[j].h[:]) < 0
	})

	// Build PNAM chunk containing all pack file names.
	var pnam bytes.Buffer
	for _, name := range packNames {
		pnam.WriteString(name)
		pnam.WriteByte(0) // NUL terminator required by format
	}

	// Build OIDF chunk containing the fanout table for fast hash prefix lookup.
	var fanout [256]uint32
	for i, obj := range allObjs {
		idx := obj.h[0]
		for j := int(idx); j < 256; j++ {
			fanout[j] = uint32(i + 1)
		}
	}
	var oidf bytes.Buffer
	for _, c := range fanout {
		binary.Write(&oidf, binary.BigEndian, c)
	}

	// Build OIDL chunk containing sorted object IDs.
	var oidl bytes.Buffer
	for _, obj := range allObjs {
		oidl.Write(obj.h[:])
	}

	// Build OOFF chunk containing offset and pack ID pairs.
	// NOTE: The format is offset FIRST, then pack ID!
	var ooff bytes.Buffer
	for _, obj := range allObjs {
		binary.Write(&ooff, binary.BigEndian, obj.off32)  // offset first
		binary.Write(&ooff, binary.BigEndian, obj.packID) // pack ID second
	}

	// Assemble the complete midx file structure.
	type chunk struct {
		id   string
		data []byte
	}
	chunks := []chunk{
		{"OIDF", oidf.Bytes()},
		{"OIDL", oidl.Bytes()},
		{"OOFF", ooff.Bytes()},
		{"PNAM", pnam.Bytes()},
	}

	// Ensure chunk IDs are exactly 4 characters as required by format.
	for i := range chunks {
		chunks[i].id = fmt.Sprintf("%-4s", chunks[i].id)[:4]
	}

	// Chunks are written in the order specified above, not sorted by ID

	const headerSize = 12
	chunkTableSize := (len(chunks) + 1) * 12 // +1 for sentinel entry
	offset := headerSize + chunkTableSize

	var file bytes.Buffer

	// Write midx file header with magic signature and version information.
	file.WriteString("MIDX")                                  // magic signature
	file.WriteByte(1)                                         // version number
	file.WriteByte(1)                                         // hash algorithm ID (SHA-1)
	file.WriteByte(byte(len(chunks)))                         // number of chunks
	file.WriteByte(0)                                         // reserved byte
	binary.Write(&file, binary.BigEndian, uint32(len(packs))) // number of packs

	// Write chunk directory table mapping chunk IDs to file offsets.
	for _, c := range chunks {
		file.WriteString(c.id)
		binary.Write(&file, binary.BigEndian, uint64(offset))
		offset += len(c.data)
	}
	// Sentinel entry marking end of chunk table.
	file.Write([]byte{0, 0, 0, 0})
	binary.Write(&file, binary.BigEndian, uint64(offset))

	// Write chunk payloads in the order specified by the chunk table.
	for _, c := range chunks {
		file.Write(c.data)
	}

	midxPath := filepath.Join(packDir, "multi-pack-index")
	err := os.WriteFile(midxPath, file.Bytes(), 0644)
	require.NoError(tb, err, "Failed to write multi-pack-index")

	tb.Logf("Created multi-pack-index with %d objects from %d packs", len(allObjs), len(packs))
}

// BenchmarkParseMidx measures the performance of parsing multi-pack index files.
// This benchmark focuses on the midx file parsing overhead, which includes
// reading the header, chunk table, and constructing in-memory lookup structures.
func BenchmarkParseMidx(b *testing.B) {
	packDir := setupBenchmarkRepoWithMidx(b)

	midxPath := filepath.Join(packDir, "multi-pack-index")
	if _, err := os.Stat(midxPath); os.IsNotExist(err) {
		b.Skip("No multi-pack-index file found")
	}

	b.ResetTimer()
	for b.Loop() {
		ra, err := mmap.Open(midxPath)
		require.NoError(b, err)

		packCache := make(map[string]*mmap.ReaderAt)
		_, err = parseMidx(packDir, ra, packCache)
		require.NoError(b, err)

		ra.Close()
	}
}

// BenchmarkOpenWithMidx measures the performance of opening a Store when a
// multi-pack index is present. This includes the overhead of midx parsing
// plus normal pack file discovery and mapping.
func BenchmarkOpenWithMidx(b *testing.B) {
	packDir := setupBenchmarkRepoWithMidx(b)

	midxPath := filepath.Join(packDir, "multi-pack-index")
	if _, err := os.Stat(midxPath); os.IsNotExist(err) {
		b.Skip("No multi-pack-index file found")
	}

	b.ResetTimer()
	for b.Loop() {
		store, err := Open(packDir)
		require.NoError(b, err)

		// Verify that the midx was successfully loaded and utilized.
		require.NotNil(b, store.midx, "Store should have loaded midx")

		store.Close()
	}
}

// BenchmarkGetMidxCold measures object retrieval performance through the multi-pack
// index with cold cache. This benchmark clears the cache before each lookup to
// measure worst-case performance including decompression overhead.
func BenchmarkGetMidxCold(b *testing.B) {
	packDir := setupBenchmarkRepoWithMidx(b)

	store, err := Open(packDir)
	require.NoError(b, err)
	defer store.Close()

	if store.midx == nil {
		b.Skip("No midx loaded")
	}

	if len(store.midx.objectIDs) == 0 {
		b.Skip("No objects in midx")
	}

	someHash := store.midx.objectIDs[0]

	b.ResetTimer()
	for b.Loop() {
		// Force cold cache lookup by purging cached objects.
		store.cache.Purge()
		_, _, err := store.Get(someHash)
		require.NoError(b, err)
	}
}

// BenchmarkGetMidxWarm measures object retrieval performance through the multi-pack
// index with warm cache. This represents best-case performance when objects are
// already decompressed and cached in memory.
func BenchmarkGetMidxWarm(b *testing.B) {
	packDir := setupBenchmarkRepoWithMidx(b)

	store, err := Open(packDir)
	require.NoError(b, err)
	defer store.Close()

	if store.midx == nil {
		b.Skip("No midx loaded")
	}

	if len(store.midx.objectIDs) == 0 {
		b.Skip("No objects in midx")
	}

	someHash := store.midx.objectIDs[0]

	// Warm the cache with an initial lookup.
	_, _, err = store.Get(someHash)
	require.NoError(b, err)

	b.ResetTimer()
	for b.Loop() {
		_, _, err := store.Get(someHash)
		require.NoError(b, err)
	}
}

// BenchmarkFindObject_MidxVsIdx compares the performance of object lookup using
// multi-pack index versus traditional individual pack index files. This helps
// quantify the performance benefits of midx for object location.
func BenchmarkFindObject_MidxVsIdx(b *testing.B) {
	packDir := setupBenchmarkRepoWithMidx(b)

	store, err := Open(packDir)
	require.NoError(b, err)
	defer store.Close()

	if store.midx == nil || len(store.packs) == 0 {
		b.Skip("Need both midx and regular packs for comparison")
	}

	// Select a test object that exists in both lookup mechanisms.
	var testHash Hash
	if len(store.midx.objectIDs) > 0 {
		testHash = store.midx.objectIDs[0]
	} else if len(store.packs) > 0 && len(store.packs[0].oidTable) > 0 {
		testHash = store.packs[0].oidTable[0]
	} else {
		b.Skip("No objects found for benchmark")
	}

	b.Run("midx", func(b *testing.B) {
		for b.Loop() {
			_, _, found := store.midx.findObject(testHash)
			require.True(b, found, "Object should be found in midx")
		}
	})

	b.Run("idx", func(b *testing.B) {
		for b.Loop() {
			found := false
			// Simulate the linear search through multiple pack files that
			// would be required without a multi-pack index.
			for _, pack := range store.packs {
				if _, found = pack.findObject(testHash); found {
					break
				}
			}
			require.True(b, found, "Object should be found in regular idx")
		}
	})
}

// BenchmarkMemoryUsage_MidxVsMultipleIdx compares memory allocation patterns
// when using multi-pack index versus multiple individual index files. This
// benchmark uses b.ReportAllocs() to track allocation overhead.
func BenchmarkMemoryUsage_MidxVsMultipleIdx(b *testing.B) {
	packDir := setupBenchmarkRepoWithMidx(b)

	b.Run("with_midx", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			store, err := Open(packDir)
			require.NoError(b, err)

			// Exercise the data structures to ensure realistic allocation patterns.
			if store.midx != nil && len(store.midx.objectIDs) > 0 {
				hash := store.midx.objectIDs[0]
				store.Get(hash)
			}

			store.Close()
		}
	})

	// Remove midx file to force fallback to individual index files.
	midxPath := filepath.Join(packDir, "multi-pack-index")
	err := os.Remove(midxPath)
	require.True(b, err == nil || os.IsNotExist(err), "Failed to remove midx: %v", err)

	b.Run("without_midx", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			store, err := Open(packDir)
			require.NoError(b, err)

			// Perform equivalent operations using traditional pack index lookup.
			if len(store.packs) > 0 && len(store.packs[0].oidTable) > 0 {
				hash := store.packs[0].oidTable[0]
				store.Get(hash)
			}

			store.Close()
		}
	})
}

func TestStore_MidxOnly_NoIdx(t *testing.T) {
	dir := t.TempDir()

	pack := filepath.Join(dir, "solo.pack")
	blob := []byte("midx only")
	oid := calculateHash(ObjBlob, blob)

	require.NoError(t, createMinimalPack(pack, blob))
	// idx is required **only** to build the midx – create & delete afterwards.
	idxPath := strings.TrimSuffix(pack, ".pack") + ".idx"
	require.NoError(t, createV2IndexFile(idxPath, []Hash{oid}, []uint64{12}))

	createValidMidxFile(t, dir, filepath.Base(pack), []Hash{oid}, []uint64{12})
	require.NoError(t, os.Remove(idxPath)) // simulate "only midx"

	store, err := Open(dir)
	require.NoError(t, err)
	defer store.Close()

	assert.Nil(t, store.packs[0].idx, "Store should tolerate missing .idx when midx present")

	data, typ, err := store.Get(oid)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, blob, data)
}

func TestCRCVerification(t *testing.T) {
	// Skip when Git is not available (e.g. unusual CI images).
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	// Build a one‑commit repository.
	tmp := t.TempDir()

	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = tmp
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t",
			"GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t",
			"GIT_COMMITTER_EMAIL=t@example.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}

	run("init", "--quiet")
	os.WriteFile(filepath.Join(tmp, "secret.txt"), []byte("shh"), 0o644)
	run("add", "secret.txt")
	run("commit", "-m", "initial", "--quiet")

	// Repack so that objects live in a single *.pack.
	run("repack", "-adq")

	packDir := filepath.Join(tmp, ".git", "objects", "pack")

	// Need the HEAD commit hash for retrieval.
	headHashBytes, err := exec.Command("git", "-C", tmp, "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	headHash, err := ParseHash(string(headHashBytes[:40]))
	require.NoError(t, err)

	// Open store & fetch object with CRC verification.
	store, err := Open(packDir)
	require.NoError(t, err)
	defer store.Close()
	store.VerifyCRC = true

	_, _, err = store.Get(headHash)
	require.NoError(t, err)
}

func TestMidxFanoutAcrossPacks(t *testing.T) {
	packDir := setupBenchmarkRepoWithMidx(t)

	store, err := Open(packDir)
	require.NoError(t, err)
	defer store.Close()

	require.Greater(t, len(store.packs), 2, "need >2 packs")
	require.NotNil(t, store.midx)

	packCounts := make(map[uint32]int)
	for _, entry := range store.midx.entries {
		packCounts[entry.packID]++
	}

	// Choose an object from the *third* pack to prove fan‑out works.
	var target Hash
	for i, entry := range store.midx.entries {
		if entry.packID == 2 { // third pack (0‑based)
			target = store.midx.objectIDs[i]
			break
		}
	}
	if (target == Hash{}) {
		t.Skip("benchmark helper did not place objects in 3rd pack")
	}

	_, _, err = store.Get(target)
	require.NoError(t, err, "midx fan‑out should locate object across packs")
}

func TestThinPackCrossPackViaMidx(t *testing.T) {
	dir := t.TempDir()

	// Build Pack A containing the base blob.
	blobBase := []byte("cross-pack base")
	oidBase := calculateHash(ObjBlob, blobBase)

	packA := filepath.Join(dir, "packA.pack")
	require.NoError(t, createMinimalPack(packA, blobBase))
	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(packA, ".pack")+".idx",
		[]Hash{oidBase},
		[]uint64{12},
	))

	// Build Pack B as a thin pack containing a REF_DELTA that references the base blob in Pack A.
	blobDelta := []byte("cross-pack derived")
	oidDelta := calculateHash(ObjBlob, blobDelta)

	packB := filepath.Join(dir, "packB.pack")
	var packBuf bytes.Buffer

	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2)) // version
	binary.Write(&packBuf, binary.BigEndian, uint32(1)) // object count

	// Use the helper function to create a properly formatted REF_DELTA object.
	refDeltaObj, err := createRefDeltaObject(oidBase, blobDelta, blobBase)
	require.NoError(t, err)

	packBuf.Write(refDeltaObj)

	packChecksum := sha1.Sum(packBuf.Bytes())
	packBuf.Write(packChecksum[:])

	require.NoError(t, os.WriteFile(packB, packBuf.Bytes(), 0644))

	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(packB, ".pack")+".idx",
		[]Hash{oidDelta},
		[]uint64{12}, // offset after pack header
	))

	// Create a multi-pack index that maps both objects to their respective packs.
	createTwoPackMidxFile(
		t, dir,
		[]string{filepath.Base(packA), filepath.Base(packB)},
		[]Hash{oidBase, oidDelta},
		[]uint32{0, 1},   // pack IDs
		[]uint64{12, 12}, // offsets
	)

	// Verify that the store can resolve cross-pack deltas.
	store, err := Open(dir)
	require.NoError(t, err)
	defer store.Close()

	data, typ, err := store.Get(oidDelta)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, blobDelta, data)
}

// createTwoPackMidxFile writes a minimal, spec-compliant multi-pack-index
// v1 (SHA-1) that covers two or more packs.
func createTwoPackMidxFile(
	t *testing.T,
	dir string,
	packNames []string,
	oids []Hash,
	packIdx []uint32,
	offsets []uint64,
) {
	t.Helper()
	if len(packNames) == 0 ||
		len(oids) != len(packIdx) || len(oids) != len(offsets) {
		t.Fatalf("invalid input lengths")
	}

	const (
		hashIDSHA1   = 1
		chunkHdrSize = 12
	)

	var buf bytes.Buffer
	write := func(v any) { _ = binary.Write(&buf, binary.BigEndian, v) }

	buf.WriteString("MIDX")
	buf.WriteByte(1)
	buf.WriteByte(hashIDSHA1)
	buf.WriteByte(4)
	buf.WriteByte(0)
	write(uint32(len(packNames)))

	chunkTableOff := buf.Len()
	buf.Write(make([]byte, (4+1)*chunkHdrSize))

	type patch struct {
		id    [4]byte
		start uint64
	}
	var patches []patch
	addChunk := func(id string, body func()) {
		var idArr [4]byte
		copy(idArr[:], id)
		start := uint64(buf.Len())
		body()
		patches = append(patches, patch{idArr, start})
	}

	addChunk("OIDF", func() {
		var fanout [256]uint32
		for _, h := range oids {
			fanout[h[0]]++
		}
		var sum uint32
		for i := 0; i < 256; i++ {
			sum += fanout[i]
			write(sum)
		}
	})

	addChunk("OIDL", func() {
		for _, h := range oids {
			buf.Write(h[:])
		}
	})

	addChunk("OOFF", func() {
		for i := range oids {
			write(packIdx[i])
			write(uint32(offsets[i]))
		}
	})

	addChunk("PNAM", func() {
		for _, n := range packNames {
			buf.WriteString(n)
			buf.WriteByte(0)
		}
	})

	trailerStart := uint64(buf.Len())
	buf.Write(make([]byte, 40))

	sort.Slice(patches, func(i, j int) bool {
		return bytes.Compare(patches[i].id[:], patches[j].id[:]) < 0
	})

	for i, p := range patches {
		row := chunkTableOff + i*chunkHdrSize
		copy(buf.Bytes()[row:row+4], p.id[:])
		binary.BigEndian.PutUint64(buf.Bytes()[row+4:row+12], p.start)
	}
	termRow := chunkTableOff + 4*chunkHdrSize
	binary.BigEndian.PutUint64(buf.Bytes()[termRow+4:termRow+12], trailerStart)

	midxPath := filepath.Join(dir, "multi-pack-index")
	err := os.WriteFile(midxPath, buf.Bytes(), 0o644)
	require.NoError(t, err, "Failed to write multi-pack-index")
}
