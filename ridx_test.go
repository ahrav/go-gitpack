// ridx_test.go tests the reverse index (.ridx / .rev) loader and the
// in-memory builder that derives a reverse index from sorted pack offsets.
//
// A reverse index maps positions in offset-descending order back to the
// corresponding index positions in the .idx file. This is used for efficient
// pack traversal in offset order without scanning the entire index.
//
// The tests cover:
//   - Loading a .ridx file from disk (TestLoadReverseIndex_FromFile).
//   - Falling back to building the reverse index from offsets when no .ridx
//     file is present (TestLoadReverseIndex_BuildFromOffsets).
//   - Backward compatibility with the older .rev extension
//     (TestLoadReverseIndex_OldRevExtension).
//   - Validation of magic bytes, version, object counts, fanout, and trailer
//     checksums (TestLoadReverseIndex_InvalidFiles, _TrailerVerification).
//   - Edge cases such as nil packfile handles and midx-only packs.
//   - Benchmarks for building the reverse index and resolving index positions.

package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

// createValidRidxFile writes a Git .rev file: "RIDX", version 1, hash
// function 1 (SHA-1), one .idx position per object in ascending pack-offset
// order, the pack checksum, and the file's own SHA-1 trailer. A nil
// packChecksum writes zeros, which tryLoadRidxFile accepts only when the pack
// is not mapped.
func createValidRidxFile(
	t testing.TB,
	ridxPath string,
	positions []uint32,
	packChecksum []byte,
) error {
	var buf bytes.Buffer

	buf.WriteString(ridxMagic)
	binary.Write(&buf, binary.BigEndian, uint32(1))
	binary.Write(&buf, binary.BigEndian, uint32(ridxHashSHA1))
	for _, pos := range positions {
		binary.Write(&buf, binary.BigEndian, pos)
	}
	if packChecksum != nil {
		buf.Write(packChecksum)
	} else {
		buf.Write(make([]byte, hashSize))
	}
	self := sha1.Sum(buf.Bytes())
	buf.Write(self[:])

	return os.WriteFile(ridxPath, buf.Bytes(), 0644)
}

// ascendingPositions returns the .idx positions 0..n-1, the .rev table of a
// pack whose objects sit in ascending offset order by idx position.
func ascendingPositions(n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i)
	}
	return out
}

// revPositionsFor returns pf's .idx positions in ascending pack-offset
// order, which is the table a .rev file carries for that pack.
func revPositionsFor(pf *idxFile) []uint32 {
	out := make([]uint32, len(pf.entries))
	for i := range out {
		out[i] = uint32(i)
	}
	sort.Slice(out, func(a, b int) bool { return pf.entries[out[a]].offset < pf.entries[out[b]].offset })
	return out
}

// TestLoadReverseIndex_FromFile verifies that loadReverseIndex correctly reads
// a .ridx file from disk and returns the expected mapping. The test creates a
// minimal pack with a single blob object, writes a corresponding .ridx file,
// and asserts that the returned reverse index has one entry mapping to idx
// position 0.
func TestLoadReverseIndex_FromFile(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")
	ridxPath := filepath.Join(dir, "test.ridx")

	blob := []byte("test content")
	hash := calculateHash(ObjBlob, blob)

	require.NoError(t, createMinimalPack(packPath, blob))
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)

	require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{0}, nil))

	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)

	assert.Len(t, ridx, 1)
	assert.Equal(t, uint32(0), ridx[0])
}

// TestLoadReverseIndex_BuildFromOffsets verifies the fallback path where no
// .ridx file exists on disk. In this case loadReverseIndex builds the reverse
// index in memory from the sorted offsets in the parsed index file. The test
// creates three objects at offsets [12, 50, 100] and confirms the reverse
// mapping sorts them in descending offset order: offset 100 -> idx 2,
// offset 50 -> idx 1, offset 12 -> idx 0.
func TestLoadReverseIndex_BuildFromOffsets(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	content1 := []byte("first object")
	content2 := []byte("second object")
	content3 := []byte("third object")

	hash1 := calculateHash(ObjBlob, content1)
	hash2 := calculateHash(ObjBlob, content2)
	hash3 := calculateHash(ObjBlob, content3)

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(3))

	offsets := []uint64{12, 50, 100}

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash1, hash2, hash3}, offsets))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)

	// Should build from offsets since no ridx file exists.
	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)

	assert.Len(t, ridx, 3)

	// Verify that the ridx correctly maps descending offset positions
	// back to the entries-table indices. For each descending position k,
	// ridx[k] should give us the entries index whose offset is
	// sortedOffsets[n-1-k].
	n := len(pf.sortedOffsets)
	for k := range n {
		idxPos := int(ridx[k])
		require.Less(t, idxPos, len(pf.entries), "ridx[%d] out of range", k)
		wantOff := pf.sortedOffsets[n-1-k]
		gotOff := pf.entries[idxPos].offset
		assert.Equal(t, wantOff, gotOff,
			"ridx[%d] should map to entry with offset %d, got offset %d", k, wantOff, gotOff)
	}
}

func TestLoadReverseIndex_OldRevExtension(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")
	revPath := filepath.Join(dir, "test.rev")

	blob := []byte("rev extension test")
	hash := calculateHash(ObjBlob, blob)

	require.NoError(t, createMinimalPack(packPath, blob))
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)

	require.NoError(t, createValidRidxFile(t, revPath, []uint32{0}, nil))

	// Should find and use .rev file for backward compatibility.
	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)
	assert.Len(t, ridx, 1)
}

func TestLoadReverseIndex_InvalidFiles(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	require.NoError(t, os.WriteFile(packPath, []byte("minimal pack"), 0644))

	blob := []byte("test")
	hash := calculateHash(ObjBlob, blob)
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)

	t.Run("bad_magic", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString("NOPE")
		binary.Write(&buf, binary.BigEndian, uint32(1))
		binary.Write(&buf, binary.BigEndian, uint32(ridxHashSHA1))
		buf.Write(make([]byte, ridxTrailerSize))
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad magic")

		ridx, err := loadReverseIndex(packPath, pf)
		assert.NoError(t, err)
		assert.NotNil(t, ridx)
	})

	t.Run("bad_version", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(99))
		binary.Write(&buf, binary.BigEndian, uint32(ridxHashSHA1))
		buf.Write(make([]byte, ridxTrailerSize))
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported version")

		ridx, err := loadReverseIndex(packPath, pf)
		assert.NoError(t, err)
		assert.NotNil(t, ridx)
	})

	t.Run("object_count_mismatch", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		require.NoError(t, createValidRidxFile(t, ridxPath, ascendingPositions(5), nil))
		defer os.Remove(ridxPath)

		_, err := tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "object count mismatch")

		ridx, err := loadReverseIndex(packPath, pf)
		assert.NoError(t, err)
		assert.NotNil(t, ridx)
	})

	t.Run("unsupported_hash", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(1))
		binary.Write(&buf, binary.BigEndian, uint32(2)) // SHA-256
		binary.Write(&buf, binary.BigEndian, uint32(0))
		buf.Write(make([]byte, ridxTrailerSize))
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported hash function")

		ridx, err := loadReverseIndex(packPath, pf)
		assert.NoError(t, err)
		assert.NotNil(t, ridx)
	})

	t.Run("table_not_word_aligned", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(1))
		binary.Write(&buf, binary.BigEndian, uint32(ridxHashSHA1))
		buf.Write([]byte{0, 0, 0}) // three bytes of table
		buf.Write(make([]byte, ridxTrailerSize))
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)

		ridx, err := loadReverseIndex(packPath, pf)
		assert.NoError(t, err)
		assert.NotNil(t, ridx)
	})

	// Positions at or above 1<<31 are negative as a 32-bit int; the bounds
	// check must still reject them (exercised by GOARCH=386).
	for _, pos := range []uint32{1, 1 << 31, 0xffffffff} {
		t.Run(fmt.Sprintf("position_out_of_range_%#x", pos), func(t *testing.T) {
			ridxPath := filepath.Join(dir, "test.ridx")
			require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{pos}, nil))
			defer os.Remove(ridxPath)

			_, err := tryLoadRidxFile(ridxPath, pf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "names idx position")

			ridx, err := loadReverseIndex(packPath, pf)
			require.NoError(t, err)
			assert.Equal(t, []uint32{0}, ridx)
		})
	}

	t.Run("bad_file_checksum", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{0}, nil))
		data, err := os.ReadFile(ridxPath)
		require.NoError(t, err)
		data[len(data)-1] ^= 0xff
		require.NoError(t, os.WriteFile(ridxPath, data, 0644))
		defer os.Remove(ridxPath)

		_, err = tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file checksum mismatch")

		ridx, err := loadReverseIndex(packPath, pf)
		assert.NoError(t, err)
		assert.NotNil(t, ridx)
	})

	t.Run("missing_trailer", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(1))
		binary.Write(&buf, binary.BigEndian, uint32(ridxHashSHA1))
		binary.Write(&buf, binary.BigEndian, uint32(0))
		// File ends here without trailer checksums.
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		packRA, err := mmap.Open(packPath)
		require.NoError(t, err)
		defer packRA.Close()
		pf.pack = packRA

		_, err = tryLoadRidxFile(ridxPath, pf)
		assert.Error(t, err)

		// The truncated file is ignored and the mapping rebuilt from the idx.
		ridx, err := loadReverseIndex(packPath, pf)
		require.NoError(t, err)
		assert.Len(t, ridx, 1)
	})
}

func TestLoadReverseIndex_TrailerVerification(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")
	ridxPath := filepath.Join(dir, "test.ridx")

	packContent := []byte("PACK\x00\x00\x00\x02\x00\x00\x00\x00test data")
	packChecksum := sha1.Sum(packContent)
	packContent = append(packContent, packChecksum[:]...)
	require.NoError(t, os.WriteFile(packPath, packContent, 0644))

	blob := []byte("test")
	hash := calculateHash(ObjBlob, blob)
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	idxData, err := os.ReadFile(idxPath)
	require.NoError(t, err)
	idxChecksum := idxData[len(idxData)-hashSize:]

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)
	pf.pack = packRA
	pf.idx = idxRA

	_ = idxChecksum
	require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{0}, packChecksum[:]))

	ridx, err := tryLoadRidxFile(ridxPath, pf)
	require.NoError(t, err)
	assert.Equal(t, []uint32{0}, ridx)

	// Test with wrong pack checksum.
	os.Remove(ridxPath)
	wrongChecksum := sha1.Sum([]byte("wrong"))
	require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{0}, wrongChecksum[:]))

	_, err = tryLoadRidxFile(ridxPath, pf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pack checksum mismatch")

	_, err = loadReverseIndex(packPath, pf)
	assert.NoError(t, err)
}

func TestBuildReverseFromEntries(t *testing.T) {
	tests := []struct {
		name     string
		pf       *idxFile
		expected []uint32
	}{
		{
			name: "single object",
			pf: &idxFile{
				entries:       []idxEntry{{offset: 100}},
				sortedOffsets: []uint64{100},
			},
			expected: []uint32{0},
		},
		{
			name: "multiple objects ascending",
			pf: &idxFile{
				entries:       []idxEntry{{offset: 10}, {offset: 50}, {offset: 100}, {offset: 200}},
				sortedOffsets: []uint64{10, 50, 100, 200},
			},
			expected: []uint32{3, 2, 1, 0}, // descending order
		},
		{
			name: "empty",
			pf: &idxFile{
				entries:       []idxEntry{},
				sortedOffsets: []uint64{},
			},
			expected: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildReverseFromEntries(tt.pf)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReverseIndexMapping(t *testing.T) {
	// Test that the reverse index correctly maps offset positions to idx positions.
	// offsets:  [10, 50, 100, 200] (sorted ascending)
	// idx pos:  [ 0,  1,   2,   3]
	// reverse:  [ 3,  2,   1,   0] (maps descending offset order to idx pos)

	offsets := []uint64{10, 50, 100, 200}
	pf := &idxFile{
		entries:       []idxEntry{{offset: 10}, {offset: 50}, {offset: 100}, {offset: 200}},
		sortedOffsets: offsets,
	}
	ridx := buildReverseFromEntries(pf)

	// In offset-descending order:
	// Position 0 (offset 200) -> idx position 3
	// Position 1 (offset 100) -> idx position 2
	// Position 2 (offset 50)  -> idx position 1
	// Position 3 (offset 10)  -> idx position 0

	assert.Equal(t, uint32(3), ridx[0], "Largest offset (200) should map to idx pos 3")
	assert.Equal(t, uint32(2), ridx[1], "Second largest offset (100) should map to idx pos 2")
	assert.Equal(t, uint32(1), ridx[2], "Third largest offset (50) should map to idx pos 1")
	assert.Equal(t, uint32(0), ridx[3], "Smallest offset (10) should map to idx pos 0")
}

func TestResolveIdxPos(t *testing.T) {
	pf := &idxFile{
		ridx: []uint32{5, 4, 3, 2, 1, 0},
	}

	tests := []struct {
		name        string
		bit         int
		expected    uint32
		shouldPanic bool
	}{
		{"first bit", 0, 5, false},
		{"middle bit", 3, 2, false},
		{"last bit", 5, 0, false},
		{"negative bit", -1, 0, true},
		{"out of range", 6, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					pf.resolveIdxPos(tt.bit)
				})
			} else {
				result := pf.resolveIdxPos(tt.bit)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestResolveIdxPos_NilRidx(t *testing.T) {
	pf := &idxFile{ridx: nil}

	assert.Panics(t, func() {
		pf.resolveIdxPos(0)
	})
}

func TestLoadReverseIndex_NilPf(t *testing.T) {
	assert.Panics(t, func() {
		loadReverseIndex("test.pack", nil)
	})
}

func TestLoadReverseIndex_MissingPackAndIdx(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")
	ridxPath := filepath.Join(dir, "test.ridx")

	blob := []byte("test")
	hash := calculateHash(ObjBlob, blob)
	require.NoError(t, createMinimalPack(packPath, blob))
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)
	pf.pack = nil // No pack handle

	require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{0}, nil))

	// Should still load since trailer verification is skipped when pack is nil.
	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)
	assert.Len(t, ridx, 1)
}

func TestLoadReverseIndex_MidxOnlyPack(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")
	ridxPath := filepath.Join(dir, "test.ridx")

	blob := []byte("midx only test")
	hash := calculateHash(ObjBlob, blob)
	require.NoError(t, createMinimalPack(packPath, blob))

	// Create idx for building ridx, then remove it to simulate midx-only scenario.
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	pf, err := parseIdx(idxRA)
	require.NoError(t, err)
	idxRA.Close()

	packData, _ := os.ReadFile(packPath)
	packChecksum := packData[len(packData)-hashSize:]
	idxData, _ := os.ReadFile(idxPath)
	idxChecksum := idxData[len(idxData)-hashSize:]
	_ = idxChecksum
	require.NoError(t, createValidRidxFile(t, ridxPath, []uint32{0}, packChecksum))

	// Remove idx to simulate midx-only scenario.
	os.Remove(idxPath)

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	// Setup pf as midx-only (no idx handle).
	pf.pack = packRA
	pf.idx = nil

	// Should still load and skip idx trailer check.
	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)
	assert.Len(t, ridx, 1)
}

// BenchmarkBuildReverseFromOffsets measures the time to build a reverse index
// from 10,000 sorted offsets. This simulates a moderately-sized packfile and
// exercises the sort-based construction path.
func BenchmarkBuildReverseFromEntries(b *testing.B) {
	// Create a realistic set of offsets (10,000 objects spaced 100 bytes apart).
	numObjects := 10000
	entries := make([]idxEntry, numObjects)
	offsets := make([]uint64, numObjects)
	for i := range numObjects {
		offsets[i] = uint64(i * 100)
		entries[i] = idxEntry{offset: uint64(i * 100)}
	}
	pf := &idxFile{entries: entries, sortedOffsets: offsets}

	b.ResetTimer()
	for b.Loop() {
		_ = buildReverseFromEntries(pf)
	}
}

// BenchmarkLoadReverseIndex_FromFile measures the time to load and parse a
// .ridx file containing 1,000 objects. This covers the mmap + binary parse
// hot path used when a pre-built reverse index is available.
func BenchmarkLoadReverseIndex_FromFile(b *testing.B) {
	dir := b.TempDir()
	packPath := filepath.Join(dir, "bench.pack")
	ridxPath := filepath.Join(dir, "bench.ridx")

	numObjects := 1000
	require.NoError(b, createValidRidxFile(b, ridxPath, ascendingPositions(numObjects), nil))

	// Entries whose offsets ascend with idx position, so the ascending table
	// above is the correct .rev content.
	pf := &idxFile{entries: make([]idxEntry, numObjects)}
	for i := range pf.entries {
		pf.entries[i].offset = uint64(i * 100)
	}

	b.ResetTimer()
	for b.Loop() {
		pf.sortedOffsets = nil
		ridx, err := loadReverseIndex(packPath, pf)
		require.NoError(b, err)
		_ = ridx
	}
}

// BenchmarkResolveIdxPos measures the cost of resolving bit positions through
// the reverse index. It tests three positions (first, middle, last) per
// iteration across a 10,000-entry index to capture any position-dependent
// performance differences.
func BenchmarkResolveIdxPos(b *testing.B) {
	numObjects := 10000
	ridx := make([]uint32, numObjects)
	for i := 0; i < numObjects; i++ {
		ridx[i] = uint32(numObjects - 1 - i)
	}

	pf := &idxFile{ridx: ridx}

	b.ResetTimer()
	for b.Loop() {
		_ = pf.resolveIdxPos(0)
		_ = pf.resolveIdxPos(numObjects / 2)
		_ = pf.resolveIdxPos(numObjects - 1)
	}
}

func TestLoadReverseIndex_Integration(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")
	ridxPath := filepath.Join(dir, "test.ridx")

	// Create pack with multiple objects at known offsets.
	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(3))

	// Track offsets where objects will be placed.
	offsets := []uint64{12, 50, 100}
	hashes := make([]Hash, 3)

	packContent := make([]byte, 150)
	copy(packContent, packBuf.Bytes())
	require.NoError(t, os.WriteFile(packPath, packContent, 0644))

	for i := range hashes {
		hashes[i] = calculateHash(ObjBlob, []byte(fmt.Sprintf("object %d", i)))
	}
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, hashes, offsets))

	idxRA, err := mmap.Open(idxPath)
	require.NoError(t, err)
	defer idxRA.Close()

	pf, err := parseIdx(idxRA)
	require.NoError(t, err)

	// A .rev file lists idx positions in ascending offset order.
	asc := revPositionsFor(pf)
	require.NoError(t, createValidRidxFile(t, ridxPath, asc, nil))

	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)
	require.NotNil(t, ridx)

	pf.ridx = ridx

	// In memory the table is descending: bit 0 is the largest offset.
	for k := range 3 {
		assert.Equal(t, asc[2-k], pf.resolveIdxPos(k), "bit %d", k)
		assert.Equal(t, pf.sortedOffsets[2-k], pf.entries[pf.resolveIdxPos(k)].offset)
	}
}

func TestSyntheticRidxCRCLookup(t *testing.T) {
	t.Parallel()

	f := &idxFile{
		entries: []idxEntry{
			{offset: 500, crc: 0x1111},
			{offset: 100, crc: 0x2222},
			{offset: 300, crc: 0x3333},
		},
		sortedOffsets: []uint64{100, 300, 500},
	}

	f.ridx = buildReverseFromEntries(f)
	f.ridxCRCTrusted = true
	t.Logf("synthetic ridx: %v", f.ridx)

	crc, ok := f.crcAtOffset(100)
	require.True(t, ok, "should find offset 100")
	assert.Equal(t, uint32(0x2222), crc,
		"CRC for offset 100 should be BBB's CRC (0x2222), not AAA's (0x1111)")

	crc, ok = f.crcAtOffset(300)
	require.True(t, ok, "should find offset 300")
	assert.Equal(t, uint32(0x3333), crc,
		"CRC for offset 300 should be CCC's CRC (0x3333)")

	crc, ok = f.crcAtOffset(500)
	require.True(t, ok, "should find offset 500")
	assert.Equal(t, uint32(0x1111), crc,
		"CRC for offset 500 should be AAA's CRC (0x1111)")
}

func TestSyntheticRidxCRCLookupMatchingOrder(t *testing.T) {
	t.Parallel()

	f := &idxFile{
		entries: []idxEntry{
			{offset: 100, crc: 0x1111},
			{offset: 300, crc: 0x2222},
			{offset: 500, crc: 0x3333},
		},
		sortedOffsets: []uint64{100, 300, 500},
	}

	f.ridx = buildReverseFromEntries(f)
	f.ridxCRCTrusted = true

	crc, ok := f.crcAtOffset(100)
	require.True(t, ok)
	assert.Equal(t, uint32(0x1111), crc, "should get correct CRC when orders match")

	crc, ok = f.crcAtOffset(300)
	require.True(t, ok)
	assert.Equal(t, uint32(0x2222), crc)

	crc, ok = f.crcAtOffset(500)
	require.True(t, ok)
	assert.Equal(t, uint32(0x3333), crc)
}
