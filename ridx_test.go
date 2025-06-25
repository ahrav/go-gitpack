package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

// createValidRidxFile creates a minimal reverse-index file for testing.
// The file maps descending pack offsets to index positions.
// For objCount=3, it writes [2, 1, 0] to map:
//   - Bit 0 (largest offset) -> idx position 2
//   - Bit 1 (middle offset)  -> idx position 1
//   - Bit 2 (smallest offset) -> idx position 0
func createValidRidxFile(
	t testing.TB,
	ridxPath string,
	objCount uint32,
	packChecksum []byte,
	idxChecksum []byte,
) error {
	var buf bytes.Buffer

	buf.WriteString(ridxMagic)
	binary.Write(&buf, binary.BigEndian, uint32(1))

	// Fanout table contains 256 entries.
	for range 256 {
		binary.Write(&buf, binary.BigEndian, objCount)
	}

	// Main table entries in descending order: (n-1, n-2, ..., 1, 0).
	for i := objCount; i > 0; i-- {
		binary.Write(&buf, binary.BigEndian, uint32(i-1))
	}

	if packChecksum != nil {
		buf.Write(packChecksum)
	} else {
		buf.Write(make([]byte, hashSize))
	}
	if idxChecksum != nil {
		buf.Write(idxChecksum)
	} else {
		buf.Write(make([]byte, hashSize))
	}

	return os.WriteFile(ridxPath, buf.Bytes(), 0644)
}

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

	packChecksum := sha1.Sum([]byte("pack"))
	idxChecksum := sha1.Sum([]byte("idx"))
	require.NoError(t, createValidRidxFile(t, ridxPath, 1, packChecksum[:], idxChecksum[:]))

	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)

	assert.Len(t, ridx, 1)
	assert.Equal(t, uint32(0), ridx[0])
}

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
	// Verify descending offset order mapping.
	assert.Equal(t, uint32(2), ridx[0]) // offset 100 -> idx 2
	assert.Equal(t, uint32(1), ridx[1]) // offset 50 -> idx 1
	assert.Equal(t, uint32(0), ridx[2]) // offset 12 -> idx 0
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

	require.NoError(t, createValidRidxFile(t, revPath, 1, nil, nil))

	// Should find and use .rev file for backward compatibility.
	ridx, err := loadReverseIndex(packPath, pf)
	require.NoError(t, err)
	assert.Len(t, ridx, 1)
}

func TestLoadReverseIndex_InvalidFiles(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

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

	t.Run("bad_magic", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		// Write bad magic plus version to avoid EOF during header parsing.
		var buf bytes.Buffer
		buf.WriteString("BADM")
		binary.Write(&buf, binary.BigEndian, uint32(1))
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := loadReverseIndex(packPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "bad magic")
	})

	t.Run("bad_version", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(99))
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := loadReverseIndex(packPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported version")
	})

	t.Run("object_count_mismatch", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		require.NoError(t, createValidRidxFile(t, ridxPath, 5, nil, nil))
		defer os.Remove(ridxPath)

		_, err := loadReverseIndex(packPath, pf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "object count mismatch")
	})

	t.Run("truncated_fanout", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(1))
		// Write only partial fanout table (100 entries instead of required 256).
		for range 100 {
			binary.Write(&buf, binary.BigEndian, uint32(1))
		}
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := loadReverseIndex(packPath, pf)
		assert.Error(t, err)
	})

	t.Run("truncated_entries", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(1))
		// Write full fanout table claiming 10 objects but only provide 5 entries.
		for range 256 {
			binary.Write(&buf, binary.BigEndian, uint32(10))
		}
		for i := 0; i < 5; i++ {
			binary.Write(&buf, binary.BigEndian, uint32(i))
		}
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		_, err := loadReverseIndex(packPath, pf)
		assert.Error(t, err)
	})

	t.Run("missing_trailer", func(t *testing.T) {
		ridxPath := filepath.Join(dir, "test.ridx")
		var buf bytes.Buffer
		buf.WriteString(ridxMagic)
		binary.Write(&buf, binary.BigEndian, uint32(1))
		for range 256 {
			binary.Write(&buf, binary.BigEndian, uint32(1))
		}
		binary.Write(&buf, binary.BigEndian, uint32(0))
		// File ends here without trailer checksums.
		require.NoError(t, os.WriteFile(ridxPath, buf.Bytes(), 0644))
		defer os.Remove(ridxPath)

		packRA, err := mmap.Open(packPath)
		require.NoError(t, err)
		defer packRA.Close()
		pf.pack = packRA

		// Should still load since trailer verification is best-effort.
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

	require.NoError(t, createValidRidxFile(t, ridxPath, 1, packChecksum[:], idxChecksum))

	_, err = loadReverseIndex(packPath, pf)
	require.NoError(t, err)

	// Test with wrong pack checksum.
	os.Remove(ridxPath)
	wrongChecksum := sha1.Sum([]byte("wrong"))
	require.NoError(t, createValidRidxFile(t, ridxPath, 1, wrongChecksum[:], idxChecksum))

	_, err = loadReverseIndex(packPath, pf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pack checksum mismatch")

	// Test with wrong idx checksum.
	os.Remove(ridxPath)
	require.NoError(t, createValidRidxFile(t, ridxPath, 1, packChecksum[:], wrongChecksum[:]))

	_, err = loadReverseIndex(packPath, pf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "idx checksum mismatch")
}

func TestBuildReverseFromOffsets(t *testing.T) {
	tests := []struct {
		name     string
		offsets  []uint64
		expected []uint32
	}{
		{
			name:     "single object",
			offsets:  []uint64{100},
			expected: []uint32{0},
		},
		{
			name:     "multiple objects ascending",
			offsets:  []uint64{10, 50, 100, 200},
			expected: []uint32{3, 2, 1, 0}, // descending order
		},
		{
			name:     "empty",
			offsets:  []uint64{},
			expected: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildReverseFromOffsets(tt.offsets)
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
	ridx := buildReverseFromOffsets(offsets)

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

// func TestResolveIdxPos(t *testing.T) {
// 	pf := &idxFile{
// 		ridx: []uint32{5, 4, 3, 2, 1, 0},
// 	}

// 	tests := []struct {
// 		name      string
// 		bit       int
// 		expected  uint32
// 		shouldPanic bool
// 	}{
// 		{"first bit", 0, 5, false},
// 		{"middle bit", 3, 2, false},
// 		{"last bit", 5, 0, false},
// 		{"negative bit", -1, 0, true},
// 		{"out of range", 6, 0, true},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if tt.shouldPanic {
// 				assert.Panics(t, func() {
// 					pf.resolveIdxPos(tt.bit)
// 				})
// 			} else {
// 				result := pf.resolveIdxPos(tt.bit)
// 				assert.Equal(t, tt.expected, result)
// 			}
// 		})
// 	}
// }

// func TestResolveIdxPos_NilRidx(t *testing.T) {
// 	pf := &idxFile{ridx: nil}

// 	assert.Panics(t, func() {
// 		pf.resolveIdxPos(0)
// 	})
// }

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

	require.NoError(t, createValidRidxFile(t, ridxPath, 1, nil, nil))

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
	require.NoError(t, createValidRidxFile(t, ridxPath, 1, packChecksum, idxChecksum))

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

func BenchmarkBuildReverseFromOffsets(b *testing.B) {
	// Create a realistic set of offsets.
	numObjects := 10000
	offsets := make([]uint64, numObjects)
	for i := range numObjects {
		offsets[i] = uint64(i * 100)
	}

	b.ResetTimer()
	for b.Loop() {
		_ = buildReverseFromOffsets(offsets)
	}
}

func BenchmarkLoadReverseIndex_FromFile(b *testing.B) {
	dir := b.TempDir()
	packPath := filepath.Join(dir, "bench.pack")
	ridxPath := filepath.Join(dir, "bench.ridx")

	numObjects := uint32(1000)
	require.NoError(b, createValidRidxFile(b, ridxPath, numObjects, nil, nil))

	// Create dummy idx file.
	pf := &idxFile{
		sortedOffsets: make([]uint64, numObjects),
	}
	for i := uint32(0); i < numObjects; i++ {
		pf.sortedOffsets[i] = uint64(i * 100)
	}

	b.ResetTimer()
	for b.Loop() {
		ridx, err := loadReverseIndex(packPath, pf)
		require.NoError(b, err)
		_ = ridx
	}
}

// func BenchmarkResolveIdxPos(b *testing.B) {
// 	numObjects := 10000
// 	ridx := make([]uint32, numObjects)
// 	for i := 0; i < numObjects; i++ {
// 		ridx[i] = uint32(numObjects - 1 - i)
// 	}

// 	pf := &idxFile{ridx: ridx}

// 	b.ResetTimer()
// 	for b.Loop() {
// 		// Access various positions
// 		_ = pf.resolveIdxPos(0)
// 		_ = pf.resolveIdxPos(numObjects / 2)
// 		_ = pf.resolveIdxPos(numObjects - 1)
// 	}
// }

// func TestLoadReverseIndex_Integration(t *testing.T) {
// 	dir := t.TempDir()
// 	packPath := filepath.Join(dir, "test.pack")
// 	ridxPath := filepath.Join(dir, "test.ridx")

// 	// Create pack with multiple objects at known offsets
// 	var packBuf bytes.Buffer
// 	packBuf.Write([]byte("PACK"))
// 	binary.Write(&packBuf, binary.BigEndian, uint32(2))
// 	binary.Write(&packBuf, binary.BigEndian, uint32(3))

// 	// Track offsets where objects will be placed
// 	offsets := []uint64{12, 50, 100}
// 	hashes := make([]Hash, 3)

// 	// Write pack content (simplified - just placeholders)
// 	packContent := make([]byte, 150)
// 	copy(packContent, packBuf.Bytes())
// 	require.NoError(t, os.WriteFile(packPath, packContent, 0644))

// 	// Create idx file
// 	for i := range hashes {
// 		hashes[i] = calculateHash(ObjBlob, []byte(fmt.Sprintf("object %d", i)))
// 	}
// 	idxPath := filepath.Join(dir, "test.idx")
// 	require.NoError(t, createV2IndexFile(idxPath, hashes, offsets))

// 	// Parse idx
// 	idxRA, err := mmap.Open(idxPath)
// 	require.NoError(t, err)
// 	defer idxRA.Close()

// 	pf, err := parseIdx(idxRA)
// 	require.NoError(t, err)

// 	// Create ridx with proper mapping
// 	// offsets [12, 50, 100] -> descending [100, 50, 12] -> idx positions [2, 1, 0]
// 	require.NoError(t, createValidRidxFile(t, ridxPath, 3, nil, nil))

// 	// Load and verify
// 	ridx, err := loadReverseIndex(packPath, pf)
// 	require.NoError(t, err)
// 	require.NotNil(t, ridx)

// 	// Set the ridx on the idxFile
// 	pf.ridx = ridx

// 	// Verify the mapping:
// 	// Bit 0 (largest offset 100) -> idx position 2
// 	// Bit 1 (middle offset 50) -> idx position 1
// 	// Bit 2 (smallest offset 12) -> idx position 0
// 	assert.Equal(t, uint32(2), pf.resolveIdxPos(0))
// 	assert.Equal(t, uint32(1), pf.resolveIdxPos(1))
// 	assert.Equal(t, uint32(0), pf.resolveIdxPos(2))
// }
