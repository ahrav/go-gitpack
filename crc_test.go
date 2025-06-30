// crc_test.go - Simplified version that should work with existing helpers
package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

func TestVerifyCRC32_ValidObject(t *testing.T) {
	dir := t.TempDir()

	packPath := filepath.Join(dir, "test.pack")
	blob := []byte("test content for CRC")
	require.NoError(t, createMinimalPack(packPath, blob))

	hash := calculateHash(ObjBlob, blob)
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	store, err := OpenForTesting(dir)
	require.NoError(t, err)
	defer store.Close()

	store.VerifyCRC = true

	data, typ, err := store.Get(hash)
	assert.NoError(t, err, "CRC verification should pass for valid object")
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, blob, data)
}

func TestVerifyCRC32_ObjectExceedsPackBounds(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))

	obj1Offset := uint64(packBuf.Len())
	packBuf.Write([]byte{0x90, 0x01, 0x00})

	trailer := sha1.Sum(packBuf.Bytes())
	packBuf.Write(trailer[:])

	packSize := uint64(packBuf.Len())
	trailerStart := packSize - 20

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	obj2Offset := trailerStart + 5

	idx := &idxFile{
		pack:          packRA,
		sortedOffsets: []uint64{obj1Offset, obj2Offset},
		entriesByOff: map[uint64]idxEntry{
			obj1Offset: {offset: obj1Offset, crc: 0x12345678},
		},
	}

	err = verifyCRC32(idx, obj1Offset, 0x12345678)
	assert.ErrorIs(t, err, ErrObjectExceedsPackBounds)
}

func TestVerifyCRC32_NonMonotonicOffsets(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	packBuf.Write(make([]byte, 100))
	trailer := sha1.Sum(packBuf.Bytes())
	packBuf.Write(trailer[:])

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	idx := &idxFile{
		pack:          packRA,
		sortedOffsets: []uint64{50, 50},
		entriesByOff: map[uint64]idxEntry{
			50: {offset: 50, crc: 0x12345678},
		},
	}

	err = verifyCRC32(idx, 50, 0x12345678)
	assert.ErrorIs(t, err, ErrNonMonotonicOffsets)
}

func TestVerifyCRC32_MismatchedCRC(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(1))

	objOffset := uint64(packBuf.Len())
	testData := []byte{0x78, 0x9c, 0x01, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01}
	packBuf.Write(testData)

	trailer := sha1.Sum(packBuf.Bytes())
	packBuf.Write(trailer[:])

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	actualCRC := crc32.ChecksumIEEE(testData)
	wrongCRC := uint32(0xDEADBEEF)

	idx := &idxFile{
		pack:          packRA,
		sortedOffsets: []uint64{objOffset},
		entriesByOff: map[uint64]idxEntry{
			objOffset: {offset: objOffset, crc: wrongCRC},
		},
	}

	err = verifyCRC32(idx, objOffset, wrongCRC)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crc mismatch")
	assert.Contains(t, err.Error(), fmt.Sprintf("%08x", wrongCRC))
	assert.Contains(t, err.Error(), fmt.Sprintf("%08x", actualCRC))
}

func TestVerifyPackTrailer_Valid(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(0))
	packBuf.Write([]byte("test pack content"))

	checksum := sha1.Sum(packBuf.Bytes())
	packBuf.Write(checksum[:])

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	err = verifyPackTrailer(packRA)
	assert.NoError(t, err)
}

func TestVerifyPackTrailer_Corrupt(t *testing.T) {
	dir := t.TempDir()
	packPath := filepath.Join(dir, "test.pack")

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(0))
	packBuf.Write([]byte("test pack content"))
	packBuf.Write(make([]byte, 20))

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	packRA, err := mmap.Open(packPath)
	require.NoError(t, err)
	defer packRA.Close()

	err = verifyPackTrailer(packRA)
	assert.ErrorIs(t, err, ErrPackTrailerCorrupt)
}

func TestStore_VerifyPackTrailers(t *testing.T) {
	dir := t.TempDir()

	createValidPackWithTrailer(t, filepath.Join(dir, "good.pack"))
	createCorruptPackWithTrailer(t, filepath.Join(dir, "bad.pack"))

	store, err := OpenForTesting(dir)
	require.NoError(t, err)
	defer store.Close()

	foundCorrupt := false
	for _, pf := range store.packs {
		if pf.pack != nil {
			if err := verifyPackTrailer(pf.pack); err != nil {
				foundCorrupt = true
				assert.ErrorIs(t, err, ErrPackTrailerCorrupt)
			}
		}
	}
	assert.True(t, foundCorrupt, "Should find corrupt pack")
}

func createValidPackWithTrailer(t *testing.T, packPath string) {
	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(0))
	packBuf.Write([]byte("valid pack content"))

	checksum := sha1.Sum(packBuf.Bytes())
	packBuf.Write(checksum[:])

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"
	require.NoError(t, createV2IndexFile(idxPath, []Hash{}, []uint64{}))
}

func createCorruptPackWithTrailer(t *testing.T, packPath string) {
	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(0))
	packBuf.Write([]byte("corrupt pack"))
	packBuf.Write(bytes.Repeat([]byte{0xFF}, 20))

	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"
	require.NoError(t, createV2IndexFile(idxPath, []Hash{}, []uint64{}))
}

// Add other MIDX v2 tests here...

// TestMidxV2CRCParsing tests parsing of MIDX v2 files with CRC data.
func TestMidxV2CRCParsing(t *testing.T) {
	// NOTE: This test will fail until MIDX v2 support is implemented in parseMidx
	t.Skip("MIDX v2 parsing not yet implemented")

	dir := t.TempDir()

	// Create a pack with known objects
	packPath := filepath.Join(dir, "test.pack")
	blob := []byte("test blob for midx v2")
	hash := calculateHash(ObjBlob, blob)
	expectedCRC := uint32(0xDEADBEEF)

	require.NoError(t, createMinimalPack(packPath, blob))
	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(packPath, ".pack")+".idx",
		[]Hash{hash},
		[]uint64{12},
	))

	// Create MIDX v2 with CRCS chunk
	createMidxV2WithCRCs(t, dir, "test.pack", []Hash{hash}, []uint64{12}, []uint32{expectedCRC})

	// Parse the MIDX
	midxRA, err := mmap.Open(filepath.Join(dir, "multi-pack-index"))
	require.NoError(t, err)
	defer midxRA.Close()

	packCache := make(map[string]*mmap.ReaderAt)
	midx, err := parseMidx(dir, midxRA, packCache)
	require.NoError(t, err)

	// Verify version and CRC data
	assert.Equal(t, byte(2), midx.version, "Should parse as version 2")
	assert.Len(t, midx.entries, 1)
	assert.Equal(t, expectedCRC, midx.entries[0].crc, "Should parse CRC from CRCS chunk")
}

// createMidxV2WithCRCs creates a MIDX v2 file with CRCS chunk
func createMidxV2WithCRCs(
	t *testing.T,
	packDir string,
	packName string,
	hashes []Hash,
	offsets []uint64,
	crcs []uint32,
) {
	require.Equal(t, len(hashes), len(offsets))
	require.Equal(t, len(hashes), len(crcs))

	// Sort objects by hash
	type obj struct {
		h     Hash
		off32 uint32
		crc   uint32
	}
	objs := make([]obj, len(hashes))
	for i := range hashes {
		require.Less(t, offsets[i], uint64(0x80000000))
		objs[i] = obj{hashes[i], uint32(offsets[i]), crcs[i]}
	}
	sort.Slice(objs, func(i, j int) bool {
		return bytes.Compare(objs[i].h[:], objs[j].h[:]) < 0
	})

	// Build chunks
	var pnam bytes.Buffer
	pnam.WriteString(packName)
	pnam.WriteByte(0)

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

	var oidl bytes.Buffer
	for _, o := range objs {
		oidl.Write(o.h[:])
	}

	var ooff bytes.Buffer
	for _, o := range objs {
		binary.Write(&ooff, binary.BigEndian, uint32(0)) // pack-id
		binary.Write(&ooff, binary.BigEndian, o.off32)
	}

	// NEW: CRCS chunk for v2
	var crcsChunk bytes.Buffer
	for _, o := range objs {
		binary.Write(&crcsChunk, binary.BigEndian, o.crc)
	}

	// Assemble file
	type chunk struct {
		id   string
		data []byte
	}
	chunks := []chunk{
		{"OIDF", oidf.Bytes()},
		{"OIDL", oidl.Bytes()},
		{"OOFF", ooff.Bytes()},
		{"PNAM", pnam.Bytes()},
		{"CRCS", crcsChunk.Bytes()}, // NEW
	}

	const headerSize = 12
	chunkTableSize := (len(chunks) + 1) * 12
	offset := headerSize + chunkTableSize

	var file bytes.Buffer

	// Header with version 2
	file.WriteString("MIDX")
	file.WriteByte(2) // Version 2!
	file.WriteByte(1) // SHA-1
	file.WriteByte(byte(len(chunks)))
	file.WriteByte(0)
	binary.Write(&file, binary.BigEndian, uint32(1)) // pack-count

	// Chunk table
	for _, c := range chunks {
		file.WriteString(fmt.Sprintf("%-4s", c.id)[:4])
		binary.Write(&file, binary.BigEndian, uint64(offset))
		offset += len(c.data)
	}
	// Sentinel
	file.Write([]byte{0, 0, 0, 0})
	binary.Write(&file, binary.BigEndian, uint64(offset))

	// Payload
	for _, c := range chunks {
		file.Write(c.data)
	}

	midxPath := filepath.Join(packDir, "multi-pack-index")
	require.NoError(t, os.WriteFile(midxPath, file.Bytes(), 0644))
}
