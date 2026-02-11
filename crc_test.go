// crc_test.go contains tests for CRC-32 verification of individual pack
// objects, SHA-1 pack trailer validation, and MIDX v2 CRC chunk parsing.
// Each test constructs minimal pack files in a temporary directory, writes
// the correct (or deliberately incorrect) checksums, and asserts the expected
// error sentinels from the verification routines.
package objstore

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/zlib"
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

// TestVerifyCRC32_ValidObject writes a pack containing a single zlib-compressed
// blob, creates a matching index, opens the store with CRC verification
// enabled, and confirms that retrieval succeeds without error.
func TestVerifyCRC32_ValidObject(t *testing.T) {
	dir := t.TempDir()

	packPath := filepath.Join(dir, "test.pack")
	blob := []byte("test content for CRC")

	var packBuf bytes.Buffer
	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2))
	binary.Write(&packBuf, binary.BigEndian, uint32(1))

	var compressedData bytes.Buffer
	zw, _ := zlib.NewWriterLevel(&compressedData, zlib.DefaultCompression)
	_, err := zw.Write(blob)
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	packBuf.WriteByte(0xb4) // type blob, size > 15
	packBuf.WriteByte(0x01) // remaining size
	packBuf.Write(compressedData.Bytes())

	// Pack trailer.
	checksum := sha1.Sum(packBuf.Bytes())
	packBuf.Write(checksum[:])
	require.NoError(t, os.WriteFile(packPath, packBuf.Bytes(), 0644))

	hash := calculateHash(ObjBlob, blob)
	idxPath := filepath.Join(dir, "test.idx")
	require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))

	store, err := OpenForTesting(dir)
	require.NoError(t, err)
	defer store.Close()

	store.VerifyCRC = true

	data, typ, err := store.get(hash)
	assert.NoError(t, err, "CRC verification should pass for valid object")
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, blob, data)
}

// TestVerifyCRC32_ObjectExceedsPackBounds constructs an idxFile whose second
// object offset falls past the pack trailer boundary and verifies that
// verifyCRC32 returns ErrObjectExceedsPackBounds.
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
	}

	err = verifyCRC32(idx, obj1Offset, 0x12345678)
	assert.ErrorIs(t, err, ErrObjectExceedsPackBounds)
}

// TestVerifyCRC32_NonMonotonicOffsets creates a sorted-offset slice with two
// identical entries and asserts that verifyCRC32 detects the non-strictly-
// increasing sequence via ErrNonMonotonicOffsets.
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
	}

	err = verifyCRC32(idx, 50, 0x12345678)
	assert.ErrorIs(t, err, ErrNonMonotonicOffsets)
}

// TestVerifyCRC32_MismatchedCRC writes a pack object whose on-disk bytes are
// raw zlib data (0x78 0x9c header followed by an empty stored block) so that
// the IEEE CRC-32 of those bytes is known. It then calls verifyCRC32 with an
// intentionally wrong expected CRC (0xDEADBEEF) and checks that the returned
// error message includes both the expected and actual CRC values.
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
	}

	err = verifyCRC32(idx, objOffset, wrongCRC)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "crc mismatch")
	assert.Contains(t, err.Error(), fmt.Sprintf("%08x", wrongCRC))
	assert.Contains(t, err.Error(), fmt.Sprintf("%08x", actualCRC))
}

// TestIdxFileCRCAtOffset_SyntheticReverseIndex verifies that crcAtOffset does
// not trust the fallback reverse-index shape when it is only a descending rank
// list and not a real offset->entry mapping.
func TestIdxFileCRCAtOffset_SyntheticReverseIndex(t *testing.T) {
	f := &idxFile{
		entries: []idxEntry{
			{offset: 30, crc: uint32(0xAAAA0001)},
			{offset: 10, crc: uint32(0xBBBB0002)},
			{offset: 20, crc: uint32(0xCCCC0003)},
		},
		sortedOffsets: []uint64{10, 20, 30},
	}
	f.ridx = buildReverseFromEntries(f)
	f.ridxCRCTrusted = true // synthetic ridx from buildReverseFromEntries is correct

	crc, ok := f.crcAtOffset(10)
	require.True(t, ok)
	assert.Equal(t, uint32(0xBBBB0002), crc)

	crc, ok = f.crcAtOffset(20)
	require.True(t, ok)
	assert.Equal(t, uint32(0xCCCC0003), crc)

	crc, ok = f.crcAtOffset(30)
	require.True(t, ok)
	assert.Equal(t, uint32(0xAAAA0001), crc)
}

// TestVerifyPackTrailer_Valid builds a pack whose trailing 20 bytes are the
// correct SHA-1 of the preceding content and confirms that verifyPackTrailer
// returns no error.
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

// TestVerifyPackTrailer_Corrupt writes a pack with 20 zero bytes as the
// trailer instead of a valid SHA-1 checksum and asserts that verifyPackTrailer
// returns ErrPackTrailerCorrupt.
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

// TestStore_VerifyPackTrailers opens a store containing one valid and one
// corrupt pack and confirms that the corrupt trailer is detected.
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

// createValidPackWithTrailer writes a minimal pack file (header + payload)
// with a correct SHA-1 trailer and a companion empty index file. The pack
// contains zero objects; it is used solely to test trailer verification.
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

// createCorruptPackWithTrailer writes a pack file whose trailing 20 bytes are
// all 0xFF instead of the correct SHA-1. A companion empty index is also
// created so that the store can open the pack for trailer verification.
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

// TestMidxV2CRCParsing is a forward-looking test for MIDX v2 CRC chunk
// parsing. It is currently skipped because parseMidx does not yet support
// version 2. When v2 support lands, this test validates that CRC values
// embedded in the CRCS chunk are correctly associated with each object.
func TestMidxV2CRCParsing(t *testing.T) {
	// NOTE: This test will fail until MIDX v2 support is implemented in parseMidx.
	t.Skip("MIDX v2 parsing not yet implemented")

	dir := t.TempDir()

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

	// Create MIDX v2 with CRCS chunk.
	createMidxV2WithCRCs(t, dir, "test.pack", []Hash{hash}, []uint64{12}, []uint32{expectedCRC})

	midxRA, err := mmap.Open(filepath.Join(dir, "multi-pack-index"))
	require.NoError(t, err)
	defer midxRA.Close()

	packCache := make(map[string]*mmap.ReaderAt)
	midx, err := parseMidx(dir, midxRA, packCache)
	require.NoError(t, err)

	assert.Equal(t, byte(2), midx.version, "Should parse as version 2")
	assert.Len(t, midx.entries, 1)
	assert.Equal(t, expectedCRC, midx.entries[0].crc, "Should parse CRC from CRCS chunk")
}

// createMidxV2WithCRCs creates a MIDX v2 file with CRCS chunk.
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

	// Build chunks.
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

	var crcsChunk bytes.Buffer
	for _, o := range objs {
		binary.Write(&crcsChunk, binary.BigEndian, o.crc)
	}

	type chunk struct {
		id   string
		data []byte
	}
	chunks := []chunk{
		{"OIDF", oidf.Bytes()},
		{"OIDL", oidl.Bytes()},
		{"OOFF", ooff.Bytes()},
		{"PNAM", pnam.Bytes()},
		{"CRCS", crcsChunk.Bytes()},
	}

	const headerSize = 12
	chunkTableSize := (len(chunks) + 1) * 12
	offset := headerSize + chunkTableSize

	var file bytes.Buffer

	// Header with version 2.
	file.WriteString("MIDX")
	file.WriteByte(2) // Version 2!
	file.WriteByte(1) // SHA-1
	file.WriteByte(byte(len(chunks)))
	file.WriteByte(0)
	binary.Write(&file, binary.BigEndian, uint32(1)) // pack-count

	// Chunk table.
	for _, c := range chunks {
		file.WriteString(fmt.Sprintf("%-4s", c.id)[:4])
		binary.Write(&file, binary.BigEndian, uint64(offset))
		offset += len(c.data)
	}
	// Sentinel.
	file.Write([]byte{0, 0, 0, 0})
	binary.Write(&file, binary.BigEndian, uint64(offset))

	// Payload.
	for _, c := range chunks {
		file.Write(c.data)
	}

	midxPath := filepath.Join(packDir, "multi-pack-index")
	require.NoError(t, os.WriteFile(midxPath, file.Bytes(), 0644))
}
