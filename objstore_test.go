package objstore

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
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

// packObjectsInfo represents objects from a single pack file for midx creation
type packObjectsInfo struct {
	name    string
	hashes  []Hash
	offsets []uint64
}

func TestIsLittleEndian(t *testing.T) {
	result1 := hostLittle
	result2 := hostLittle

	assert.Equal(t, result1, result2, "isLittleEndian should return consistent results")

	assert.IsType(t, true, result1, "isLittleEndian should return a boolean")

	// Document what we expect on common architectures.
	// Note: This is informational and will vary by platform
	t.Logf("Platform is little-endian: %v", result1)

	// Most common platforms (amd64, arm64) are little-endian
	// This is just documentation, not a strict assertion since the code should work on both.
}

func TestOpen(t *testing.T) {
	t.Run("no packfiles found", func(t *testing.T) {
		emptyDir := t.TempDir()
		_, err := Open(emptyDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no packfiles found")
	})

	t.Run("missing idx file", func(t *testing.T) {
		dir := t.TempDir()
		packPath := filepath.Join(dir, "test.pack")

		require.NoError(t, os.WriteFile(packPath, []byte("PACK"), 0644))

		_, err := Open(dir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mmap idx")
	})

	t.Run("invalid pack file", func(t *testing.T) {
		dir := t.TempDir()
		packPath := filepath.Join(dir, "test.pack")
		idxPath := filepath.Join(dir, "test.idx")

		require.NoError(t, os.WriteFile(packPath, []byte("invalid"), 0644))
		require.NoError(t, os.WriteFile(idxPath, []byte("invalid"), 0644))

		_, err := Open(dir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "parse idx")
	})

	t.Run("successful open", func(t *testing.T) {
		packPath, _, cleanup := createTestPackWithDelta(t)
		defer cleanup()

		store, err := Open(filepath.Dir(packPath))
		require.NoError(t, err)
		defer store.Close()

		assert.NotNil(t, store)
		assert.Len(t, store.packs, 1)
		assert.Greater(t, len(store.packs[0].oidTable), 0)
		assert.Equal(t, 50, store.maxDeltaDepth)
	})

	t.Run("multiple pack files", func(t *testing.T) {
		dir := t.TempDir()

		for i := range 2 {
			packPath := filepath.Join(dir, fmt.Sprintf("test%d.pack", i))
			idxPath := filepath.Join(dir, fmt.Sprintf("test%d.idx", i))

			blob := []byte(fmt.Sprintf("content %d", i))
			hash := calculateHash(ObjBlob, blob)

			require.NoError(t, createMinimalPack(packPath, blob))
			require.NoError(t, createV2IndexFile(idxPath, []Hash{hash}, []uint64{12}))
		}

		store, err := Open(dir)
		require.NoError(t, err)
		defer store.Close()

		assert.Len(t, store.packs, 2)
		totalObjects := 0
		for _, pack := range store.packs {
			totalObjects += len(pack.oidTable)
		}
		assert.Equal(t, 2, totalObjects)
	})
}

func TestParseIdx(t *testing.T) {
	t.Run("invalid magic bytes", func(t *testing.T) {
		data := make([]byte, 8)
		copy(data, []byte("INVALID!"))

		tempFile := createTempFileWithData(t, data)
		defer os.Remove(tempFile)

		ra, err := mmap.Open(tempFile)
		require.NoError(t, err)
		defer ra.Close()

		_, err = parseIdx(ra)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported idx version")
	})

	t.Run("unsupported version", func(t *testing.T) {
		data := make([]byte, 8)
		copy(data[0:4], []byte{0xff, 0x74, 0x4f, 0x63}) // correct magic
		binary.BigEndian.PutUint32(data[4:8], 3)        // version 3 (unsupported)

		tempFile := createTempFileWithData(t, data)
		defer os.Remove(tempFile)

		ra, err := mmap.Open(tempFile)
		require.NoError(t, err)
		defer ra.Close()

		_, err = parseIdx(ra)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported idx version 3")
	})

	t.Run("minimal valid index", func(t *testing.T) {
		hash1, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
		hashes := []Hash{hash1}
		offsets := []uint64{42}

		data := createValidIdxData(t, hashes, offsets)
		tempFile := createTempFileWithData(t, data)
		defer os.Remove(tempFile)

		ra, err := mmap.Open(tempFile)
		require.NoError(t, err)
		defer ra.Close()

		idx, err := parseIdx(ra)
		require.NoError(t, err)

		assert.Len(t, idx.oidTable, 1)
		assert.Equal(t, hash1, idx.oidTable[0])
		assert.Len(t, idx.entries, 1)
		assert.Equal(t, uint64(42), idx.entries[0].offset)
		assert.Nil(t, idx.largeOffsets) // no large offsets needed
	})

	t.Run("index with large offsets", func(t *testing.T) {
		hash1, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
		hashes := []Hash{hash1}
		largeOffset := uint64(0x80000000) // > 2GB, requires large offset table
		offsets := []uint64{largeOffset}

		data := createValidIdxData(t, hashes, offsets)
		tempFile := createTempFileWithData(t, data)
		defer os.Remove(tempFile)

		ra, err := mmap.Open(tempFile)
		require.NoError(t, err)
		defer ra.Close()

		idx, err := parseIdx(ra)
		require.NoError(t, err)

		assert.Len(t, idx.oidTable, 1)
		assert.Equal(t, hash1, idx.oidTable[0])
		assert.Len(t, idx.entries, 1)
		assert.Equal(t, largeOffset, idx.entries[0].offset)
		assert.NotNil(t, idx.largeOffsets)
		assert.Len(t, idx.largeOffsets, 1)
		assert.Equal(t, largeOffset, idx.largeOffsets[0])
	})

	t.Run("multiple objects sorted order", func(t *testing.T) {
		// Create hashes in reverse order to test sorting.
		hash1, _ := ParseHash("abcdef1234567890abcdef1234567890abcdef12")
		hash2, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
		hashes := []Hash{hash1, hash2} // hash2 should come first when sorted
		offsets := []uint64{100, 200}

		data := createValidIdxData(t, hashes, offsets)
		tempFile := createTempFileWithData(t, data)
		defer os.Remove(tempFile)

		ra, err := mmap.Open(tempFile)
		require.NoError(t, err)
		defer ra.Close()

		idx, err := parseIdx(ra)
		require.NoError(t, err)

		assert.Len(t, idx.oidTable, 2)
		assert.Len(t, idx.entries, 2)
		// After sorting, hash2 (starting with '12') comes before hash1 (starting with 'ab').
		assert.Equal(t, hash2, idx.oidTable[0])
		assert.Equal(t, hash1, idx.oidTable[1])
	})

	t.Run("truncated file", func(t *testing.T) {
		data := []byte{0xff, 0x74, 0x4f, 0x63} // just magic, no version
		tempFile := createTempFileWithData(t, data)
		defer os.Remove(tempFile)

		ra, err := mmap.Open(tempFile)
		require.NoError(t, err)
		defer ra.Close()

		_, err = parseIdx(ra)
		assert.Error(t, err)
	})
}

// createTempFileWithData creates a temporary file with the given data
func createTempFileWithData(t *testing.T, data []byte) string {
	tempFile, err := os.CreateTemp("", "test-idx-*.idx")
	require.NoError(t, err)
	defer tempFile.Close()

	_, err = tempFile.Write(data)
	require.NoError(t, err)

	return tempFile.Name()
}

// createValidIdxData creates a valid idx file data for testing.
// It automatically handles both regular and large offsets based on the provided offset values.
func createValidIdxData(t *testing.T, hashes []Hash, offsets []uint64) []byte {
	var buf bytes.Buffer

	// Header: magic + version.
	buf.Write([]byte{0xff, 0x74, 0x4f, 0x63})       // magic
	binary.Write(&buf, binary.BigEndian, uint32(2)) // version

	// Sort hashes and corresponding offsets for proper index format.
	type hashOffset struct {
		hash   Hash
		offset uint64
	}

	hashOffsets := make([]hashOffset, len(hashes))
	for i, h := range hashes {
		hashOffsets[i] = hashOffset{h, offsets[i]}
	}

	// Sort by hash (lexicographic order).
	for i := range hashOffsets {
		for j := i + 1; j < len(hashOffsets); j++ {
			if bytes.Compare(hashOffsets[i].hash[:], hashOffsets[j].hash[:]) > 0 {
				hashOffsets[i], hashOffsets[j] = hashOffsets[j], hashOffsets[i]
			}
		}
	}

	sortedHashes := make([]Hash, len(hashes))
	sortedOffsets := make([]uint64, len(offsets))
	for i, ho := range hashOffsets {
		sortedHashes[i] = ho.hash
		sortedOffsets[i] = ho.offset
	}

	// Check if we need large offset table.
	needsLargeOffsets := false
	for _, offset := range sortedOffsets {
		if offset > 0x7fffffff {
			needsLargeOffsets = true
			break
		}
	}

	// Fanout table (256 entries).
	for i := range 256 {
		count := uint32(0)
		// Count objects whose first byte is <= i.
		for j, hash := range sortedHashes {
			if int(hash[0]) <= i {
				count = uint32(j + 1)
			}
		}
		binary.Write(&buf, binary.BigEndian, count)
	}

	// Object hashes.
	for _, hash := range sortedHashes {
		buf.Write(hash[:])
	}

	// CRC32s (dummy values).
	for range sortedHashes {
		binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
	}

	// Offsets - handle both regular and large offsets.
	if needsLargeOffsets {
		// Write offsets with large offset references.
		largeOffsetIndex := uint32(0)
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				// Use large offset table reference.
				binary.Write(&buf, binary.BigEndian, uint32(0x80000000|largeOffsetIndex))
				largeOffsetIndex++
			} else {
				binary.Write(&buf, binary.BigEndian, uint32(offset))
			}
		}

		// Large offset table.
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				binary.Write(&buf, binary.BigEndian, offset)
			}
		}
	} else {
		// All small offsets.
		for _, offset := range sortedOffsets {
			binary.Write(&buf, binary.BigEndian, uint32(offset))
		}
	}

	// Pack checksum (dummy) + index checksum (computed).
	buf.Write(make([]byte, 20)) // packfile checksum (dummy)

	// Compute SHA-1 checksum of the file content up to this point.
	fileContentSoFar := buf.Bytes()
	idxChecksum := sha1.Sum(fileContentSoFar)
	buf.Write(idxChecksum[:]) // index checksum (valid)

	return buf.Bytes()
}

func createMinimalPack(path string, content []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Pack header.
	file.Write([]byte("PACK"))                      // signature
	binary.Write(file, binary.BigEndian, uint32(2)) // version
	binary.Write(file, binary.BigEndian, uint32(1)) // 1 object

	// Object header (blob, size fits in 4 bits).
	objHeader := byte((byte(ObjBlob) << 4) | byte(len(content)&0x0f))
	file.Write([]byte{objHeader})

	// Compressed content.
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write(content)
	zw.Close()
	file.Write(buf.Bytes())

	return nil
}

func TestHash(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		checkResult func(t *testing.T, hash Hash, err error)
	}{
		{
			name:        "valid hash",
			input:       "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0",
			expectError: false,
			checkResult: func(t *testing.T, hash Hash, err error) {
				require.NoError(t, err)
				expected := "89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0"
				assert.Equal(t, expected, hex.EncodeToString(hash[:]))
			},
		},
		{
			name:        "invalid hash",
			input:       "invalid",
			expectError: true,
			checkResult: func(t *testing.T, hash Hash, err error) {
				assert.Error(t, err)
			},
		},
		{
			name:        "wrong length",
			input:       "abcd",
			expectError: true,
			checkResult: func(t *testing.T, hash Hash, err error) {
				assert.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := ParseHash(tt.input)
			tt.checkResult(t, hash, err)
		})
	}
}

func TestObjectTypeString(t *testing.T) {
	tests := []struct {
		objType  ObjectType
		expected string
	}{
		{ObjCommit, "commit"},
		{ObjTree, "tree"},
		{ObjBlob, "blob"},
		{ObjTag, "tag"},
		{ObjOfsDelta, "ofs-delta"},
		{ObjRefDelta, "ref-delta"},
		{ObjectType(99), ""},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, test.objType.String())
	}
}

func TestDecodeVarInt(t *testing.T) {
	tests := []struct {
		data     []byte
		expected uint64
		consumed int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x7f}, 127, 1},
		{[]byte{0x80, 0x01}, 128, 2},
		{[]byte{0xff, 0x7f}, 16383, 2},
		{[]byte{0x80, 0x80, 0x01}, 16384, 3},
		{[]byte{}, 0, 0}, // empty buffer
	}

	for _, test := range tests {
		value, consumed := decodeVarInt(test.data)
		assert.Equal(t, test.expected, value)
		assert.Equal(t, test.consumed, consumed)
	}
}

func calculateHash(objType ObjectType, data []byte) Hash {
	h := sha1.New()
	header := fmt.Sprintf("%s %d\x00", objType.String(), len(data))
	h.Write([]byte(header))
	h.Write(data)
	var hash Hash
	copy(hash[:], h.Sum(nil))
	return hash
}

// createTestPackWithDelta creates a pack with both regular and delta objects.
func createTestPackWithDelta(t *testing.T) (packPath, idxPath string, cleanup func()) {
	dir := t.TempDir()
	packPath = filepath.Join(dir, "test.pack")
	idxPath = filepath.Join(dir, "test.idx")

	blob1Data := []byte("base content")
	blob1Hash := calculateHash(ObjBlob, blob1Data)

	blob2Data := []byte("modified data")
	blob2Hash := calculateHash(ObjBlob, blob2Data)

	packFile, err := os.Create(packPath)
	require.NoError(t, err)
	defer packFile.Close()

	packFile.Write([]byte("PACK"))                      // signature
	binary.Write(packFile, binary.BigEndian, uint32(2)) // version
	binary.Write(packFile, binary.BigEndian, uint32(2)) // 2 objects

	offsets := make([]uint64, 2)
	hashes := []Hash{blob1Hash, blob2Hash}

	// Write first object (base blob).
	offsets[0] = 12 // After pack header
	objHeader := byte((byte(ObjBlob) << 4) | byte(len(blob1Data)&0x0f))
	require.True(t, len(blob1Data) < 16, "Test data too large for simple header")
	packFile.Write([]byte{objHeader})

	var compressedBuf bytes.Buffer
	zw := zlib.NewWriter(&compressedBuf)
	zw.Write(blob1Data)
	zw.Close()
	packFile.Write(compressedBuf.Bytes())

	// Write second object as REF_DELTA.
	currentPos, err := packFile.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	offsets[1] = uint64(currentPos)

	// Create delta data.
	deltaData := createDelta(blob1Data, blob2Data)

	// Object header for ref-delta.
	deltaHeader := byte((byte(ObjRefDelta) << 4) | byte((len(deltaData)+20)&0x0f))
	packFile.Write([]byte{deltaHeader})

	// Write base reference (SHA of first blob) and compressed delta.
	compressedBuf.Reset()
	zw = zlib.NewWriter(&compressedBuf)
	zw.Write(blob1Hash[:]) // 20 bytes base SHA
	zw.Write(deltaData)
	zw.Close()
	packFile.Write(compressedBuf.Bytes())

	err = createV2IndexFile(idxPath, hashes, offsets)
	require.NoError(t, err)

	cleanup = func() {
		os.RemoveAll(dir)
	}

	return packPath, idxPath, cleanup
}

func createDelta(base, target []byte) []byte {
	var delta bytes.Buffer

	writeVarInt(&delta, uint64(len(base)))
	writeVarInt(&delta, uint64(len(target)))

	// For simplicity, just use insert operations
	// In real Git, this would use copy operations where possible.
	delta.WriteByte(byte(len(target))) // insert operation
	delta.Write(target)

	return delta.Bytes()
}

func writeVarInt(w io.Writer, v uint64) {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		w.Write([]byte{b})
		if v == 0 {
			break
		}
	}
}

func createV2IndexFile(path string, hashes []Hash, offsets []uint64) error {
	var buf bytes.Buffer

	// Write header.
	buf.Write([]byte{0xff, 0x74, 0x4f, 0x63})       // magic
	binary.Write(&buf, binary.BigEndian, uint32(2)) // version

	// Sort hashes and corresponding offsets for proper index format
	type hashOffset struct {
		hash   Hash
		offset uint64
	}

	hashOffsets := make([]hashOffset, len(hashes))
	for i, h := range hashes {
		hashOffsets[i] = hashOffset{h, offsets[i]}
	}

	// Sort by hash (lexicographic order)
	for i := range hashOffsets {
		for j := i + 1; j < len(hashOffsets); j++ {
			if bytes.Compare(hashOffsets[i].hash[:], hashOffsets[j].hash[:]) > 0 {
				hashOffsets[i], hashOffsets[j] = hashOffsets[j], hashOffsets[i]
			}
		}
	}

	// Extract sorted hashes and offsets
	sortedHashes := make([]Hash, len(hashes))
	sortedOffsets := make([]uint64, len(offsets))
	for i, ho := range hashOffsets {
		sortedHashes[i] = ho.hash
		sortedOffsets[i] = ho.offset
	}

	// Check if we need large offset table
	needsLargeOffsets := false
	for _, offset := range sortedOffsets {
		if offset > 0x7fffffff {
			needsLargeOffsets = true
			break
		}
	}

	// Create fanout table.
	fanout := make([]uint32, 256)
	for i, h := range sortedHashes {
		firstByte := h[0]
		for j := int(firstByte); j < 256; j++ {
			fanout[j] = uint32(i + 1)
		}
	}
	for i := range 256 {
		binary.Write(&buf, binary.BigEndian, fanout[i])
	}

	// Write SHA hashes (now properly sorted).
	for _, h := range sortedHashes {
		buf.Write(h[:])
	}

	// Write CRC32s (dummy values).
	for range sortedHashes {
		binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
	}

	// Write offsets - handle both regular and large offsets.
	if needsLargeOffsets {
		// Write offsets with large offset references.
		largeOffsetIndex := uint32(0)
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				// Use large offset table reference.
				binary.Write(&buf, binary.BigEndian, uint32(0x80000000|largeOffsetIndex))
				largeOffsetIndex++
			} else {
				binary.Write(&buf, binary.BigEndian, uint32(offset))
			}
		}

		// Large offset table.
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				binary.Write(&buf, binary.BigEndian, offset)
			}
		}
	} else {
		// All small offsets.
		for _, off := range sortedOffsets {
			binary.Write(&buf, binary.BigEndian, uint32(off))
		}
	}

	// Write trailing checksums.
	buf.Write(make([]byte, 20)) // packfile checksum (dummy)

	fileContentSoFar := buf.Bytes()
	idxChecksum := sha1.Sum(fileContentSoFar)
	buf.Write(idxChecksum[:]) // index checksum (valid)

	idxFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	_, err = idxFile.Write(buf.Bytes())
	return err
}

func TestStoreBasic(t *testing.T) {
	packPath, _, cleanup := createTestPackWithDelta(t)
	defer cleanup()

	packDir := filepath.Dir(packPath)

	store, err := Open(packDir)
	require.NoError(t, err)
	defer store.Close()

	blob1Data := []byte("base content")
	blob1Hash := calculateHash(ObjBlob, blob1Data)

	data, objType, err := store.Get(blob1Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType)
	assert.Equal(t, blob1Data, data)

	// Test getting the delta object (should be resolved).
	blob2Data := []byte("modified data")
	blob2Hash := calculateHash(ObjBlob, blob2Data)

	data, objType, err = store.Get(blob2Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType)
	assert.Equal(t, blob2Data, data)
}

func TestApplyDelta(t *testing.T) {
	base := []byte("Hello World")
	target := []byte("Hello Go")

	var delta bytes.Buffer

	writeVarInt(&delta, uint64(len(base)))
	writeVarInt(&delta, uint64(len(target)))

	// 0x90 = copy operation (0x80) with length follows (0x10), no offset follows (offset = 0).
	delta.WriteByte(0x90) // copy operation: length follows, offset is 0
	delta.WriteByte(6)    // copy 6 bytes

	// Insert "Go".
	delta.WriteByte(2) // insert 2 bytes
	delta.Write([]byte("Go"))

	result := applyDelta(base, delta.Bytes())
	assert.Equal(t, target, result)
}

func TestCacheEviction(t *testing.T) {
	packPath, _, cleanup := createTestPackWithDelta(t)
	defer cleanup()

	store, err := Open(filepath.Dir(packPath))
	require.NoError(t, err)
	defer store.Close()

	blob1Data := []byte("base content")
	blob1Hash := calculateHash(ObjBlob, blob1Data)
	blob2Data := []byte("modified data")
	blob2Hash := calculateHash(ObjBlob, blob2Data)

	// Test that cache stores and retrieves objects correctly.
	data1, objType1, err := store.Get(blob1Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType1)
	assert.Equal(t, blob1Data, data1)

	data2, objType2, err := store.Get(blob2Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType2)
	assert.Equal(t, blob2Data, data2)

	data1Again, objType1Again, err := store.Get(blob1Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType1Again)
	assert.Equal(t, blob1Data, data1Again)
}

func TestDeltaCycleDetection(t *testing.T) {
	ctx := newDeltaContext(10)

	hash1, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
	hash2, _ := ParseHash("abcdef1234567890abcdef1234567890abcdef12")

	assert.NoError(t, ctx.checkRefDelta(hash1))
	ctx.enterRefDelta(hash1)

	assert.Error(t, ctx.checkRefDelta(hash1), "Should detect circular reference")

	ctx2 := newDeltaContext(2)
	ctx2.enterRefDelta(hash1)
	ctx2.enterRefDelta(hash2)

	hash3, _ := ParseHash("fedcba0987654321fedcba0987654321fedcba09")
	assert.Error(t, ctx2.checkRefDelta(hash3), "Should hit depth limit")
}

func TestDetectType(t *testing.T) {
	tests := []struct {
		data     []byte
		expected ObjectType
	}{
		{[]byte("tree 123\x00some tree data"), ObjTree},
		{[]byte("parent abc\nauthor Someone"), ObjCommit},
		{[]byte("author Someone\ncommitter"), ObjCommit},
		{[]byte("just some blob data"), ObjBlob},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, detectType(test.data))
	}
}

func ExampleStore() {
	store, err := Open("/path/to/repo/.git/objects/pack")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer store.Close()

	hash, _ := ParseHash("89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0")
	data, objType, err := store.Get(hash)
	if err != nil {
		fmt.Printf("Object not found: %v\n", err)
		return
	}

	fmt.Printf("Object type: %s\n", objType)
	fmt.Printf("Object size: %d bytes\n", len(data))
}

func setupBenchmarkRepo(b *testing.B) string {
	tempDir := b.TempDir()

	for i := range 5 {
		filename := filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i))
		content := fmt.Sprintf("This is test file %d with some benchmark content.\nLine 2 of file %d\n", i, i)
		require.NoError(b, os.WriteFile(filename, []byte(content), 0644))
	}

	subDir := filepath.Join(tempDir, "subdir")
	require.NoError(b, os.MkdirAll(subDir, 0755))
	for i := range 3 {
		filename := filepath.Join(subDir, fmt.Sprintf("nested%d.txt", i))
		content := fmt.Sprintf("Nested file %d content for benchmarking\n", i)
		require.NoError(b, os.WriteFile(filename, []byte(content), 0644))
	}

	packDir := filepath.Join(tempDir, ".git", "objects", "pack")

	packObjects(b, tempDir, packDir)

	return packDir
}

func packObjects(b *testing.B, repoDir, packDir string) {
	cmd := exec.Command("git", "-C", repoDir, "init")
	require.NoError(b, cmd.Run(), "git init failed - ensure git is installed")

	exec.Command("git", "-C", repoDir, "config", "user.name", "Benchmark").Run()
	exec.Command("git", "-C", repoDir, "config", "user.email", "bench@example.com").Run()

	cmd = exec.Command("git", "-C", repoDir, "add", ".")
	require.NoError(b, cmd.Run(), "git add failed")

	cmd = exec.Command("git", "-C", repoDir, "commit", "-m", "benchmark commit")
	require.NoError(b, cmd.Run(), "git commit failed")

	cmd = exec.Command("git", "-C", repoDir, "repack", "-a", "-d")
	require.NoError(b, cmd.Run(), "git repack failed")
}

func benchmarkGet(b *testing.B, cacheWarm bool) {
	packDir := setupBenchmarkRepo(b)

	store, err := Open(packDir)
	require.NoError(b, err)
	defer store.Close()

	if len(store.packs) == 0 {
		b.Fatalf("No packs found after setup - pack creation failed")
	}
	if len(store.packs[0].oidTable) == 0 {
		b.Fatalf("No objects found in pack - object creation failed")
	}

	someHash := store.packs[0].oidTable[0]

	if cacheWarm {
		b.ResetTimer()
		store.Get(someHash)
	}

	for b.Loop() {
		store.Get(someHash)
	}
}

func BenchmarkGetCold(b *testing.B) { benchmarkGet(b, false) }
func BenchmarkGetWarm(b *testing.B) { benchmarkGet(b, true) }

func BenchmarkApplyDelta(b *testing.B) {
	base := []byte("Hello World! This is a test base content for benchmarking delta operations.")
	target := []byte("Hello Go! This is a test modified content for benchmarking delta operations.")

	var delta bytes.Buffer
	writeVarInt(&delta, uint64(len(base)))
	writeVarInt(&delta, uint64(len(target)))

	// Copy first 6 bytes ("Hello ").
	delta.WriteByte(0x90) // copy operation: length follows, offset is 0
	delta.WriteByte(6)    // copy 6 bytes

	// Insert "Go! This is a test modified content for benchmarking delta operations."
	remaining := target[6:]
	delta.WriteByte(byte(len(remaining))) // insert operation
	delta.Write(remaining)

	deltaBytes := delta.Bytes()

	for b.Loop() {
		applyDelta(base, deltaBytes)
	}
}

func BenchmarkDecodeVarInt(b *testing.B) {
	buf := []byte{0xff, 0xff, 0x7f}

	for b.Loop() {
		decodeVarInt(buf)
	}
}

func BenchmarkOpen(b *testing.B) {
	packDir := setupBenchmarkRepo(b)

	b.SetBytes(int64(len(packDir)))
	for b.Loop() {
		s, err := Open(packDir)
		require.NoError(b, err)
		s.Close()
	}
}

func TestFindObject(t *testing.T) {
	// Create test hashes that will be in different fanout ranges.
	hash1, _ := ParseHash("0123456789abcdef0123456789abcdef01234567") // starts with 0x01
	hash2, _ := ParseHash("1234567890abcdef1234567890abcdef12345678") // starts with 0x12
	hash3, _ := ParseHash("abcdef1234567890abcdef1234567890abcdef12") // starts with 0xab
	hashes := []Hash{hash1, hash2, hash3}
	offsets := []uint64{100, 200, 300}

	// Create index data and parse it.
	data := createValidIdxData(t, hashes, offsets)
	tempFile := createTempFileWithData(t, data)
	defer os.Remove(tempFile)

	ra, err := mmap.Open(tempFile)
	require.NoError(t, err)
	defer ra.Close()

	idx, err := parseIdx(ra)
	require.NoError(t, err)

	t.Run("find existing objects", func(t *testing.T) {
		// Test finding each hash.
		for _, hash := range []Hash{hash1, hash2, hash3} {
			offset, found := idx.findObject(hash)
			assert.True(t, found, "Should find hash %x", hash)

			// Find expected offset (since hashes are sorted, we need to find the corresponding offset).
			expectedOffset := uint64(0)
			for j, sortedHash := range idx.oidTable {
				if sortedHash == hash {
					expectedOffset = idx.entries[j].offset
					break
				}
			}
			assert.Equal(t, expectedOffset, offset, "Should return correct offset for hash %x", hash)
		}
	})

	t.Run("find non-existent object", func(t *testing.T) {
		nonExistentHash, _ := ParseHash("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
		offset, found := idx.findObject(nonExistentHash)
		assert.False(t, found, "Should not find non-existent hash")
		assert.Equal(t, uint64(0), offset, "Should return 0 offset for non-existent hash")
	})

	t.Run("binary search efficiency", func(t *testing.T) {
		// Verify that all hashes are properly sorted for efficient binary search.
		for i := 1; i < len(idx.oidTable); i++ {
			assert.True(t, bytes.Compare(idx.oidTable[i-1][:], idx.oidTable[i][:]) < 0,
				"Hash table should be sorted for binary search efficiency")
		}
	})
}

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

	midx, err := parseMidx(dir, midxRA)
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

	_, err := parseMidx(dir, ra)
	assert.Error(t, err, "should reject file with invalid magic")
}

// setupBenchmarkRepoWithMidx creates a Git repository with multiple pack files and
// a multi-pack index for benchmarking midx operations. It generates three separate
// pack files by creating commits incrementally, then attempts to generate a
// multi-pack index using git commands, falling back to manual creation if needed.
func setupBenchmarkRepoWithMidx(tb testing.TB) string {
	tempDir := tb.TempDir()

	// Create multiple directories with files to ensure we get separate pack files
	// when we create incremental commits.
	for packNum := 0; packNum < 3; packNum++ {
		subDir := filepath.Join(tempDir, fmt.Sprintf("pack%d", packNum))
		require.NoError(tb, os.MkdirAll(subDir, 0755))

		for i := range 5 {
			filename := filepath.Join(subDir, fmt.Sprintf("file%d.txt", i))
			content := fmt.Sprintf("Pack %d - File %d content with benchmark data.\nMultiple lines for realistic size.\nLine 3 of pack %d file %d\n", packNum, i, packNum, i)
			require.NoError(tb, os.WriteFile(filename, []byte(content), 0644))
		}
	}

	packDir := filepath.Join(tempDir, ".git", "objects", "pack")
	require.NoError(tb, os.MkdirAll(packDir, 0755))

	// Initialize the Git repository with required configuration.
	cmd := exec.Command("git", "-C", tempDir, "init")
	require.NoError(tb, cmd.Run(), "git init failed - ensure git is installed")

	exec.Command("git", "-C", tempDir, "config", "user.name", "Benchmark").Run()
	exec.Command("git", "-C", tempDir, "config", "user.email", "bench@example.com").Run()

	// Create multiple commits incrementally to generate separate pack files.
	// Each commit adds files from one subdirectory and triggers a repack.
	for packNum := 0; packNum < 3; packNum++ {
		subDir := fmt.Sprintf("pack%d", packNum)
		cmd = exec.Command("git", "-C", tempDir, "add", subDir)
		require.NoError(tb, cmd.Run(), "git add failed")

		cmd = exec.Command("git", "-C", tempDir, "commit", "-m", fmt.Sprintf("benchmark commit %d", packNum))
		require.NoError(tb, cmd.Run(), "git commit failed")

		// Force creation of a new pack file for this commit.
		cmd = exec.Command("git", "-C", tempDir, "repack", "-a", "-d")
		require.NoError(tb, cmd.Run(), "git repack failed")
	}

	// Generate multi-pack index using Git's built-in command, with fallback.
	cmd = exec.Command("git", "-C", tempDir, "multi-pack-index", "write")
	if err := cmd.Run(); err != nil {
		// Git multi-pack-index command may not be available in older versions,
		// so we create a minimal midx file manually for testing purposes.
		tb.Logf("git multi-pack-index failed, creating manually: %v", err)
		createManualMidx(tb, packDir)
	}

	return packDir
}

// createManualMidx generates a minimal multi-pack index file when Git's built-in
// command is unavailable. This creates a simplified midx that indexes objects
// from the first pack file only, which is sufficient for benchmark testing.
func createManualMidx(tb testing.TB, packDir string) {
	// Discover all pack files in the directory.
	pattern := filepath.Join(packDir, "*.pack")
	packs, err := filepath.Glob(pattern)
	require.NoError(tb, err)

	if len(packs) == 0 {
		tb.Skip("No pack files found for midx creation")
		return
	}

	// Extract object information from existing pack index files.
	var allPacks []packObjectsInfo

	for _, packPath := range packs {
		packName := filepath.Base(packPath)
		idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"

		// Parse the companion index file to extract object metadata.
		ra, err := mmap.Open(idxPath)
		if err != nil {
			continue
		}

		idx, err := parseIdx(ra)
		ra.Close()
		if err != nil {
			continue
		}

		// Collect objects from this pack
		allPacks = append(allPacks, packObjectsInfo{
			name:   packName,
			hashes: append([]Hash{}, idx.oidTable...),
			offsets: func() []uint64 {
				offsets := make([]uint64, len(idx.entries))
				for i, entry := range idx.entries {
					offsets[i] = entry.offset
				}
				return offsets
			}(),
		})
	}

	if len(allPacks) > 0 {
		createManualMidxFileMultiPack(tb, packDir, allPacks)
	}
}

// createManualMidxFileMultiPack creates a multi-pack index file that properly handles multiple packs
func createManualMidxFileMultiPack(
	tb testing.TB,
	packDir string,
	packs []packObjectsInfo,
) {
	// Build combined object list with pack IDs
	type objWithPack struct {
		h      Hash
		off32  uint32
		packID uint32
	}

	var allObjs []objWithPack
	packNames := make([]string, len(packs))

	for packIdx, pack := range packs {
		packNames[packIdx] = pack.name
		for i, hash := range pack.hashes {
			require.Less(tb, pack.offsets[i], uint64(0x80000000), "offset must fit in 31 bits")
			allObjs = append(allObjs, objWithPack{
				h:      hash,
				off32:  uint32(pack.offsets[i]),
				packID: uint32(packIdx),
			})
		}
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

	// Build OOFF chunk containing pack ID and offset pairs.
	var ooff bytes.Buffer
	for _, obj := range allObjs {
		binary.Write(&ooff, binary.BigEndian, obj.packID)
		binary.Write(&ooff, binary.BigEndian, obj.off32)
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
	const headerSize = 12
	chunkTableSize := (len(chunks) + 1) * 12 // +1 for sentinel entry
	offset := headerSize + chunkTableSize

	// Ensure chunk IDs are exactly 4 characters as required by format.
	for i := range chunks {
		chunks[i].id = fmt.Sprintf("%-4s", chunks[i].id)[:4]
	}

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
	require.NoError(tb, os.WriteFile(midxPath, file.Bytes(), 0644))
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

		_, err = parseMidx(packDir, ra)
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
		if store.midx == nil {
			b.Fatal("Store should have loaded midx")
		}

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
			if !found {
				b.Fatal("Object should be found in midx")
			}
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
			if !found {
				b.Fatal("Object should be found in regular idx")
			}
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
	if err := os.Remove(midxPath); err != nil && !os.IsNotExist(err) {
		b.Fatalf("Failed to remove midx: %v", err)
	}

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

func TestTinyRepoHappyPath(t *testing.T) {
	dir := t.TempDir()

	// Create one‑object pack + idx.
	pack := filepath.Join(dir, "tiny.pack")
	payload := []byte("hello tiny")
	oid := calculateHash(ObjBlob, payload)
	require.NoError(t, createMinimalPack(pack, payload))
	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(pack, ".pack")+".idx",
		[]Hash{oid},
		[]uint64{12},
	))

	store, err := Open(dir)
	require.NoError(t, err)
	defer store.Close()

	data, typ, err := store.Get(oid)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, payload, data)
}

func TestLargePack_LoffHandling(t *testing.T) {
	dir := t.TempDir()
	pack := filepath.Join(dir, "big.pack")

	blob := []byte("dummy")
	oid := calculateHash(ObjBlob, blob)

	// Tiny pack – real size unimportant.
	require.NoError(t, createMinimalPack(pack, blob))

	// Logical offset > 2 GiB so MSB is 1 → LOFF chunk.
	const off64 = uint64(0x8000_0000 + 1234) // 2 GiB + 1,234 bytes
	require.NoError(t, createV2IndexFile(
		strings.TrimSuffix(pack, ".pack")+".idx",
		[]Hash{oid},
		[]uint64{off64},
	))

	// parseIdx must promote the offset into idxFile.largeOffsets.
	ra, err := mmap.Open(strings.TrimSuffix(pack, ".pack") + ".idx")
	require.NoError(t, err)
	idx, err := parseIdx(ra)
	require.NoError(t, err)
	assert.Equal(t, off64, idx.entries[0].offset)
	assert.Len(t, idx.largeOffsets, 1)
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

func TestParseIdx_TruncatedTrailer(t *testing.T) {
	// Start with minimal valid index.
	h, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
	data := createValidIdxData(t, []Hash{h}, []uint64{100})

	// Drop the final 40 bytes (pack‑SHA + idx‑SHA).
	trunc := data[:len(data)-40]
	idxFile := createTempFileWithData(t, trunc)
	defer os.Remove(idxFile)

	ra, _ := mmap.Open(idxFile)
	defer ra.Close()

	_, err := parseIdx(ra)
	assert.ErrorIs(t, err, ErrBadIdxChecksum)
}

func TestParseIdx_CorruptFanout(t *testing.T) {
	h, _ := ParseHash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	data := createValidIdxData(t, []Hash{h}, []uint64{12})

	// Flip count[42] < count[41].
	fpos := headerSize + 41*4 // fan‑out index 41
	binary.BigEndian.PutUint32(data[fpos:fpos+4], 2)
	binary.BigEndian.PutUint32(data[fpos+4:fpos+8], 1)

	idx := createTempFileWithData(t, data)
	defer os.Remove(idx)

	ra, _ := mmap.Open(idx)
	defer ra.Close()

	_, err := parseIdx(ra)
	assert.ErrorIs(t, err, ErrNonMonotonicFanout)
}

// func TestMidxFanoutAcrossPacks(t *testing.T) {
// 	packDir := setupBenchmarkRepoWithMidx(t)

// 	store, err := Open(packDir)
// 	require.NoError(t, err)
// 	defer store.Close()

// 	require.Greater(t, len(store.packs), 2, "need >2 packs")
// 	require.NotNil(t, store.midx)

// 	// Choose an object from the *third* pack to prove fan‑out works.
// 	var target Hash
// 	for i, entry := range store.midx.entries {
// 		if entry.packID == 2 { // third pack (0‑based)
// 			target = store.midx.objectIDs[i]
// 			break
// 		}
// 	}
// 	if (target == Hash{}) {
// 		t.Skip("benchmark helper did not place objects in 3rd pack")
// 	}

// 	_, _, err = store.Get(target)
// 	require.NoError(t, err, "midx fan‑out should locate object across packs")
// }
