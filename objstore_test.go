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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	idxFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	// Write header.
	idxFile.Write([]byte{0xff, 0x74, 0x4f, 0x63})      // magic
	binary.Write(idxFile, binary.BigEndian, uint32(2)) // version

	// Create fanout table.
	fanout := make([]uint32, 256)
	for i, h := range hashes {
		firstByte := h[0]
		for j := int(firstByte); j < 256; j++ {
			fanout[j] = uint32(i + 1)
		}
	}
	for i := range 256 {
		binary.Write(idxFile, binary.BigEndian, fanout[i])
	}

	// Write SHA hashes (must be sorted).
	for _, h := range hashes {
		idxFile.Write(h[:])
	}

	// Write CRC32s (dummy values).
	for range hashes {
		binary.Write(idxFile, binary.BigEndian, uint32(0x12345678))
	}

	// Write offsets.
	for _, off := range offsets {
		if off > 0x7fffffff {
			return fmt.Errorf("offset too large for test")
		}
		binary.Write(idxFile, binary.BigEndian, uint32(off))
	}

	// Write trailing checksums (dummy).
	idxFile.Write(make([]byte, 20)) // packfile checksum
	idxFile.Write(make([]byte, 20)) // index checksum

	return nil
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

	store.SetMaxCacheSize(1)

	blob1Data := []byte("base content")
	blob1Hash := calculateHash(ObjBlob, blob1Data)
	blob2Data := []byte("modified data")
	blob2Hash := calculateHash(ObjBlob, blob2Data)

	store.Get(blob1Hash)
	store.Get(blob2Hash)

	store.mu.Lock()
	cacheSize := len(store.cache)
	store.mu.Unlock()

	assert.LessOrEqual(t, cacheSize, 1)
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
		store.Get(someHash)
	}

	for b.Loop() {
		store.Get(someHash)
	}
}

func BenchmarkGetCold(b *testing.B) { benchmarkGet(b, false) }
func BenchmarkGetWarm(b *testing.B) { benchmarkGet(b, true) }

func BenchmarkApplyDelta(b *testing.B) {
	base := make([]byte, 8<<10)
	delta := make([]byte, 4<<10)

	for b.Loop() {
		applyDelta(base, delta)
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

	for b.Loop() {
		s, err := Open(packDir)
		require.NoError(b, err)
		s.Close()
	}
}
