package objstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"bufio"

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

func TestOpen(t *testing.T) {
	t.Run("empty directory returns empty store", func(t *testing.T) {
		emptyDir := t.TempDir()
		store, err := OpenForTesting(emptyDir)
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()

		assert.Len(t, store.packs, 0)
		assert.NotNil(t, store.packMap)
		assert.Equal(t, defaultMaxDeltaDepth, store.maxDeltaDepth)
	})

	t.Run("missing idx file", func(t *testing.T) {
		dir := t.TempDir()
		packPath := filepath.Join(dir, "test.pack")

		require.NoError(t, os.WriteFile(packPath, []byte("PACK"), 0644))

		_, err := OpenForTesting(dir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mmap idx")
	})

	t.Run("invalid pack file", func(t *testing.T) {
		dir := t.TempDir()
		packPath := filepath.Join(dir, "test.pack")
		idxPath := filepath.Join(dir, "test.idx")

		require.NoError(t, os.WriteFile(packPath, []byte("invalid"), 0644))
		require.NoError(t, os.WriteFile(idxPath, []byte("invalid"), 0644))

		_, err := OpenForTesting(dir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "parse idx")
	})

	t.Run("successful open", func(t *testing.T) {
		packPath, _, cleanup := createTestPackWithDelta(t)
		defer cleanup()

		store, err := OpenForTesting(filepath.Dir(packPath))
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

		store, err := OpenForTesting(dir)
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

func TestReadVarIntFromReader(t *testing.T) {
	tests := []struct {
		data        []byte
		expected    uint64
		consumed    int
		expectError bool
	}{
		{[]byte{0x00}, 0, 1, false},
		{[]byte{0x7f}, 127, 1, false},
		{[]byte{0x80, 0x01}, 128, 2, false},
		{[]byte{0xff, 0x7f}, 16383, 2, false},
		{[]byte{0x80, 0x80, 0x01}, 16384, 3, false},
		{[]byte{}, 0, -1, true}, // empty buffer now returns error
	}

	for _, test := range tests {
		reader := bufio.NewReader(bytes.NewReader(test.data))
		value, consumed, err := readVarIntFromReader(reader)

		if test.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, test.expected, value)
			assert.Equal(t, test.consumed, consumed)
		}
	}
}

func TestStoreBasic(t *testing.T) {
	packPath, _, cleanup := createTestPackWithDelta(t)
	defer cleanup()

	packDir := filepath.Dir(packPath)

	store, err := OpenForTesting(packDir)
	require.NoError(t, err)
	defer store.Close()

	blob1Data := []byte("base content")
	blob1Hash := calculateHash(ObjBlob, blob1Data)

	data, objType, err := store.get(blob1Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType)
	assert.Equal(t, blob1Data, data)

	// Test getting the delta object (should be resolved).
	blob2Data := []byte("modified data")
	blob2Hash := calculateHash(ObjBlob, blob2Data)

	data, objType, err = store.get(blob2Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType)
	assert.Equal(t, blob2Data, data)
}

func TestCacheEviction(t *testing.T) {
	packPath, _, cleanup := createTestPackWithDelta(t)
	defer cleanup()

	store, err := OpenForTesting(filepath.Dir(packPath))
	require.NoError(t, err)
	defer store.Close()

	blob1Data := []byte("base content")
	blob1Hash := calculateHash(ObjBlob, blob1Data)
	blob2Data := []byte("modified data")
	blob2Hash := calculateHash(ObjBlob, blob2Data)

	// Test that cache stores and retrieves objects correctly.
	data1, objType1, err := store.get(blob1Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType1)
	assert.Equal(t, blob1Data, data1)

	data2, objType2, err := store.get(blob2Hash)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, objType2)
	assert.Equal(t, blob2Data, data2)

	data1Again, objType1Again, err := store.get(blob1Hash)
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

func ExampleHistoryScanner() {
	scanner, err := NewHistoryScanner("/path/to/repo/.git")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer scanner.Close()

	hash, _ := ParseHash("89e5a3e7d8f6c4b2a1e0d9c8b7a6f5e4d3c2b1a0")
	data, objType, err := scanner.get(hash)
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

	store, err := OpenForTesting(packDir)
	require.NoError(b, err)
	defer store.Close()

	require.NotEmpty(b, store.packs, "No packs found after setup - pack creation failed")
	require.NotEmpty(b, store.packs[0].oidTable, "No objects found in pack - object creation failed")

	someHash := store.packs[0].oidTable[0]

	if cacheWarm {
		b.ResetTimer()
		store.get(someHash)
	}

	for b.Loop() {
		store.get(someHash)
	}
}

func BenchmarkGetCold(b *testing.B) { benchmarkGet(b, false) }
func BenchmarkGetWarm(b *testing.B) { benchmarkGet(b, true) }

func BenchmarkReadVarIntFromReader(b *testing.B) {
	buf := []byte{0xff, 0xff, 0x7f}
	reader := bufio.NewReader(bytes.NewReader(buf))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader = bufio.NewReader(bytes.NewReader(buf))
		readVarIntFromReader(reader)
	}
}

func BenchmarkOpen(b *testing.B) {
	packDir := setupBenchmarkRepo(b)

	b.SetBytes(int64(len(packDir)))
	for b.Loop() {
		s, err := OpenForTesting(packDir)
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

	store, err := OpenForTesting(dir)
	require.NoError(t, err)
	defer store.Close()

	data, typ, err := store.get(oid)
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

func TestParseIdx_TruncatedTrailer(t *testing.T) {
	// Start with minimal valid index.
	h, _ := ParseHash("1234567890abcdef1234567890abcdef12345678")
	data := createValidIdxData(t, []Hash{h}, []uint64{100})

	// Drop the final 40 bytes (pack‑SHA + idx‑SHA).
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

func TestStore_DeltaObjectRetrieval(t *testing.T) {
	t.Run("delta objects should not cause zlib errors", func(t *testing.T) {
		packPath, _, cleanup := createTestPackWithDelta(t)
		defer cleanup()

		store, err := OpenForTesting(filepath.Dir(packPath))
		require.NoError(t, err)
		defer store.Close()

		require.NotEmpty(t, store.packs, "Pack should be loaded")
		require.NotEmpty(t, store.packs[0].oidTable, "Pack should contain objects")

		// Test each object in the pack - this will include both base and delta objects.
		for i, oid := range store.packs[0].oidTable {
			t.Run(fmt.Sprintf("object_%d_%x", i, oid[:4]), func(t *testing.T) {
				data, objType, err := store.get(oid)

				if err != nil {
					assert.NotContains(t, err.Error(), "zlib: invalid header",
						"Delta objects should not cause zlib decompression errors")
					assert.NotContains(t, err.Error(), "zlib",
						"Should not have any zlib-related errors for object %x", oid)
				}

				if err == nil {
					assert.NotEmpty(t, data, "Retrieved object data should not be empty")
					assert.NotEqual(t, ObjBad, objType, "Object type should be valid")
				}
			})
		}
	})

	t.Run("specific delta types", func(t *testing.T) {
		packPath, _, cleanup := createTestPackWithDelta(t)
		defer cleanup()

		store, err := OpenForTesting(filepath.Dir(packPath))
		require.NoError(t, err)
		defer store.Close()

		// Look for delta objects specifically by examining the pack directly.
		pack := store.packs[0]
		for i, oid := range pack.oidTable {
			offset := pack.entries[i].offset

			objType, _, err := peekObjectType(pack.pack, offset)
			require.NoError(t, err)

			if objType == ObjRefDelta || objType == ObjOfsDelta {
				t.Logf("Testing %s object %x at offset %d", objType, oid[:4], offset)

				data, retrievedType, err := store.get(oid)
				require.NoError(t, err,
					"Getting %s object should not fail with zlib error", objType)

				assert.NotEqual(t, ObjRefDelta, retrievedType, "Delta should be resolved")
				assert.NotEqual(t, ObjOfsDelta, retrievedType, "Delta should be resolved")
				assert.NotEmpty(t, data, "Resolved delta should have data")
			}
		}
	})
}
