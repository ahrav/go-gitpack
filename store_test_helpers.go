package objstore

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// createTempFileWithData creates a temporary file with the given data.
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
func createValidIdxData(t testing.TB, hashes []Hash, offsets []uint64) []byte {
	t.Helper()

	var buf bytes.Buffer

	// Write the pack index header with magic number and version.
	buf.Write([]byte{0xff, 0x74, 0x4f, 0x63})
	binary.Write(&buf, binary.BigEndian, uint32(2))

	// Sort hashes and corresponding offsets for proper index format.
	type hashOffset struct {
		hash   Hash
		offset uint64
	}

	hashOffsets := make([]hashOffset, len(hashes))
	for i, h := range hashes {
		hashOffsets[i] = hashOffset{h, offsets[i]}
	}

	// Sort entries by hash in lexicographic order as required by the index format.
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

	// Check if we need the large offset table for offsets exceeding 31-bit limit.
	needsLargeOffsets := false
	for _, offset := range sortedOffsets {
		if offset > 0x7fffffff {
			needsLargeOffsets = true
			break
		}
	}

	// Write the fanout table with cumulative object counts for each first byte value.
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

	// Write all object hashes in sorted order.
	for _, hash := range sortedHashes {
		buf.Write(hash[:])
	}

	// Write CRC32 values for each object (using dummy values since pack file may not exist).
	for range sortedHashes {
		binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
	}

	// Write object offsets, handling both regular and large offsets appropriately.
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

		// Write the large offset table for offsets that don't fit in 31 bits.
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				binary.Write(&buf, binary.BigEndian, offset)
			}
		}
	} else {
		// Write all offsets as 32-bit values since none exceed the limit.
		for _, offset := range sortedOffsets {
			binary.Write(&buf, binary.BigEndian, uint32(offset))
		}
	}

	// Write trailing checksums: packfile checksum (dummy) followed by index checksum.
	buf.Write(make([]byte, 20))

	// Compute SHA-1 checksum of the file content up to this point.
	fileContentSoFar := buf.Bytes()
	idxChecksum := sha1.Sum(fileContentSoFar)
	buf.Write(idxChecksum[:])

	return buf.Bytes()
}

func createMinimalPack(path string, content []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write pack header with signature, version, and object count.
	file.Write([]byte("PACK"))
	binary.Write(file, binary.BigEndian, uint32(2))
	binary.Write(file, binary.BigEndian, uint32(1))

	// Write object header for a blob with size encoded in the lower 4 bits.
	objHeader := byte((byte(ObjBlob) << 4) | byte(len(content)&0x0f))
	file.Write([]byte{objHeader})

	// Write zlib-compressed content.
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write(content)
	zw.Close()
	file.Write(buf.Bytes())

	return nil
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

	// Write pack header.
	packFile.Write([]byte("PACK"))
	binary.Write(packFile, binary.BigEndian, uint32(2))
	binary.Write(packFile, binary.BigEndian, uint32(2))

	offsets := make([]uint64, 2)
	hashes := []Hash{blob1Hash, blob2Hash}

	// Write first object as a base blob.
	offsets[0] = 12 // After pack header
	objHeader := byte((byte(ObjBlob) << 4) | byte(len(blob1Data)&0x0f))
	require.True(t, len(blob1Data) < 16, "Test data too large for simple header")
	packFile.Write([]byte{objHeader})

	var compressedBuf bytes.Buffer
	zw := zlib.NewWriter(&compressedBuf)
	zw.Write(blob1Data)
	zw.Close()
	packFile.Write(compressedBuf.Bytes())

	// Write second object as a reference delta.
	currentPos, err := packFile.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	offsets[1] = uint64(currentPos)

	// Create delta data.
	deltaData := createDelta(blob1Data, blob2Data)

	// For ref-delta, the compressed data contains: base hash (20 bytes) + delta instructions.
	var deltaPayload bytes.Buffer
	deltaPayload.Write(blob1Hash[:])
	deltaPayload.Write(deltaData)

	// Compress the entire delta payload (base hash + delta instructions).
	var compressedDelta bytes.Buffer
	zw = zlib.NewWriter(&compressedDelta)
	zw.Write(deltaPayload.Bytes())
	zw.Close()

	// Write object header for ref-delta with the size of compressed delta data.
	compressedSize := compressedDelta.Len()
	deltaHeader := byte((byte(ObjRefDelta) << 4) | byte(compressedSize&0x0f))
	packFile.Write([]byte{deltaHeader})

	// Write the compressed delta data.
	packFile.Write(compressedDelta.Bytes())

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

	// Use insert operations for simplicity instead of optimized copy operations.
	// In real Git, this would use copy operations where possible.
	delta.WriteByte(byte(len(target)))
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
	buf.Write([]byte{0xff, 0x74, 0x4f, 0x63})
	binary.Write(&buf, binary.BigEndian, uint32(2))

	// Sort hashes and corresponding offsets for proper index format.
	type hashOffset struct {
		hash   Hash
		offset uint64
	}

	hashOffsets := make([]hashOffset, len(hashes))
	for i, h := range hashes {
		hashOffsets[i] = hashOffset{h, offsets[i]}
	}

	// Sort by hash in lexicographic order.
	for i := range hashOffsets {
		for j := i + 1; j < len(hashOffsets); j++ {
			if bytes.Compare(hashOffsets[i].hash[:], hashOffsets[j].hash[:]) > 0 {
				hashOffsets[i], hashOffsets[j] = hashOffsets[j], hashOffsets[i]
			}
		}
	}

	// Extract sorted hashes and offsets.
	sortedHashes := make([]Hash, len(hashes))
	sortedOffsets := make([]uint64, len(offsets))
	for i, ho := range hashOffsets {
		sortedHashes[i] = ho.hash
		sortedOffsets[i] = ho.offset
	}

	// Check if we need the large offset table.
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

	// Write SHA hashes in sorted order.
	for _, h := range sortedHashes {
		buf.Write(h[:])
	}

	// Write CRC32s by computing actual values from pack data if available.
	packPath := strings.TrimSuffix(path, ".idx") + ".pack"
	packData, err := os.ReadFile(packPath)
	if err != nil {
		// Use dummy values if pack file can't be read.
		for range sortedHashes {
			binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
		}
	} else {
		for _, offset := range sortedOffsets {
			// Find the object end to determine compressed data length.
			var objEnd uint64
			objEnd = uint64(len(packData) - 20) // default to pack trailer start.
			for _, nextOffset := range sortedOffsets {
				if nextOffset > offset && nextOffset < objEnd {
					objEnd = nextOffset
				}
			}

			// Calculate CRC of the entire object data including header.
			// This matches what verifyCRC32 does: from objOff to objEnd.
			if offset < objEnd && objEnd <= uint64(len(packData)) {
				objectData := packData[offset:objEnd]
				crc := crc32.ChecksumIEEE(objectData)
				binary.Write(&buf, binary.BigEndian, crc)
			} else {
				// Use dummy value if we can't calculate.
				binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
			}
		}
	}

	// Write offsets, handling both regular and large offsets.
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

		// Write the large offset table.
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				binary.Write(&buf, binary.BigEndian, offset)
			}
		}
	} else {
		// Write all offsets as 32-bit values.
		for _, off := range sortedOffsets {
			binary.Write(&buf, binary.BigEndian, uint32(off))
		}
	}

	// Write trailing checksums.
	buf.Write(make([]byte, 20))

	fileContentSoFar := buf.Bytes()
	idxChecksum := sha1.Sum(fileContentSoFar)
	buf.Write(idxChecksum[:])

	idxFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	_, err = idxFile.Write(buf.Bytes())
	return err
}

// encodeObjHeader returns the variable-length header used by packfiles.
func encodeObjHeader(typ uint8, size uint64) []byte {
	var out []byte
	b := byte((typ&7)<<4) | byte(size&0x0F)
	size >>= 4
	for {
		if size == 0 {
			out = append(out, b)
			return out
		}
		out = append(out, b|0x80)
		b = byte(size & 0x7F)
		size >>= 7
	}
}

// buildSelfContainedDelta creates a delta payload that inserts the full blobDelta.
func buildSelfContainedDelta(blobBase, blobDelta []byte) []byte {
	var d bytes.Buffer
	writeVarInt(&d, uint64(len(blobBase)))
	writeVarInt(&d, uint64(len(blobDelta)))
	d.WriteByte(byte(len(blobDelta)))
	d.Write(blobDelta)
	return d.Bytes()
}
