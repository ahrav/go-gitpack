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

	baseData := []byte("base content")
	baseHash := calculateHash(ObjBlob, baseData)

	// The target data is what the delta should produce when applied to the base.
	targetData := []byte("modified data")
	targetHash := calculateHash(ObjBlob, targetData)

	var packBuf bytes.Buffer

	packBuf.Write([]byte("PACK"))
	binary.Write(&packBuf, binary.BigEndian, uint32(2)) // version
	binary.Write(&packBuf, binary.BigEndian, uint32(2)) // 2 objects

	// Write the base object first as a regular blob.
	baseHeader := encodeObjHeader(uint8(ObjBlob), uint64(len(baseData)))
	packBuf.Write(baseHeader)

	var compressedBase bytes.Buffer
	zw := zlib.NewWriter(&compressedBase)
	zw.Write(baseData)
	zw.Close()
	packBuf.Write(compressedBase.Bytes())

	baseOffset := uint64(12) // after pack header

	deltaOffset := uint64(packBuf.Len())

	// Use the helper function to create a properly formatted REF_DELTA object.
	refDeltaObj, err := createRefDeltaObject(baseHash, targetData, baseData)
	require.NoError(t, err)
	packBuf.Write(refDeltaObj)

	packChecksum := sha1.Sum(packBuf.Bytes())
	packBuf.Write(packChecksum[:])

	err = os.WriteFile(packPath, packBuf.Bytes(), 0644)
	require.NoError(t, err)

	err = createV2IndexFile(
		idxPath,
		[]Hash{baseHash, targetHash},
		[]uint64{baseOffset, deltaOffset},
	)
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

	// Use simple insert operations instead of optimized copy operations for testing.
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

	needsLargeOffsets := false
	for _, offset := range sortedOffsets {
		if offset > 0x7fffffff {
			needsLargeOffsets = true
			break
		}
	}

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

	for _, h := range sortedHashes {
		buf.Write(h[:])
	}

	// Compute CRC32s from pack data if available, otherwise use dummy values.
	packPath := strings.TrimSuffix(path, ".idx") + ".pack"
	packData, err := os.ReadFile(packPath)
	if err != nil {
		for range sortedHashes {
			binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
		}
	} else {
		for _, offset := range sortedOffsets {
			var objEnd uint64
			objEnd = uint64(len(packData) - 20)
			for _, nextOffset := range sortedOffsets {
				if nextOffset > offset && nextOffset < objEnd {
					objEnd = nextOffset
				}
			}

			// Calculate CRC of entire object data to match verifyCRC32 behavior.
			if offset < objEnd && objEnd <= uint64(len(packData)) {
				objectData := packData[offset:objEnd]
				crc := crc32.ChecksumIEEE(objectData)
				binary.Write(&buf, binary.BigEndian, crc)
			} else {
				binary.Write(&buf, binary.BigEndian, uint32(0x12345678))
			}
		}
	}

	// Handle both regular and large offsets.
	if needsLargeOffsets {
		largeOffsetIndex := uint32(0)
		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				binary.Write(&buf, binary.BigEndian, uint32(0x80000000|largeOffsetIndex))
				largeOffsetIndex++
			} else {
				binary.Write(&buf, binary.BigEndian, uint32(offset))
			}
		}

		for _, offset := range sortedOffsets {
			if offset > 0x7fffffff {
				binary.Write(&buf, binary.BigEndian, offset)
			}
		}
	} else {
		for _, off := range sortedOffsets {
			binary.Write(&buf, binary.BigEndian, uint32(off))
		}
	}

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

func createRefDeltaObject(baseOID Hash, targetData []byte, baseData []byte) ([]byte, error) {
	// Build the delta instructions that transform baseData into targetData.
	deltaInstructions := buildSelfContainedDelta(baseData, targetData)

	// For REF_DELTA, the size in the header is the size of the delta instructions only.
	header := encodeObjHeader(uint8(ObjRefDelta), uint64(len(deltaInstructions)))

	// Build the complete object: [header] [20-byte base OID uncompressed] [compressed delta instructions].
	var buf bytes.Buffer

	buf.Write(header)

	// The base OID is stored uncompressed (20 bytes).
	buf.Write(baseOID[:])

	// Only the delta instructions are compressed using zlib.
	var compressedDelta bytes.Buffer
	zw := zlib.NewWriter(&compressedDelta)
	if _, err := zw.Write(deltaInstructions); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}

	buf.Write(compressedDelta.Bytes())

	return buf.Bytes(), nil
}
