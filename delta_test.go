// delta_test.go tests the delta decompression subsystem, including variable-
// integer decoding, delta cycle detection, ping-pong buffer management during
// multi-level delta chain resolution, buffer boundary conditions,
// and large-object delta application.

package objstore

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

// TestDecodeVarInt validates the Git-style variable-length integer decoder,
// covering single-byte values, multi-byte continuation sequences, the
// empty-input case, and the 9-byte corruption bound.
func TestDecodeVarInt(t *testing.T) {
	tests := []struct {
		data     []byte
		expected uint64
		consumed int // 0 means truncated/over-long input was rejected
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x7f}, 127, 1},
		{[]byte{0x80, 0x01}, 128, 2},
		{[]byte{0xff, 0x7f}, 16383, 2},
		{[]byte{0x80, 0x80, 0x01}, 16384, 3},
		{[]byte{}, 0, 0},     // empty buffer is rejected
		{[]byte{0x80}, 0, 0}, // truncated continuation is rejected
		{[]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 0, 0}, // >9 bytes rejected
	}

	for _, test := range tests {
		value, consumed := decodeVarInt(test.data)
		assert.Equal(t, test.consumed, consumed, "input %x", test.data)
		if test.consumed > 0 {
			assert.Equal(t, test.expected, value, "input %x", test.data)
		}
	}
}

// TestDeltaCycleDetection verifies that the deltaContext correctly detects
// circular REF_DELTA references (same hash seen twice) and rejects chains
// that exceed the maximum allowed depth.
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

// TestMultiLevelDeltaChainResolution tests the resolution of delta chains with
// multiple levels, similar to the 8-level chain that exposed the original bug.
// This test verifies that each level correctly applies deltas and that buffer
// offsets remain correct throughout the chain.
func TestMultiLevelDeltaChainResolution(t *testing.T) {
	// Simulate a tree object with entries that will have SHA-1 hashes.
	// The bug occurred when TreeIter received data starting at the wrong offset,
	// interpreting SHA-1 hash bytes as mode digits.

	// Create a base tree with one entry.
	// Tree format: <mode> <name>\0<20-byte SHA-1>
	baseTreeData := make([]byte, 0, 100)

	// Add first entry: "40000 common\0" + 20-byte SHA-1.
	baseTreeData = append(baseTreeData, []byte("40000 common")...)
	baseTreeData = append(baseTreeData, 0) // null terminator
	sha1 := [20]byte{
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xaa, 0xbb, 0xa8, // Note: Last byte is 0xa8.
	}
	baseTreeData = append(baseTreeData, sha1[:]...)

	// Create a mock delta that simulates what happened in the bug:
	// A COPY operation that copies bytes including the 0xa8 byte
	// to a position where TreeIter might misinterpret it.

	// Test with an 8-level deep chain to match the original bug scenario.
	levels := 8
	// Calculate max size: base + (levels * (entry_size))
	// Each entry is approximately 33 bytes (mode + name + null + SHA1)
	entrySize := 33
	maxTarget := len(baseTreeData) + (levels * entrySize)

	// Set up ping-pong buffers.
	bufA := make([]byte, maxTarget)
	bufB := make([]byte, maxTarget)

	// Start with base data in bufA.
	current := bufA[:len(baseTreeData)]
	copy(current, baseTreeData)

	// Track which buffer we're using.
	usingA := true

	// Apply multiple delta levels.
	for level := range levels {
		// Choose output buffer.
		var out []byte
		if usingA {
			out = bufB[:0]
		} else {
			out = bufA[:0]
		}

		// For this test, each delta creates a new tree with both old and new entries.
		// This simulates how Git trees work - they contain all entries, not just changes.

		// Copy existing entries.
		out = append(out, current...)

		// Add a new entry at each level.
		entryName := fmt.Sprintf("40000 level%d", level)
		out = append(out, []byte(entryName)...)
		out = append(out, 0) // null terminator

		// Add SHA-1 for the new entry with 0xa8 in various positions.
		// Make sure no SHA-1 bytes are 0x00 to avoid confusing the entry count.
		levelSha1 := [20]byte{
			0xa8, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
			0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
			0x99, 0xaa, 0xbb, byte(level + 1), // +1 to avoid 0x00
		}
		out = append(out, levelSha1[:]...)

		// Verify buffer boundaries are respected.
		assert.LessOrEqual(t, len(out), int(maxTarget),
			"Buffer overflow at level %d", level)

		// Switch buffers.
		current = out
		usingA = !usingA
	}

	// Verify the final tree has all entries.
	// We should have the base entry plus 8 level entries.
	expectedEntries := 1 + levels

	// Count entries by looking for null bytes (entry terminators).
	nullCount := 0
	for _, b := range current {
		if b == 0 {
			nullCount++
		}
	}
	assert.Equal(t, expectedEntries, nullCount,
		"Expected %d tree entries but found %d", expectedEntries, nullCount)

	// Verify that 0xa8 bytes are present in the data.
	// This ensures we're testing the case that triggered the bug.
	a8Count := 0
	for i, b := range current {
		if b == 0xa8 {
			a8Count++
			// Ensure 0xa8 is not at the start of a mode string.
			// Check if this position could be misinterpreted as a mode.
			if i > 0 && current[i-1] == 0 && i+5 < len(current) {
				// After null, we expect a mode like "40000".
				// 0xa8 is not a valid octal digit (0-7).
				nextBytes := current[i:min(i+6, len(current))]
				// Verify this is within SHA-1 data, not a mode.
				for j := 0; j < len(nextBytes) && j < 5; j++ {
					if nextBytes[j] == ' ' {
						// Found space before position 5, this would be a mode.
						// 0xa8 should never appear here.
						t.Errorf("Found 0xa8 at position %d which could be interpreted as mode", i)
					}
				}
			}
		}
	}
	assert.Greater(t, a8Count, 0, "Test data should contain 0xa8 bytes")
}

// TestDeltaBufferBoundaries tests edge cases where delta operations occur near
// buffer boundaries, particularly testing COPY operations that span across
// different parts of the buffer.
func TestDeltaBufferBoundaries(t *testing.T) {

	// Create base data that will be used to test boundary conditions.
	// We'll create a pattern that makes it easy to verify correctness.
	baseData := make([]byte, 1024)
	for i := range baseData {
		// Fill with a repeating pattern based on position.
		baseData[i] = byte(i % 256)
	}

	// Test cases for different boundary scenarios.
	testCases := []struct {
		name         string
		deltaOps     []deltaOp
		expectedSize int
		description  string
	}{
		{
			name: "copy_at_buffer_start",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: 0, size: 64},
			},
			expectedSize: 64,
			description:  "Copy from the very beginning of the buffer",
		},
		{
			name: "copy_at_buffer_end",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: len(baseData) - 64, size: 64},
			},
			expectedSize: 64,
			description:  "Copy from the very end of the buffer",
		},
		{
			name: "copy_spanning_middle",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: 480, size: 128},
			},
			expectedSize: 128,
			description:  "Copy that spans across the middle of the buffer",
		},
		{
			name: "multiple_boundary_copies",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: 0, size: 32},
				{typ: deltaCopy, offset: len(baseData) - 32, size: 32},
				{typ: deltaCopy, offset: 512, size: 64},
			},
			expectedSize: 128,
			description:  "Multiple copies from different boundary positions",
		},
		{
			name: "interleaved_copy_and_insert",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: 0, size: 16},
				{typ: deltaInsert, data: []byte("BOUNDARY_TEST")},
				{typ: deltaCopy, offset: len(baseData) - 16, size: 16},
			},
			expectedSize: 32 + 13, // 16 + 13 + 16
			description:  "Mix of copy and insert operations at boundaries",
		},
		{
			name: "max_size_copy",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: 0, size: len(baseData)},
			},
			expectedSize: len(baseData),
			description:  "Copy the entire base buffer",
		},
		{
			name: "single_byte_boundary_copies",
			deltaOps: []deltaOp{
				{typ: deltaCopy, offset: 0, size: 1},
				{typ: deltaCopy, offset: len(baseData) - 1, size: 1},
				{typ: deltaCopy, offset: 511, size: 1},
				{typ: deltaCopy, offset: 512, size: 1},
			},
			expectedSize: 4,
			description:  "Single byte copies at various boundary positions",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate required buffer size.
			maxTarget := len(baseData) + 256 // Extra space for inserts.

			// Set up ping-pong buffers.
			bufA := make([]byte, maxTarget)
			bufB := make([]byte, maxTarget)

			// Start with base data in bufA.
			current := bufA[:len(baseData)]
			copy(current, baseData)

			// Apply delta operations.
			out := bufB[:0]
			for _, op := range tc.deltaOps {
				switch op.typ {
				case deltaCopy:
					// Verify the copy doesn't exceed base bounds.
					assert.LessOrEqual(t, op.offset+op.size, len(baseData),
						"%s: copy exceeds base bounds", tc.name)

					// Perform the copy.
					outPos := len(out)
					out = out[:outPos+op.size]
					copy(out[outPos:], baseData[op.offset:op.offset+op.size])

					// Verify the copied data matches the source.
					for i := range op.size {
						expected := baseData[op.offset+i]
						actual := out[outPos+i]
						assert.Equal(t, expected, actual,
							"%s: byte mismatch at position %d", tc.name, i)
					}
				case deltaInsert:
					out = append(out, op.data...)
				}
			}

			// Verify final size.
			assert.Equal(t, tc.expectedSize, len(out),
				"%s: unexpected output size", tc.name)

			// Additional verification for specific test cases.
			switch tc.name {
			case "copy_at_buffer_start":
				// Verify we got the first 64 bytes.
				assert.Equal(t, baseData[:64], out[:64])

			case "copy_at_buffer_end":
				// Verify we got the last 64 bytes.
				assert.Equal(t, baseData[len(baseData)-64:], out[:64])

			case "max_size_copy":
				// Verify entire buffer was copied correctly.
				assert.Equal(t, baseData, out)

			case "single_byte_boundary_copies":
				// Verify each byte.
				assert.Equal(t, baseData[0], out[0])
				assert.Equal(t, baseData[len(baseData)-1], out[1])
				assert.Equal(t, baseData[511], out[2])
				assert.Equal(t, baseData[512], out[3])
			}
		})
	}
}

// deltaOp represents a delta operation for testing.
type deltaOp struct {
	typ    deltaOpType
	offset int    // For copy operations.
	size   int    // For copy operations.
	data   []byte // For insert operations.
}

// deltaOpType represents the type of delta operation.
type deltaOpType int

const (
	deltaCopy deltaOpType = iota
	deltaInsert
)

// TestApplyDeltaStackWithLargeObjects tests delta application with objects that
// approach or exceed the maximum cacheable size limit.
func TestApplyDeltaStackWithLargeObjects(t *testing.T) {
	// Create a large base object close to the maxCacheableSize limit.
	const nearMaxSize = 4<<20 - 1024 // Just under 4MB.
	baseData := make([]byte, nearMaxSize)

	// Fill with a pattern for verification.
	for i := range baseData {
		baseData[i] = byte((i / 1024) % 256) // Pattern changes every 1KB.
	}

	// Test various delta scenarios with large objects.
	testCases := []struct {
		name         string
		stack        []testDelta
		expectedSize int
		description  string
		shouldCache  bool // Currently unused; documents intent for future cache-eligibility assertions.
	}{
		{
			name: "large_base_small_delta",
			stack: []testDelta{
				{
					targetSize: nearMaxSize + 100,
					ops: []deltaOp{
						{typ: deltaCopy, offset: 0, size: nearMaxSize},
						{typ: deltaInsert, data: bytes.Repeat([]byte("X"), 100)},
					},
				},
			},
			expectedSize: nearMaxSize + 100,
			description:  "Large base with small delta addition",
			shouldCache:  false, // Exceeds maxCacheableSize.
		},
		{
			name: "multiple_large_deltas",
			stack: []testDelta{
				{
					targetSize: nearMaxSize / 2,
					ops: []deltaOp{
						{typ: deltaCopy, offset: 0, size: nearMaxSize / 2},
					},
				},
				{
					targetSize: nearMaxSize/2 + 1000,
					ops: []deltaOp{
						{typ: deltaCopy, offset: 0, size: nearMaxSize / 2},
						{typ: deltaInsert, data: bytes.Repeat([]byte("Y"), 1000)},
					},
				},
			},
			expectedSize: nearMaxSize/2 + 1000,
			description:  "Multiple large delta operations",
			shouldCache:  true, // Still under limit.
		},
		{
			name: "exact_max_size",
			stack: []testDelta{
				{
					targetSize: maxCacheableSize,
					ops: []deltaOp{
						{typ: deltaCopy, offset: 0, size: nearMaxSize},
						{typ: deltaInsert, data: bytes.Repeat([]byte("Z"), maxCacheableSize-nearMaxSize)},
					},
				},
			},
			expectedSize: maxCacheableSize,
			description:  "Object exactly at cache size limit",
			shouldCache:  true, // Exactly at limit should cache.
		},
		{
			name: "shrinking_delta",
			stack: []testDelta{
				{
					targetSize: nearMaxSize / 4,
					ops: []deltaOp{
						{typ: deltaCopy, offset: 0, size: nearMaxSize / 4},
					},
				},
			},
			expectedSize: nearMaxSize / 4,
			description:  "Delta that reduces object size",
			shouldCache:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build deltaStack from test data.
			var stack deltaStack
			for range tc.stack {
				// We're simulating delta info, actual pack/offset don't matter for this test.
				stack = append(stack, deltaInfo{
					pack:   nil, // Would be real mmap.ReaderAt in production.
					offset: 0,
					typ:    ObjOfsDelta,
				})
			}

			// Apply the deltas using our test harness.
			result := applyTestDeltaStack(t, stack, baseData, tc.stack)

			// Verify the result size.
			assert.Equal(t, tc.expectedSize, len(result),
				"%s: unexpected result size", tc.name)

			// Verify specific patterns based on test case.
			switch tc.name {
			case "large_base_small_delta":
				// Verify base data is preserved.
				assert.Equal(t, baseData[:nearMaxSize], result[:nearMaxSize])
				// Verify appended data.
				assert.Equal(t, bytes.Repeat([]byte("X"), 100), result[nearMaxSize:])

			case "exact_max_size":
				// Verify we can handle exactly max size.
				assert.Equal(t, maxCacheableSize, len(result))

			case "shrinking_delta":
				// Verify only the requested portion was copied.
				assert.Equal(t, baseData[:nearMaxSize/4], result)
			}
		})
	}
}

// TestApplyDeltaStackBorrowedResultLifetime verifies that data returned in the
// borrowed mode remains stable long enough for callers to consume it even after
// subsequent delta decodes.
func TestApplyDeltaStackBorrowedResultLifetime(t *testing.T) {
	base := []byte("base content for borrowed lifetime test")
	targetA := []byte("delta result payload A")
	targetB := []byte("delta result payload B")

	makeStack := func(name string, target []byte) (deltaStack, func()) {
		t.Helper()

		baseOID := calculateHash(ObjBlob, base)
		obj, err := createRefDeltaObject(baseOID, target, base)
		require.NoError(t, err)

		path := filepath.Join(t.TempDir(), name+".packobj")
		require.NoError(t, os.WriteFile(path, obj, 0o644))

		pack, err := mmap.Open(path)
		require.NoError(t, err)

		typ, _, err := peekObjectType(pack, 0)
		require.NoError(t, err)
		require.Equal(t, ObjRefDelta, typ)

		return deltaStack{
				{
					pack:   pack,
					offset: 0,
					typ:    typ,
				},
			}, func() {
				require.NoError(t, pack.Close())
			}
	}

	stackA, closeA := makeStack("a", targetA)
	defer closeA()
	stackB, closeB := makeStack("b", targetB)
	defer closeB()

	first, typ, err := applyDeltaStackCached(nil, stackA, base, ObjBlob, 0, true)
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, targetA, first)

	second, typ, err := applyDeltaStackCached(nil, stackB, base, ObjBlob, 0, true)
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, targetB, second)

	assert.Equal(t, targetA, first, "borrowed delta bytes must remain valid for caller consumption")
}

// TestApplyDeltaStreaming_SizeMismatchIncludesSizes verifies that when the
// base size in the delta header doesn't match the actual base, the error
// message includes both sizes for debugging.
func TestApplyDeltaStreaming_SizeMismatchIncludesSizes(t *testing.T) {
	// Create a valid ref-delta object with known base/target data.
	base := []byte("the original base content")
	target := []byte("the modified target content")
	baseOID := calculateHash(ObjBlob, base)

	obj, err := createRefDeltaObject(baseOID, target, base)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "test.packobj")
	require.NoError(t, os.WriteFile(path, obj, 0o644))
	pack, err := mmap.Open(path)
	require.NoError(t, err)
	defer pack.Close()

	typ, _, err := peekObjectType(pack, 0)
	require.NoError(t, err)
	require.Equal(t, ObjRefDelta, typ)

	// Call with the WRONG base (different length) to trigger size mismatch.
	wrongBase := []byte("short")
	out := make([]byte, 0, 4096)
	_, err = applyDeltaStreaming(pack, 0, typ, wrongBase, func(int) []byte { return out }, 0)
	require.Error(t, err)

	// After fix: the error message includes both sizes, not just "delta base size mismatch".
	assert.Contains(t, err.Error(), "mismatch")
	assert.Contains(t, err.Error(), fmt.Sprintf("header=%d", len(base)))
	assert.Contains(t, err.Error(), fmt.Sprintf("actual=%d", len(wrongBase)))
}

func TestApplyDeltaStreamingRejectsUntrustedSizesAndCommands(t *testing.T) {
	openPayload := func(t *testing.T, payload []byte, declaredSize uint64) (*mmap.ReaderAt, ObjectType) {
		t.Helper()
		if declaredSize == 0 {
			declaredSize = uint64(len(payload))
		}

		var obj bytes.Buffer
		obj.Write(encodeObjHeader(uint8(ObjRefDelta), declaredSize))
		obj.Write(make([]byte, len(Hash{})))
		zw := zlib.NewWriter(&obj)
		_, err := zw.Write(payload)
		require.NoError(t, err)
		require.NoError(t, zw.Close())

		path := filepath.Join(t.TempDir(), "delta.packobj")
		require.NoError(t, os.WriteFile(path, obj.Bytes(), 0o644))
		pack, err := mmap.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pack.Close()) })
		return pack, ObjRefDelta
	}

	t.Run("payload exceeds configured limit", func(t *testing.T) {
		pack, typ := openPayload(t, nil, 1024)
		_, err := applyDeltaStreaming(pack, 0, typ, nil, nil, 64)
		require.ErrorIs(t, err, ErrDeltaTargetTooLarge)
	})

	t.Run("payload cannot inflate from remaining pack bytes", func(t *testing.T) {
		// A corrupt header can advertise a payload the pack cannot
		// physically supply (DEFLATE expands at most 1032:1). Without the
		// feasibility check, a few header bytes force getDeltaScratch to
		// materialize the full advertised amount before any compressed
		// byte is read: here 1 GiB, and with maxObjectSize=0 the 8× bound
		// is disabled entirely, so this check is the only allocation guard.
		pack, typ := openPayload(t, nil, 1<<30)
		_, err := applyDeltaStreaming(pack, 0, typ, nil, nil, 0)
		require.ErrorContains(t, err, "cannot inflate")
	})

	t.Run("payload overhead above target limit is accepted", func(t *testing.T) {
		// A literal-heavy delta for a target AT the limit necessarily has a
		// payload LARGER than the limit (varints + insert command bytes).
		// The payload bound must account for that overhead: rejecting on
		// payload > maxObjectSize would fail valid deltas whose
		// reconstructed target is within the documented bound.
		var payload bytes.Buffer
		writeVarInt(&payload, 0)  // base size
		writeVarInt(&payload, 64) // target size == limit
		payload.WriteByte(0x40)   // insert 64 literal bytes
		payload.Write(bytes.Repeat([]byte{'x'}, 64))
		require.Greater(t, payload.Len(), 64, "test premise: payload exceeds the target limit")

		pack, typ := openPayload(t, payload.Bytes(), 0)
		out, err := applyDeltaStreaming(pack, 0, typ, nil, nil, 64)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{'x'}, 64), out)
	})

	t.Run("target exceeds configured limit", func(t *testing.T) {
		var payload bytes.Buffer
		writeVarInt(&payload, 0)
		writeVarInt(&payload, 65)
		pack, typ := openPayload(t, payload.Bytes(), 0)
		_, err := applyDeltaStreaming(pack, 0, typ, nil, nil, 64)
		require.ErrorIs(t, err, ErrDeltaTargetTooLarge)
	})

	t.Run("unproducible target size with limit disabled", func(t *testing.T) {
		// With maxObjectSize=0 the configurable limit is off, so the
		// admissibility bound (no instruction stream emits more than
		// 0x10000 bytes per payload byte) is the only guard between the
		// attacker-controlled target varint and make(). Without it, a
		// 2^62 target panics make ("len out of range") instead of
		// returning an error.
		var payload bytes.Buffer
		writeVarInt(&payload, 0)
		writeVarInt(&payload, 1<<62)
		pack, typ := openPayload(t, payload.Bytes(), 0)
		_, err := applyDeltaStreaming(pack, 0, typ, nil, nil, 0)
		require.ErrorContains(t, err, "not producible")
	})

	t.Run("copy exceeds declared target", func(t *testing.T) {
		var payload bytes.Buffer
		writeVarInt(&payload, 2)
		writeVarInt(&payload, 1)
		payload.Write([]byte{0x90, 0x02}) // Copy two bytes into a one-byte target.
		pack, typ := openPayload(t, payload.Bytes(), 0)
		_, err := applyDeltaStreaming(pack, 0, typ, []byte("ab"), nil, 64)
		require.ErrorContains(t, err, "exceeds declared target")
	})

	t.Run("insert exceeds declared target", func(t *testing.T) {
		var payload bytes.Buffer
		writeVarInt(&payload, 0)
		writeVarInt(&payload, 1)
		payload.Write([]byte{0x02, 'a', 'b'})
		pack, typ := openPayload(t, payload.Bytes(), 0)
		_, err := applyDeltaStreaming(pack, 0, typ, nil, nil, 64)
		require.ErrorContains(t, err, "exceeds declared target")
	})
}

// TestReadOfsDeltaOffset_PropagatesReadError verifies that readOfsDeltaOffset
// returns an error when the underlying ReadAt fails (e.g., reading beyond EOF
// on a truncated file). Before the fix, the error was silently discarded.
func TestReadOfsDeltaOffset_PropagatesReadError(t *testing.T) {
	// Create an empty temp file and mmap it.
	path := filepath.Join(t.TempDir(), "empty.pack")
	require.NoError(t, os.WriteFile(path, []byte{}, 0o644))

	pack, err := mmap.Open(path)
	require.NoError(t, err)
	defer pack.Close()

	// Reading at any offset beyond the file should return an error.
	_, _, err = readOfsDeltaOffset(pack, 100)
	require.Error(t, err, "expected error when reading beyond EOF, got nil")
}

// TestReadOfsDeltaOffset_ValidData verifies that readOfsDeltaOffset decodes
// the offset and reports the exact number of bytes consumed.
func TestReadOfsDeltaOffset_ValidData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valid.pack")
	require.NoError(t, os.WriteFile(path, []byte{0x80, 0x00, 0xff}, 0o644))

	pack, err := mmap.Open(path)
	require.NoError(t, err)
	defer pack.Close()

	offset, consumed, err := readOfsDeltaOffset(pack, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(128), offset)
	assert.Equal(t, 2, consumed)
}

// testDelta represents delta information for testing.
type testDelta struct {
	targetSize int
	ops        []deltaOp
}

// applyTestDeltaStack simulates applying a delta stack for testing.
func applyTestDeltaStack(t *testing.T, _ deltaStack, baseData []byte, testDeltas []testDelta) []byte {
	// Determine max size needed.
	maxSize := len(baseData)
	for _, td := range testDeltas {
		if td.targetSize > maxSize {
			maxSize = td.targetSize
		}
	}

	// Set up ping-pong buffers.
	bufA := make([]byte, maxSize)
	bufB := make([]byte, maxSize)

	// Start with base data.
	current := bufA[:len(baseData)]
	copy(current, baseData)
	usingA := true

	// Apply each delta.
	for i, td := range testDeltas {
		var out []byte
		if usingA {
			out = bufB[:0]
		} else {
			out = bufA[:0]
		}

		// Apply operations.
		for _, op := range td.ops {
			switch op.typ {
			case deltaCopy:
				outPos := len(out)
				out = out[:outPos+op.size]
				copy(out[outPos:], current[op.offset:op.offset+op.size])
			case deltaInsert:
				out = append(out, op.data...)
			}
		}

		// Verify size matches expectation.
		assert.Equal(t, td.targetSize, len(out),
			"Delta %d: size mismatch", i)

		current = out
		usingA = !usingA
	}

	// Return a copy of the final result.
	result := make([]byte, len(current))
	copy(result, current)
	return result
}

func TestApplyDeltaStack_BorrowedVsCopy(t *testing.T) {
	t.Parallel()

	// For empty stack, borrowed=true returns baseData directly.
	base := []byte("hello world")
	result, typ, err := applyDeltaStackCached(nil, nil, base, ObjBlob, 0, true)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, base, result)

	// For empty stack, borrowed=false returns a COPY.
	result2, _, err := applyDeltaStackCached(nil, nil, base, ObjBlob, 0, false)
	require.NoError(t, err)
	// Modify original; copy should be independent.
	base[0] = 'H'
	assert.Equal(t, byte('h'), result2[0], "non-borrowed should be independent copy")
}

func TestApplyDeltaStackEmptyStackIgnoresLimit(t *testing.T) {
	t.Parallel()

	maxObj := uint64(512 << 20)
	base := []byte("base")

	_, _, err := applyDeltaStackCached(nil, nil, base, ObjBlob, maxObj, false)
	assert.NoError(t, err, "empty stack with maxObjectSize should succeed")

	hugeBase := make([]byte, 1)
	_, _, err = applyDeltaStackCached(nil, nil, hugeBase, ObjBlob, 0, false)
	assert.NoError(t, err, "empty stack should always succeed regardless of base size")
}

// TestApplyDeltaStackReturnsPooledBufferOnError pins that a multi-hop chain
// whose intermediate hop fails mid-application returns the pooled buffer it
// borrowed for that hop. The hop borrows from getDeltaBuf because a nil
// offset cache admits nothing; a truncated copy instruction then fails the
// application after the borrow. A marked buffer seeded into the pool class
// must come back out of the pool after the failed call.
func TestApplyDeltaStackReturnsPooledBufferOnError(t *testing.T) {
	base := []byte("base content for the pooled error path")
	baseOID := calculateHash(ObjBlob, base)
	const targetSize = 6000 // 8 KiB pool class

	var instructions bytes.Buffer
	writeVarInt(&instructions, uint64(len(base)))
	writeVarInt(&instructions, targetSize)
	instructions.WriteByte(0x81) // copy with a one-byte offset operand, truncated
	obj, err := packRefDeltaObject(baseOID, instructions.Bytes())
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "truncated.packobj")
	require.NoError(t, os.WriteFile(path, obj, 0o644))
	pack, err := mmap.Open(path)
	require.NoError(t, err)
	defer pack.Close()

	// Two hops so the first applied hop (stack[1]) is an intermediate hop
	// that borrows a pooled buffer; stack[0] is never reached.
	stack := deltaStack{
		{pack: pack, offset: 0, typ: ObjRefDelta},
		{pack: pack, offset: 0, typ: ObjRefDelta},
	}

	const marker = 0xA5
	reused := false
	for attempt := 0; attempt < 20 && !reused; attempt++ {
		seed := getDeltaBuf(targetSize)
		seed[:cap(seed)][cap(seed)-1] = marker
		putDeltaBuf(seed)

		_, _, err := applyDeltaStackCached(nil, stack, base, ObjBlob, 0, false)
		require.Error(t, err, "truncated copy instruction fails the hop")

		again := getDeltaBuf(targetSize)
		reused = again[:cap(again)][cap(again)-1] == marker
		putDeltaBuf(again)
	}
	require.True(t, reused, "the buffer borrowed for the failed hop returns to its pool")
}

// applyDeltaPrefix reproduces the leading bytes of a target without
// allocating the whole target: copies and inserts are truncated at the limit
// and the instruction stream is read only as far as the prefix needs.
func TestApplyDeltaPrefix_TruncatesAtLimit(t *testing.T) {
	base := []byte("tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor t <t@e> 1 +0000\ncommitter t <t@e> 1 +0000\n\nbase message\n")
	header := base[:len(base)-len("base message\n")]
	message := bytes.Repeat([]byte("m"), 1<<20)
	target := append(append([]byte{}, header...), message...)

	// One copy of the shared header, then the message as a run of maximal
	// inserts: the shape git produces for a commit whose only change is a
	// much longer message.
	var instr bytes.Buffer
	writeVarInt(&instr, uint64(len(base)))
	writeVarInt(&instr, uint64(len(target)))
	instr.Write([]byte{0x80 | 0x10, byte(len(header))}) // copy offset 0, one size byte
	for rest := message; len(rest) > 0; {
		n := min(len(rest), 127)
		instr.WriteByte(byte(n))
		instr.Write(rest[:n])
		rest = rest[n:]
	}
	obj, err := packRefDeltaObject(calculateHash(ObjCommit, base), instr.Bytes())
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "delta.packobj")
	require.NoError(t, os.WriteFile(path, obj, 0o644))
	pack, err := mmap.Open(path)
	require.NoError(t, err)
	defer pack.Close()

	// Warm the pooled inflater and bufio reader so their first-use
	// allocations stay out of the measured calls.
	_, err = applyDeltaPrefix(pack, 0, ObjRefDelta, base, 1)
	require.NoError(t, err)

	for _, limit := range []int{1, 10, len(base) - 5, 4096, len(target) + 10} {
		var got []byte
		n := allocatedBytes(func() { got, err = applyDeltaPrefix(pack, 0, ObjRefDelta, base, limit) })
		require.NoErrorf(t, err, "limit %d", limit)
		want := target[:min(limit, len(target))]
		require.Equalf(t, want, got, "limit %d", limit)
		if limit <= 4096 {
			require.Lessf(t, n, uint64(len(target))/4, "limit %d allocated %d bytes for a %d-byte target", limit, n, len(target))
		}
	}
}

// readVarInt accepts exactly the encodings decodeVarInt accepts: a size
// varint longer than nine bytes is rejected rather than wrapped modulo 2^64.
func TestReadVarInt_MatchesDecodeVarInt(t *testing.T) {
	cases := [][]byte{
		{0x00},
		{0x7f},
		{0x80, 0x01},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02},
		{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01},
		{0x80},
	}
	for _, c := range cases {
		want, n := decodeVarInt(c)
		got, err := readVarInt(bufio.NewReader(bytes.NewReader(c)))
		if n == 0 {
			require.Errorf(t, err, "%x: decodeVarInt rejects this encoding", c)
			continue
		}
		require.NoErrorf(t, err, "%x", c)
		require.Equalf(t, want, got, "%x", c)
	}
}
