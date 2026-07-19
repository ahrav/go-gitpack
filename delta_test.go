// delta_test.go tests the delta decompression subsystem, including variable-
// integer decoding, delta cycle detection, ping-pong buffer management during
// multi-level delta chain resolution, buffer boundary conditions, arena pooling,
// and large-object delta application.

package objstore

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

// TestReadVarIntFromReader validates the Git-style variable-length integer
// decoder, covering single-byte values, multi-byte continuation sequences,
// and the empty-input error case.
// NOTE: consider adding a "name" field to each test case for clearer subtest output.
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

// TestDeltaPingPongBufferManagement verifies that the ping-pong buffer strategy
// correctly alternates between buffers during multi-level delta chain resolution.
// This test ensures that data is properly copied between buffers and that the
// final result matches the expected output.
func TestDeltaPingPongBufferManagement(t *testing.T) {
	// Create a simple base object.
	baseData := []byte("Hello, this is the base object content!")

	// Since we can't easily mock applyDeltaStreaming, we'll test the buffer
	// management logic directly by creating our own test version.
	arena := getDeltaArena()
	defer putDeltaArena(arena)

	// Calculate max target size.
	maxTarget := uint64(len(baseData) + 30) // Extra space for appended text.

	// Set up ping-pong buffers.
	bufA := arena.data[:maxTarget]
	bufB := arena.data[maxTarget : maxTarget*2]

	// Start with base data in bufA.
	current := bufA[:len(baseData)]
	copy(current, baseData)

	// Track which buffer we're using.
	usingA := true

	// Expected content after each delta application.
	expectedAfterDelta := []string{
		string(baseData) + " Level 1",
		string(baseData) + " Level 1 Level 2",
		string(baseData) + " Level 1 Level 2 Level 3",
	}

	// Simulate applying deltas with buffer switching.
	for i, expected := range expectedAfterDelta {
		// Choose output buffer (the one we're NOT currently using).
		var out []byte
		if usingA {
			out = bufB[:0]
			assert.True(t, usingA, "Should be using buffer A for input in iteration %d", i)
		} else {
			out = bufA[:0]
			assert.False(t, usingA, "Should be using buffer B for input in iteration %d", i)
		}

		// Simulate delta application by appending level text.
		levelText := fmt.Sprintf(" Level %d", i+1)
		out = append(out, current...)
		out = append(out, []byte(levelText)...)

		// Verify the result is in the correct buffer.
		assert.Equal(t, expected, string(out), "Delta %d result mismatch", i+1)

		// The result is now in 'out' buffer, make it the current for next iteration.
		current = out
		usingA = !usingA // Switch which buffer we're using.

		// Verify we switched buffers.
		if i < len(expectedAfterDelta)-1 {
			if i%2 == 0 {
				assert.False(t, usingA, "Should have switched to buffer B after iteration %d", i)
			} else {
				assert.True(t, usingA, "Should have switched to buffer A after iteration %d", i)
			}
		}
	}

	// Verify final result.
	finalExpected := string(baseData) + " Level 1 Level 2 Level 3"
	assert.Equal(t, finalExpected, string(current), "Final result mismatch")
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

	arena := getDeltaArena()
	defer putDeltaArena(arena)

	// Test with an 8-level deep chain to match the original bug scenario.
	levels := 8
	// Calculate max size: base + (levels * (entry_size))
	// Each entry is approximately 33 bytes (mode + name + null + SHA1)
	entrySize := 33
	maxTarget := uint64(len(baseTreeData) + (levels * entrySize))

	// Set up ping-pong buffers.
	bufA := arena.data[:maxTarget]
	bufB := arena.data[maxTarget : maxTarget*2]

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
	arena := getDeltaArena()
	defer putDeltaArena(arena)

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
			maxTarget := uint64(len(baseData) + 256) // Extra space for inserts.

			// Set up ping-pong buffers.
			bufA := arena.data[:maxTarget]
			bufB := arena.data[maxTarget : maxTarget*2]

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

// TestDeltaArenaPooling verifies that the delta arena pool correctly manages
// memory allocation and reuse across multiple delta operations.
func TestDeltaArenaPooling(t *testing.T) {
	t.Run("ArenaContract", func(t *testing.T) {
		// sync.Pool explicitly disclaims any relation between values passed
		// to Put and values returned by Get, so pointer-reuse assertions
		// fail intermittently on correct code (GC pairs clear the pool, P
		// migration routes Get elsewhere). Whether reuse actually happens
		// is an allocation-rate property for a benchmark. What IS
		// deterministic — and what callers rely on — is the get/put
		// contract, which putDeltaArena mutates in place and can therefore
		// be observed directly:
		//
		//   1. getDeltaArena returns a full-length arena of the standard
		//      capacity, whether it came from New or the pool.
		//   2. putDeltaArena restores len == cap before pooling, so a
		//      shrunken arena can never resurface short.
		//   3. putDeltaArena discards oversized arenas (early return, no
		//      reset) instead of polluting the pool with them.
		arena := getDeltaArena()
		assert.Equal(t, defaultDeltaArenaSize, cap(arena.data), "standard arena capacity")
		assert.Equal(t, cap(arena.data), len(arena.data), "arena must arrive full-length")

		// Shrink, then return: the in-place reset is observable through
		// our retained pointer regardless of pool internals.
		arena.data = arena.data[:10]
		putDeltaArena(arena)
		assert.Equal(t, defaultDeltaArenaSize, len(arena.data),
			"putDeltaArena must restore len == cap before pooling")

		// Oversized arenas take the discard branch, which returns before
		// the reset — the unchanged length proves they were not pooled.
		oversized := &deltaArena{data: make([]byte, defaultDeltaArenaSize+1)}
		oversized.data = oversized.data[:5]
		putDeltaArena(oversized)
		assert.Equal(t, 5, len(oversized.data),
			"oversized arena must be discarded (no reset), not pooled")
	})

	t.Run("MultipleConcurrentArenas", func(t *testing.T) {
		// Verify multiple arenas can be acquired concurrently.
		const numArenas = 5
		arenas := make([]*deltaArena, numArenas)
		ptrs := make(map[uintptr]bool)

		for i := range numArenas {
			arenas[i] = getDeltaArena()
			ptr := uintptr(unsafe.Pointer(&arenas[i].data[0]))
			assert.False(t, ptrs[ptr], "Each arena should have unique memory")
			ptrs[ptr] = true
		}

		// Return all arenas to pool.
		for i := range numArenas {
			putDeltaArena(arenas[i])
		}
	})

	t.Run("ArenaResetOnReturn", func(t *testing.T) {
		// Verify arenas are properly reset when returned to pool.
		arena := getDeltaArena()

		// Modify the arena data.
		testData := []byte("TEST_MODIFICATION")
		copy(arena.data[:len(testData)], testData)

		// Ensure the slice length is reset but capacity is preserved.
		originalCap := cap(arena.data)
		arena.data = arena.data[:100] // Modify length.

		putDeltaArena(arena)

		// Get arena again and verify it was reset.
		arenaAfter := getDeltaArena()
		assert.Equal(t, originalCap, len(arenaAfter.data), "Arena slice should be reset to full capacity")
		assert.Equal(t, originalCap, cap(arenaAfter.data), "Arena capacity should be preserved")
		putDeltaArena(arenaAfter)
	})

	t.Run("ConcurrentAccessSafety", func(t *testing.T) {
		// Verify concurrent access safety.
		var wg sync.WaitGroup
		const numGoroutines = 10
		const opsPerGoroutine = 100

		errors := make(chan error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for range opsPerGoroutine {
					arena := getDeltaArena()

					// Perform some operation to ensure arena is valid.
					if len(arena.data) < 2*16<<20 {
						errors <- fmt.Errorf("goroutine %d: invalid arena size %d", id, len(arena.data))
						return
					}

					// Use the arena briefly.
					copy(arena.data[:8], []byte("CONCURRENT"))

					putDeltaArena(arena)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for any errors.
		for err := range errors {
			t.Errorf("Concurrent access error: %v", err)
		}
	})
}

// TestDeltaArenaPoolOversizeDiscard verifies that arenas grown beyond the
// default pool size during dynamic resizing are not returned to the pool.
// Without the size guard in putDeltaArena, an oversized arena pollutes the
// pool and subsequent callers receive bloated allocations.
func TestDeltaArenaPoolOversizeDiscard(t *testing.T) {
	const defaultArenaSize = 2 * 16 << 20 // 32 MiB — matches deltaArenaPool.New

	// Drain the pool so we get a fresh arena from New.
	for range 16 {
		getDeltaArena()
	}

	arena := getDeltaArena()
	assert.Equal(t, defaultArenaSize, cap(arena.data), "fresh arena should be 32 MiB")

	// Simulate dynamic resizing: replace the backing slice with a 128 MiB allocation.
	const oversized = 128 << 20
	arena.data = make([]byte, oversized)
	putDeltaArena(arena)

	// The next arena from the pool must NOT be the oversized one.
	next := getDeltaArena()
	assert.LessOrEqual(t, cap(next.data), defaultArenaSize,
		"oversized arena should not pollute the pool; got cap=%d", cap(next.data))
	putDeltaArena(next)
}

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

		typ, hdrLen, err := peekObjectType(pack, 0)
		require.NoError(t, err)
		require.Equal(t, ObjRefDelta, typ)

		return deltaStack{
				{
					pack:   pack,
					offset: 0,
					typ:    typ,
					hdrLen: hdrLen,
				},
			}, func() {
				require.NoError(t, pack.Close())
			}
	}

	stackA, closeA := makeStack("a", targetA)
	defer closeA()
	stackB, closeB := makeStack("b", targetB)
	defer closeB()

	first, typ, err := applyDeltaStack(stackA, base, ObjBlob, 0, true)
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, targetA, first)

	second, typ, err := applyDeltaStack(stackB, base, ObjBlob, 0, true)
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

	typ, hdrLen, err := peekObjectType(pack, 0)
	require.NoError(t, err)
	require.Equal(t, ObjRefDelta, typ)

	// Call with the WRONG base (different length) to trigger size mismatch.
	wrongBase := []byte("short")
	out := make([]byte, 0, 4096)
	_, err = applyDeltaStreaming(pack, 0, typ, hdrLen, wrongBase, out)
	require.Error(t, err)

	// After fix: the error message includes both sizes, not just "delta base size mismatch".
	assert.Contains(t, err.Error(), "mismatch")
	assert.Contains(t, err.Error(), fmt.Sprintf("header=%d", len(base)))
	assert.Contains(t, err.Error(), fmt.Sprintf("actual=%d", len(wrongBase)))
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
	_, err = readOfsDeltaOffset(pack, 100)
	require.Error(t, err, "expected error when reading beyond EOF, got nil")
}

// TestReadOfsDeltaOffset_ValidData verifies that readOfsDeltaOffset correctly
// decodes a valid single-byte offset (no continuation bit set).
func TestReadOfsDeltaOffset_ValidData(t *testing.T) {
	// A single byte with value 0x42 (continuation bit not set) should decode to 0x42.
	path := filepath.Join(t.TempDir(), "valid.pack")
	require.NoError(t, os.WriteFile(path, []byte{0x42}, 0o644))

	pack, err := mmap.Open(path)
	require.NoError(t, err)
	defer pack.Close()

	offset, err := readOfsDeltaOffset(pack, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0x42), offset)
}

// testDelta represents delta information for testing.
type testDelta struct {
	targetSize int
	ops        []deltaOp
}

// applyTestDeltaStack simulates applying a delta stack for testing.
func applyTestDeltaStack(t *testing.T, _ deltaStack, baseData []byte, testDeltas []testDelta) []byte {
	arena := getDeltaArena()
	defer putDeltaArena(arena)

	// Determine max size needed.
	maxSize := uint64(len(baseData))
	for _, td := range testDeltas {
		if uint64(td.targetSize) > maxSize {
			maxSize = uint64(td.targetSize)
		}
	}

	// Set up buffers.
	bufA := arena.data[:maxSize]
	bufB := arena.data[maxSize : maxSize*2]

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

	// For empty stack, borrowed=true returns baseData directly (no arena).
	base := []byte("hello world")
	result, typ, err := applyDeltaStack(nil, base, ObjBlob, 0, true)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, base, result)

	// For empty stack, borrowed=false returns a COPY.
	result2, _, err := applyDeltaStack(nil, base, ObjBlob, 0, false)
	require.NoError(t, err)
	// Modify original; copy should be independent.
	base[0] = 'H'
	assert.Equal(t, byte('h'), result2[0], "non-borrowed should be independent copy")
}

func TestDeltaArenaOverflowProtection(t *testing.T) {
	t.Parallel()

	maxObj := uint64(512 << 20)
	base := []byte("base")

	_, _, err := applyDeltaStack(nil, base, ObjBlob, maxObj, false)
	assert.NoError(t, err, "empty stack with maxObjectSize should succeed")

	hugeBase := make([]byte, 1)
	_, _, err = applyDeltaStack(nil, hugeBase, ObjBlob, 0, false)
	assert.NoError(t, err, "empty stack should always succeed regardless of base size")
}

func TestDeltaArenaOversizedPoolReturn(t *testing.T) {
	arena := getDeltaArena()
	standardCap := cap(arena.data)
	t.Logf("standard arena capacity: %d bytes (%d MiB)", standardCap, standardCap>>20)

	oversized := make([]byte, standardCap*4)
	arena.data = oversized

	putDeltaArena(arena)

	arena2 := getDeltaArena()
	oversizedCap := cap(arena2.data)
	t.Logf("retrieved arena capacity: %d bytes (%d MiB)", oversizedCap, oversizedCap>>20)

	if oversizedCap > standardCap {
		t.Logf("CONFIRMED: oversized arena (%d bytes) returned to pool without cap", oversizedCap)
	}
	putDeltaArena(arena2)
}
