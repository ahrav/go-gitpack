package objstore

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTreeIter_Empty(t *testing.T) {
	iter := newTreeIter([]byte{})

	name, oid, mode, ok, err := iter.Next()
	assert.False(t, ok, "expected ok=false for empty tree")
	assert.Equal(t, io.EOF, err, "expected io.EOF")
	assert.Equal(t, "", name, "expected empty name")
	assert.Equal(t, Hash{}, oid, "expected zero oid")
	assert.Equal(t, uint32(0), mode, "expected zero mode")
}

func TestTreeIter_SingleEntry(t *testing.T) {
	// Represents a single raw tree entry: 100644 hello.txt\0<20-byte-sha>
	sha := bytes.Repeat([]byte{0xab}, 20)
	raw := []byte("100644 hello.txt\x00")
	raw = append(raw, sha...)

	iter := newTreeIter(raw)

	name, oid, mode, ok, err := iter.Next()
	require.True(t, ok, "expected ok=true, got false with err=%v", err)
	require.NoError(t, err, "unexpected error")

	assert.Equal(t, "hello.txt", name)
	assert.Equal(t, uint32(0100644), mode)
	expectedOid := Hash{}
	copy(expectedOid[:], sha)
	assert.Equal(t, expectedOid, oid)

	// The second call should return io.EOF.
	_, _, _, ok, err = iter.Next()
	assert.False(t, ok, "expected ok=false after exhausting entries")
	assert.Equal(t, io.EOF, err, "expected io.EOF")
}

func TestTreeIter_MultipleEntries(t *testing.T) {
	entries := []struct {
		mode uint32
		name string
		sha  []byte
	}{
		{0100644, "README.md", bytes.Repeat([]byte{0x11}, 20)},
		{040000, "src", bytes.Repeat([]byte{0x22}, 20)},
		{0100755, "script.sh", bytes.Repeat([]byte{0x33}, 20)},
		{0120000, "link", bytes.Repeat([]byte{0x44}, 20)},
		{0160000, "submodule", bytes.Repeat([]byte{0x55}, 20)},
	}

	var raw []byte
	for _, e := range entries {
		raw = append(raw, []byte(octStr(e.mode))...)
		raw = append(raw, ' ')
		raw = append(raw, []byte(e.name)...)
		raw = append(raw, 0)
		raw = append(raw, e.sha...)
	}

	iter := newTreeIter(raw)

	for i, expected := range entries {
		name, oid, mode, ok, err := iter.Next()
		require.True(t, ok, "entry %d: expected ok=true, got false with err=%v", i, err)
		require.NoError(t, err, "entry %d: unexpected error: %v", i, err)

		assert.Equal(t, expected.name, name, "entry %d: name mismatch", i)
		assert.Equal(t, expected.mode, mode, "entry %d: mode mismatch", i)
		expectedOid := Hash{}
		copy(expectedOid[:], expected.sha)
		assert.Equal(t, expectedOid, oid, "entry %d: oid mismatch", i)
	}

	// The iterator should be exhausted now.
	_, _, _, ok, err := iter.Next()
	assert.False(t, ok, "expected ok=false after all entries")
	assert.Equal(t, io.EOF, err, "expected io.EOF")
}

func TestTreeIter_SpecialNames(t *testing.T) {
	testCases := []string{
		"file with spaces.txt",
		"file-with-dashes.go",
		"file_with_underscores.py",
		"file.with.dots.js",
		"UPPERCASE.TXT",
		"ファイル.txt", // Japanese
		"файл.txt", // Russian
		"🚀.md",     // Emoji
		"a",        // Single character
		"very-long-filename-that-goes-on-and-on-and-on-and-on-and-on.txt",
	}

	for _, testName := range testCases {
		t.Run(testName, func(t *testing.T) {
			sha := bytes.Repeat([]byte{0x99}, 20)
			raw := []byte("100644 ")
			raw = append(raw, []byte(testName)...)
			raw = append(raw, 0)
			raw = append(raw, sha...)

			iter := newTreeIter(raw)

			name, _, _, ok, err := iter.Next()
			require.True(t, ok, "failed to parse entry: ok=%v, err=%v", ok, err)
			require.NoError(t, err)
			assert.Equal(t, testName, name)
		})
	}
}

func TestTreeIter_ModeParsing(t *testing.T) {
	testCases := []struct {
		modeStr  string
		expected uint32
	}{
		{"100644", 0100644},
		{"100755", 0100755},
		{"040000", 040000},
		{"120000", 0120000},
		{"160000", 0160000},
		{"000644", 0644}, // Leading zeros are valid.
		{"644", 0644},    // No leading zeros is also valid.
		{"0", 0},         // Zero is a valid mode.
		{"777", 0777},    // Max permissions.
	}

	for _, tc := range testCases {
		t.Run(tc.modeStr, func(t *testing.T) {
			sha := bytes.Repeat([]byte{0xaa}, 20)
			raw := []byte(tc.modeStr + " file\x00")
			raw = append(raw, sha...)

			iter := newTreeIter(raw)

			_, _, mode, ok, err := iter.Next()
			require.True(t, ok, "failed to parse: ok=%v, err=%v", ok, err)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, mode)
		})
	}
}

func TestTreeIter_InvalidMode(t *testing.T) {
	invalidModes := []string{
		"100844",  // Contains '8', which is not a valid octal digit.
		"10a644",  // Contains 'a'.
		"10064x",  // Contains 'x'.
		"10064\t", // A tab is used instead of a space.
	}

	for _, badMode := range invalidModes {
		t.Run(badMode, func(t *testing.T) {
			sha := bytes.Repeat([]byte{0xbb}, 20)
			raw := []byte(badMode + " file\x00")
			raw = append(raw, sha...)

			iter := newTreeIter(raw)

			_, _, _, ok, err := iter.Next()
			assert.False(t, ok, "expected ok=false for invalid mode")
			assert.ErrorIs(t, err, ErrCorruptTree, "expected ErrCorruptTree")
		})
	}

	// A mode containing a space is technically valid but ambiguous. The parser
	// stops at the first space, treating the rest as the filename.
	t.Run("mode with space parses as mode+filename", func(t *testing.T) {
		sha := bytes.Repeat([]byte{0xbb}, 20)
		raw := []byte("100 44 file\x00")
		raw = append(raw, sha...)

		iter := newTreeIter(raw)

		name, _, mode, ok, err := iter.Next()
		require.True(t, ok, "expected successful parse: ok=%v, err=%v", ok, err)
		require.NoError(t, err)
		assert.Equal(t, uint32(0100), mode)
		assert.Equal(t, "44 file", name)
	})

	// An empty mode string is parsed as mode 0.
	t.Run("empty mode parses as zero", func(t *testing.T) {
		sha := bytes.Repeat([]byte{0xbb}, 20)
		raw := []byte(" file\x00")
		raw = append(raw, sha...)

		iter := newTreeIter(raw)

		name, _, mode, ok, err := iter.Next()
		require.True(t, ok, "expected successful parse: ok=%v, err=%v", ok, err)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), mode)
		assert.Equal(t, "file", name)
	})
}

func TestTreeIter_MissingSpace(t *testing.T) {
	raw := []byte("100644file\x00")
	raw = append(raw, bytes.Repeat([]byte{0xcc}, 20)...)

	iter := newTreeIter(raw)

	_, _, _, ok, err := iter.Next()
	assert.False(t, ok, "expected ok=false for missing space")
	assert.ErrorIs(t, err, ErrCorruptTree, "expected ErrCorruptTree")
}

func TestTreeIter_MissingNull(t *testing.T) {
	raw := []byte("100644 file")
	raw = append(raw, bytes.Repeat([]byte{0xdd}, 20)...)

	iter := newTreeIter(raw)

	_, _, _, ok, err := iter.Next()
	assert.False(t, ok, "expected ok=false for missing null")
	assert.ErrorIs(t, err, ErrCorruptTree, "expected ErrCorruptTree")
}

func TestTreeIter_TruncatedSHA(t *testing.T) {
	// All lengths are less than the required 20 bytes for a SHA.
	testCases := []int{0, 1, 5, 10, 19}

	for _, shaLen := range testCases {
		t.Run(hex.EncodeToString([]byte{byte(shaLen)}), func(t *testing.T) {
			raw := []byte("100644 file\x00")
			raw = append(raw, bytes.Repeat([]byte{0xee}, shaLen)...)

			iter := newTreeIter(raw)

			_, _, _, ok, err := iter.Next()
			assert.False(t, ok, "expected ok=false for truncated SHA")
			assert.ErrorIs(t, err, ErrCorruptTree, "expected ErrCorruptTree")
		})
	}
}

func TestTreeIter_PartialEntry(t *testing.T) {
	testCases := [][]byte{
		[]byte("100644"),      // Mode only.
		[]byte("100644 "),     // Mode and space.
		[]byte("100644 file"), // No null terminator.
		[]byte("100644 \x00"), // Empty name.
	}

	for i, raw := range testCases {
		t.Run(hex.EncodeToString([]byte{byte(i)}), func(t *testing.T) {
			iter := newTreeIter(raw)

			_, _, _, ok, err := iter.Next()
			assert.False(t, ok, "expected ok=false for partial entry")
			assert.ErrorIs(t, err, ErrCorruptTree, "expected ErrCorruptTree")
		})
	}
}

func TestTreeIter_EmptyName(t *testing.T) {
	// Git itself does not allow empty filenames, but we test that the parser
	// can handle them gracefully.
	sha := bytes.Repeat([]byte{0xff}, 20)
	raw := []byte("100644 \x00")
	raw = append(raw, sha...)

	iter := newTreeIter(raw)

	name, _, _, ok, err := iter.Next()
	require.True(t, ok, "unexpected parse failure: ok=%v, err=%v", ok, err)
	require.NoError(t, err)
	assert.Empty(t, name, "expected empty name")
}

func TestTreeIter_NonMutatingIteration(t *testing.T) {
	// Verifies that iterating does not modify the underlying byte slice.
	sha := bytes.Repeat([]byte{0x12}, 20)
	original := []byte("100644 test.txt\x00")
	original = append(original, sha...)

	backup := make([]byte, len(original))
	copy(backup, original)

	iter := newTreeIter(original)
	_, _, _, _, _ = iter.Next()
	_, _, _, _, _ = iter.Next() // Exhaust the iterator.

	assert.Equal(t, backup, original, "iteration modified the original buffer")
}

func TestTreeIter_ConcurrentIterators(t *testing.T) {
	// Ensures that multiple iterators on the same buffer do not interfere with
	// each other.
	sha1 := bytes.Repeat([]byte{0x11}, 20)
	sha2 := bytes.Repeat([]byte{0x22}, 20)
	raw := []byte("100644 a.txt\x00")
	raw = append(raw, sha1...)
	raw = append(raw, []byte("100755 b.sh\x00")...)
	raw = append(raw, sha2...)

	iter1 := newTreeIter(raw)
	iter2 := newTreeIter(raw)

	name1, _, _, ok1, err1 := iter1.Next()
	require.True(t, ok1, "iter1 first: ok=%v, err=%v", ok1, err1)
	require.NoError(t, err1)
	assert.Equal(t, "a.txt", name1, "iter1: name mismatch")

	name2a, _, _, ok2a, err2a := iter2.Next()
	require.True(t, ok2a, "iter2 first: ok=%v, err=%v", ok2a, err2a)
	require.NoError(t, err2a)
	assert.Equal(t, "a.txt", name2a, "iter2: name mismatch")

	name2b, _, _, ok2b, err2b := iter2.Next()
	require.True(t, ok2b, "iter2 second: ok=%v, err=%v", ok2b, err2b)
	require.NoError(t, err2b)
	assert.Equal(t, "b.sh", name2b, "iter2: name mismatch")

	name1b, _, _, ok1b, err1b := iter1.Next()
	require.True(t, ok1b, "iter1 second: ok=%v, err=%v", ok1b, err1b)
	require.NoError(t, err1b)
	assert.Equal(t, "b.sh", name1b, "iter1: name mismatch")
}

func TestTreeIter_RealWorldExample(t *testing.T) {
	// A realistic tree with various entry types.
	entries := []struct {
		mode uint32
		name string
	}{
		{040000, ".github"},
		{100644, ".gitignore"},
		{100644, "LICENSE"},
		{100644, "README.md"},
		{100755, "build.sh"},
		{040000, "cmd"},
		{100644, "go.mod"},
		{100644, "go.sum"},
		{040000, "internal"},
		{120000, "latest"},
		{040000, "pkg"},
		{040000, "test"},
	}

	var raw []byte
	for i, e := range entries {
		raw = append(raw, []byte(octStr(e.mode))...)
		raw = append(raw, ' ')
		raw = append(raw, []byte(e.name)...)
		raw = append(raw, 0)
		// Use a different SHA for each entry to ensure correct parsing.
		sha := bytes.Repeat([]byte{byte(i + 1)}, 20)
		raw = append(raw, sha...)
	}

	iter := newTreeIter(raw)
	count := 0

	for {
		name, oid, mode, ok, err := iter.Next()
		if !ok {
			require.Equal(t, io.EOF, err, "unexpected error at end")
			break
		}

		require.Less(t, count, len(entries), "too many entries returned")

		expected := entries[count]
		assert.Equal(t, expected.name, name, "entry %d: name mismatch", count)
		assert.Equal(t, expected.mode, mode, "entry %d: mode mismatch", count)

		expectedSHA := bytes.Repeat([]byte{byte(count + 1)}, 20)
		assert.Equal(t, expectedSHA, oid[:], "entry %d: unexpected SHA", count)

		count++
	}

	assert.Equal(t, len(entries), count, "mismatched entry count")
}

func TestTreeIter_LargeTree(t *testing.T) {
	const numEntries = 1000

	var raw []byte
	for i := 0; i < numEntries; i++ {
		name := "file" + octStr(uint32(i))
		raw = append(raw, []byte("100644 ")...)
		raw = append(raw, []byte(name)...)
		raw = append(raw, 0)
		// Use the index as the SHA byte value, repeated.
		sha := bytes.Repeat([]byte{byte(i % 256)}, 20)
		raw = append(raw, sha...)
	}

	iter := newTreeIter(raw)
	count := 0

	for {
		name, _, _, ok, err := iter.Next()
		if !ok {
			require.Equal(t, io.EOF, err, "unexpected error")
			break
		}

		expectedName := "file" + octStr(uint32(count))
		assert.Equal(t, expectedName, name, "entry %d: name mismatch", count)

		count++
		require.LessOrEqual(t, count, numEntries, "iterator returned too many entries")
	}

	assert.Equal(t, numEntries, count, "mismatched entry count")
}

// Tests that TreeIter conforms to its documented behavior.
func TestTreeIter_DocumentedBehavior(t *testing.T) {
	t.Run("EOF on exhausted iterator", func(t *testing.T) {
		iter := newTreeIter([]byte{})
		_, _, _, ok, err := iter.Next()
		assert.False(t, ok)
		assert.Equal(t, io.EOF, err, "expected io.EOF for exhausted iterator")
	})

	t.Run("Multiple EOF calls", func(t *testing.T) {
		iter := newTreeIter([]byte{})
		for i := 0; i < 3; i++ {
			_, _, _, ok, err := iter.Next()
			assert.False(t, ok, "call %d: expected ok=false", i)
			assert.Equal(t, io.EOF, err, "call %d: expected io.EOF", i)
		}
	})
}

// BenchmarkTreeIter_Typical benchmarks iteration over a realistic Git tree.
// This covers the common case most applications will encounter.
func BenchmarkTreeIter_Typical(b *testing.B) {
	// Realistic project structure with ~20 entries
	entries := []struct {
		mode uint32
		name string
	}{
		{040000, ".github"},
		{100644, ".gitignore"},
		{100644, "LICENSE"},
		{100644, "README.md"},
		{100644, "go.mod"},
		{100644, "go.sum"},
		{100755, "build.sh"},
		{040000, "cmd"},
		{040000, "internal"},
		{040000, "pkg"},
		{040000, "test"},
		{100644, "main.go"},
		{100644, "config.yaml"},
		{120000, "latest"}, // symlink
		{040000, "docs"},
		{100644, "Dockerfile"},
		{100644, "Makefile"},
		{040000, "scripts"},
		{160000, "vendor"}, // submodule
		{100644, "version.txt"},
	}

	var raw []byte
	for i, e := range entries {
		raw = append(raw, []byte(octStr(e.mode))...)
		raw = append(raw, ' ')
		raw = append(raw, []byte(e.name)...)
		raw = append(raw, 0)
		sha := bytes.Repeat([]byte{byte(i + 1)}, 20)
		raw = append(raw, sha...)
	}

	b.ReportAllocs()

	for b.Loop() {
		iter := newTreeIter(raw)
		for {
			name, oid, mode, ok, err := iter.Next()
			if !ok {
				break
			}
			// Use all returned values to prevent compiler optimization
			_ = name
			_ = oid
			_ = mode
			_ = err
		}
	}
}

// BenchmarkTreeIter_Large benchmarks iteration over a large tree.
// This covers the edge case of repositories with many files in a directory.
func BenchmarkTreeIter_Large(b *testing.B) {
	const numEntries = 2000 // Large but realistic (e.g., node_modules, vendor dirs)

	var raw []byte
	for i := 0; i < numEntries; i++ {
		// Mix of file types to be realistic
		mode := "100644"
		if i%15 == 0 {
			mode = "040000" // Directory (~7%)
		} else if i%25 == 0 {
			mode = "100755" // Executable (~4%)
		}

		name := "file" + octStr(uint32(i)) + ".ext"
		sha := bytes.Repeat([]byte{byte(i%256 + 1)}, 20)

		raw = append(raw, []byte(mode)...)
		raw = append(raw, ' ')
		raw = append(raw, []byte(name)...)
		raw = append(raw, 0)
		raw = append(raw, sha...)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		iter := newTreeIter(raw)
		count := 0
		for {
			name, oid, mode, ok, err := iter.Next()
			if !ok {
				break
			}
			count++
			// Use all returned values to prevent compiler optimization
			_ = name
			_ = oid
			_ = mode
			_ = err
		}
		if count != numEntries {
			b.Fatalf("expected %d entries, got %d", numEntries, count)
		}
	}
}

// octStr converts a uint32 to an octal string.
func octStr(n uint32) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte(n&7) + '0'
		n >>= 3
	}
	return string(buf[i:])
}
