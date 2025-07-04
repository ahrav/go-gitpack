package objstore

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaCacheConcurrency(t *testing.T) {
	var h Hash
	g := &commitGraphData{Timestamps: []int64{123}, OrderedOIDs: []Hash{h},
		OIDToIndex: map[Hash]int{h: 0}}
	s := &store{} // nil internals; we won't call get() which would call readCommitHeader

	mc := newMetaCache(g, s)
	mc.m[h] = AuthorInfo{Name: "test", Email: "test@test.com", When: time.Unix(123, 0)}

	meta, err := mc.get(h)
	require.NoError(t, err)
	if meta.Timestamp != 123 {
		t.Fatalf("timestamp mismatch")
	}

	done := make(chan struct{}, 100)
	for range 100 {
		go func() {
			_, _ = mc.get(h)
			done <- struct{}{}
		}()
	}
	for range 100 {
		<-done
	}
}

func TestAddedHunks(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected []AddedHunk
	}{
		// Basic functionality tests.
		{
			name:     "both empty",
			old:      []byte(""),
			new:      []byte(""),
			expected: nil,
		},
		{
			name:     "identical content",
			old:      []byte("hello\nworld"),
			new:      []byte("hello\nworld"),
			expected: nil,
		},
		{
			name: "old empty, new has content",
			old:  []byte(""),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedHunk{
				{StartLine: 1, Lines: []string{"line1", "line2", "line3"}},
			},
		},
		{
			name:     "new empty, old has content",
			old:      []byte("line1\nline2"),
			new:      []byte(""),
			expected: nil,
		},

		// Single hunk addition tests.
		{
			name: "simple addition at end",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedHunk{
				{StartLine: 3, Lines: []string{"line3"}},
			},
		},

		{
			name: "simple addition at beginning",
			old:  []byte("line2\nline3"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedHunk{
				{StartLine: 1, Lines: []string{"line1"}},
			},
		},

		{
			name: "simple addition in middle",
			old:  []byte("line1\nline3"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2"}},
			},
		},

		// Multiple hunks tests.
		{
			name: "multiple consecutive additions",
			old:  []byte("line1\nline4"),
			new:  []byte("line1\nline2\nline3\nline4\nline5"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2", "line3"}},
				{StartLine: 5, Lines: []string{"line5"}},
			},
		},

		{
			name: "multiple separate hunks",
			old:  []byte("line1\nline3\nline5"),
			new:  []byte("line1\nline2\nline3\nline4\nline5\nline6"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2"}},
				{StartLine: 4, Lines: []string{"line4"}},
				{StartLine: 6, Lines: []string{"line6"}},
			},
		},

		{
			name: "replacement creates single hunk",
			old:  []byte("line1\nold_line\nline3"),
			new:  []byte("line1\nnew_line\nline3"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"new_line"}},
			},
		},

		{
			name: "complete replacement",
			old:  []byte("old1\nold2"),
			new:  []byte("new1\nnew2\nnew3"),
			expected: []AddedHunk{
				{StartLine: 1, Lines: []string{"new1", "new2", "new3"}},
			},
		},

		// Edge cases with special characters.
		{
			name: "hunk with plus signs",
			old:  []byte("line1\n"),
			new:  []byte("line1\n+this starts with plus\n++multiple plus"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"+this starts with plus", "++multiple plus"}},
			},
		},

		{
			name: "empty lines in hunk",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\n\n\nline2"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"", ""}},
			},
		},

		// Whitespace handling.
		{
			name: "whitespace only lines",
			old:  []byte("line1\n"),
			new:  []byte("line1\n   \n\t\t\n "),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"   ", "\t\t", " "}},
			},
		},

		// Unicode content.
		{
			name: "unicode characters",
			old:  []byte("hello\n"),
			new:  []byte("hello\n世界\n🚀\némoji"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"世界", "🚀", "émoji"}},
			},
		},

		// Line ending variations.
		{
			name: "windows line endings (CRLF)",
			old:  []byte("line1\r\nline2"),
			new:  []byte("line1\r\nline2\r\nline3"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2\r", "line3"}},
			},
		},

		{
			name: "mixed additions and context",
			old:  []byte("line1\nline2\nline3\nline4\nline5"),
			new:  []byte("line1\nmodified2\nline3\nadded1\nadded2\nline5\nadded3"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"modified2"}},
				{StartLine: 4, Lines: []string{"added1", "added2"}},
				{StartLine: 7, Lines: []string{"added3"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedHunksWithPos(tt.old, tt.new)

			assert.Equal(t, tt.expected, result,
				"addedHunksWithPos() failed for %s", tt.name)

			// Verify EndLine method works correctly.
			for i, hunk := range result {
				var expectedEndLine uint32
				if len(hunk.Lines) == 0 {
					expectedEndLine = hunk.StartLine
				} else {
					expectedEndLine = hunk.StartLine + uint32(len(hunk.Lines)) - 1
				}
				assert.Equal(t, expectedEndLine, hunk.EndLine(),
					"EndLine() mismatch for hunk %d in test %s", i, tt.name)
			}
		})
	}
}

func TestAddedHunkProperties(t *testing.T) {
	t.Run("identical_content_returns_nil", func(t *testing.T) {
		content := []byte("any\ncontent\nhere")
		result := addedHunksWithPos(content, content)
		assert.Nil(t, result, "expected nil for identical content")
	})

	t.Run("endline_calculation", func(t *testing.T) {
		hunk := AddedHunk{
			StartLine: 5,
			Lines:     []string{"line1", "line2", "line3"},
		}
		assert.Equal(t, uint32(7), hunk.EndLine(), "EndLine should be StartLine + len(Lines) - 1")
	})

	t.Run("empty_hunk_endline", func(t *testing.T) {
		hunk := AddedHunk{
			StartLine: 10,
			Lines:     []string{},
		}
		assert.Equal(t, uint32(10), hunk.EndLine(), "Empty hunk EndLine should equal StartLine")
	})

	t.Run("single_line_hunk", func(t *testing.T) {
		hunk := AddedHunk{
			StartLine: 3,
			Lines:     []string{"single line"},
		}
		assert.Equal(t, uint32(3), hunk.EndLine(), "Single line hunk EndLine should equal StartLine")
	})
}

func BenchmarkAddedHunks(b *testing.B) {
	cases := []struct {
		name string
		old  []byte
		new  []byte
	}{
		{
			name: "small_identical",
			old:  []byte("line1\nline2\nline3"),
			new:  []byte("line1\nline2\nline3"),
		},
		{
			name: "small_diff",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\ninserted\nline2\nadded"),
		},
		{
			name: "medium_diff",
			old:  []byte(strings.Repeat("line\n", 100)),
			new:  []byte(strings.Repeat("line\nnew\n", 100)),
		},
		{
			name: "large_diff",
			old:  []byte(strings.Repeat("old line\n", 1000)),
			new:  []byte(strings.Repeat("new line\n", 1000)),
		},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			for b.Loop() {
				_ = addedHunksWithPos(bc.old, bc.new)
			}
		})
	}
}

func FuzzAddedHunks(f *testing.F) {
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte("line1\nline2"), []byte("line1\nline2\nline3"))
	f.Add([]byte("test"), []byte("test\n+++ header\n+ line"))

	f.Fuzz(func(t *testing.T, old, new []byte) {
		result := addedHunksWithPos(old, new)

		// Verify that the result is always valid.
		for _, hunk := range result {
			assert.Greater(t, hunk.StartLine, uint32(0), "StartLine must be positive")
			assert.GreaterOrEqual(t, hunk.EndLine(), hunk.StartLine, "EndLine must be >= StartLine")
			assert.NotNil(t, hunk.Lines, "Lines should not be nil")
		}

		// Fundamental invariant: identical inputs should return nil.
		if bytes.Equal(old, new) {
			assert.Nil(t, result, "expected nil for equal inputs")
		}
	})
}
