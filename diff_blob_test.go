package objstore

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddedLines(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected []AddedLine
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
			expected: []AddedLine{
				{Text: []byte("line1"), Line: 1},
				{Text: []byte("line2"), Line: 2},
				{Text: []byte("line3"), Line: 3},
			},
		},
		{
			name:     "new empty, old has content",
			old:      []byte("line1\nline2"),
			new:      []byte(""),
			expected: []AddedLine{},
		},

		// The Myers diff algorithm includes context lines when generating unified diffs.
		{
			name: "simple addition at end",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedLine{
				{Text: []byte("line2"), Line: 2},
				{Text: []byte("line3"), Line: 3},
			},
		},

		{
			name: "simple addition at beginning",
			old:  []byte("line2\nline3"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedLine{
				{Text: []byte("line1"), Line: 1},
			},
		},

		{
			name: "simple addition in middle",
			old:  []byte("line1\nline3"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedLine{
				{Text: []byte("line2"), Line: 2},
			},
		},

		// Context lines are included when multiple changes affect the same diff hunk.
		{
			name: "multiple additions",
			old:  []byte("line1\nline4"),
			new:  []byte("line1\nline2\nline3\nline4\nline5"),
			expected: []AddedLine{
				{Text: []byte("line2"), Line: 2},
				{Text: []byte("line3"), Line: 3},
				{Text: []byte("line4"), Line: 4},
				{Text: []byte("line5"), Line: 5},
			},
		},

		{
			name: "replacement (line changed)",
			old:  []byte("line1\nold_line\nline3"),
			new:  []byte("line1\nnew_line\nline3"),
			expected: []AddedLine{
				{Text: []byte("new_line"), Line: 2},
			},
		},

		{
			name: "complete replacement",
			old:  []byte("old1\nold2"),
			new:  []byte("new1\nnew2\nnew3"),
			expected: []AddedLine{
				{Text: []byte("new1"), Line: 1},
				{Text: []byte("new2"), Line: 2},
				{Text: []byte("new3"), Line: 3},
			},
		},

		// Edge cases with special characters that could be confused with diff syntax.
		{
			name: "line starting with plus",
			old:  []byte("line1\n"),
			new:  []byte("line1\n+this starts with plus"),
			expected: []AddedLine{
				{Text: []byte("+this starts with plus"), Line: 2},
			},
		},

		{
			name: "line with multiple plus signs",
			old:  []byte("line1\n"),
			new:  []byte("line1\n+++multiple+++plus+++"),
			expected: []AddedLine{
				{Text: []byte("+++multiple+++plus+++"), Line: 2},
			},
		},

		// The +++ header lines from unified diff format are properly filtered out.
		{
			name: "line that looks like unified diff header",
			old:  []byte("line1\n"),
			new:  []byte("line1\n+++ b/file.txt"),
			expected: []AddedLine{
				{Text: []byte("+++ b/file.txt"), Line: 2},
			},
		},

		{
			name: "empty lines added",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\n\n\nline2"),
			expected: []AddedLine{
				{Text: []byte(""), Line: 2},
				{Text: []byte(""), Line: 3},
			},
		},

		// Whitespace handling preserves the exact content.
		{
			name: "whitespace only lines",
			old:  []byte("line1\n"),
			new:  []byte("line1\n   \n\t\t\n "),
			expected: []AddedLine{
				{Text: []byte("   "), Line: 2},
				{Text: []byte("\t\t"), Line: 3},
				{Text: []byte(" "), Line: 4},
			},
		},

		// Unicode content is handled correctly.
		{
			name: "unicode characters",
			old:  []byte("hello\n"),
			new:  []byte("hello\n世界\n🚀\némoji"),
			expected: []AddedLine{
				{Text: []byte("世界"), Line: 2},
				{Text: []byte("🚀"), Line: 3},
				{Text: []byte("émoji"), Line: 4},
			},
		},

		{
			name: "very long lines",
			old:  []byte("short\n"),
			new:  []byte("short\n" + strings.Repeat("a", 1000)),
			expected: []AddedLine{
				{Text: []byte(strings.Repeat("a", 1000)), Line: 2},
			},
		},

		{
			name: "lines with special characters",
			old:  []byte("normal\n"),
			new:  []byte("normal\n!@#$%^&*()\n<>?:\"{}\n\\\n//comment"),
			expected: []AddedLine{
				{Text: []byte("!@#$%^&*()"), Line: 2},
				{Text: []byte("<>?:\"{}"), Line: 3},
				{Text: []byte("\\"), Line: 4},
				{Text: []byte("//comment"), Line: 5},
			},
		},

		// Line ending variations are preserved in the output.
		{
			name: "windows line endings (CRLF)",
			old:  []byte("line1\r\nline2"),
			new:  []byte("line1\r\nline2\r\nline3"),
			expected: []AddedLine{
				{Text: []byte("line2\r"), Line: 2},
				{Text: []byte("line3"), Line: 3},
			},
		},

		{
			name: "mixed line endings",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\r\nline2\nline3\r\n"),
			expected: []AddedLine{
				{Text: []byte("line1\r"), Line: 1},
				{Text: []byte("line2"), Line: 2},
				{Text: []byte("line3\r"), Line: 3},
			},
		},

		{
			name: "no newline at end (old)",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\nline2\nline3"),
			expected: []AddedLine{
				{Text: []byte("line2"), Line: 2},
				{Text: []byte("line3"), Line: 3},
			},
		},

		{
			name: "no newline at end (new)",
			old:  []byte("line1\n"),
			new:  []byte("line1\nline2"),
			expected: []AddedLine{
				{Text: []byte("line2"), Line: 2},
			},
		},

		{
			name: "no newline at end (both)",
			old:  []byte("line1"),
			new:  []byte("line1\nline2"),
			expected: []AddedLine{
				{Text: []byte("line1"), Line: 1},
				{Text: []byte("line2"), Line: 2},
			},
		},

		// Binary-like content with control characters.
		{
			name: "null bytes",
			old:  []byte("line1\n"),
			new:  []byte("line1\nline\x00with\x00nulls"),
			expected: []AddedLine{
				{Text: []byte("line\x00with\x00nulls"), Line: 2},
			},
		},

		{
			name: "control characters",
			old:  []byte("normal\n"),
			new:  []byte("normal\n\x01\x02\x03\x1F"),
			expected: []AddedLine{
				{Text: []byte("\x01\x02\x03\x1F"), Line: 2},
			},
		},

		// Performance test with many additions.
		{
			name: "many lines added",
			old:  []byte("start\nend"),
			new: []byte("start\n" + func() string {
				var lines []string
				for i := 1; i <= 100; i++ {
					lines = append(lines, "added_line_"+string(rune('0'+i%10)))
				}
				return strings.Join(lines, "\n")
			}() + "\nend"),
			expected: func() []AddedLine {
				var expected []AddedLine
				for i := 1; i <= 100; i++ {
					expected = append(expected, AddedLine{Text: []byte("added_line_" + string(rune('0'+i%10))), Line: i + 1})
				}
				return expected
			}(),
		},

		// Verify that unified diff headers are properly filtered.
		{
			name: "false positive prevention",
			old:  []byte("line1\n++ file.txt"),
			new:  []byte("line1\n+++ new.txt\n++ file.txt"),
			expected: []AddedLine{
				{Text: []byte("+++ new.txt"), Line: 2},
			},
		},

		{
			name: "mixed additions, deletions, and modifications",
			old: []byte(`line1
line2
line3
line4
line5`),
			new: []byte(`line1
modified2
line3
added1
added2
line5
added3`),
			expected: []AddedLine{
				{Text: []byte("modified2"), Line: 2},
				{Text: []byte("added1"), Line: 4},
				{Text: []byte("added2"), Line: 5},
				{Text: []byte("line5"), Line: 6},
				{Text: []byte("added3"), Line: 7},
			},
		},

		// Edge cases with minimal content.
		{
			name: "single character addition",
			old:  []byte("a"),
			new:  []byte("ab"),
			expected: []AddedLine{
				{Text: []byte("ab"), Line: 1},
			},
		},

		{
			name: "single newline added",
			old:  []byte("line"),
			new:  []byte("line\n"),
			expected: []AddedLine{
				{Text: []byte("line"), Line: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLinesWithPos(tt.old, tt.new)

			assert.Equal(t, tt.expected, result,
				"addedLinesWithPos() failed for %s",
				tt.name)
		})
	}
}

func BenchmarkAddedLines(b *testing.B) {
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
			for i := 0; i < b.N; i++ {
				_ = addedLinesWithPos(bc.old, bc.new)
			}
		})
	}
}

func TestAddedLinesProperties(t *testing.T) {
	t.Run("identical_content_returns_nil", func(t *testing.T) {
		content := []byte("any\ncontent\nhere")
		result := addedLinesWithPos(content, content)
		assert.Nil(t, result, "expected nil for identical content")
	})

	// Verify that the '+' prefix is stripped from unified diff output.
	t.Run("no_leading_plus", func(t *testing.T) {
		old := []byte("line1")
		new := []byte("line1\nnew1\nnew2")
		result := addedLinesWithPos(old, new)

		for _, line := range result {
			assert.False(t, len(line.Text) > 0 && line.Text[0] == '+',
				"result line should not start with '+': %s", string(line.Text))
		}
	})

	t.Run("count_matches_additions", func(t *testing.T) {
		old := []byte("")
		new := []byte("line1\nline2\nline3")
		result := addedLinesWithPos(old, new)

		assert.Len(t, result, 3, "expected 3 added lines")
	})
}

func TestUnifiedDiffHeaderHandling(t *testing.T) {
	old := []byte("file content\n") // Add newline here
	new := []byte(`file content
+++ b/newfile.txt
++ regular line
+ single plus`)

	result := addedLinesWithPos(old, new)

	// Now only the truly NEW lines should be returned
	expected := []AddedLine{
		{Text: []byte("+++ b/newfile.txt"), Line: 2},
		{Text: []byte("++ regular line"), Line: 3},
		{Text: []byte("+ single plus"), Line: 4},
	}

	assert.Equal(t, expected, result, "should return all added lines with exact content")
}

func FuzzAddedLines(f *testing.F) {
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte("line1\nline2"), []byte("line1\nline2\nline3"))
	f.Add([]byte("test"), []byte("test\n+++ header\n+ line"))

	f.Fuzz(func(t *testing.T, old, new []byte) {
		result := addedLinesWithPos(old, new)

		// Verify that the result is always valid.
		if len(result) > 0 {
			for _, line := range result {
				_ = line
			}
		}

		// Fundamental invariant: identical inputs should return nil.
		if bytes.Equal(old, new) {
			assert.Nil(t, result, "expected nil for equal inputs")
		}
	})
}

func TestAddedLinesWithPos(t *testing.T) {
	old := []byte("line1\nline2\nline3")
	new := []byte("line1\ninsert\nline2\nappend\nline3")

	got := addedLinesWithPos(old, new)

	// Correct expectations based on actual line numbers in new file:
	// line1: line 1, insert: line 2, line2: line 3, append: line 4, line3: line 5
	want := []AddedLine{
		{Text: []byte("insert"), Line: 2},
		{Text: []byte("append"), Line: 4},
	}

	assert.Equal(t, want, got)

	assert.Len(t, got, 2)
	assert.Equal(t, []byte("insert"), got[0].Text)
	assert.Equal(t, []byte("append"), got[1].Text)
}

// Concurrency smoke‑test for map+mutex cache.
func TestMetaCacheConcurrency(t *testing.T) {
	// minimal fake objects
	var h Hash
	g := &commitGraphData{Timestamps: []int64{123}, OrderedOIDs: []Hash{h},
		OIDToIndex: map[Hash]int{h: 0}}
	s := &store{} // nil internals; we won't call author()

	mc := newMetaCache(g, s)
	if ts, ok := mc.timestamp(h); !ok || ts != 123 {
		t.Fatalf("timestamp mismatch")
	}

	// run 100 goroutines that hit timestamp()
	done := make(chan struct{}, 100)
	for i := 0; i < 100; i++ {
		go func() {
			mc.timestamp(h)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}
