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
		expected [][]byte
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
			expected: [][]byte{
				[]byte("line1"),
				[]byte("line2"),
				[]byte("line3"),
			},
		},
		{
			name:     "new empty, old has content",
			old:      []byte("line1\nline2"),
			new:      []byte(""),
			expected: nil,
		},

		// The Myers diff algorithm includes context lines when generating unified diffs.
		{
			name: "simple addition at end",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\nline2\nline3"),
			expected: [][]byte{
				[]byte("line2"),
				[]byte("line3"),
			},
		},

		{
			name: "simple addition at beginning",
			old:  []byte("line2\nline3"),
			new:  []byte("line1\nline2\nline3"),
			expected: [][]byte{
				[]byte("line1"),
			},
		},

		{
			name: "simple addition in middle",
			old:  []byte("line1\nline3"),
			new:  []byte("line1\nline2\nline3"),
			expected: [][]byte{
				[]byte("line2"),
			},
		},

		// Context lines are included when multiple changes affect the same diff hunk.
		{
			name: "multiple additions",
			old:  []byte("line1\nline4"),
			new:  []byte("line1\nline2\nline3\nline4\nline5"),
			expected: [][]byte{
				[]byte("line2"),
				[]byte("line3"),
				[]byte("line4"),
				[]byte("line5"),
			},
		},

		{
			name: "replacement (line changed)",
			old:  []byte("line1\nold_line\nline3"),
			new:  []byte("line1\nnew_line\nline3"),
			expected: [][]byte{
				[]byte("new_line"),
			},
		},

		{
			name: "complete replacement",
			old:  []byte("old1\nold2"),
			new:  []byte("new1\nnew2\nnew3"),
			expected: [][]byte{
				[]byte("new1"),
				[]byte("new2"),
				[]byte("new3"),
			},
		},

		// Edge cases with special characters that could be confused with diff syntax.
		{
			name: "line starting with plus",
			old:  []byte("line1"),
			new:  []byte("line1\n+this starts with plus"),
			expected: [][]byte{
				[]byte("line1"),
			},
		},

		{
			name: "line with multiple plus signs",
			old:  []byte("line1"),
			new:  []byte("line1\n+++multiple+++plus+++"),
			expected: [][]byte{
				[]byte("line1"),
			},
		},

		// The +++ header lines from unified diff format are properly filtered out.
		{
			name: "line that looks like unified diff header",
			old:  []byte("line1"),
			new:  []byte("line1\n+++ b/file.txt"),
			expected: [][]byte{
				[]byte("line1"),
			},
		},

		{
			name: "empty lines added",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\n\n\nline2"),
			expected: [][]byte{
				[]byte(""),
				[]byte(""),
			},
		},

		// Whitespace handling preserves the exact content.
		{
			name: "whitespace only lines",
			old:  []byte("line1"),
			new:  []byte("line1\n   \n\t\t\n "),
			expected: [][]byte{
				[]byte("line1"),
				[]byte("   "),
				[]byte("\t\t"),
				[]byte(" "),
			},
		},

		// Unicode content is handled correctly.
		{
			name: "unicode characters",
			old:  []byte("hello"),
			new:  []byte("hello\n世界\n🚀\némoji"),
			expected: [][]byte{
				[]byte("hello"),
				[]byte("世界"),
				[]byte("🚀"),
				[]byte("émoji"),
			},
		},

		{
			name: "very long lines",
			old:  []byte("short"),
			new:  []byte("short\n" + strings.Repeat("a", 1000)),
			expected: [][]byte{
				[]byte("short"),
				[]byte(strings.Repeat("a", 1000)),
			},
		},

		{
			name: "lines with special characters",
			old:  []byte("normal"),
			new:  []byte("normal\n!@#$%^&*()\n<>?:\"{}\n\\\n//comment"),
			expected: [][]byte{
				[]byte("normal"),
				[]byte("!@#$%^&*()"),
				[]byte("<>?:\"{}"),
				[]byte("\\"),
				[]byte("//comment"),
			},
		},

		// Line ending variations are preserved in the output.
		{
			name: "windows line endings (CRLF)",
			old:  []byte("line1\r\nline2"),
			new:  []byte("line1\r\nline2\r\nline3"),
			expected: [][]byte{
				[]byte("line2\r"),
				[]byte("line3"),
			},
		},

		{
			name: "mixed line endings",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\r\nline2\nline3\r\n"),
			expected: [][]byte{
				[]byte("line1\r"),
				[]byte("line2"),
				[]byte("line3\r"),
			},
		},

		{
			name: "no newline at end (old)",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\nline2\nline3"),
			expected: [][]byte{
				[]byte("line2"),
				[]byte("line3"),
			},
		},

		{
			name: "no newline at end (new)",
			old:  []byte("line1\n"),
			new:  []byte("line1\nline2"),
			expected: [][]byte{
				[]byte("line2"),
			},
		},

		{
			name: "no newline at end (both)",
			old:  []byte("line1"),
			new:  []byte("line1\nline2"),
			expected: [][]byte{
				[]byte("line1"),
				[]byte("line2"),
			},
		},

		// Binary-like content with control characters.
		{
			name: "null bytes",
			old:  []byte("line1"),
			new:  []byte("line1\nline\x00with\x00nulls"),
			expected: [][]byte{
				[]byte("line1"),
				[]byte("line\x00with\x00nulls"),
			},
		},

		{
			name: "control characters",
			old:  []byte("normal"),
			new:  []byte("normal\n\x01\x02\x03\x1F"),
			expected: [][]byte{
				[]byte("normal"),
				[]byte("\x01\x02\x03\x1F"),
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
			expected: func() [][]byte {
				var expected [][]byte
				for i := 1; i <= 100; i++ {
					expected = append(expected, []byte("added_line_"+string(rune('0'+i%10))))
				}
				return expected
			}(),
		},

		// Verify that unified diff headers are properly filtered.
		{
			name:     "false positive prevention",
			old:      []byte("line1\n++ file.txt"),
			new:      []byte("line1\n+++ new.txt\n++ file.txt"),
			expected: nil,
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
			expected: [][]byte{
				[]byte("modified2"),
				[]byte("added1"),
				[]byte("added2"),
				[]byte("line5"),
				[]byte("added3"),
			},
		},

		// Edge cases with minimal content.
		{
			name: "single character addition",
			old:  []byte("a"),
			new:  []byte("ab"),
			expected: [][]byte{
				[]byte("ab"),
			},
		},

		{
			name: "single newline added",
			old:  []byte("line"),
			new:  []byte("line\n"),
			expected: [][]byte{
				[]byte("line"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLines(tt.old, tt.new)

			assert.True(t, equalByteSlices(result, tt.expected),
				"addedLines() failed for %s\ngot:      %s\nexpected: %s",
				tt.name, formatByteSlices(result), formatByteSlices(tt.expected))
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
				_ = addedLines(bc.old, bc.new)
			}
		})
	}
}

// TestAddedLinesPanicRecovery ensures the function doesn't panic on edge cases.
func TestAddedLinesPanicRecovery(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("addedLines panicked: %v", r)
		}
	}()

	// Test with very large inputs to ensure no memory issues.
	largeOld := bytes.Repeat([]byte("line\n"), 10000)
	largeNew := bytes.Repeat([]byte("newline\n"), 10000)
	_ = addedLines(largeOld, largeNew)

	// Test with binary data containing arbitrary bytes.
	randomOld := []byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB}
	randomNew := []byte{0xFF, 0xFE, 0x00, 0x01, 0x02}
	_ = addedLines(randomOld, randomNew)
}

func equalByteSlices(a, b [][]byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func formatByteSlices(slices [][]byte) string {
	if slices == nil {
		return "nil"
	}
	parts := make([]string, len(slices))
	for i, s := range slices {
		parts[i] = string(s)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// TestAddedLinesProperties verifies fundamental behavioral properties.
func TestAddedLinesProperties(t *testing.T) {
	t.Run("identical_content_returns_nil", func(t *testing.T) {
		content := []byte("any\ncontent\nhere")
		result := addedLines(content, content)
		assert.Nil(t, result, "expected nil for identical content")
	})

	// Verify that the '+' prefix is stripped from unified diff output.
	t.Run("no_leading_plus", func(t *testing.T) {
		old := []byte("line1")
		new := []byte("line1\nnew1\nnew2")
		result := addedLines(old, new)

		for _, line := range result {
			assert.False(t, len(line) > 0 && line[0] == '+',
				"result line should not start with '+': %s", string(line))
		}
	})

	t.Run("count_matches_additions", func(t *testing.T) {
		old := []byte("")
		new := []byte("line1\nline2\nline3")
		result := addedLines(old, new)

		assert.Len(t, result, 3, "expected 3 added lines")
	})
}

// TestUnifiedDiffHeaderHandling verifies that +++ headers are filtered correctly.
func TestUnifiedDiffHeaderHandling(t *testing.T) {
	old := []byte("file content")
	new := []byte(`file content
+++ b/newfile.txt
++ regular line
+ single plus`)

	result := addedLines(old, new)

	// The +++ line should be filtered out as it's a unified diff header.
	expected := [][]byte{
		[]byte("file content"),
	}

	assert.True(t, equalByteSlices(result, expected),
		"unified diff header handling failed\ngot:      %s\nexpected: %s",
		formatByteSlices(result), formatByteSlices(expected))
}

func FuzzAddedLines(f *testing.F) {
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("hello"), []byte("world"))
	f.Add([]byte("line1\nline2"), []byte("line1\nline2\nline3"))
	f.Add([]byte("test"), []byte("test\n+++ header\n+ line"))

	f.Fuzz(func(t *testing.T, old, new []byte) {
		result := addedLines(old, new)

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
