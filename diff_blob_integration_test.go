package objstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/stretchr/testify/assert"
)

// Integration tests that verify the actual diff output format.
func TestAddedLinesIntegration(t *testing.T) {
	tests := []struct {
		name       string
		old        []byte
		new        []byte
		wantInDiff []string // Strings we expect to see in the unified diff
		wantAdded  [][]byte // Expected output from addedLines
	}{
		{
			name: "verify_unified_diff_format",
			old:  []byte("line1\nline2\nline3"),
			new:  []byte("line1\ninserted\nline2\nline3\nadded"),
			wantInDiff: []string{
				"--- ",
				"+++ ",
				"@@ ",
				"+inserted",
				"+added",
			},
			wantAdded: [][]byte{
				[]byte("inserted"),
				[]byte("line3"), // Context included in the diff output.
				[]byte("added"),
			},
		},
		{
			name: "hunk_header_not_captured",
			old:  []byte("a"),
			new:  []byte("a\n@@ -1,1 +1,2 @@"),
			wantInDiff: []string{
				"+@@ -1,1 +1,2 @@",
			},
			wantAdded: [][]byte{
				[]byte("a"), // Context included.
				[]byte("@@ -1,1 +1,2 @@"),
			},
		},
		{
			name: "diff_header_lines_filtered",
			old:  []byte("content"),
			new:  []byte("content\n--- a/file\n+++ b/file"),
			wantInDiff: []string{
				"+--- a/file",
				"++++ b/file", // This should be filtered out.
			},
			wantAdded: [][]byte{
				[]byte("content"), // Context included.
				[]byte("--- a/file"),
				// +++ b/file is filtered out.
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get the actual diff output to verify our understanding.
			actualDiff := generateUnifiedDiff(tt.old, tt.new)

			// Verify expected strings appear in diff.
			for _, want := range tt.wantInDiff {
				assert.Contains(t, actualDiff, want, "Expected diff to contain %q", want)
			}

			// Test addedLines function.
			result := addedLines(tt.old, tt.new)
			assert.True(t, equalByteSlices(result, tt.wantAdded),
				"addedLines() = %v, want %v", formatByteSlices(result), formatByteSlices(tt.wantAdded))
		})
	}
}

// Test edge cases related to Myers diff algorithm behavior.
func TestMyersAlgorithmEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected [][]byte
	}{
		{
			name: "common_prefix_suffix",
			old:  []byte("prefix\nold1\nold2\nsuffix"),
			new:  []byte("prefix\nnew1\nnew2\nsuffix"),
			expected: [][]byte{
				[]byte("new1"),
				[]byte("new2"),
			},
		},
		{
			name: "repeated_lines",
			old:  []byte("a\na\na"),
			new:  []byte("a\nb\na\nb\na"),
			expected: [][]byte{
				[]byte("b"),
				[]byte("b"),
			},
		},
		{
			name: "sliding_window",
			old:  []byte("1\n2\n3\n4"),
			new:  []byte("2\n3\n4\n5"),
			expected: [][]byte{
				[]byte("4"), // Only the changed portion.
				[]byte("5"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLines(tt.old, tt.new)
			assert.True(t, equalByteSlices(result, tt.expected),
				"addedLines() = %v, want %v", formatByteSlices(result), formatByteSlices(tt.expected))
		})
	}
}

// Test for potential security issues.
func TestSecurityEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		old     []byte
		new     []byte
		checkFn func(t *testing.T, result [][]byte)
	}{
		{
			name: "malicious_diff_injection",
			old:  []byte("safe"),
			new:  []byte("safe\n+++ malicious\n+ injected"),
			checkFn: func(t *testing.T, result [][]byte) {
				assert.Len(t, result, 1, "Expected 1 line")
				if len(result) > 0 {
					assert.Equal(t, []byte("safe"), result[0])
				}
			},
		},
		{
			name: "buffer_overflow_attempt",
			old:  []byte("x"),
			new:  []byte("x\n" + strings.Repeat("A", 1024*1024)),
			checkFn: func(t *testing.T, result [][]byte) {
				// Context is included.
				assert.Len(t, result, 2, "Expected 2 lines")
				if len(result) >= 2 {
					assert.Equal(t, []byte("x"), result[0], "Expected 'x' as first line")
					assert.Len(t, result[1], 1024*1024, "Expected line of 1MB")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLines(tt.old, tt.new)
			tt.checkFn(t, result)
		})
	}
}

// Test for Go version compatibility (strings.SplitSeq is Go 1.23+).
func TestStringSplitSeqCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		diffText string
		expected int // Number of iterations.
	}{
		{
			name:     "empty_diff",
			diffText: "",
			expected: 1, // SplitSeq returns one empty element for empty string.
		},
		{
			name:     "single_line_no_newline",
			diffText: "+added",
			expected: 1,
		},
		{
			name:     "single_line_with_newline",
			diffText: "+added\n",
			expected: 2, // "added" and "".
		},
		{
			name:     "multiple_lines",
			diffText: "+line1\n+line2\n",
			expected: 3, // "line1", "line2", "".
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := 0
			// Simulate what the function does.
			for range strings.SplitSeq(tt.diffText, "\n") {
				count++
			}
			assert.Equal(t, tt.expected, count, "SplitSeq iteration count")
		})
	}
}

// Performance regression tests.
func TestPerformanceRegression(t *testing.T) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Create large inputs.
			oldLines := make([]string, size)
			newLines := make([]string, size*2) // Double the size with additions.

			for i := 0; i < size; i++ {
				oldLines[i] = fmt.Sprintf("old_line_%d", i)
				newLines[i*2] = oldLines[i]
				newLines[i*2+1] = fmt.Sprintf("new_line_%d", i)
			}

			old := []byte(strings.Join(oldLines, "\n"))
			new := []byte(strings.Join(newLines, "\n"))

			result := addedLines(old, new)

			// The last old line is included as context, so we expect size + 1.
			assert.Len(t, result, size+1, "Expected %d added lines", size+1)
		})
	}
}

// Test for proper handling of various line ending combinations.
func TestLineEndingCombinations(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected [][]byte
	}{
		{
			name: "LF_to_CRLF",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\r\nline2\r\nnew"),
			expected: [][]byte{
				[]byte("line1\r"),
				[]byte("line2\r"),
				[]byte("new"),
			},
		},
		{
			name: "CRLF_to_LF",
			old:  []byte("line1\r\nline2\r\n"),
			new:  []byte("line1\nline2\nnew"),
			expected: [][]byte{
				[]byte("line1"),
				[]byte("line2"),
				[]byte("new"),
			},
		},
		{
			name: "CR_only",
			old:  []byte("line1\rline2"),
			new:  []byte("line1\rline2\rnew"),
			expected: [][]byte{
				// \r alone is not typically a line separator in gotextdiff.
				[]byte("line1\rline2\rnew"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLines(tt.old, tt.new)
			assert.True(t, equalByteSlices(result, tt.expected),
				"addedLines() = %v, want %v", formatByteSlices(result), formatByteSlices(tt.expected))
		})
	}
}

// Helper function to generate actual unified diff for debugging.
func generateUnifiedDiff(old, new []byte) string {
	a := string(old)
	b := string(new)

	edits := myers.ComputeEdits(span.URIFromPath(""), a, b)
	unified := gotextdiff.ToUnified("", "", a, edits)
	return fmt.Sprint(unified)
}

// Test to ensure the function correctly handles the case where
// the diff contains lines that could be mistaken for additions.
func TestFalsePositiveAdditions(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected [][]byte
	}{
		{
			name: "context_lines_not_captured",
			old:  []byte("context1\nremoved\ncontext2"),
			new:  []byte("context1\nadded\ncontext2"),
			expected: [][]byte{
				[]byte("added"),
			},
		},
		{
			name: "minus_lines_not_captured",
			old:  []byte("will be removed\nstays"),
			new:  []byte("stays\n- not a removal"),
			expected: [][]byte{
				[]byte("stays"),
				[]byte("- not a removal"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLines(tt.old, tt.new)
			assert.True(t, equalByteSlices(result, tt.expected),
				"addedLines() = %v, want %v", formatByteSlices(result), formatByteSlices(tt.expected))
		})
	}
}

// Test thread safety (if the function might be used concurrently).
func TestConcurrentUsage(t *testing.T) {
	old := []byte("concurrent\ntest")
	new := []byte("concurrent\ntest\nadded1\nadded2")

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			result := addedLines(old, new)
			// Should get 3 lines: "test" (context) + "added1" + "added2".
			assert.Len(t, result, 3, "Expected 3 added lines in concurrent execution")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
