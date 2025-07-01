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

func TestAddedLinesIntegration(t *testing.T) {
	tests := []struct {
		name       string
		old        []byte
		new        []byte
		wantInDiff []string    // Strings we expect to see in the unified diff
		wantAdded  []AddedLine // Expected output from addedLinesWithPos
	}{
		{
			// Simplified to avoid Myers algorithm complexity with middle insertions
			name: "verify_unified_diff_format",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\ninserted\nline2\nadded"),
			wantInDiff: []string{
				"--- ",
				"+++ ",
				"@@ ",
				"+inserted",
				"+added",
			},
			wantAdded: []AddedLine{
				{Text: []byte("inserted"), Line: 2},
				{Text: []byte("line2"), Line: 3},
				{Text: []byte("added"), Line: 4},
			},
		},
		{
			name: "hunk_header_not_captured",
			old:  []byte("a\n"), // Add newline for consistency
			new:  []byte("a\n@@ -1,1 +1,2 @@"),
			wantInDiff: []string{
				"+@@ -1,1 +1,2 @@",
			},
			wantAdded: []AddedLine{
				{Text: []byte("@@ -1,1 +1,2 @@"), Line: 2},
			},
		},
		{
			name: "actual_content_that_looks_like_diff_headers",
			old:  []byte("content\n"), // Add newline for consistency
			new:  []byte("content\n--- a/file\n+++ b/file"),
			wantInDiff: []string{
				"+--- a/file",
				"++++ b/file",
			},
			wantAdded: []AddedLine{
				{Text: []byte("--- a/file"), Line: 2},
				{Text: []byte("+++ b/file"), Line: 3},
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
			// Test addedLinesWithPos function.
			result := addedLinesWithPos(tt.old, tt.new)
			assert.Equal(t, tt.wantAdded, result,
				"addedLinesWithPos() test failed")
		})
	}
}

func TestMyersAlgorithmEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected []AddedLine
	}{
		{
			name: "common_prefix_suffix",
			old:  []byte("prefix\nold1\nold2\nsuffix"),
			new:  []byte("prefix\nnew1\nnew2\nsuffix"),
			expected: []AddedLine{
				{Text: []byte("new1"), Line: 2},
				{Text: []byte("new2"), Line: 3},
			},
		},
		{
			name: "repeated_lines",
			old:  []byte("a\na\na"),
			new:  []byte("a\nb\na\nb\na"),
			expected: []AddedLine{
				{Text: []byte("b"), Line: 2},
				{Text: []byte("b"), Line: 4},
			},
		},
		{
			name: "sliding_window",
			old:  []byte("1\n2\n3\n4"),
			new:  []byte("2\n3\n4\n5"),
			expected: []AddedLine{
				{Text: []byte("4"), Line: 3},
				{Text: []byte("5"), Line: 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLinesWithPos(tt.old, tt.new)
			assert.Equal(t, tt.expected, result,
				"addedLinesWithPos() test failed")
		})
	}
}

func TestSecurityEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		old     []byte
		new     []byte
		checkFn func(t *testing.T, result []AddedLine)
	}{
		{
			name: "malicious_diff_injection",
			old:  []byte("safe"),
			new:  []byte("safe\n+++ malicious\n+ injected"),
			checkFn: func(t *testing.T, result []AddedLine) {
				assert.Len(t, result, 3, "Expected 3 lines")
				if len(result) >= 3 {
					assert.Equal(t, []byte("safe"), result[0].Text)
					assert.Equal(t, []byte("+++ malicious"), result[1].Text)
					assert.Equal(t, []byte("+ injected"), result[2].Text)
					assert.Equal(t, 1, result[0].Line)
					assert.Equal(t, 2, result[1].Line)
					assert.Equal(t, 3, result[2].Line)
				}
			},
		},
		{
			name: "buffer_overflow_attempt",
			old:  []byte("x"),
			new:  []byte("x\n" + strings.Repeat("A", 1024*1024)),
			checkFn: func(t *testing.T, result []AddedLine) {
				assert.Len(t, result, 2, "Expected 2 lines")
				if len(result) >= 2 {
					assert.Equal(t, []byte("x"), result[0].Text)
					assert.Len(t, result[1].Text, 1024*1024)
					assert.Equal(t, 1, result[0].Line)
					assert.Equal(t, 2, result[1].Line)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLinesWithPos(tt.old, tt.new)
			tt.checkFn(t, result)
		})
	}
}

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

func TestLineEndingCombinations(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected []AddedLine
	}{
		{
			name: "LF_to_CRLF",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\r\nline2\r\nnew"),
			expected: []AddedLine{
				{Text: []byte("line1\r"), Line: 1},
				{Text: []byte("line2\r"), Line: 2},
				{Text: []byte("new"), Line: 3},
			},
		},
		{
			name: "CRLF_to_LF",
			old:  []byte("line1\r\nline2\r\n"),
			new:  []byte("line1\nline2\nnew"),
			expected: []AddedLine{
				{Text: []byte("line1"), Line: 1},
				{Text: []byte("line2"), Line: 2},
				{Text: []byte("new"), Line: 3},
			},
		},
		{
			name: "CR_only",
			old:  []byte("line1\rline2"),
			new:  []byte("line1\rline2\rnew"),
			expected: []AddedLine{
				// \r alone is not typically a line separator in gotextdiff.
				{Text: []byte("line1\rline2\rnew"), Line: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLinesWithPos(tt.old, tt.new)
			assert.Equal(t, tt.expected, result,
				"addedLinesWithPos() test failed")
		})
	}
}

func TestFalsePositiveAdditions(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected []AddedLine
	}{
		{
			name: "context_lines_not_captured",
			old:  []byte("context1\nremoved\ncontext2"),
			new:  []byte("context1\nadded\ncontext2"),
			expected: []AddedLine{
				{Text: []byte("added"), Line: 2},
			},
		},
		{
			name: "minus_lines_not_captured",
			old:  []byte("will be removed\nstays"),
			new:  []byte("stays\n- not a removal"),
			expected: []AddedLine{
				{Text: []byte("stays"), Line: 1},
				{Text: []byte("- not a removal"), Line: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedLinesWithPos(tt.old, tt.new)
			assert.Equal(t, tt.expected, result,
				"addedLinesWithPos() test failed")
		})
	}
}

func TestConcurrentUsage(t *testing.T) {
	old := []byte("concurrent\ntest")
	new := []byte("concurrent\ntest\nadded1\nadded2")

	done := make(chan bool, 10)
	for range 10 {
		go func() {
			result := addedLinesWithPos(old, new)
			// Should get 3 lines: "test" (context) + "added1" + "added2".
			assert.Len(t, result, 3, "Expected 3 added lines in concurrent execution")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
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
