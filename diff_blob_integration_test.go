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

func TestAddedHunksIntegration(t *testing.T) {
	tests := []struct {
		name       string
		old        []byte
		new        []byte
		wantInDiff []string    // Strings we expect to see in the unified diff
		wantHunks  []AddedHunk // Expected output from addedHunksWithPos
	}{
		{
			name: "verify_unified_diff_format_with_hunks",
			old:  []byte("line1\nline2"),
			new:  []byte("line1\ninserted\nline2\nadded"),
			wantInDiff: []string{
				"--- ",
				"+++ ",
				"@@ ",
				"+inserted",
				"+added",
			},
			wantHunks: []AddedHunk{
				{StartLine: 2, Lines: []string{"inserted"}},
				{StartLine: 4, Lines: []string{"added"}},
			},
		},
		{
			name: "multiple_separate_hunks",
			old:  []byte("line1\nline3\nline5"),
			new:  []byte("line1\nline2\nline3\nline4\nline5\nline6"),
			wantInDiff: []string{
				"+line2",
				"+line4",
				"+line6",
			},
			wantHunks: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2"}},
				{StartLine: 4, Lines: []string{"line4"}},
				{StartLine: 6, Lines: []string{"line6"}},
			},
		},
		{
			name: "consecutive_insertions_form_single_hunk",
			old:  []byte("line1\nline5"),
			new:  []byte("line1\nline2\nline3\nline4\nline5"),
			wantInDiff: []string{
				"+line2",
				"+line3",
				"+line4",
			},
			wantHunks: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2", "line3", "line4"}},
			},
		},
		{
			name: "hunk_header_not_captured",
			old:  []byte("a\n"),
			new:  []byte("a\n@@ -1,1 +1,2 @@"),
			wantInDiff: []string{
				"+@@ -1,1 +1,2 @@",
			},
			wantHunks: []AddedHunk{
				{StartLine: 2, Lines: []string{"@@ -1,1 +1,2 @@"}},
			},
		},
		{
			name: "content_that_looks_like_diff_headers",
			old:  []byte("content\n"),
			new:  []byte("content\n--- a/file\n+++ b/file"),
			wantInDiff: []string{
				"+--- a/file",
				"++++ b/file",
			},
			wantHunks: []AddedHunk{
				{StartLine: 2, Lines: []string{"--- a/file", "+++ b/file"}},
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
			// Test addedHunksWithPos function.
			result := addedHunksWithPos(tt.old, tt.new)
			assert.Equal(t, tt.wantHunks, result,
				"addedHunksWithPos() test failed")

			// Verify EndLine calculations for all hunks
			for i, hunk := range result {
				var expectedEndLine uint32
				if len(hunk.Lines) == 0 {
					expectedEndLine = hunk.StartLine
				} else {
					expectedEndLine = hunk.StartLine + uint32(len(hunk.Lines)) - 1
				}
				assert.Equal(t, expectedEndLine, hunk.EndLine(),
					"EndLine calculation incorrect for hunk %d", i)
			}
		})
	}
}

func TestAddedHunksEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		old      []byte
		new      []byte
		expected []AddedHunk
	}{
		{
			name:     "identical_content",
			old:      []byte("same\ncontent"),
			new:      []byte("same\ncontent"),
			expected: nil,
		},
		{
			name: "single_line_addition",
			old:  []byte("line1"),
			new:  []byte("line1\nline2"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"line2"}},
			},
		},
		{
			name: "empty_to_content",
			old:  []byte(""),
			new:  []byte("new\ncontent"),
			expected: []AddedHunk{
				{StartLine: 1, Lines: []string{"new", "content"}},
			},
		},
		{
			name:     "content_to_empty",
			old:      []byte("old\ncontent"),
			new:      []byte(""),
			expected: nil,
		},
		{
			name: "interspersed_additions",
			old:  []byte("keep1\nkeep3\nkeep5"),
			new:  []byte("keep1\nadd2\nkeep3\nadd4\nkeep5\nadd6"),
			expected: []AddedHunk{
				{StartLine: 2, Lines: []string{"add2"}},
				{StartLine: 4, Lines: []string{"add4"}},
				{StartLine: 6, Lines: []string{"add6"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addedHunksWithPos(tt.old, tt.new)
			assert.Equal(t, tt.expected, result)

			// Validate all hunks have proper properties
			for i, hunk := range result {
				assert.Greater(t, hunk.StartLine, uint32(0), "Hunk %d StartLine should be positive", i)
				assert.NotEmpty(t, hunk.Lines, "Hunk %d should have lines", i)
				var expectedEndLine uint32
				if len(hunk.Lines) == 0 {
					expectedEndLine = hunk.StartLine
				} else {
					expectedEndLine = hunk.StartLine + uint32(len(hunk.Lines)) - 1
				}
				assert.Equal(t, expectedEndLine, hunk.EndLine(),
					"Hunk %d EndLine calculation should be correct", i)
			}
		})
	}
}

func BenchmarkAddedHunksIntegration(b *testing.B) {
	old := []byte(strings.Repeat("line\n", 1000))
	new := []byte(strings.Repeat("line\nnew\n", 1000))

	b.Run("Hunks", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = addedHunksWithPos(old, new)
		}
	})
}

// Helper function to generate actual unified diff for debugging.
func generateUnifiedDiff(old, new []byte) string {
	a := string(old)
	b := string(new)

	edits := myers.ComputeEdits(span.URIFromPath(""), a, b)
	unified := gotextdiff.ToUnified("", "", a, edits)
	return fmt.Sprint(unified)
}
