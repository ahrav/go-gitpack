// diff_blob.go  – hunk-based diff implementation
package objstore

import (
	"bytes"
	"strings"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
)

// AddedHunk represents a contiguous block of added lines in a diff.
// Multiple consecutive insertions are grouped into a single hunk to
// provide better context for processing large additions.
type AddedHunk struct {
	// Lines contains all the added lines in this hunk, with trailing
	// newlines already stripped.
	Lines [][]byte

	// StartLine is the 1-based line number where this hunk begins
	// in the new version of the file.
	StartLine int
}

// EndLine returns the 1-based line number where this hunk ends
// in the new version of the file.
func (h *AddedHunk) EndLine() int {
	if len(h.Lines) == 0 {
		return h.StartLine
	}
	return h.StartLine + len(h.Lines) - 1
}

// addedHunksWithPos returns contiguous blocks of lines that exist in *new* but not in *old*,
// grouping consecutive insertions into hunks with their position in the new file.
//
// It performs a line-oriented diff using the Myers algorithm provided by
// github.com/hexops/gotextdiff. Consecutive insertion lines are grouped into
// a single AddedHunk to provide better context for processing large additions.
//
// If the two byte slices are identical or the diff contains no insertions,
// addedHunksWithPos returns nil.
func addedHunksWithPos(oldB, newB []byte) []AddedHunk {
	if bytes.Equal(oldB, newB) {
		return nil
	}

	a, b := btostr(oldB), btostr(newB)
	edits := myers.ComputeEdits(span.URIFromPath(""), a, b)
	u := gotextdiff.ToUnified("", "", a, edits) // structured diff

	if len(u.Hunks) == 0 {
		return nil
	}

	var hunks []AddedHunk
	var currentHunk *AddedHunk

	for _, h := range u.Hunks {
		lineNo := h.ToLine // already 1-based

		for _, ln := range h.Lines {
			switch ln.Kind {
			case gotextdiff.Insert:
				text := strings.TrimSuffix(ln.Content, "\n")

				if currentHunk == nil {
					// Start new hunk
					currentHunk = &AddedHunk{
						StartLine: lineNo,
						Lines:     [][]byte{[]byte(text)},
					}
				} else {
					// Continue current hunk
					currentHunk.Lines = append(currentHunk.Lines, []byte(text))
				}
				lineNo++

			case gotextdiff.Equal, gotextdiff.Delete:
				// End current hunk if we hit non-insertion
				if currentHunk != nil {
					hunks = append(hunks, *currentHunk)
					currentHunk = nil
				}
				if ln.Kind == gotextdiff.Equal {
					lineNo++
				}
			}
		}

		// Don't forget the last hunk at end of this diff hunk
		if currentHunk != nil {
			hunks = append(hunks, *currentHunk)
			currentHunk = nil
		}
	}

	return hunks
}
