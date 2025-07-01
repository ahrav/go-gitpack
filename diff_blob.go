// diff_blob.go  – final "all additions" implementation
package objstore

import (
	"bytes"
	"strings"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
)

// AddedLine describes one line that exists in the _new_ revision of a file
// but not in the _old_ revision produced by a diff.
// It is returned by addedLinesWithPos and serves as a minimal, value-type
// representation of an insertion hunk: the caller gets the inserted text
// together with the 1-based line number at which it appears in the new file.
// Consumers should treat the fields as read-only; the zero value is not
// meaningful.
type AddedLine struct {
	// Text holds the entire contents of the inserted line with the trailing
	// newline already stripped.
	Text []byte

	// Line records the 1-based line number of the inserted line in the new
	// version of the file.
	Line int
}

// addedLinesWithPos returns every line that exists in *new* but not in *old*,
// preserving the 1-based line number in the new file.
//
// It performs a line-oriented diff using the Myers algorithm provided by
// github.com/hexops/gotextdiff. Only insertion hunks are converted to
// AddedLine values; unchanged and deletion hunks are ignored.
//
// The function pre-allocates the result slice to roughly half the number of
// edits, a heuristic that tracks the fact that an insertion hunk generally
// consumes two edits (one Insert, one Equal).
// If the two byte slices are identical or the diff contains no insertions,
// addedLinesWithPos returns nil.
func addedLinesWithPos(oldB, newB []byte) []AddedLine {
	if bytes.Equal(oldB, newB) {
		return nil
	}

	a, b := btostr(oldB), btostr(newB)
	edits := myers.ComputeEdits(span.URIFromPath(""), a, b)
	u := gotextdiff.ToUnified("", "", a, edits) // structured diff

	if len(u.Hunks) == 0 {
		return nil
	}

	// Rough capacity: #insert hunks ≈ len(edits)/2 (heuristic).
	out := make([]AddedLine, 0, len(edits))

	for _, h := range u.Hunks {
		lineNo := h.ToLine // already 1-based

		for _, ln := range h.Lines {
			switch ln.Kind {
			case gotextdiff.Insert:
				// ln.Content still ends with "\n"; trim cheaply.
				text := strings.TrimSuffix(ln.Content, "\n")
				out = append(out, AddedLine{Text: []byte(text), Line: lineNo})
				lineNo++
			case gotextdiff.Equal:
				lineNo++
			}
		}
	}
	return out
}
