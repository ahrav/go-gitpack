// diff_blob.go – “+” lines via gotextdiff/myers  (FIXED)
package objstore

import (
	"bytes"
	"strings"
	"unsafe"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
)

// addedLines returns every line that is *present in new but absent in old*.
// The slice elements are the raw line bytes with the trailing '\n' removed.
// The implementation relies on gotextdiff's Myers algorithm and requires Go ≥1.20
// for zero‑copy slice→string conversion via unsafe.String.
func addedLines(old, new []byte) [][]byte {
	// Short‑circuit: identical blobs
	if bytes.Equal(old, new) {
		return nil
	}

	// zero‑copy []byte → string (safe as long as old/new aren’t mutated afterwards).
	btostr := func(b []byte) string {
		if len(b) == 0 {
			return ""
		}
		return unsafe.String(&b[0], len(b))
	}
	a := btostr(old)
	b := btostr(new)

	// Compute full‑line edits.
	edits := myers.ComputeEdits(span.URIFromPath(""), a, b)
	edits = gotextdiff.LineEdits(a, edits)

	var out [][]byte
	for _, e := range edits {
		if e.NewText == "" {
			continue // pure deletions
		}
		lines := strings.Split(e.NewText, "\n")

		// Drop final empty element that follows the trailing '\n'.
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		for _, ln := range lines {
			// Filter unified‑diff headers and “+” false‑positives.
			if strings.HasPrefix(ln, "+++ ") || (len(ln) > 0 && ln[0] == '+') {
				continue
			}
			// Preserve exact bytes (including CR for CRLF).
			out = append(out, []byte(ln))
		}
	}
	return out
}
