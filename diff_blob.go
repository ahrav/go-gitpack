// diff_blob.go – “+” lines via gotextdiff/myers  (FIXED)
package objstore

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
)

// addedLines returns every line that was *inserted* in new with respect to old.
// The leading ‘+’ from each inserted line in the unified diff is stripped
// before the line is returned.
func addedLines(old, new []byte) [][]byte {
	if len(old) == len(new) && bytes.Equal(old, new) {
		return nil
	}

	// gotextdiff works on strings, so convert once up front.
	a := string(old)
	b := string(new)

	edits := myers.ComputeEdits(span.URIFromPath(""), a, b)
	unified := gotextdiff.ToUnified("", "", a, edits)

	// fmt.Sprint invokes Unified.Format, yielding the unified-diff text.
	diffText := fmt.Sprint(unified)

	var out [][]byte
	for l := range strings.SplitSeq(diffText, "\n") {
		// A line that starts with “+” and is not the “+++ …” header
		// represents an insertion we care about.
		if len(l) > 0 && l[0] == '+' && !(len(l) > 1 && l[1] == '+') {
			out = append(out, []byte(l[1:]))
		}
	}
	return out
}
