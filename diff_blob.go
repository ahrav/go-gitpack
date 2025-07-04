// diff_blob.go
//
// Diff analysis utilities for identifying blocks of added lines between two
// versions of a file. The implementation performs line-by-line comparison
// to find additions and groups consecutive added lines into "hunks" for
// efficient representation.
//
// The algorithm uses an adaptive optimization strategy:
//   - For files with >50 lines: builds a hash map for O(1) line lookups
//   - For smaller files: uses linear search to minimize memory overhead
//
// This file provides support for diff-related operations within the object
// store, useful for analyzing changes between blob objects or computing
// additions in commit diffs.
//
// The tokenization is zero-copy, creating string views into the original
// byte slices to minimize allocations during diff computation.

package objstore

import (
	"bytes"
)

// AddedHunk represents a contiguous block of added lines in a diff.
// The struct groups consecutive lines that were added to a file, tracking
// both the content and position of the additions.
type AddedHunk struct {
	// Lines contains the actual text content of the added lines.
	// Each string represents one line without its trailing newline character.
	Lines []string

	// StartLine indicates the 1-based line number where this hunk begins in the new file.
	// Line numbers start at 1 to match standard diff output conventions.
	StartLine uint32
}

// EndLine returns the 1-based line number of the last line in this hunk.
// If the hunk contains no lines, EndLine returns the StartLine value.
// For a hunk starting at line 10 with 3 lines, EndLine returns 12.
func (h *AddedHunk) EndLine() uint32 {
	if len(h.Lines) == 0 {
		return h.StartLine
	}
	return h.StartLine + uint32(len(h.Lines)) - 1
}

// tokenize splits a byte slice into individual lines without copying the underlying data.
// The function recognizes '\n' as the line delimiter and excludes it from the results.
// Empty input returns nil rather than an empty slice.
// The returned strings share memory with the input slice for zero-copy efficiency.
func tokenize(src []byte) []string {
	if len(src) == 0 {
		return nil
	}

	// Count lines first to pre-allocate exact capacity.
	lineCount := 1
	for _, c := range src {
		if c == '\n' {
			lineCount++
		}
	}

	lines := make([]string, 0, lineCount)
	start := 0
	for i, c := range src {
		if c == '\n' {
			lines = append(lines, btostr(src[start:i])) // Exclude the newline character.
			start = i + 1
		}
	}
	if start < len(src) { // Handle the last line without a newline.
		lines = append(lines, btostr(src[start:]))
	}
	return lines
}

// FuseHunks merges consecutive AddedHunks when the number of untouched lines
// between them is less than or equal to 2*ctx + inter.
//
// ctx specifies the amount of ordinary “context” you intend to display around
// each hunk, while inter represents an additional “inter-hunk” allowance
// (equivalent to Git’s --inter-hunk-context flag).
// The function never inserts the untouched lines into the resulting hunks; it
// only extends the range metadata and concatenates the added lines.
func FuseHunks(hunks []AddedHunk, ctx, inter int) []AddedHunk {
	if len(hunks) < 2 {
		return hunks
	}
	maxGap := 2*ctx + inter
	out := make([]AddedHunk, 0, len(hunks))
	cur := hunks[0]

	for i := 1; i < len(hunks); i++ {
		gap := int(hunks[i].StartLine) - int(cur.EndLine()) - 1
		if gap <= maxGap {
			// Merge – we *do not* insert the untouched lines into Lines,
			// we just extend the range & byte count.
			cur.Lines = append(cur.Lines, hunks[i].Lines...)
		} else {
			out = append(out, cur)
			cur = hunks[i]
		}
	}
	out = append(out, cur)
	return out
}

// addedHunksWithPos compares two byte slices and identifies contiguous blocks of added lines.
// The function performs a line-by-line comparison between oldB and newB to find additions.
// It groups consecutive added lines into hunks for efficient diff representation.
//
// The algorithm uses an optimized lookup strategy: for files with more than 50 lines,
// it builds a hash map for O(1) line lookups; for smaller files, it uses linear search.
// This adaptive approach balances memory usage and performance based on file size.
//
// Returns nil if the byte slices are identical.
// Returns a slice of AddedHunk structs representing all additions found in newB.
func addedHunksWithPos(oldB, newB []byte) []AddedHunk {
	// First, check for the trivial case where the files are identical.
	if bytes.Equal(oldB, newB) {
		return nil
	}

	// Tokenize the old and new byte slices into lines for comparison.
	// This is a zero-copy operation, creating string views into the original slices.
	oldLines, newLines := tokenize(oldB), tokenize(newB)

	// For larger files, create a map of old lines to their line numbers (positions).
	// This provides a significant performance boost by allowing O(1) lookups.
	// A threshold is used to avoid the overhead of map creation for small files.
	const threshold = 50
	var oldLinePositions map[string][]uint32
	if len(oldLines) > threshold {
		oldLinePositions = make(map[string][]uint32)
		for i, line := range oldLines {
			// A line may appear multiple times, so we store all its positions.
			oldLinePositions[line] = append(oldLinePositions[line], uint32(i))
		}
	}

	var hunks []AddedHunk
	var cur *AddedHunk
	oldIdx := 0

	for newIdx, newLine := range newLines {
		lineNum := uint32(newIdx) + 1
		isAdded := false

		// If we've exhausted all lines in the old file,
		// any remaining lines in the new file are additions.
		if oldIdx >= len(oldLines) {
			isAdded = true
		} else if newLine != oldLines[oldIdx] {
			// The lines do not match. We need to determine if the new line is an addition
			// or if it's a line that was moved from later in the old file.
			foundLater := false

			if oldLinePositions != nil {
				if positions, exists := oldLinePositions[newLine]; exists {
					// The line exists in the old file.
					// Check if it appears at or after our current position.
					for _, pos := range positions {
						if pos >= uint32(oldIdx) {
							// Found a match.
							// We fast-forward the old index to this position,
							// effectively treating the lines between oldIdx and pos as deleted.
							foundLater = true
							oldIdx = int(pos)
							break
						}
					}
				}
			} else {
				// For smaller files, perform a linear search to find the line.
				for j := oldIdx; j < len(oldLines); j++ {
					if newLine == oldLines[j] {
						foundLater = true
						oldIdx = j
						break
					}
				}
			}

			// If the line was not found later in the old file,
			// it's a genuine addition.
			if !foundLater {
				isAdded = true
			}
		}

		if isAdded {
			// This line is an addition. Add it to the current hunk.
			if cur == nil || lineNum != cur.EndLine()+1 {
				// If there's no current hunk or the new line is not contiguous
				// with the previous one,
				// finalize the previous hunk (if it exists) and start a new one.
				if cur != nil {
					hunks = append(hunks, *cur)
				}
				cur = &AddedHunk{StartLine: lineNum}
			}
			cur.Lines = append(cur.Lines, newLine)
		} else {
			// The line is not an addition;
			// it matches the corresponding line in the old file.
			// If we were building a hunk, it's now complete.
			if cur != nil {
				hunks = append(hunks, *cur)
				cur = nil
			}
			oldIdx++
		}
	}

	// After the loop, if there's still an open hunk, add it to the list.
	// This handles cases where the file ends with a block of added lines.
	if cur != nil {
		hunks = append(hunks, *cur)
	}

	return hunks
}
