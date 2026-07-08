package objstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// referenceAddedHunksWithPos is the addedHunksWithPos implementation exactly
// as it existed at commit 73a0f5f, before the prefix-skip, lazy-index, and
// open-addressed lineIndex optimizations. It serves as the behavioral oracle
// for differential testing: the optimized path must produce identical hunks
// for every input.
func referenceAddedHunksWithPos(oldB, newB []byte) []AddedHunk {
	if bytes.Equal(oldB, newB) {
		return nil
	}

	oldLines, newLines := tokenize(oldB), tokenize(newB)

	const threshold = 50
	var oldLinePositions map[string][]uint32
	if len(oldLines) > threshold {
		oldLinePositions = make(map[string][]uint32)
		for i, line := range oldLines {
			oldLinePositions[line] = append(oldLinePositions[line], uint32(i))
		}
	}

	var hunks []AddedHunk
	var cur *AddedHunk
	oldIdx := 0

	for newIdx, newLine := range newLines {
		lineNum := uint32(newIdx) + 1
		isAdded := false

		if oldIdx >= len(oldLines) {
			isAdded = true
		} else if newLine != oldLines[oldIdx] {
			foundLater := false

			if oldLinePositions != nil {
				if positions, exists := oldLinePositions[newLine]; exists {
					for _, pos := range positions {
						if pos >= uint32(oldIdx) {
							foundLater = true
							oldIdx = int(pos)
							break
						}
					}
				}
			} else {
				for j := oldIdx; j < len(oldLines); j++ {
					if newLine == oldLines[j] {
						foundLater = true
						oldIdx = j
						break
					}
				}
			}

			if !foundLater {
				isAdded = true
			}
		}

		if isAdded {
			if cur == nil || lineNum != cur.EndLine()+1 {
				if cur != nil {
					hunks = append(hunks, *cur)
				}
				cur = &AddedHunk{StartLine: lineNum}
			}
			cur.Lines = append(cur.Lines, newLine)
		} else {
			if cur != nil {
				hunks = append(hunks, *cur)
				cur = nil
			}
			oldIdx++
		}
	}

	if cur != nil {
		hunks = append(hunks, *cur)
	}

	return hunks
}

// genFile builds a synthetic text file from a small line vocabulary so that
// duplicate lines (the hard case for position matching) occur frequently.
func genFile(r *rand.Rand, nLines int, vocab []string, trailingNL bool) []byte {
	var buf bytes.Buffer
	for i := 0; i < nLines; i++ {
		buf.WriteString(vocab[r.Intn(len(vocab))])
		if i < nLines-1 || trailingNL {
			buf.WriteByte('\n')
		}
	}
	return buf.Bytes()
}

// mutate produces a plausible "next version" of src: random insertions,
// deletions, and replacements at line granularity.
func mutate(r *rand.Rand, src []byte, vocab []string) []byte {
	lines := bytes.Split(src, []byte{'\n'})
	nEdits := 1 + r.Intn(5)
	for e := 0; e < nEdits; e++ {
		if len(lines) == 0 {
			lines = append(lines, []byte(vocab[r.Intn(len(vocab))]))
			continue
		}
		pos := r.Intn(len(lines))
		switch r.Intn(3) {
		case 0: // insert
			lines = append(lines[:pos], append([][]byte{[]byte(vocab[r.Intn(len(vocab))])}, lines[pos:]...)...)
		case 1: // delete
			lines = append(lines[:pos], lines[pos+1:]...)
		case 2: // replace
			lines[pos] = []byte(vocab[r.Intn(len(vocab))])
		}
	}
	return bytes.Join(lines, []byte{'\n'})
}

// TestAddedHunksWithPos_DifferentialAgainstReference fuzzes the optimized
// diff against the pre-optimization implementation across a wide range of
// shapes: tiny files (linear-search path), large files (indexed path),
// heavy duplication, shared prefixes, missing trailing newlines, and empty
// sides.
func TestAddedHunksWithPos_DifferentialAgainstReference(t *testing.T) {
	t.Parallel()

	vocabs := [][]string{
		{"a", "b", "c"}, // tiny vocabulary → many duplicate lines
		{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"},
		func() []string { // large vocabulary → mostly unique lines
			v := make([]string, 200)
			for i := range v {
				v[i] = fmt.Sprintf("line-%d some content here", i)
			}
			return v
		}(),
	}

	r := rand.New(rand.NewSource(0xC0FFEE))
	for iter := 0; iter < 3000; iter++ {
		vocab := vocabs[r.Intn(len(vocabs))]
		nLines := r.Intn(300) // spans both linear (<50) and indexed (>50) paths
		trailingNL := r.Intn(2) == 0

		oldB := genFile(r, nLines, vocab, trailingNL)
		var newB []byte
		switch r.Intn(4) {
		case 0:
			newB = mutate(r, oldB, vocab)
		case 1: // mutate twice (bigger drift)
			newB = mutate(r, mutate(r, oldB, vocab), vocab)
		case 2: // unrelated file
			newB = genFile(r, r.Intn(300), vocab, trailingNL)
		case 3: // shared prefix + divergent tail (exercises prefix skip)
			tail := genFile(r, r.Intn(50), vocab, trailingNL)
			newB = append(append([]byte{}, oldB...), tail...)
		}

		want := referenceAddedHunksWithPos(oldB, newB)
		got := addedHunksWithPos(oldB, newB)

		require.Equal(t, want, got,
			"iter=%d\nold=%q\nnew=%q", iter, oldB, newB)
	}
}

// TestAddedHunksWithPos_DifferentialEdgeCases pins specific boundary shapes.
func TestAddedHunksWithPos_DifferentialEdgeCases(t *testing.T) {
	t.Parallel()

	cases := [][2]string{
		{"", "a"},
		{"a", ""},
		{"a\n", "a"},
		{"a", "a\n"},
		{"\n", ""},
		{"", "\n"},
		{"\n\n\n", "\n\n"},
		{"a\nb\nc", "a\nb\nc\nd"},
		{"a\nb\nc\n", "c\nb\na\n"},    // reorder
		{"x\nx\nx\n", "x\nx\nx\nx\n"}, // duplicates
		{"a\nb\n", "b\na\n"},          // swap
		{"common\ncommon\nold\n", "common\ncommon\nnew\n"}, // shared prefix
	}
	for i, c := range cases {
		oldB, newB := []byte(c[0]), []byte(c[1])
		want := referenceAddedHunksWithPos(oldB, newB)
		got := addedHunksWithPos(oldB, newB)
		require.Equal(t, want, got, "case %d: old=%q new=%q", i, c[0], c[1])
	}
}
