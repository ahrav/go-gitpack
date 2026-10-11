package objstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// lcsLen is the O(NM) dynamic-programming longest-common-subsequence length,
// the oracle for the minimality of an added-line set.
func lcsLen(a, b []string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				cur[j] = prev[j-1] + 1
			} else {
				cur[j] = max(prev[j], cur[j-1])
			}
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// requireMinimalAddedHunks checks hunks against the definition of the diff:
// every hunk line is the new line at its position, no line is reported
// twice, the unreported new lines form a subsequence of the old lines, the
// reported count equals len(new) - LCS(old, new), and hunks are maximal runs
// in increasing order. Any longest common subsequence satisfies this, so the
// check is insensitive to tie-breaking between equally short edit scripts.
func requireMinimalAddedHunks(t testing.TB, oldB, newB []byte, hunks []AddedHunk) {
	t.Helper()
	oldLines, newLines := tokenize(oldB), tokenize(newB)
	addedAt := make([]bool, len(newLines))
	addedCount := 0
	for i, h := range hunks {
		require.NotEmpty(t, h.Lines, "hunk %d is empty\nold=%q\nnew=%q", i, oldB, newB)
		if i > 0 {
			prev := hunks[i-1]
			require.Greater(t, h.StartLine, prev.StartLine+uint32(len(prev.Lines)), "hunks %d and %d are adjacent or out of order\nold=%q\nnew=%q\nhunks=%v", i-1, i, oldB, newB, hunks)
		}
		for j, l := range h.Lines {
			idx := int(h.StartLine) - 1 + j
			require.True(t, idx >= 0 && idx < len(newLines), "hunk line %d outside new file\nold=%q\nnew=%q", idx+1, oldB, newB)
			require.Equal(t, newLines[idx], l, "hunk line %d\nold=%q\nnew=%q", idx+1, oldB, newB)
			require.False(t, addedAt[idx], "line %d reported twice", idx+1)
			addedAt[idx] = true
			addedCount++
		}
	}
	j := 0
	for i, l := range newLines {
		if addedAt[i] {
			continue
		}
		for j < len(oldLines) && oldLines[j] != l {
			j++
		}
		require.Less(t, j, len(oldLines), "unreported lines are not a subsequence of old\nold=%q\nnew=%q\nhunks=%v", oldB, newB, hunks)
		j++
	}
	require.Equal(t, len(newLines)-lcsLen(oldLines, newLines), addedCount, "added-line count\nold=%q\nnew=%q\nhunks=%v", oldB, newB, hunks)
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

// TestAddedHunksWithPos_DifferentialAgainstReference fuzzes the diff against
// the LCS oracle across a wide range of shapes: tiny and large files, heavy
// duplication, shared prefixes, missing trailing newlines, and empty sides.
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

		requireMinimalAddedHunks(t, oldB, newB, addedHunksWithPos(oldB, newB))
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
	for _, c := range cases {
		oldB, newB := []byte(c[0]), []byte(c[1])
		requireMinimalAddedHunks(t, oldB, newB, addedHunksWithPos(oldB, newB))
	}
}
