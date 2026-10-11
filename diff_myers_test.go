package objstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddedLinesMyersMinimalOnLargerEdits(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	vocab := make([]string, 40)
	for i := range vocab {
		vocab[i] = fmt.Sprintf("v%d", i)
	}
	vocab = append(vocab, "", "}", "{")
	pick := func() string { return vocab[r.Intn(len(vocab))] }
	for iter := 0; iter < 400; iter++ {
		oldLines := make([]string, 50+r.Intn(400))
		for i := range oldLines {
			oldLines[i] = pick()
		}
		var newLines []string
		switch r.Intn(3) {
		case 0: // many scattered edits
			newLines = append([]string(nil), oldLines...)
			for e := 0; e < 1+r.Intn(40); e++ {
				pos := r.Intn(len(newLines) + 1)
				switch r.Intn(3) {
				case 0:
					newLines = append(newLines[:pos], append([]string{pick()}, newLines[pos:]...)...)
				case 1:
					if pos < len(newLines) {
						newLines = append(newLines[:pos], newLines[pos+1:]...)
					}
				case 2:
					if pos < len(newLines) {
						newLines[pos] = pick()
					}
				}
			}
		case 1: // block insertion whose lines repeat elsewhere in the file
			pos := r.Intn(len(oldLines))
			block := make([]string, 3+r.Intn(30))
			for i := range block {
				block[i] = pick()
			}
			newLines = append(append(append([]string(nil), oldLines[:pos]...), block...), oldLines[pos:]...)
		case 2: // unrelated content
			newLines = make([]string, r.Intn(400))
			for i := range newLines {
				newLines[i] = pick()
			}
		}
		oldB := []byte(strings.Join(oldLines, "\n") + "\n")
		newB := []byte(strings.Join(newLines, "\n") + "\n")
		if bytes.Equal(oldB, newB) {
			continue
		}
		requireMinimalAddedHunks(t, oldB, newB, addedHunksWithPos(oldB, newB))
	}
}

// A block inserted before an identical line is reported as the block alone;
// the greedy walk this replaced jumped to the later copy and reported the
// rest of the file.
func TestAddedHunksInsertedBlockBeforeRepeatedLine(t *testing.T) {
	oldB := []byte("case a:\n\tcfg: simple\n\tx: 1\ncase b:\n\tcfg: simple\n\tx: 2\n")
	newB := []byte("case a:\n\tcfg: simple\n\tkey: SECRET\n\tx: 1\ncase b:\n\tcfg: simple\n\tx: 2\n")
	require.Equal(t, []AddedHunk{{StartLine: 3, Lines: []string{"\tkey: SECRET"}}}, addedHunksWithPos(oldB, newB))

	oldB = []byte("import (\n\t\"a\"\n\t\"utils\"\n\t\"b\"\n\t\"secrets\"\n)\ntoken := \"sha256~abc\"\n")
	newB = []byte("import (\n\t\"a\"\n\t\"b\"\n\t\"utils\"\n\n\t\"secrets\"\n)\ntoken := \"sha256~abc\"\n")
	hunks := addedHunksWithPos(oldB, newB)
	requireMinimalAddedHunks(t, oldB, newB, hunks)
	for _, h := range hunks {
		require.NotContains(t, h.Lines, "token := \"sha256~abc\"", "an unchanged line is never reported")
	}
}

// Beyond the work budget the greedy walk answers; its output is a valid
// added-line set, here the whole new file.
func TestAddedLinesMyersFallbackBeyondBudget(t *testing.T) {
	const n = 20000 // (n+m)·(n+m)/2 comparisons exceed the budget
	oldLines := make([]string, n)
	newLines := make([]string, n)
	for i := range oldLines {
		oldLines[i] = fmt.Sprintf("old-%d", i)
		newLines[i] = fmt.Sprintf("new-%d", i)
	}
	oldB := []byte(strings.Join(oldLines, "\n") + "\n")
	newB := []byte(strings.Join(newLines, "\n") + "\n")
	sc := getDiffScratch()
	defer putDiffScratch(sc)
	require.False(t, addedLinesMyers(tokenize(oldB), tokenize(newB), sc))
	hunks := addedHunksWithPos(oldB, newB)
	require.Len(t, hunks, 1)
	require.Equal(t, uint32(1), hunks[0].StartLine)
	require.Len(t, hunks[0].Lines, n)
}

// TestEditDistanceLowerBound pins the bound against brute-force edit
// distances on small random line sequences: never above the true distance,
// and exact when every line is unique to its side.
func TestEditDistanceLowerBound(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	sc := getDiffScratch()
	defer putDiffScratch(sc)
	for iter := 0; iter < 500; iter++ {
		n, m := r.Intn(12), r.Intn(12)
		a := make([]string, n)
		b := make([]string, m)
		for i := range a {
			a[i] = fmt.Sprintf("l%d", r.Intn(8))
		}
		for i := range b {
			b[i] = fmt.Sprintf("l%d", r.Intn(8))
		}
		fa := lineFingerprints(nil, a)
		fb := lineFingerprints(nil, b)
		lb := editDistanceLowerBound(fa, fb, sc)
		require.LessOrEqual(t, lb, n+m-2*lcsLen(a, b), "a=%v b=%v", a, b)
		require.GreaterOrEqual(t, lb, 0)
	}
	a := []string{"a", "b", "c"}
	b := []string{"x", "y"}
	require.Equal(t, 5, editDistanceLowerBound(lineFingerprints(nil, a), lineFingerprints(nil, b), sc))
	require.Equal(t, 0, editDistanceLowerBound(lineFingerprints(nil, a), lineFingerprints(nil, a), sc))
}

// TestAddedLinesMyersBoundSkipsHopelessSearch pins that a large pair whose
// lower bound exceeds the budget is refused before the search, with the
// same answer as the search at the budget would give.
func TestAddedLinesMyersBoundSkipsHopelessSearch(t *testing.T) {
	const n = 20000
	oldLines := make([]string, n)
	newLines := make([]string, n)
	for i := range oldLines {
		oldLines[i] = fmt.Sprintf("old-%d", i)
		newLines[i] = fmt.Sprintf("new-%d", i)
	}
	sc := getDiffScratch()
	defer putDiffScratch(sc)
	fa := lineFingerprints(nil, oldLines)
	fb := lineFingerprints(nil, newLines)
	half := min((2*n+1)/2, myersWorkBudget/(2*n+1)+1)
	require.Greater(t, editDistanceLowerBound(fa, fb, sc), 2*min((2*n+1)/2, half))
	start := time.Now()
	require.False(t, addedLinesMyers(oldLines, newLines, sc))
	require.Less(t, time.Since(start), time.Second)
}

func TestAddedLinesMyersWholesaleRewriteWithinBudget(t *testing.T) {
	// A full rewrite of a 1024-line file: edit distance 2048, 2M comparisons.
	oldLines := make([]string, 1024)
	newLines := make([]string, 1024)
	for i := range oldLines {
		oldLines[i] = fmt.Sprintf("old-%d", i)
		newLines[i] = fmt.Sprintf("new-%d", i)
	}
	sc := getDiffScratch()
	defer putDiffScratch(sc)
	require.True(t, addedLinesMyers(oldLines, newLines, sc))
	for i := range newLines {
		require.True(t, sc.added[i])
	}
}

func TestCommonSuffixLineBoundary(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"a\nb\nc\n", "x\nb\nc\n", 4},
		{"a\nb\nc\n", "a\nb\nc\n", 6},
		{"ab\nc\n", "xb\nc\n", 2},
		{"ab\nc", "xb\nc", 1},
		{"abc", "xbc", 0},
		{"", "x\n", 0},
		{"c\n", "x\nc\n", 2},
		{"a\nc\n", "c\n", 2},
	}
	for _, c := range cases {
		require.Equal(t, c.want, commonSuffixLineBoundary([]byte(c.a), []byte(c.b)), "commonSuffixLineBoundary(%q, %q)", c.a, c.b)
	}
}

// commonSuffixLineBoundaryRef is the byte-at-a-time definition the chunked
// implementation must match.
func commonSuffixLineBoundaryRef(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	if i == 0 {
		return 0
	}
	startA, startB := len(a)-i, len(b)-i
	if (startA == 0 || a[startA-1] == '\n') && (startB == 0 || b[startB-1] == '\n') {
		return i
	}
	j := bytes.IndexByte(a[startA:], '\n')
	if j < 0 {
		return 0
	}
	return i - j - 1
}

// TestCommonSuffixLineBoundaryMatchesReference drives the chunked suffix
// compare across mismatch positions on every side of its 256-byte and 8-byte
// steps, with and without newlines near the boundary, against the byte loop.
func TestCommonSuffixLineBoundaryMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 20000; iter++ {
		n := rng.Intn(1200)
		tail := make([]byte, n)
		for i := range tail {
			if rng.Intn(6) == 0 {
				tail[i] = '\n'
			} else {
				tail[i] = byte('a' + rng.Intn(4))
			}
		}
		headA := make([]byte, rng.Intn(40))
		headB := make([]byte, rng.Intn(40))
		for i := range headA {
			headA[i] = byte(rng.Intn(256))
		}
		for i := range headB {
			headB[i] = byte(rng.Intn(256))
		}
		a := append(append([]byte(nil), headA...), tail...)
		b := append(append([]byte(nil), headB...), tail...)
		if n > 0 && rng.Intn(2) == 0 {
			// Flip one byte inside the shared tail so the mismatch lands
			// inside a chunk or a word.
			k := len(b) - 1 - rng.Intn(n)
			b[k] ^= 1
		}
		require.Equal(t, commonSuffixLineBoundaryRef(a, b), commonSuffixLineBoundary(a, b), "a=%q b=%q", a, b)
	}
}

func TestLineFingerprintsEqualLinesEqualFingerprints(t *testing.T) {
	lines := []string{"", "a", "abcdefg", "abcdefgh", "abcdefghi", "the same long line here", "the same long line here", "the same long line herE"}
	fps := lineFingerprints(nil, lines)
	require.Len(t, fps, len(lines))
	require.Equal(t, fps[5], fps[6])
	require.NotEqual(t, fps[5], fps[7])
	require.NotEqual(t, fps[2], fps[3])
	require.NotEqual(t, fps[0], fps[1])
	again := lineFingerprints(fps, lines)
	require.Equal(t, fps, again, "reuses the destination")
}
