package objstore

import (
	"bytes"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// referenceAddedHunks tokenizes both inputs in full and runs the greedy
// forward walk over every line; it defines the output addedHunksWithPos must
// preserve through its prefix and suffix shortcuts.
func referenceAddedHunks(oldB, newB []byte) []AddedHunk {
	if bytes.Equal(oldB, newB) {
		return nil
	}
	oldLines, newLines := tokenize(oldB), tokenize(newB)
	var hunks []AddedHunk
	hunkStart := -1
	flush := func(end int) {
		if hunkStart < 0 {
			return
		}
		hunks = append(hunks, AddedHunk{
			StartLine: uint32(hunkStart) + 1,
			Lines:     append([]string(nil), newLines[hunkStart:end]...),
		})
		hunkStart = -1
	}
	oldIdx := 0
	for newIdx, line := range newLines {
		added := false
		if oldIdx >= len(oldLines) {
			added = true
		} else if line != oldLines[oldIdx] {
			found := false
			for j := oldIdx; j < len(oldLines); j++ {
				if oldLines[j] == line {
					found, oldIdx = true, j
					break
				}
			}
			added = !found
		}
		if added {
			if hunkStart < 0 {
				hunkStart = newIdx
			}
		} else {
			flush(newIdx)
			oldIdx++
		}
	}
	flush(len(newLines))
	return hunks
}

func TestAddedHunksWithPos_MatchesReferenceOnRandomEdits(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	words := []string{"a", "b", "c", "d", "", "x y", "foo", "bar\tbaz", "longer line here", "q", "a", "b"}
	gen := func(n int) []byte {
		var out []byte
		for i := 0; i < n; i++ {
			out = append(out, words[rng.Intn(len(words))]...)
			if i < n-1 || rng.Intn(3) > 0 {
				out = append(out, '\n')
			}
		}
		return out
	}
	mutate := func(src []byte) []byte {
		lines := bytes.Split(src, []byte{'\n'})
		if len(src) > 0 && src[len(src)-1] == '\n' {
			lines = lines[:len(lines)-1]
		}
		for k := rng.Intn(6) + 1; k > 0; k-- {
			if len(lines) == 0 {
				lines = append(lines, []byte(words[rng.Intn(len(words))]))
				continue
			}
			i := rng.Intn(len(lines))
			switch rng.Intn(5) {
			case 0:
				lines = slices.Insert(lines, i, []byte(words[rng.Intn(len(words))]))
			case 1:
				lines = slices.Delete(lines, i, i+1)
			case 2:
				lines[i] = []byte(words[rng.Intn(len(words))])
			case 3:
				j := rng.Intn(len(lines))
				lines[i], lines[j] = lines[j], lines[i]
			case 4:
				for m := 20 + rng.Intn(20); m > 0; m-- {
					lines = slices.Insert(lines, i, []byte(words[rng.Intn(len(words))]))
				}
			}
		}
		out := bytes.Join(lines, []byte{'\n'})
		if rng.Intn(3) > 0 {
			out = append(out, '\n')
		}
		return out
	}
	for iter := 0; iter < 50000; iter++ {
		old := gen(rng.Intn(150))
		var nw []byte
		switch rng.Intn(3) {
		case 0:
			nw = mutate(old)
		case 1:
			nw = gen(rng.Intn(150))
		case 2:
			nw = append(append([]byte{}, old...), gen(rng.Intn(5))...)
		}
		got := addedHunksWithPos(old, nw)
		want := referenceAddedHunks(old, nw)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("old=%q\nnew=%q\ngot  %+v\nwant %+v", old, nw, got, want)
		}
	}
}

func TestCommonSuffixLineBoundary(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a\nb\n", "a\nb\n", 4},
		{"x\nA\nB\n", "y\nA\nB\n", 4},
		{"xA\nB\n", "yA\nB\n", 2},
		{"a\nb", "c\nb", 1},
		{"a\nb", "c\nxb", 0},
		{"\nb\n", "q\nb\n", 2},
		{"b\n", "a\nb\n", 2},
		{"tail", "head\ntail", 4},
		{"xtail", "head\ntail", 0},
	}
	for _, c := range cases {
		got := commonSuffixLineBoundary([]byte(c.a), []byte(c.b))
		assert.Equalf(t, c.want, got, "a=%q b=%q", c.a, c.b)
		require.LessOrEqual(t, got, min(len(c.a), len(c.b)))
		if got > 0 {
			sa, sb := c.a[len(c.a)-got:], c.b[len(c.b)-got:]
			assert.Equal(t, sa, sb)
			pa, pb := len(c.a)-got, len(c.b)-got
			assert.True(t, pa == 0 || c.a[pa-1] == '\n')
			assert.True(t, pb == 0 || c.b[pb-1] == '\n')
		}
	}
	// Long inputs exercise the chunked comparison against the byte loop.
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		n := 5000 + rng.Intn(20000)
		shared := make([]byte, n)
		for i := range shared {
			if rng.Intn(40) == 0 {
				shared[i] = '\n'
			} else {
				shared[i] = byte('a' + rng.Intn(26))
			}
		}
		a := append([]byte("p\n"), shared...)
		b := append([]byte("qq\n"), shared...)
		if rng.Intn(2) == 0 {
			b[3+rng.Intn(len(shared)/2)] ^= 1
		}
		got := commonSuffixLineBoundary(a, b)
		i := 0
		for i < min(len(a), len(b)) && a[len(a)-1-i] == b[len(b)-1-i] {
			i++
		}
		for i > 0 {
			pa, pb := len(a)-i, len(b)-i
			if (pa == 0 || a[pa-1] == '\n') && (pb == 0 || b[pb-1] == '\n') {
				break
			}
			i--
		}
		assert.Equal(t, i, got)
	}
}

func TestTokenizeIntoReusesCapacity(t *testing.T) {
	buf := make([]string, 0, 8)
	lines := tokenizeInto(buf, []byte("a\nb\nc"))
	assert.Equal(t, []string{"a", "b", "c"}, lines)
	assert.Equal(t, 8, cap(lines), "fits in the given capacity")
	lines = tokenizeInto(buf, []byte("1\n2\n3\n4\n5\n6\n7\n8\n9\n"))
	assert.Len(t, lines, 9)
	assert.Equal(t, tokenize([]byte("1\n2\n3\n4\n5\n6\n7\n8\n9\n")), lines)
	assert.Nil(t, tokenize(nil))
	assert.Len(t, tokenizeInto(nil, nil), 0)
}

func TestLineScratchReleaseDropsOversizedSlices(t *testing.T) {
	s := &lineScratch{old: make([]string, lineScratchMaxCap+1), new: nil}
	s.release()
	got := lineScratchPool.Get().(*lineScratch)
	assert.LessOrEqual(t, cap(got.old), lineScratchMaxCap)
	got.old = append(got.old[:0], "keep")
	got.release()
	again := lineScratchPool.Get().(*lineScratch)
	for _, l := range again.old[:cap(again.old)] {
		assert.Empty(t, l, "released scratch holds no line references")
	}
}
