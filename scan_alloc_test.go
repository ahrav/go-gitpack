package objstore

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"
)

func TestAddedHunksWithPosScratch_MinimalOnRandomEdits(t *testing.T) {
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
	sc := &lineScratch{}
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
		var got []AddedHunk
		if iter%2 == 0 {
			got = addedHunksWithPosScratch(old, nw, sc)
		} else {
			got = addedHunksWithPosScratch(old, nw, nil)
		}
		requireMinimalAddedHunks(t, old, nw, got)
	}
}
