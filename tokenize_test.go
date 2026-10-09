package objstore

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
)

// tokenizeReference is the straightforward per-line split tokenize must match.
func tokenizeReference(src []byte) []string {
	if len(src) == 0 {
		return nil
	}
	var lines []string
	rest := src
	for {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			break
		}
		lines = append(lines, string(rest[:i]))
		rest = rest[i+1:]
	}
	if len(rest) > 0 {
		lines = append(lines, string(rest))
	}
	return lines
}

func requireSameLines(t *testing.T, src []byte, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%q: got %d lines, want %d", src, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%q: line %d: got %q want %q", src, i, got[i], want[i])
		}
	}
}

func TestTokenizeMatchesReference(t *testing.T) {
	cases := []string{
		"",
		"\n",
		"\n\n\n",
		"a",
		"a\n",
		"a\nb",
		"a\nb\n",
		"abcdefg\n",       // newline at byte 7, last byte of the first word
		"abcdefgh\n",      // newline at byte 8, first byte of the second word
		"abcdefghijklmno", // 15 bytes, no newline
		"\nabcdefghijklmno\n",
		strings.Repeat("x", 7) + "\n" + strings.Repeat("y", 9) + "\n\n" + strings.Repeat("z", 16),
		"line with \x00 nul\nand \xff high bytes\n",
		"\x0b\x09\x0a\x0b", // bytes adjacent to '\n' in value
		strings.Repeat("\n", 64),
		strings.Repeat("0123456\n", 100),
	}
	for _, c := range cases {
		src := []byte(c)
		requireSameLines(t, src, tokenize(src), tokenizeReference(src))
	}
}

func TestTokenizeRandomMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	alphabet := []byte("ab\n\n\n\x00\xff\x0b\x09 ")
	for iter := 0; iter < 5000; iter++ {
		src := make([]byte, rng.Intn(200))
		for i := range src {
			src[i] = alphabet[rng.Intn(len(alphabet))]
		}
		requireSameLines(t, src, tokenize(src), tokenizeReference(src))
	}
}

func TestTokenizeAliasesSource(t *testing.T) {
	src := []byte("alpha\nbeta\ngamma")
	lines := tokenize(src)
	if len(lines) != 3 {
		t.Fatalf("got %d lines", len(lines))
	}
	// Mutating src shows through the returned strings: they are views.
	src[0] = 'A'
	if lines[0] != "Alpha" {
		t.Fatalf("expected aliasing view, got %q", lines[0])
	}
}

func FuzzTokenize(f *testing.F) {
	f.Add([]byte("a\nb\n"))
	f.Add([]byte("abcdefg\nhijklmnop"))
	f.Add([]byte("\n\n\n\n\n\n\n\n\n"))
	f.Fuzz(func(t *testing.T, src []byte) {
		requireSameLines(t, src, tokenize(src), tokenizeReference(src))
	})
}

func BenchmarkTokenize(b *testing.B) {
	var sb strings.Builder
	rng := rand.New(rand.NewSource(7))
	for sb.Len() < 1<<20 {
		n := 10 + rng.Intn(60)
		for i := 0; i < n; i++ {
			sb.WriteByte(byte('a' + rng.Intn(26)))
		}
		sb.WriteByte('\n')
	}
	src := []byte(sb.String())
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	for b.Loop() {
		_ = tokenize(src)
	}
}
